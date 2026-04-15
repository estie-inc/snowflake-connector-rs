use std::convert::Infallible;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{
    ACCESS_CONTROL_REQUEST_HEADERS, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, ORIGIN, VARY,
};
use hyper::http::StatusCode;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    net::TcpListener,
    sync::Mutex,
    sync::{oneshot, watch},
    task::{JoinError, JoinHandle, JoinSet},
};

use crate::external_browser_payload::{ParsedTokenAndConsent, parse_token_and_consent_from_pairs};

type ListenerError = Box<dyn Error + Send + Sync>;
type ConnectionTaskResult = Result<(), ListenerError>;

#[derive(Debug, Deserialize)]
struct TokenPayload {
    token: Option<String>,
    consent: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackPayload {
    pub token: String,
    pub consent: Option<bool>,
}

pub struct ListenerConfig {
    /// Application name used in the human-facing HTML response.
    ///
    /// This corresponds to Python connector's `AuthByWebBrowser._application`.
    pub application: Option<String>,
    pub host: IpAddr,
    pub port: u16,
    pub protocol: String,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            application: None,
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            protocol: "http".to_string(),
        }
    }
}

pub struct RunningListener {
    pub addr: SocketAddr,
    pub shutdown: oneshot::Sender<()>,
    pub handle: JoinHandle<Result<(), ListenerError>>,
    pub payloads: watch::Receiver<Option<CallbackPayload>>,
}

struct ListenerShared {
    expected_origin: String,
    application: Option<String>,
    payload_tx: watch::Sender<Option<CallbackPayload>>,
    validated_origin: Mutex<Option<String>>,
}

impl ListenerShared {
    fn new(
        expected_origin: String,
        application: Option<String>,
        payload_tx: watch::Sender<Option<CallbackPayload>>,
    ) -> Self {
        Self {
            expected_origin,
            application,
            payload_tx,
            validated_origin: Mutex::new(None),
        }
    }

    async fn handle(&self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
        let request_origin = header_to_string(req.headers().get(ORIGIN));

        match *req.method() {
            Method::OPTIONS => self.handle_preflight(request_origin, &req).await,
            Method::GET => {
                let payload = extract_payload_from_query(req.uri().query());
                self.handle_callback(payload).await
            }
            Method::POST => {
                let whole_body = req
                    .into_body()
                    .collect()
                    .await
                    .map(|c| c.to_bytes())
                    .unwrap_or_default();
                let payload = extract_payload_from_body(&whole_body);
                self.handle_callback(payload).await
            }
            _ => Ok(Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(Full::new(Bytes::from_static(b"method not allowed")))
                .expect("building method not allowed response failed")),
        }
    }

    async fn handle_preflight(
        &self,
        request_origin: Option<String>,
        req: &Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let expected_origin = self.expected_origin.clone();
        let allowed_origin =
            request_origin.filter(|origin| origin_allowed_value(origin, &expected_origin));
        if allowed_origin.is_none() {
            return Ok(forbidden());
        }

        self.set_validated_origin(allowed_origin.clone()).await;

        let allow_headers =
            requested_headers(req).unwrap_or_else(|| "Content-Type, Origin".to_string());
        Ok(preflight_cors_response(
            Response::builder().status(StatusCode::OK),
            allowed_origin
                .as_deref()
                .unwrap_or(expected_origin.as_str()),
            &allow_headers,
        )
        .header(VARY, "Accept-Encoding, Origin")
        .header(CONTENT_LENGTH, "0")
        .body(Full::new(Bytes::new()))
        .expect("building preflight response failed"))
    }

    async fn handle_callback(
        &self,
        payload: Option<CallbackPayload>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        self.publish_payload(&payload);
        let stored_origin = self.validated_origin().await;
        Ok(ok_callback_response(
            payload.as_ref(),
            stored_origin.as_deref(),
            self.application.as_deref(),
        ))
    }

    fn publish_payload(&self, payload: &Option<CallbackPayload>) {
        if let Some(payload) = payload {
            let _ = self.payload_tx.send(Some(payload.clone()));
        }
    }

    async fn set_validated_origin(&self, origin: Option<String>) {
        let mut guard = self.validated_origin.lock().await;
        *guard = origin;
    }

    async fn validated_origin(&self) -> Option<String> {
        self.validated_origin.lock().await.clone()
    }
}

struct ConnectionTasks {
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    tasks: JoinSet<ConnectionTaskResult>,
}

impl ConnectionTasks {
    fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            shutdown_tx,
            shutdown_rx,
            tasks: JoinSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn spawn(&mut self, stream: tokio::net::TcpStream, shared: Arc<ListenerShared>) {
        let shutdown = self.shutdown_rx.clone();
        self.tasks
            .spawn(async move { run_connection(stream, shared, shutdown).await });
    }

    async fn join_next(&mut self) -> Option<Result<ConnectionTaskResult, JoinError>> {
        self.tasks.join_next().await
    }

    async fn shutdown_and_drain(&mut self) {
        let _ = self.shutdown_tx.send(true);
        while !self.is_empty() {
            log_connection_task_result(self.join_next().await);
        }
    }
}

struct ListenerRuntime {
    listener: TcpListener,
    shared: Arc<ListenerShared>,
    shutdown: oneshot::Receiver<()>,
    connections: ConnectionTasks,
}

impl ListenerRuntime {
    fn new(
        listener: TcpListener,
        expected_origin: String,
        application: Option<String>,
        shutdown: oneshot::Receiver<()>,
        payload_tx: watch::Sender<Option<CallbackPayload>>,
    ) -> Self {
        Self {
            listener,
            shared: Arc::new(ListenerShared::new(
                expected_origin,
                application,
                payload_tx,
            )),
            shutdown,
            connections: ConnectionTasks::new(),
        }
    }

    async fn run(mut self) -> Result<(), ListenerError> {
        loop {
            tokio::select! {
                join_result = self.connections.join_next(), if !self.connections.is_empty() => {
                    log_connection_task_result(join_result);
                }
                res = self.listener.accept() => {
                    let (stream, _peer) = match res {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("accept error: {e}");
                            continue;
                        }
                    };
                    self.connections.spawn(stream, self.shared.clone());
                }
                _ = &mut self.shutdown => {
                    break;
                }
            }
        }

        drop(self.listener);
        self.connections.shutdown_and_drain().await;
        Ok(())
    }
}

pub async fn spawn_listener(cfg: ListenerConfig) -> Result<RunningListener, ListenerError> {
    let (listener, local_addr) = bind_listener(&cfg)?;
    let expected_origin = build_origin(&cfg.protocol, cfg.host, local_addr.port());
    let application = cfg.application.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (payload_tx, payload_rx) = watch::channel(None);
    let handle = tokio::spawn(
        ListenerRuntime::new(
            listener,
            expected_origin,
            application,
            shutdown_rx,
            payload_tx,
        )
        .run(),
    );

    Ok(RunningListener {
        addr: local_addr,
        shutdown: shutdown_tx,
        handle,
        payloads: payload_rx,
    })
}

async fn run_connection(
    stream: tokio::net::TcpStream,
    shared: Arc<ListenerShared>,
    mut shutdown: watch::Receiver<bool>,
) -> ConnectionTaskResult {
    let io = TokioIo::new(stream);
    let svc = service_fn(move |req| {
        let shared = shared.clone();
        async move { shared.handle(req).await }
    });
    let conn = http1::Builder::new().serve_connection(io, svc);
    tokio::pin!(conn);

    tokio::select! {
        res = conn.as_mut() => {
            res.map_err(|err| Box::new(err) as ListenerError)
        }
        _ = shutdown.changed() => {
            conn.as_mut().graceful_shutdown();
            conn.await.map_err(|err| Box::new(err) as ListenerError)
        }
    }
}

fn log_connection_task_result(join_result: Option<Result<ConnectionTaskResult, JoinError>>) {
    match join_result {
        Some(Ok(Ok(()))) => {}
        Some(Ok(Err(err))) => eprintln!("serve error: {err}"),
        Some(Err(err)) => eprintln!("connection task join error: {err}"),
        None => {}
    }
}

fn extract_payload_from_body(body: &Bytes) -> Option<CallbackPayload> {
    // 1) JSON: {"token": "...", "consent": true}
    serde_json::from_slice::<TokenPayload>(body)
        .ok()
        .and_then(|parsed| {
            parsed.token.map(|token| CallbackPayload {
                token,
                consent: parsed.consent,
            })
        })
        // 2) x-www-form-urlencoded / key=value fallback
        .or_else(|| {
            std::str::from_utf8(body)
                .ok()
                .map(|body_str| {
                    parse_token_and_consent_from_pairs(url::form_urlencoded::parse(
                        body_str.as_bytes(),
                    ))
                })
                .and_then(parsed_to_payload)
        })
}

fn preflight_cors_response(
    mut builder: hyper::http::response::Builder,
    allowed_origin: &str,
    allow_headers: &str,
) -> hyper::http::response::Builder {
    builder = builder
        .header("Access-Control-Allow-Origin", allowed_origin)
        .header("Access-Control-Allow-Methods", "POST, GET")
        .header("Access-Control-Allow-Headers", allow_headers)
        .header("Access-Control-Max-Age", "86400");
    builder
}

fn ok_callback_response(
    payload: Option<&CallbackPayload>,
    stored_origin: Option<&str>,
    application: Option<&str>,
) -> Response<Full<Bytes>> {
    if payload.is_none() {
        return Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/plain")
            .header(CONTENT_LENGTH, "17")
            .body(Full::new(Bytes::from_static(b"no token provided")))
            .expect("building no token provided response failed");
    }

    // - If a validated Origin exists (OPTIONS preflight succeeded), return JSON with consent only.
    // - Otherwise, return a simple HTML page telling the user they can close the window.
    let cors_mode = stored_origin.is_some();
    let (content_type, body) = if cors_mode {
        let consent = payload.and_then(|p| p.consent).unwrap_or(true);
        let msg = serde_json::json!({"consent": consent}).to_string();
        ("text/html", msg)
    } else {
        let app_line = application
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|app| format!("Client application: {app}.\n"))
            .unwrap_or_default();
        let msg = format!(
            r#"<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>
<link rel=\"icon\" href=\"data:,\">
<title>SAML Response for Snowflake</title></head>
<body>
Your identity was confirmed and propagated to Snowflake.
{app_line}You can close this window now and go back where you started from.
</body></html>"#
        );
        ("text/html", msg)
    };

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header(CONTENT_LENGTH, body.len().to_string());

    if let Some(origin) = stored_origin {
        builder = builder
            .header("Access-Control-Allow-Origin", origin)
            .header(VARY, "Accept-Encoding, Origin");
    }

    builder
        .body(Full::new(Bytes::from(body)))
        .expect("building OK callback response failed")
}

fn forbidden() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .body(Full::new(Bytes::from_static(b"origin not allowed")))
        .expect("building forbidden response failed")
}

fn extract_payload_from_query(query: Option<&str>) -> Option<CallbackPayload> {
    query
        .map(|q| parse_token_and_consent_from_pairs(url::form_urlencoded::parse(q.as_bytes())))
        .and_then(parsed_to_payload)
}

fn parsed_to_payload(parsed: ParsedTokenAndConsent) -> Option<CallbackPayload> {
    parsed.token.map(|token| CallbackPayload {
        token,
        consent: parsed.consent,
    })
}

fn build_origin(protocol: &str, host: IpAddr, port: u16) -> String {
    format!("{protocol}://{}", SocketAddr::new(host, port))
}

fn bind_listener(
    cfg: &ListenerConfig,
) -> Result<(TcpListener, SocketAddr), Box<dyn Error + Send + Sync>> {
    let target_addr = SocketAddr::new(cfg.host, cfg.port);

    let domain = match target_addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_nonblocking(true)?;
    socket.set_reuse_address(true)?;
    socket.bind(&target_addr.into())?;
    socket.listen(128)?;

    let listener = TcpListener::from_std(socket.into())?;
    let local_addr = listener.local_addr()?;
    Ok((listener, local_addr))
}

fn header_to_string(value: Option<&HeaderValue>) -> Option<String> {
    value.and_then(|v| v.to_str().ok()).map(|s| s.to_string())
}

fn origin_allowed_value(origin: &str, expected: &str) -> bool {
    origin.eq_ignore_ascii_case(expected)
}

fn requested_headers(req: &Request<Incoming>) -> Option<String> {
    req.headers()
        .get(ACCESS_CONTROL_REQUEST_HEADERS)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::net::TcpListener as StdTcpListener;

    use reqwest::StatusCode as ReqwestStatusCode;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::{Duration, sleep};

    use super::*;

    async fn read_http_response(stream: &mut TcpStream) -> io::Result<String> {
        let mut buf = Vec::new();
        let mut header_end = None;

        while header_end.is_none() {
            let mut chunk = [0_u8; 1024];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream closed before headers completed",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
            header_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
        }

        let header_end = header_end.expect("header end must exist") + 4;
        let headers = String::from_utf8_lossy(&buf[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);

        while buf.len() < header_end + content_length {
            let mut chunk = [0_u8; 1024];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream closed before body completed",
                ));
            }
            buf.extend_from_slice(&chunk[..n]);
        }

        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    fn pick_unused_port() -> u16 {
        let listener =
            StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("failed to reserve test port");
        listener
            .local_addr()
            .expect("failed to get local addr")
            .port()
    }

    async fn with_listener<F, Fut>(test: F)
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let running = spawn_listener(ListenerConfig {
            application: Some("testapp".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();
        let base = format!("http://{}", running.addr);

        // give the server a moment to start accepting
        sleep(Duration::from_millis(50)).await;

        test(base).await;

        let _ = running.shutdown.send(());
        let _ = running.handle.await;
    }

    #[test]
    fn extract_payload_from_body_json() {
        let body = Bytes::from_static(br#"{"token":"t","consent":false}"#);
        let payload = extract_payload_from_body(&body).unwrap();
        assert_eq!(payload.token, "t");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn extract_payload_from_body_form_urlencoded() {
        let body = Bytes::from_static(b"token=t&consent=true");
        let payload = extract_payload_from_body(&body).unwrap();
        assert_eq!(payload.token, "t");
        assert_eq!(payload.consent, Some(true));
    }

    #[test]
    fn extract_payload_from_query_parses_consent() {
        let payload = extract_payload_from_query(Some("token=t&consent=false")).unwrap();
        assert_eq!(payload.token, "t");
        assert_eq!(payload.consent, Some(false));
    }

    #[test]
    fn extract_payload_from_query_prefers_first_token() {
        let payload = extract_payload_from_query(Some("token=first&token=second")).unwrap();
        assert_eq!(payload.token, "first");
    }

    #[tokio::test]
    async fn ok_callback_response_cors_mode_returns_consent_only() {
        let payload = CallbackPayload {
            token: "secret".to_string(),
            consent: Some(false),
        };
        let resp = ok_callback_response(Some(&payload), Some("http://localhost:1"), None);
        assert_eq!(resp.status(), StatusCode::OK);
        let headers = resp.headers().clone();

        assert_eq!(
            headers.get(CONTENT_TYPE).unwrap().to_str().unwrap(),
            "text/html"
        );
        assert_eq!(
            headers
                .get("access-control-allow-origin")
                .unwrap()
                .to_str()
                .unwrap(),
            "http://localhost:1"
        );
        assert_eq!(
            headers.get(VARY).unwrap().to_str().unwrap(),
            "Accept-Encoding, Origin"
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("\"consent\":false"));
        assert!(!body_str.contains("secret"));

        let expected_len = body.len().to_string();
        assert_eq!(
            headers.get(CONTENT_LENGTH).unwrap().to_str().unwrap(),
            expected_len
        );
    }

    #[tokio::test]
    async fn ok_callback_response_html_includes_app_line() {
        let payload = CallbackPayload {
            token: "t".to_string(),
            consent: None,
        };
        let resp = ok_callback_response(Some(&payload), None, Some("myapp"));
        assert_eq!(resp.status(), StatusCode::OK);
        let headers = resp.headers().clone();
        assert!(headers.get("access-control-allow-origin").is_none());
        assert!(headers.get(VARY).is_none());
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let body_str = std::str::from_utf8(&body).unwrap();
        assert!(body_str.contains("You can close this window now"));
        assert!(body_str.contains("Client application: myapp"));
        assert!(!body_str.contains("token"));
    }

    #[tokio::test]
    async fn post_origin_ok() {
        with_listener(|base| async move {
            // Simulate browser preflight so the listener returns JSON.
            let origin = base.clone();

            let preflight = reqwest::Client::new()
                .request(reqwest::Method::OPTIONS, format!("{base}/callback"))
                .header("Origin", &origin)
                .header("Access-Control-Request-Method", "POST")
                .header("Access-Control-Request-Headers", "Content-Type")
                .send()
                .await
                .unwrap();
            assert_eq!(preflight.status(), ReqwestStatusCode::OK);

            let client = reqwest::Client::new();
            let resp = client
                .post(format!("{base}/callback"))
                .header("Origin", &origin)
                .json(&serde_json::json!({"token": "example"}))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), ReqwestStatusCode::OK);
            assert_eq!(
                resp.headers()
                    .get("access-control-allow-origin")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                origin
            );
            assert_eq!(
                resp.headers().get("vary").unwrap().to_str().unwrap(),
                "Accept-Encoding, Origin"
            );
            assert_eq!(
                resp.headers()
                    .get("content-type")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "text/html"
            );
            let body = resp.text().await.unwrap();
            assert!(body.contains("\"consent\""));
            assert!(!body.contains("example"));
        })
        .await;
    }

    #[tokio::test]
    async fn get_origin_ok() {
        with_listener(|base| async move {
            let url = format!("{base}/callback?token=example");
            let resp = reqwest::Client::new().get(url).send().await.unwrap();
            assert_eq!(resp.status(), ReqwestStatusCode::OK);
            assert_eq!(
                resp.headers()
                    .get("content-type")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "text/html"
            );
            assert!(resp.headers().get("access-control-allow-origin").is_none());
            assert!(resp.headers().get("vary").is_none());
            let body = resp.text().await.unwrap();
            assert!(body.contains("You can close this window now"));
            assert!(body.contains("Client application: testapp"));
            assert!(!body.contains("example"));
        })
        .await;
    }

    #[tokio::test]
    async fn origin_missing_is_allowed() {
        with_listener(|base| async move {
            let resp = reqwest::Client::new()
                .post(format!("{base}/callback"))
                .json(&serde_json::json!({"token": "example"}))
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), ReqwestStatusCode::OK);
            let body = resp.text().await.unwrap();
            assert!(body.contains("You can close this window now"));
            assert!(body.contains("Client application: testapp"));
            assert!(!body.contains("example"));
        })
        .await;
    }

    #[tokio::test]
    async fn origin_mismatch_is_allowed_for_get_post() {
        with_listener(|base| async move {
            let resp = reqwest::Client::new()
                .get(format!("{base}/callback?token=example"))
                .header("Origin", "http://127.0.0.1:1")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), ReqwestStatusCode::OK);
        })
        .await;
    }

    #[tokio::test]
    async fn localhost_origin_is_allowed_for_get_post() {
        with_listener(|base| async move {
            let port = base.split(':').next_back().unwrap();
            let origin = format!("http://localhost:{port}");
            let resp = reqwest::Client::new()
                .get(format!("{base}/callback?token=example"))
                .header("Origin", origin)
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), ReqwestStatusCode::OK);
            let body = resp.text().await.unwrap();
            assert!(body.contains("You can close this window now"));
            assert!(body.contains("Client application: testapp"));
            assert!(!body.contains("example"));
        })
        .await;
    }

    #[tokio::test]
    async fn options_echoes_requested_headers() {
        with_listener(|base| async move {
            let origin = base.clone();
            let resp = reqwest::Client::new()
                .request(reqwest::Method::OPTIONS, format!("{base}/callback"))
                .header("Origin", origin)
                .header("Access-Control-Request-Method", "POST")
                .header("Access-Control-Request-Headers", "Content-Type, X-Custom")
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), ReqwestStatusCode::OK);
            let headers = resp.headers();
            assert_eq!(
                headers.get("vary").unwrap().to_str().unwrap(),
                "Accept-Encoding, Origin"
            );
            assert_eq!(
                headers
                    .get("access-control-allow-headers")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "Content-Type, X-Custom"
            );
            assert_eq!(
                headers
                    .get("access-control-allow-methods")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "POST, GET"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn post_json_propagates_consent_in_payload() {
        let running = spawn_listener(ListenerConfig::default()).await.unwrap();
        let base = format!("http://{}", running.addr);
        let mut rx = running.payloads.clone();

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base}/callback"))
            .json(&serde_json::json!({"token": "example", "consent": false}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), ReqwestStatusCode::OK);

        let _ = rx.changed().await;
        let payload = rx.borrow().clone().unwrap();
        assert_eq!(payload.token, "example");
        assert_eq!(payload.consent, Some(false));

        let _ = running.shutdown.send(());
        let _ = running.handle.await;
    }

    #[tokio::test]
    async fn form_encoded_payload_is_extracted() {
        let running = spawn_listener(ListenerConfig::default()).await.unwrap();
        let base = format!("http://{}", running.addr);
        let mut rx = running.payloads.clone();

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base}/callback"))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body("token=example&consent=true")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), ReqwestStatusCode::OK);

        let _ = rx.changed().await;
        let payload = rx.borrow().clone().unwrap();
        assert_eq!(payload.token, "example");
        assert_eq!(payload.consent, Some(true));

        let _ = running.shutdown.send(());
        let _ = running.handle.await;
    }

    #[tokio::test]
    async fn query_payload_is_extracted() {
        let running = spawn_listener(ListenerConfig::default()).await.unwrap();
        let base = format!("http://{}", running.addr);
        let mut rx = running.payloads.clone();

        let client = reqwest::Client::new();
        let resp = client
            .get(format!("{base}/callback?token=example&consent=false"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), ReqwestStatusCode::OK);

        let _ = rx.changed().await;
        let payload = rx.borrow().clone().unwrap();
        assert_eq!(payload.token, "example");
        assert_eq!(payload.consent, Some(false));

        let _ = running.shutdown.send(());
        let _ = running.handle.await;
    }

    #[tokio::test]
    async fn dropping_running_listener_releases_bound_port() {
        let running = spawn_listener(ListenerConfig::default()).await.unwrap();
        let port = running.addr.port();
        let host = running.addr.ip();

        drop(running);

        let mut rebound = false;
        for _ in 0..20 {
            if bind_listener(&ListenerConfig {
                host,
                port,
                ..Default::default()
            })
            .is_ok()
            {
                rebound = true;
                break;
            }

            sleep(Duration::from_millis(25)).await;
        }

        assert!(
            rebound,
            "listener port {port} was still bound after RunningListener drop"
        );
    }

    #[tokio::test]
    async fn shutdown_drains_stale_connection_before_next_listener_reuses_port() {
        let port = pick_unused_port();
        let cfg = ListenerConfig {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port,
            ..Default::default()
        };

        let running1 = spawn_listener(ListenerConfig {
            host: cfg.host,
            port: cfg.port,
            ..Default::default()
        })
        .await
        .unwrap();

        let mut stale_rx = running1.payloads.clone();
        let mut stream = TcpStream::connect(running1.addr).await.unwrap();
        let first_request = format!(
            "GET /callback?token=first HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n",
            running1.addr
        );
        stream.write_all(first_request.as_bytes()).await.unwrap();
        let first_response = read_http_response(&mut stream).await.unwrap();
        assert!(first_response.contains("200 OK"));

        let _ = stale_rx.changed().await;
        assert_eq!(stale_rx.borrow().as_ref().unwrap().token, "first");

        let _ = running1.shutdown.send(());
        let _ = running1.handle.await;

        let mut closed_probe = [0_u8; 1];
        let close_result =
            tokio::time::timeout(Duration::from_millis(200), stream.read(&mut closed_probe))
                .await
                .expect("stale connection did not close promptly")
                .unwrap();
        assert_eq!(close_result, 0, "stale callback connection remained open");

        let running2 = spawn_listener(cfg).await.unwrap();
        let mut next_rx = running2.payloads.clone();

        let mut second_stream = TcpStream::connect(running2.addr).await.unwrap();
        let second_request = format!(
            "GET /callback?token=second HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\n\r\n",
            running2.addr
        );
        second_stream
            .write_all(second_request.as_bytes())
            .await
            .unwrap();
        let second_response = read_http_response(&mut second_stream).await.unwrap();
        assert!(second_response.contains("200 OK"));

        let second_seen_by_new_listener =
            tokio::time::timeout(Duration::from_millis(200), next_rx.changed()).await;
        assert!(
            second_seen_by_new_listener.is_ok(),
            "new listener did not observe callback on fresh connection"
        );
        assert_eq!(next_rx.borrow().as_ref().unwrap().token, "second");

        let _ = running2.shutdown.send(());
        let _ = running2.handle.await;
    }
}
