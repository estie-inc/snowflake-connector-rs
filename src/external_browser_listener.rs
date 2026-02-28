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
    task::JoinHandle,
};

use crate::external_browser_payload::{ParsedTokenAndConsent, parse_token_and_consent_from_pairs};

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
    pub handle: JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>,
    pub payloads: watch::Receiver<Option<CallbackPayload>>,
}

pub async fn spawn_listener(
    cfg: ListenerConfig,
) -> Result<RunningListener, Box<dyn Error + Send + Sync>> {
    let (listener, local_addr) = bind_listener(&cfg)?;
    let expected_origin = build_origin(&cfg.protocol, cfg.host, local_addr.port());
    let application = cfg.application.clone();
    let (tx, rx) = oneshot::channel();
    let (payload_tx, payload_rx) = watch::channel(None);
    let handle = tokio::spawn(async move {
        run_loop(listener, expected_origin, application, rx, payload_tx).await
    });

    Ok(RunningListener {
        addr: local_addr,
        shutdown: tx,
        handle,
        payloads: payload_rx,
    })
}

async fn run_loop(
    listener: TcpListener,
    expected_origin: String,
    application: Option<String>,
    mut shutdown: oneshot::Receiver<()>,
    payload_tx: watch::Sender<Option<CallbackPayload>>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // We keep the last validated Origin in state when it receives a successful OPTIONS preflight,
    // so that GET/POST can return the JSON response (without leaking the token).
    let validated_origin = Arc::new(Mutex::<Option<String>>::new(None));

    loop {
        tokio::select! {
            res = listener.accept() => {
                let (stream, _peer) = match res {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("accept error: {e}");
                        continue;
                    }
                };
                let expected = expected_origin.clone();
                let application = application.clone();
                let payload_tx = payload_tx.clone();
                let validated_origin = validated_origin.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| {
                        handler(
                            req,
                            expected.clone(),
                            application.clone(),
                            payload_tx.clone(),
                            validated_origin.clone(),
                        )
                    });
                    if let Err(err) = http1::Builder::new().serve_connection(io, svc).await {
                        eprintln!("serve error: {err}");
                    }
                });
            }
            _ = &mut shutdown => {
                break;
            }
        }
    }
    Ok(())
}

async fn handler(
    req: Request<Incoming>,
    expected_origin: String,
    application: Option<String>,
    payload_tx: watch::Sender<Option<CallbackPayload>>,
    validated_origin: Arc<Mutex<Option<String>>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let request_origin = header_to_string(req.headers().get(ORIGIN));

    match *req.method() {
        Method::OPTIONS => {
            let allowed_origin =
                request_origin.filter(|origin| origin_allowed_value(origin, &expected_origin));
            if allowed_origin.is_none() {
                return Ok(forbidden());
            }

            // Store validated Origin so that subsequent GET/POST can return JSON
            {
                let mut guard = validated_origin.lock().await;
                *guard = allowed_origin.clone();
            }

            let allow_headers =
                requested_headers(&req).unwrap_or_else(|| "Content-Type, Origin".to_string());
            Ok(preflight_cors_response(
                Response::builder().status(StatusCode::OK),
                allowed_origin.as_deref().unwrap_or(&expected_origin),
                &allow_headers,
            )
            .header(VARY, "Accept-Encoding, Origin")
            .header(CONTENT_LENGTH, "0")
            .body(Full::new(Bytes::new()))
            .expect("building preflight response failed"))
        }
        Method::GET => {
            let payload = extract_payload_from_query(req.uri().query());
            if let Some(p) = payload.clone() {
                let _ = payload_tx.send(Some(p));
            }
            let stored_origin = validated_origin.lock().await.clone();
            Ok(ok_callback_response(
                payload.as_ref(),
                stored_origin.as_deref(),
                application.as_deref(),
            ))
        }
        Method::POST => {
            let whole_body = req
                .into_body()
                .collect()
                .await
                .map(|c| c.to_bytes())
                .unwrap_or_default();
            let payload = extract_payload_from_body(&whole_body);
            if let Some(p) = payload.clone() {
                let _ = payload_tx.send(Some(p));
            }
            let stored_origin = validated_origin.lock().await.clone();
            Ok(ok_callback_response(
                payload.as_ref(),
                stored_origin.as_deref(),
                application.as_deref(),
            ))
        }
        _ => Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::from_static(b"method not allowed")))
            .expect("building method not allowed response failed")),
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
    use reqwest::StatusCode as ReqwestStatusCode;
    use tokio::time::{Duration, sleep};

    use super::*;

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
}
