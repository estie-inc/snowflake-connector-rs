mod handler;
mod runtime;

use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
};

use crate::external_browser_payload::BrowserCallbackPayload;

use runtime::{ListenerRuntime, bind_listener, build_origin};

type ListenerError = Box<dyn Error + Send + Sync>;

pub(crate) struct ListenerConfig {
    /// Application name used in the human-facing HTML response.
    ///
    /// This corresponds to Python connector's `AuthByWebBrowser._application`.
    application: Option<String>,
    host: IpAddr,
    port: u16,
    protocol: String,
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

impl ListenerConfig {
    pub(crate) fn new(
        application: Option<String>,
        host: IpAddr,
        port: u16,
        protocol: String,
    ) -> Self {
        Self {
            application,
            host,
            port,
            protocol,
        }
    }
}

pub(crate) struct RunningListener {
    addr: SocketAddr,
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<Result<(), ListenerError>>,
    payloads: watch::Receiver<Option<BrowserCallbackPayload>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallbackWaitError {
    TimedOut,
    ListenerStopped,
}

impl RunningListener {
    pub(crate) fn redirect_port(&self) -> u16 {
        self.addr.port()
    }

    pub(crate) async fn wait_for_payload(
        &mut self,
        timeout: Duration,
    ) -> std::result::Result<BrowserCallbackPayload, CallbackWaitError> {
        match tokio::time::timeout(timeout, Self::wait_for_payload_inner(&mut self.payloads)).await
        {
            Ok(res) => res,
            Err(_) => Err(CallbackWaitError::TimedOut),
        }
    }

    async fn wait_for_payload_inner(
        rx: &mut watch::Receiver<Option<BrowserCallbackPayload>>,
    ) -> std::result::Result<BrowserCallbackPayload, CallbackWaitError> {
        if let Some(payload) = rx.borrow().clone() {
            return Ok(payload);
        }

        loop {
            if rx.changed().await.is_err() {
                return Err(CallbackWaitError::ListenerStopped);
            }
            if let Some(payload) = rx.borrow().clone() {
                return Ok(payload);
            }
        }
    }

    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(());
        let _ = self.handle.await;
    }

    #[cfg(test)]
    fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    fn into_parts(
        self,
    ) -> (
        SocketAddr,
        oneshot::Sender<()>,
        JoinHandle<Result<(), ListenerError>>,
        watch::Receiver<Option<BrowserCallbackPayload>>,
    ) {
        (self.addr, self.shutdown, self.handle, self.payloads)
    }
}

pub(crate) async fn spawn_listener(cfg: ListenerConfig) -> Result<RunningListener, ListenerError> {
    let (listener, local_addr) = bind_listener(&cfg)?;
    let expected_origin = build_origin(&cfg.protocol, cfg.host, local_addr.port());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (payload_tx, payload_rx) = watch::channel(None);
    let handle = tokio::spawn(
        ListenerRuntime::new(
            listener,
            expected_origin,
            cfg.application,
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

#[cfg(test)]
mod tests {
    use std::{io, net::TcpListener as StdTcpListener};

    use reqwest::StatusCode as ReqwestStatusCode;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time::{Duration, sleep},
    };

    use super::runtime::bind_listener;
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
        let (addr, shutdown, handle, _) = running.into_parts();
        let base = format!("http://{addr}");

        // give the server a moment to start accepting
        sleep(Duration::from_millis(50)).await;

        test(base).await;

        let _ = shutdown.send(());
        let _ = handle.await;
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
                "application/json"
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
        let (addr, shutdown, handle, payloads) = running.into_parts();
        let base = format!("http://{addr}");
        let mut rx = payloads;

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

        let _ = shutdown.send(());
        let _ = handle.await;
    }

    #[tokio::test]
    async fn form_encoded_payload_is_extracted() {
        let running = spawn_listener(ListenerConfig::default()).await.unwrap();
        let (addr, shutdown, handle, payloads) = running.into_parts();
        let base = format!("http://{addr}");
        let mut rx = payloads;

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

        let _ = shutdown.send(());
        let _ = handle.await;
    }

    #[tokio::test]
    async fn query_payload_is_extracted() {
        let running = spawn_listener(ListenerConfig::default()).await.unwrap();
        let (addr, shutdown, handle, payloads) = running.into_parts();
        let base = format!("http://{addr}");
        let mut rx = payloads;

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

        let _ = shutdown.send(());
        let _ = handle.await;
    }

    #[tokio::test]
    async fn dropping_running_listener_releases_bound_port() {
        let running = spawn_listener(ListenerConfig::default()).await.unwrap();
        let port = running.redirect_port();
        let host = running.addr().ip();

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

        let (addr1, shutdown1, handle1, payloads1) = running1.into_parts();
        let mut stale_rx = payloads1;
        let mut stream = TcpStream::connect(addr1).await.unwrap();
        let first_request = format!(
            "GET /callback?token=first HTTP/1.1\r\nHost: {addr1}\r\nConnection: keep-alive\r\n\r\n",
        );
        stream.write_all(first_request.as_bytes()).await.unwrap();
        let first_response = read_http_response(&mut stream).await.unwrap();
        assert!(first_response.contains("200 OK"));

        let _ = stale_rx.changed().await;
        assert_eq!(stale_rx.borrow().as_ref().unwrap().token, "first");

        let _ = shutdown1.send(());
        let _ = handle1.await;

        let mut closed_probe = [0_u8; 1];
        let close_result =
            tokio::time::timeout(Duration::from_millis(200), stream.read(&mut closed_probe))
                .await
                .expect("stale connection did not close promptly")
                .unwrap();
        assert_eq!(close_result, 0, "stale callback connection remained open");

        let running2 = spawn_listener(cfg).await.unwrap();
        let (addr2, shutdown2, handle2, payloads2) = running2.into_parts();
        let mut next_rx = payloads2;

        let mut second_stream = TcpStream::connect(addr2).await.unwrap();
        let second_request = format!(
            "GET /callback?token=second HTTP/1.1\r\nHost: {addr2}\r\nConnection: keep-alive\r\n\r\n",
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

        let _ = shutdown2.send(());
        let _ = handle2.await;
    }
}
