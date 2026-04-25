//! In-process HTTP/1 server used by unit tests to exercise the low-level
//! HTTP surface (`StatementApiClient`, `LeaseRefresher`, `ChunkDownloader`)
//! without adding a runtime mock-framework dependency. Each helper binds a
//! fresh ephemeral port, serves a fixed number of requests, and records them
//! for assertion.

#![allow(dead_code)]

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use http::HeaderMap;
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Bytes, Incoming},
    server::conn::http1,
    service::service_fn,
    {Method, Request, Response, StatusCode},
};
use hyper_util::rt::TokioIo;
use tokio::{net::TcpListener, task::JoinHandle};

#[derive(Debug, Clone)]
pub(crate) struct RecordedRequest {
    pub(crate) method: Method,
    pub(crate) path: String,
    pub(crate) query: Option<String>,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

#[derive(Clone)]
pub(crate) struct CannedResponse {
    pub(crate) status: StatusCode,
    pub(crate) body: Bytes,
    pub(crate) headers: Vec<(String, String)>,
}

impl CannedResponse {
    pub(crate) fn ok_json(body: impl Into<String>) -> Self {
        Self {
            status: StatusCode::OK,
            body: Bytes::from(body.into()),
            headers: vec![("content-type".into(), "application/json".into())],
        }
    }

    pub(crate) fn with_status(status: StatusCode, body: impl Into<String>) -> Self {
        Self {
            status,
            body: Bytes::from(body.into()),
            headers: Vec::new(),
        }
    }
}

pub(crate) struct ServerHandle {
    pub(crate) addr: SocketAddr,
    pub(crate) recorded: Arc<Mutex<Vec<RecordedRequest>>>,
    pub(crate) responses: Arc<Mutex<Vec<CannedResponse>>>,
    pub(crate) task: JoinHandle<()>,
}

impl ServerHandle {
    pub(crate) fn base_url(&self) -> reqwest::Url {
        reqwest::Url::parse(&format!("http://{}/", self.addr))
            .expect("addr always forms a valid http URL")
    }

    /// Append a canned response after the server has been started. Useful
    /// when a response body needs to reference the server's own `addr`.
    pub(crate) fn push_response(&self, response: CannedResponse) {
        self.responses
            .lock()
            .expect("responses lock")
            .push(response);
    }
}

/// Bind a loopback TCP server that serves a fixed sequence of canned
/// responses. Each incoming request is recorded. Extra requests beyond the
/// canned sequence receive `503 Service Unavailable`.
pub(crate) fn start_server(responses: Vec<CannedResponse>) -> ServerHandle {
    let responses = Arc::new(Mutex::new(responses.into_iter().collect::<Vec<_>>()));
    let recorded: Arc<Mutex<Vec<RecordedRequest>>> = Arc::new(Mutex::new(Vec::new()));

    let recorded_for_task = Arc::clone(&recorded);
    let responses_for_task = Arc::clone(&responses);

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    listener.set_nonblocking(true).expect("set_nonblocking");
    let addr = listener.local_addr().expect("local_addr");
    let listener = TcpListener::from_std(listener).expect("from_std");

    let task = tokio::spawn(async move {
        let responses = responses_for_task;
        let recorded = recorded_for_task;
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(conn) => conn,
                Err(_) => break,
            };
            let responses = Arc::clone(&responses);
            let recorded = Arc::clone(&recorded);

            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req: Request<Incoming>| {
                    let responses = Arc::clone(&responses);
                    let recorded = Arc::clone(&recorded);
                    async move {
                        let method = req.method().clone();
                        let path = req.uri().path().to_string();
                        let query = req.uri().query().map(ToString::to_string);
                        let headers = req.headers().clone();
                        let body = req
                            .into_body()
                            .collect()
                            .await
                            .map(|c| c.to_bytes())
                            .unwrap_or_default();
                        recorded
                            .lock()
                            .expect("recorded lock")
                            .push(RecordedRequest {
                                method,
                                path,
                                query,
                                headers,
                                body,
                            });

                        let canned = {
                            let mut guard = responses.lock().expect("responses lock");
                            if guard.is_empty() {
                                None
                            } else {
                                Some(guard.remove(0))
                            }
                        };

                        let response = match canned {
                            Some(c) => {
                                let mut builder = Response::builder().status(c.status);
                                for (k, v) in &c.headers {
                                    builder = builder.header(k, v);
                                }
                                builder
                                    .body(Full::new(c.body))
                                    .expect("build canned response")
                            }
                            None => Response::builder()
                                .status(StatusCode::SERVICE_UNAVAILABLE)
                                .body(Full::new(Bytes::from_static(
                                    b"test_server: no canned response left",
                                )))
                                .expect("build fallback response"),
                        };

                        Ok::<_, Infallible>(response)
                    }
                });
                let _ = http1::Builder::new().serve_connection(io, service).await;
            });
        }
    });

    ServerHandle {
        addr,
        recorded,
        responses,
        task,
    }
}
