use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{oneshot, watch},
    task::{JoinError, JoinSet},
};

use crate::external_browser_payload::BrowserCallbackPayload;

use super::ListenerError;
use super::handler::CallbackHandler;

type ConnectionTaskResult = Result<(), ListenerError>;

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

    fn spawn(&mut self, stream: TcpStream, shared: Arc<CallbackHandler>) {
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

pub(super) struct ListenerRuntime {
    listener: TcpListener,
    shared: Arc<CallbackHandler>,
    shutdown: oneshot::Receiver<()>,
    connections: ConnectionTasks,
}

impl ListenerRuntime {
    pub(super) fn new(
        listener: TcpListener,
        expected_origin: String,
        application: Option<String>,
        shutdown: oneshot::Receiver<()>,
        payload_tx: watch::Sender<Option<BrowserCallbackPayload>>,
    ) -> Self {
        Self {
            listener,
            shared: Arc::new(CallbackHandler::new(
                expected_origin,
                application,
                payload_tx,
            )),
            shutdown,
            connections: ConnectionTasks::new(),
        }
    }

    pub(super) async fn run(mut self) -> Result<(), ListenerError> {
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

async fn run_connection(
    stream: TcpStream,
    shared: Arc<CallbackHandler>,
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

pub(super) fn bind_listener(
    cfg: &super::ListenerConfig,
) -> Result<(TcpListener, SocketAddr), ListenerError> {
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

pub(super) fn build_origin(protocol: &str, host: IpAddr, port: u16) -> String {
    format!("{protocol}://{}", SocketAddr::new(host, port))
}
