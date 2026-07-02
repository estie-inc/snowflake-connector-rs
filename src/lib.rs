//! # Snowflake Connector
//!
//! A Rust client for Snowflake. Query results are materialized into a
//! [`ResultTable`] and decoded with the [`FromRow`] / [`FromCell`] traits.
//!
//! ```rust,no_run
//! # use snowflake_connector_rs::{
//! #     Result, AuthConfig, Client, ClientConfig,
//! # };
//! # async fn run() -> Result<()> {
//! let client = Client::new(ClientConfig::new(
//!     "USERNAME",
//!     "ACCOUNT",
//!     AuthConfig::password("PASSWORD"),
//! ))?;
//! let session = client.create_session().await?;
//!
//! // Collect dynamic or typed rows in one shot.
//! let dynamic_rows = session
//!     .query("SELECT 1 AS id, 'hi' AS name")
//!     .await?
//!     .collect::<Vec<_>>()
//!     .await?;
//! assert_eq!(dynamic_rows.len(), 1);
//!
//! let typed_rows = session
//!     .query_as("SELECT 1 AS id, 'hi' AS name")
//!     .await?
//!     .collect::<Vec<(i64, String)>>()
//!     .await?;
//! assert_eq!(typed_rows, vec![(1, "hi".to_string())]);
//!
//! // Collect materialized storage as a ResultTable or TypedResultTable.
//! let table = session
//!     .query("SELECT 1 AS id")
//!     .await?
//!     .collect_table()
//!     .await?;
//! assert_eq!(table.row_count(), 1);
//!
//! let typed_table = session
//!     .query_as::<(i64,), _>("SELECT 1 AS id")
//!     .await?
//!     .collect_table()
//!     .await?;
//! assert_eq!(typed_table.row_count(), 1);
//!
//! // Or stream typed partitions for large results.
//! let mut result = session
//!     .query_as::<(i64, String), _>("SELECT 1 AS id, 'hi' AS name")
//!     .await?;
//! while let Some(table) = result.next_table().await? {
//!     for row in table.rows() {
//!         let (id, name) = row?;
//!         println!("{id} {name}");
//!     }
//! }
//!
//! // Keep the untyped streaming path when you only need materialized storage.
//! let result = session.query("SELECT 1 AS id").await?;
//! assert_eq!(result.schema().len(), 1);
//! # Ok(())
//! # }
//! ```

mod auth;
pub mod bind;
mod chunk;
mod config;
mod error;
mod query_result;
mod result;
mod rowset;
mod runtime;
mod session;
mod statement;

#[cfg(feature = "key-pair-auth")]
pub use auth::config::KeyPairConfig;
pub use auth::config::{AuthConfig, PasswordConfig};
#[cfg(feature = "external-browser-sso")]
pub use auth::external_browser::{BrowserLaunchMode, ExternalBrowserConfig};
pub use config::{
    ClientConfig, EndpointConfig, ProxyConfig, QueryConfig, SessionConfig, TransportConfig,
};
pub use error::{
    AmbiguousColumnError, CellConversionError, CellConversionErrorBuilder, CellDecodeError,
    CellDecodeResult, ColumnCountMismatchError, DuplicateColumnNameError, Error, ErrorKind,
    InvalidColumnIndexError, MissingColumnError, Result, SchemaError,
};
pub use query_result::{CollectOptions, ResultCursor, TypedResultCursor};
pub use result::{
    CellRef, CellValue, Column, ColumnIndex, ColumnType, DecimalValue, DynamicRow, FromCell,
    FromRow, ResultTable, RowPlanContext, RowRef, Rows, Schema, TypedResultTable,
};
use runtime::QueryRuntime;
pub use session::Session;
pub use statement::builder::{IntoStatement, NamedBinds, PositionalBinds, Statement, UnboundBinds};

#[cfg(feature = "derive")]
pub use snowflake_connector_rs_derive::FromRow;

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;

use std::{fmt, sync::Arc};

use url::Url;

use auth::login;
use session::SessionAuth;

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    login: ClientConfig,
    shared: Arc<ClientShared>,
}

/// Connector-wide execution state shared by every session a client creates.
pub(crate) struct ClientShared {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) query: QueryConfig,
    pub(crate) runtime: QueryRuntime,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Omits the reqwest client and runtime; config's own Debug redacts credentials.
        f.debug_struct("Client")
            .field("base_url", &self.inner.shared.base_url)
            .field("config", &self.inner.login)
            .finish_non_exhaustive()
    }
}

impl Client {
    /// Build a client from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Config` when endpoint or transport configuration is invalid.
    pub fn new(config: ClientConfig) -> Result<Self> {
        let base_url = config.endpoint().resolve(config.account())?;
        let http = config.transport().build_http_client()?;

        let shared = Arc::new(ClientShared {
            http,
            base_url,
            query: config.query().clone(),
            runtime: QueryRuntime::new(),
        });

        Ok(Self {
            inner: Arc::new(ClientInner {
                login: config,
                shared,
            }),
        })
    }

    /// Authenticate and create a new Snowflake session.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Config`, `ErrorKind::Auth`, `ErrorKind::Network`,
    /// `ErrorKind::Timeout`, `ErrorKind::Protocol`, or `ErrorKind::Internal`
    /// depending on how session establishment fails.
    pub async fn create_session(&self) -> Result<Session> {
        let session_token = login(&self.inner.login, Arc::clone(&self.inner.shared)).await?;
        Ok(Session {
            shared: Arc::clone(&self.inner.shared),
            auth: Arc::new(SessionAuth { session_token }),
        })
    }
}

#[cfg(test)]
impl ClientShared {
    /// Build a shared-state handle for unit tests with connector defaults.
    pub(crate) fn for_test(base_url: Url) -> Arc<Self> {
        Self::for_test_with(
            reqwest::Client::new(),
            base_url,
            QueryConfig::default(),
            QueryRuntime::new(),
        )
    }

    /// Build a shared-state handle for unit tests with explicit dependencies.
    pub(crate) fn for_test_with(
        http: reqwest::Client,
        base_url: Url,
        query: QueryConfig,
        runtime: QueryRuntime,
    ) -> Arc<Self> {
        Arc::new(Self {
            http,
            base_url,
            query,
            runtime,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;
    use crate::AuthConfig;

    fn test_client(base_url: Url) -> Client {
        Client::new(
            ClientConfig::new("user", "account", AuthConfig::password("secret"))
                .with_endpoint(EndpointConfig::custom_base_url(base_url)),
        )
        .expect("test client builds from validated config")
    }

    #[test]
    fn client_clone_shares_inner_state() {
        let client = test_client(Url::parse("https://example.com/").unwrap());
        let cloned = client.clone();

        assert!(Arc::ptr_eq(&client.inner, &cloned.inner));
    }

    /// Accepts `login_count` login requests, each answered with the same session token.
    async fn spawn_login_server(login_count: usize) -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            for _ in 0..login_count {
                let (mut socket, _) = listener.accept().await.unwrap();

                // Drain until the request headers finish so the response never races the client's write.
                let mut buf = Vec::new();
                let mut chunk = [0_u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = socket.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }

                let body = r#"{"success":true,"data":{"token":"session-token"}}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        addr
    }

    #[tokio::test]
    async fn create_session_shares_client_shared_state() {
        let addr = spawn_login_server(2).await;
        let client = test_client(Url::parse(&format!("http://{addr}/")).unwrap());

        let first = client.create_session().await.unwrap();
        let second = client.create_session().await.unwrap();

        // Every session reuses the one connector-wide state handle.
        assert!(Arc::ptr_eq(&first.shared, &client.inner.shared));
        assert!(Arc::ptr_eq(&first.shared, &second.shared));
        // Per-session auth is distinct storage even when the token value matches.
        assert!(!Arc::ptr_eq(&first.auth, &second.auth));
    }
}
