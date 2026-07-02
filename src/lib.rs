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
pub(crate) use config::{ClientLoginConfig, InitialSessionConfig, QueryExecutionPolicy};
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
    login: ClientLoginConfig,
    shared: Arc<ClientShared>,
}

/// Connector-wide execution state shared by every session a client creates.
pub(crate) struct ClientShared {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) query: QueryExecutionPolicy,
    pub(crate) runtime: QueryRuntime,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Prints only non-secret identity fields; credentials, the reqwest
        // client, and the runtime are omitted.
        f.debug_struct("Client")
            .field("base_url", &self.inner.shared.base_url)
            .field("username", &self.inner.login.username())
            .field("account", &self.inner.login.account())
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
        let prepared = config.prepare()?;

        let shared = Arc::new(ClientShared {
            http: prepared.shared.http,
            base_url: prepared.shared.base_url,
            query: prepared.shared.query,
            runtime: QueryRuntime::new(),
        });

        Ok(Self {
            inner: Arc::new(ClientInner {
                login: prepared.login,
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
            QueryConfig::default().into(),
            QueryRuntime::new(),
        )
    }

    /// Build a shared-state handle for unit tests with explicit dependencies.
    pub(crate) fn for_test_with(
        http: reqwest::Client,
        base_url: Url,
        query: QueryExecutionPolicy,
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
