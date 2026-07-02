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

use std::fmt;

use url::Url;

use auth::login;

#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    config: ClientConfig,
    base_url: Url,
    runtime: QueryRuntime,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Omits the reqwest client and runtime; config's own Debug redacts credentials.
        f.debug_struct("Client")
            .field("base_url", &self.base_url)
            .field("config", &self.config)
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

        Ok(Self {
            http,
            config,
            base_url,
            runtime: QueryRuntime::new(),
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
        let session_token = login(&self.http, &self.config, &self.base_url).await?;
        Ok(Session {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            session_token,
            query: self.config.query().clone(),
            runtime: self.runtime.clone(),
        })
    }
}
