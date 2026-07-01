//! # Snowflake Connector
//!
//! A Rust client for Snowflake. Query results are materialized into a
//! [`ResultTable`] and decoded with the [`FromRow`] / [`FromCell`] traits.
//!
//! ```rust,no_run
//! # use snowflake_connector_rs::{
//! #     Result, SnowflakeAuthConfig, SnowflakeClient, SnowflakeClientConfig,
//! # };
//! # async fn run() -> Result<()> {
//! let client = SnowflakeClient::new(SnowflakeClientConfig::new(
//!     "USERNAME",
//!     "ACCOUNT",
//!     SnowflakeAuthConfig::password("PASSWORD"),
//! ))?;
//! let session = client.create_session().await?;
//!
//! // Collect dynamic or typed rows in one shot.
//! let dynamic_rows = session
//!     .query("SELECT 1 AS id, 'hi' AS name")
//!     .await?
//!     .collect()
//!     .await?;
//! assert_eq!(dynamic_rows.len(), 1);
//!
//! let typed_rows = session
//!     .query_as::<(i64, String), _>("SELECT 1 AS id, 'hi' AS name")
//!     .await?
//!     .collect()
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
pub mod result;
mod rowset;
mod runtime;
mod session;
mod statement;

#[cfg(feature = "key-pair-auth")]
pub use auth::config::KeyPairAuthConfig;
pub use auth::config::{PasswordAuthConfig, SnowflakeAuthConfig};
#[cfg(feature = "external-browser-sso")]
pub use auth::external_browser::{BrowserLaunchMode, ExternalBrowserConfig};
pub use config::{
    SnowflakeClientConfig, SnowflakeEndpointConfig, SnowflakeProxyConfig, SnowflakeQueryConfig,
    SnowflakeSessionConfig, SnowflakeTransportConfig,
};
pub use error::{
    AmbiguousColumnError, CellDecodeError, ColumnCountMismatchError, DuplicateColumnNameError,
    Error, ErrorKind, InvalidColumnIndexError, MissingColumnError, Result, SchemaError,
};
pub use query_result::{CollectOptions, ResultSet, TypedResultSet};
pub use result::{DynamicRow, FromCell, FromRow, ResultTable, TypedResultTable};
use runtime::QueryRuntime;
pub use session::SnowflakeSession;
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
pub struct SnowflakeClient {
    http: reqwest::Client,
    config: SnowflakeClientConfig,
    base_url: Url,
    runtime: QueryRuntime,
}

impl fmt::Debug for SnowflakeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Omits the reqwest client and runtime; config's own Debug redacts credentials.
        f.debug_struct("SnowflakeClient")
            .field("base_url", &self.base_url)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SnowflakeClient {
    /// Build a client from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Config` when endpoint or transport configuration is invalid.
    pub fn new(config: SnowflakeClientConfig) -> Result<Self> {
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
    pub async fn create_session(&self) -> Result<SnowflakeSession> {
        let session_token = login(&self.http, &self.config, &self.base_url).await?;
        Ok(SnowflakeSession {
            http: self.http.clone(),
            base_url: self.base_url.clone(),
            session_token,
            query: self.config.query().clone(),
            runtime: self.runtime.clone(),
        })
    }
}
