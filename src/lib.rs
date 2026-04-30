//! # Snowflake Connector
//!
//! A Rust client for Snowflake. Query results are materialized into a
//! [`ResultTable`] and decoded with the [`FromRow`] / [`FromCell`] traits.
//!
//! ```rust,no_run
//! # use snowflake_connector_rs::{
//! #     Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig,
//! # };
//! # async fn run() -> Result<()> {
//! let client = SnowflakeClient::new(SnowflakeClientConfig::new(
//!     "USERNAME",
//!     "ACCOUNT",
//!     SnowflakeAuthMethod::Password("PASSWORD".to_string()),
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
mod chunk;
mod config;
mod error;
mod query;
mod query_result;
mod result;
mod rowset;
mod runtime;
mod session;
mod statement;

#[cfg(feature = "external-browser-sso")]
pub use auth::external_browser::{
    BrowserLaunchMode, ExternalBrowserConfig, WithCallbackListenerConfig,
    WithoutCallbackListenerConfig,
};
pub use config::{
    SnowflakeClientConfig, SnowflakeEndpointConfig, SnowflakeProxyConfig, SnowflakeQueryConfig,
    SnowflakeSessionConfig, SnowflakeTransportConfig,
};
pub use error::{DecodeError, Error, ParseError, Result, SchemaError};
pub use query::{Binding, BindingType, QueryRequest};
pub use query_result::{CollectOptions, ResultSet, TypedResultSet};
pub use result::{
    CellRef, Column, ColumnIndex, ColumnType, DecimalValue, DynamicRow, FromCell, FromRow,
    ResultTable, RowPlanContext, RowRef, Rows, Schema, SnowflakeValue, TypedResultTable,
};
pub use session::SnowflakeSession;

#[cfg(feature = "derive")]
pub use snowflake_connector_rs_derive::FromRow;

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod bench_support;

use url::Url;

use auth::login;

#[derive(Clone)]
pub struct SnowflakeClient {
    http: reqwest::Client,
    config: SnowflakeClientConfig,
    base_url: Url,
    runtime: runtime::QueryRuntime,
}

#[derive(Clone)]
pub enum SnowflakeAuthMethod {
    Password(String),
    KeyPair {
        encrypted_pem: String,
        password: Vec<u8>,
    },
    KeyPairUnencrypted {
        pem: String,
    },
    Oauth {
        token: String,
    },
    #[cfg(feature = "external-browser-sso")]
    /// External browser SSO authentication.
    ///
    /// This is an experimental feature.
    /// The API and behavior may change in future releases without backward compatibility guarantees.
    ///
    /// ## Typical setup patterns
    ///
    /// ### Default (auto browser launch, localhost callback with auto-picked port)
    ///
    /// ```rust
    /// use snowflake_connector_rs::{ExternalBrowserConfig, SnowflakeAuthMethod};
    ///
    /// let auth = SnowflakeAuthMethod::ExternalBrowser(ExternalBrowserConfig::default());
    /// ```
    ///
    /// ### Docker/container mode (manual open + explicit callback bind address/port)
    ///
    /// ```rust
    /// use std::net::Ipv4Addr;
    /// use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthMethod};
    ///
    /// let external_browser = ExternalBrowserConfig::with_callback_listener(
    ///     BrowserLaunchMode::Manual,
    ///     Ipv4Addr::UNSPECIFIED.into(),
    ///     3037,
    /// );
    /// let auth = SnowflakeAuthMethod::ExternalBrowser(external_browser);
    /// ```
    ///
    /// ### Without callback listener mode (manual redirected-URL input)
    ///
    /// ```rust
    /// use std::num::NonZeroU16;
    /// use snowflake_connector_rs::{BrowserLaunchMode, ExternalBrowserConfig, SnowflakeAuthMethod};
    ///
    /// let redirect_port = NonZeroU16::new(3037).unwrap();
    /// let external_browser =
    ///     ExternalBrowserConfig::without_callback_listener(BrowserLaunchMode::Manual, redirect_port);
    /// let auth = SnowflakeAuthMethod::ExternalBrowser(external_browser);
    /// ```
    ExternalBrowser(ExternalBrowserConfig),
}

impl SnowflakeClient {
    pub fn new(config: SnowflakeClientConfig) -> Result<Self> {
        let base_url = config.endpoint().resolve(config.account())?;
        let http = config.transport().build_http_client()?;

        Ok(Self {
            http,
            config,
            base_url,
            runtime: runtime::QueryRuntime::new(),
        })
    }

    /// Authenticate and create a new Snowflake session.
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
