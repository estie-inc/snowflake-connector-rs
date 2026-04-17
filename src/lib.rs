//! # Snowflake Connector
//!
//! A Rust client for Snowflake, which enables you to connect to Snowflake and run queries.
//!
//! ```rust
//! # use snowflake_connector_rs::{
//! #     Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig,
//! #     SnowflakeQueryConfig, SnowflakeSessionConfig,
//! # };
//! # async fn run() -> Result<()> {
//! let session = SnowflakeSessionConfig::default()
//!     .with_role("ROLE")
//!     .with_warehouse("WAREHOUSE")
//!     .with_database("DATABASE")
//!     .with_schema("SCHEMA");
//!
//! let query = SnowflakeQueryConfig::default()
//!     .with_async_query_completion_timeout(std::time::Duration::from_secs(30));
//!
//! let client = SnowflakeClient::new(
//!     SnowflakeClientConfig::new(
//!         "USERNAME",
//!         "ACCOUNT",
//!         SnowflakeAuthMethod::Password("PASSWORD".to_string()),
//!     )
//!     .with_session(session)
//!     .with_query(query),
//! )?;
//! let session = client.create_session().await?;
//!
//! let query = "CREATE TEMPORARY TABLE example (id NUMBER, value STRING)";
//! session.query(query).await?;
//!
//! let query = "INSERT INTO example (id, value) VALUES (1, 'hello'), (2, 'world')";
//! session.query(query).await?;
//!
//! // Fetch all results at once
//! let rows = session.query("SELECT * FROM example ORDER BY id").await?;
//! assert_eq!(rows.len(), 2);
//! assert_eq!(rows[0].get::<i64>("ID")?, 1);
//! assert_eq!(rows[0].get::<String>("VALUE")?, "hello");
//!
//! // Or stream batch by batch via ResultSet
//! let mut result = session.execute("SELECT * FROM example ORDER BY id").await?;
//! while let Some(batch) = result.next_batch().await? {
//!     for row in &batch {
//!         println!("{}", row.get::<String>("VALUE")?);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

mod auth;
mod config;
mod error;
mod query;
mod row;
mod session;

#[cfg(feature = "external-browser-sso")]
pub use auth::external_browser::{
    BrowserLaunchMode, ExternalBrowserConfig, WithCallbackListenerConfig,
    WithoutCallbackListenerConfig,
};
pub use config::{
    SnowflakeClientConfig, SnowflakeEndpointConfig, SnowflakeProxyConfig, SnowflakeQueryConfig,
    SnowflakeSessionConfig, SnowflakeTransportConfig,
};
pub use error::{Error, Result};
pub use query::{Binding, BindingType, CollectOptions, QueryRequest, ResultSet};
pub use row::{SnowflakeColumn, SnowflakeColumnType, SnowflakeDecode, SnowflakeRow};
pub use session::SnowflakeSession;

use auth::login;
use url::Url;

#[derive(Clone)]
pub struct SnowflakeClient {
    http: reqwest::Client,
    config: SnowflakeClientConfig,
    base_url: Url,
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
        })
    }
}
