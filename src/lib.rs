//! # Snowflake Connector
//!
//! A Rust client for Snowflake, which enables you to connect to Snowflake and run queries.
//!
//! ```rust
//! # use snowflake_connector_rs::{Result, SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig};
//! # async fn run() -> Result<()> {
//! let client = SnowflakeClient::new(
//!     "USERNAME",
//!     SnowflakeAuthMethod::Password("PASSWORD".to_string()),
//!     SnowflakeClientConfig {
//!         account: "ACCOUNT".to_string(),
//!         role: Some("ROLE".to_string()),
//!         warehouse: Some("WAREHOUSE".to_string()),
//!         database: Some("DATABASE".to_string()),
//!         schema: Some("SCHEMA".to_string()),
//!         timeout: Some(std::time::Duration::from_secs(30)),
//!     },
//! )?;
//! let session = client.create_session().await?;
//!
//! let query = "CREATE TEMPORARY TABLE example (id NUMBER, value STRING)";
//! session.query(query).await?;
//!
//! let query = "INSERT INTO example (id, value) VALUES (1, 'hello'), (2, 'world')";
//! session.query(query).await?;
//!
//! let query = "SELECT * FROM example ORDER BY id";
//! let rows = session.query(query).await?;
//! assert_eq!(rows.len(), 2);
//! assert_eq!(rows[0].get::<i64>("ID")?, 1);
//! assert_eq!(rows[0].get::<String>("VALUE")?, "hello");
//! # Ok(())
//! # }
//! ```

mod auth;
mod chunk;
mod error;
mod query;
mod row;
mod session;

use std::time::Duration;

pub use error::{Error, Result};
pub use query::QueryExecutor;
pub use row::{SnowflakeColumn, SnowflakeColumnType, SnowflakeDecode, SnowflakeRow};
pub use session::SnowflakeSession;

use auth::login;

use reqwest::{Client, ClientBuilder, Proxy};

#[derive(Clone)]
pub struct SnowflakeClient {
    http: Client,

    username: String,
    auth: SnowflakeAuthMethod,
    config: SnowflakeClientConfig,
    connection_config: Option<SnowflakeConnectionConfig>,
}

#[derive(Default, Clone)]
pub struct SnowflakeClientConfig {
    pub account: String,

    pub warehouse: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub role: Option<String>,
    pub timeout: Option<Duration>,
}

#[derive(Default, Clone)]
struct SnowflakeConnectionConfig {
    host: String,
    port: Option<u16>,
    protocol: Option<String>,
}

#[derive(Clone)]
pub enum SnowflakeAuthMethod {
    Password(String),
    KeyPair {
        encrypted_pem: String,
        password: Vec<u8>,
    },
    Oauth {
        token: String,
    },
}

impl SnowflakeClient {
    pub fn new(
        username: &str,
        auth: SnowflakeAuthMethod,
        config: SnowflakeClientConfig,
    ) -> Result<Self> {
        let client = ClientBuilder::new().gzip(true).use_rustls_tls().build()?;
        Ok(Self {
            http: client,
            username: username.to_string(),
            auth,
            config,
            connection_config: None,
        })
    }

    pub fn with_proxy(self, host: &str, port: u16, username: &str, password: &str) -> Result<Self> {
        let proxy = Proxy::all(format!("http://{}:{}", host, port).as_str())?
            .basic_auth(username, password);

        let client = ClientBuilder::new()
            .gzip(true)
            .use_rustls_tls()
            .proxy(proxy)
            .build()?;
        Ok(Self {
            http: client,
            username: self.username,
            auth: self.auth,
            config: self.config,
            connection_config: self.connection_config,
        })
    }

    pub fn with_address(
        self,
        host: &str,
        port: Option<u16>,
        protocol: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            http: self.http,
            username: self.username,
            auth: self.auth,
            config: self.config,
            connection_config: Some(SnowflakeConnectionConfig {
                host: host.to_string(),
                port,
                protocol,
            }),
        })
    }

    pub async fn create_session(&self) -> Result<SnowflakeSession> {
        let session_token = login(
            &self.http,
            &self.username,
            &self.auth,
            &self.config,
            &self.connection_config,
        )
        .await?;
        Ok(SnowflakeSession {
            http: self.http.clone(),
            account: self.config.account.clone(),
            session_token,
            timeout: self.config.timeout,
            host: self
                .connection_config
                .as_ref()
                .map(|conf| conf.host.clone()),
            port: self.connection_config.as_ref().and_then(|conf| conf.port),
            protocol: self
                .connection_config
                .as_ref()
                .and_then(|conf| conf.protocol.clone()),
        })
    }
}
