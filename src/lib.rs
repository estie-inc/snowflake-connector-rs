mod auth;
mod chunk;
mod error;
mod query;
mod row;
mod session;

pub use error::{Error, Result};
pub use row::{SnowflakeDecode, SnowflakeRow};
pub use session::SnowflakeSession;

use auth::login;

use reqwest::{Client, ClientBuilder};

pub struct SnowflakeClient {
    http: Client,

    username: String,
    auth: SnowflakeAuthMethod,
    config: SnowflakeClientConfig,
}

#[derive(Default)]
pub struct SnowflakeClientConfig {
    pub account: String,

    pub warehouse: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub role: Option<String>,
}

pub enum SnowflakeAuthMethod {
    Password(String),
    KeyPair {
        encrypted_pem: String,
        password: Vec<u8>,
    },
}

impl SnowflakeClient {
    pub fn new(
        username: &str,
        auth: SnowflakeAuthMethod,
        config: SnowflakeClientConfig,
    ) -> Result<Self> {
        let client = ClientBuilder::new().gzip(true).build()?;
        Ok(Self {
            http: client,
            username: username.to_string(),
            auth,
            config,
        })
    }

    pub async fn create_session(&self) -> Result<SnowflakeSession> {
        let session_token = login(&self.http, &self.username, &self.auth, &self.config).await?;
        Ok(SnowflakeSession {
            http: self.http.clone(),
            account: self.config.account.clone(),
            session_token,
        })
    }
}
