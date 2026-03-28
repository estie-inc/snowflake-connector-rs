use std::time::Duration;

use url::Url;

use crate::{Error, Result, SnowflakeAuthMethod};

/// Top-level configuration for a [`SnowflakeClient`](crate::SnowflakeClient).
#[derive(Clone)]
pub struct SnowflakeClientConfig {
    username: String,
    account: String,
    auth: SnowflakeAuthMethod,
    session: SnowflakeSessionConfig,
    query: SnowflakeQueryConfig,
    endpoint: SnowflakeEndpointConfig,
    transport: SnowflakeTransportConfig,
}

/// Server-side session context sent to Snowflake at login time.
///
/// These values are passed as query parameters in the login request and
/// determine the initial state of the Snowflake session (active warehouse,
/// database, schema, and role). They correspond directly to Snowflake's
/// session-level settings and do not affect client-side behavior.
#[derive(Default, Clone)]
pub struct SnowflakeSessionConfig {
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
}

/// Client-side query execution policy.
///
/// Controls how this connector behaves while executing queries — for example,
/// how long to poll for the completion of an async query. These settings are
/// enforced entirely on the client side and are never sent to Snowflake.
#[derive(Default, Clone)]
pub struct SnowflakeQueryConfig {
    async_query_completion_timeout: Option<Duration>,
}

/// Endpoint resolution strategy for the Snowflake API base URL.
///
/// By default the base URL is derived from the account name
/// (`https://<account>.snowflakecomputing.com`). Use
/// [`CustomBaseUrl`](Self::CustomBaseUrl) to override this — for example,
/// when connecting through a PrivateLink endpoint or a local test server.
#[non_exhaustive]
#[derive(Default, Clone)]
pub enum SnowflakeEndpointConfig {
    #[default]
    AccountDefault,
    CustomBaseUrl(Url),
}

/// HTTP transport-layer options.
///
/// Configures how requests are physically delivered to Snowflake,
/// independent of which endpoint they target.
#[derive(Default, Clone)]
pub struct SnowflakeTransportConfig {
    proxy: Option<SnowflakeProxyConfig>,
}

/// Configuration for an HTTP proxy used by [`SnowflakeTransportConfig`].
///
/// Specifies the proxy URL and optional authentication credentials.
/// Only HTTP and HTTPS proxy schemes are accepted.
#[derive(Clone)]
pub struct SnowflakeProxyConfig {
    url: Url,
    auth: SnowflakeProxyAuth,
}

/// Authentication method for a [`SnowflakeProxyConfig`].
#[non_exhaustive]
#[derive(Clone)]
pub enum SnowflakeProxyAuth {
    None,
    Basic { username: String, password: String },
}

impl SnowflakeClientConfig {
    pub fn new(
        username: impl Into<String>,
        account: impl Into<String>,
        auth: SnowflakeAuthMethod,
    ) -> Self {
        Self {
            username: username.into(),
            account: account.into(),
            auth,
            session: SnowflakeSessionConfig::default(),
            query: SnowflakeQueryConfig::default(),
            endpoint: SnowflakeEndpointConfig::default(),
            transport: SnowflakeTransportConfig::default(),
        }
    }

    pub fn with_session(mut self, session: SnowflakeSessionConfig) -> Self {
        self.session = session;
        self
    }

    pub fn with_query(mut self, query: SnowflakeQueryConfig) -> Self {
        self.query = query;
        self
    }

    pub fn with_endpoint(mut self, endpoint: SnowflakeEndpointConfig) -> Self {
        self.endpoint = endpoint;
        self
    }

    pub fn with_transport(mut self, transport: SnowflakeTransportConfig) -> Self {
        self.transport = transport;
        self
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn account(&self) -> &str {
        &self.account
    }

    pub(crate) fn auth(&self) -> &SnowflakeAuthMethod {
        &self.auth
    }

    pub(crate) fn session(&self) -> &SnowflakeSessionConfig {
        &self.session
    }

    pub(crate) fn query(&self) -> &SnowflakeQueryConfig {
        &self.query
    }

    pub(crate) fn endpoint(&self) -> &SnowflakeEndpointConfig {
        &self.endpoint
    }

    pub(crate) fn transport(&self) -> &SnowflakeTransportConfig {
        &self.transport
    }
}

impl SnowflakeSessionConfig {
    pub fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }
}

impl SnowflakeQueryConfig {
    pub fn async_query_completion_timeout(&self) -> Option<Duration> {
        self.async_query_completion_timeout
    }

    pub fn with_async_query_completion_timeout(mut self, timeout: Duration) -> Self {
        self.async_query_completion_timeout = Some(timeout);
        self
    }
}

impl SnowflakeEndpointConfig {
    pub fn custom_base_url(url: Url) -> Self {
        Self::CustomBaseUrl(url)
    }

    pub(crate) fn resolve(&self, account: &str) -> Result<Url> {
        match self {
            Self::AccountDefault => {
                Url::parse(&format!("https://{account}.snowflakecomputing.com")).map_err(Into::into)
            }
            Self::CustomBaseUrl(url) => validate_custom_base_url(url.clone()),
        }
    }
}

const ALLOWED_ENDPOINT_SCHEMES: &[&str] = &["http", "https"];

fn validate_custom_base_url(mut url: Url) -> Result<Url> {
    if !ALLOWED_ENDPOINT_SCHEMES.contains(&url.scheme()) {
        return Err(Error::Url(format!(
            "unsupported custom base URL scheme '{}'; allowed: {}",
            url.scheme(),
            ALLOWED_ENDPOINT_SCHEMES.join(", "),
        )));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::Url(
            "custom base URL must not contain query or fragment".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::Url(
            "custom base URL must not contain credentials".to_string(),
        ));
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err(Error::Url(
            "custom base URL must not contain a path".to_string(),
        ));
    }
    url.set_path("/");
    Ok(url)
}

impl SnowflakeTransportConfig {
    pub fn with_proxy(mut self, proxy: SnowflakeProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    pub(crate) fn build_http_client(&self) -> Result<reqwest::Client> {
        let builder = reqwest::ClientBuilder::new().gzip(true).use_rustls_tls();

        let builder = if let Some(proxy) = &self.proxy {
            builder.proxy(proxy.to_reqwest_proxy()?)
        } else {
            builder
        };

        Ok(builder.build()?)
    }
}

impl SnowflakeProxyConfig {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            auth: SnowflakeProxyAuth::None,
        }
    }

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = SnowflakeProxyAuth::Basic {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    pub(crate) fn to_reqwest_proxy(&self) -> Result<reqwest::Proxy> {
        let url = validate_proxy_url(self.url.clone())?;
        let mut proxy = reqwest::Proxy::all(url.as_str())?;

        if let SnowflakeProxyAuth::Basic { username, password } = &self.auth {
            proxy = proxy.basic_auth(username, password);
        }

        Ok(proxy)
    }
}

const ALLOWED_PROXY_SCHEMES: &[&str] = &["http", "https"];

fn validate_proxy_url(url: Url) -> Result<Url> {
    if !ALLOWED_PROXY_SCHEMES.contains(&url.scheme()) {
        return Err(Error::Url(format!(
            "unsupported proxy URL scheme '{}'; allowed: {}",
            url.scheme(),
            ALLOWED_PROXY_SCHEMES.join(", "),
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::Url(
            "proxy URL must not contain credentials; use SnowflakeProxyConfig::with_basic_auth() instead".to_string(),
        ));
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::Url(
            "proxy URL must not contain query or fragment".to_string(),
        ));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_http_url_is_accepted() {
        let url = Url::parse("http://proxy.example.com:8080").unwrap();
        let proxy = SnowflakeProxyConfig::new(url);
        assert!(proxy.to_reqwest_proxy().is_ok());
    }

    #[test]
    fn proxy_https_url_is_accepted() {
        let url = Url::parse("https://proxy.example.com:8080").unwrap();
        let proxy = SnowflakeProxyConfig::new(url);
        assert!(proxy.to_reqwest_proxy().is_ok());
    }

    #[test]
    fn proxy_socks5_url_is_rejected() {
        let url = Url::parse("socks5://proxy.example.com:1080").unwrap();
        let proxy = SnowflakeProxyConfig::new(url);
        let err = proxy.to_reqwest_proxy().unwrap_err();
        assert!(
            format!("{err}").contains("unsupported proxy URL scheme"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn proxy_url_with_credentials_is_rejected() {
        let url = Url::parse("http://user:pass@proxy.example.com:8080").unwrap();
        let proxy = SnowflakeProxyConfig::new(url);
        let err = proxy.to_reqwest_proxy().unwrap_err();
        assert!(
            format!("{err}").contains("must not contain credentials"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn proxy_url_with_unsupported_scheme_is_rejected() {
        let url = Url::parse("ftp://proxy.example.com").unwrap();
        let proxy = SnowflakeProxyConfig::new(url);
        let err = proxy.to_reqwest_proxy().unwrap_err();
        assert!(
            format!("{err}").contains("unsupported proxy URL scheme"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn proxy_basic_auth_builds_successfully() {
        let url = Url::parse("http://proxy.example.com:8080").unwrap();
        let proxy = SnowflakeProxyConfig::new(url).with_basic_auth("user", "pass");
        assert!(proxy.to_reqwest_proxy().is_ok());
    }

    #[test]
    fn no_proxy_builds_client_successfully() {
        let transport = SnowflakeTransportConfig::default();
        assert!(transport.build_http_client().is_ok());
    }

    #[test]
    fn proxy_url_with_query_is_rejected() {
        let url = Url::parse("http://proxy.example.com:8080?foo=bar").unwrap();
        let proxy = SnowflakeProxyConfig::new(url);
        let err = proxy.to_reqwest_proxy().unwrap_err();
        assert!(
            format!("{err}").contains("must not contain query or fragment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_url_https_is_accepted() {
        let url = Url::parse("https://custom.snowflake.example.com").unwrap();
        let result = validate_custom_base_url(url);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap().as_str(),
            "https://custom.snowflake.example.com/"
        );
    }

    #[test]
    fn custom_base_url_http_is_accepted() {
        let url = Url::parse("http://localhost:8080").unwrap();
        let result = validate_custom_base_url(url);
        assert!(result.is_ok());
    }

    #[test]
    fn custom_base_url_ftp_scheme_is_rejected() {
        let url = Url::parse("ftp://snowflake.example.com").unwrap();
        let err = validate_custom_base_url(url).unwrap_err();
        assert!(
            format!("{err}").contains("unsupported custom base URL scheme"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_url_ws_scheme_is_rejected() {
        let url = Url::parse("ws://snowflake.example.com").unwrap();
        let err = validate_custom_base_url(url).unwrap_err();
        assert!(
            format!("{err}").contains("unsupported custom base URL scheme"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_url_with_query_is_rejected() {
        let url = Url::parse("https://snowflake.example.com?foo=bar").unwrap();
        let err = validate_custom_base_url(url).unwrap_err();
        assert!(
            format!("{err}").contains("must not contain query or fragment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_url_with_fragment_is_rejected() {
        let url = Url::parse("https://snowflake.example.com#section").unwrap();
        let err = validate_custom_base_url(url).unwrap_err();
        assert!(
            format!("{err}").contains("must not contain query or fragment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_url_with_credentials_is_rejected() {
        let url = Url::parse("https://user:pass@snowflake.example.com").unwrap();
        let err = validate_custom_base_url(url).unwrap_err();
        assert!(
            format!("{err}").contains("must not contain credentials"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_url_with_path_is_rejected() {
        let url = Url::parse("https://snowflake.example.com/some/path").unwrap();
        let err = validate_custom_base_url(url).unwrap_err();
        assert!(
            format!("{err}").contains("must not contain a path"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_url_normalizes_trailing_slash() {
        let url = Url::parse("https://snowflake.example.com").unwrap();
        let result = validate_custom_base_url(url).unwrap();
        assert_eq!(result.path(), "/");
    }

    #[test]
    fn endpoint_account_default_resolves() {
        let endpoint = SnowflakeEndpointConfig::AccountDefault;
        let url = endpoint.resolve("myaccount").unwrap();
        assert_eq!(url.as_str(), "https://myaccount.snowflakecomputing.com/");
    }

    #[test]
    fn endpoint_custom_base_url_resolves() {
        let base = Url::parse("https://custom.example.com").unwrap();
        let endpoint = SnowflakeEndpointConfig::custom_base_url(base);
        let url = endpoint.resolve("ignored").unwrap();
        assert_eq!(url.as_str(), "https://custom.example.com/");
    }
}
