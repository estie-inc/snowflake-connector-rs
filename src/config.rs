use std::{collections::HashMap, fmt, num::NonZeroUsize, time::Duration};

use url::Url;

use crate::{AuthConfig, Result, error::ConfigError};

/// Top-level configuration for a [`Client`](crate::Client).
#[derive(Clone, Debug)]
pub struct ClientConfig {
    username: String,
    account: String,
    auth: AuthConfig,
    session: SessionConfig,
    query: QueryConfig,
    endpoint: EndpointConfig,
    transport: TransportConfig,
}

/// Server-side session context sent to Snowflake at login time.
///
/// These values are passed as query parameters in the login request and
/// determine the initial state of the Snowflake session (active warehouse,
/// database, schema, and role). They correspond directly to Snowflake's
/// session-level settings and do not affect client-side behavior.
#[derive(Default, Clone, Debug)]
pub struct SessionConfig {
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    session_parameters: HashMap<String, serde_json::Value>,
}

const DEFAULT_COLLECT_PREFETCH_CONCURRENCY: usize = 8;

/// Client-side query execution policy.
///
/// Controls how this connector behaves while executing queries — for example,
/// how long to poll for the completion of an async query. These settings are
/// enforced entirely on the client side and are never sent to Snowflake.
#[derive(Clone, Debug)]
pub struct QueryConfig {
    async_query_completion_timeout: Option<Duration>,
    collect_prefetch_concurrency: NonZeroUsize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            async_query_completion_timeout: None,
            collect_prefetch_concurrency: NonZeroUsize::new(DEFAULT_COLLECT_PREFETCH_CONCURRENCY)
                .expect("default concurrency is non-zero"),
        }
    }
}

/// Endpoint resolution strategy for the Snowflake API base URL.
///
/// By default the base URL is derived from the account name
/// (`https://<account>.snowflakecomputing.com`). Use
/// [`CustomBaseUrl`](Self::CustomBaseUrl) to override this — for example,
/// when connecting through a PrivateLink endpoint or a local test server.
#[non_exhaustive]
#[derive(Default, Clone, Debug)]
pub enum EndpointConfig {
    #[default]
    AccountDefault,
    CustomBaseUrl(Url),
}

/// HTTP transport-layer options.
///
/// Configures how requests are physically delivered to Snowflake,
/// independent of which endpoint they target.
#[derive(Default, Clone, Debug)]
pub struct TransportConfig {
    proxy: Option<ProxyConfig>,
}

/// Configuration for an HTTP proxy used by [`TransportConfig`].
///
/// Specifies the proxy URL and optional authentication credentials.
/// Only HTTP and HTTPS proxy schemes are accepted.
#[derive(Clone, Debug)]
pub struct ProxyConfig {
    url: Url,
    auth: ProxyAuth,
}

#[derive(Clone)]
pub(crate) enum ProxyAuth {
    None,
    Basic { username: String, password: String },
}

impl fmt::Debug for ProxyAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"<redacted>")
                .finish(),
        }
    }
}

impl ClientConfig {
    pub fn new(username: impl Into<String>, account: impl Into<String>, auth: AuthConfig) -> Self {
        Self {
            username: username.into(),
            account: account.into(),
            auth,
            session: SessionConfig::default(),
            query: QueryConfig::default(),
            endpoint: EndpointConfig::default(),
            transport: TransportConfig::default(),
        }
    }

    pub fn with_session(mut self, session: SessionConfig) -> Self {
        self.session = session;
        self
    }

    pub fn with_query(mut self, query: QueryConfig) -> Self {
        self.query = query;
        self
    }

    pub fn with_endpoint(mut self, endpoint: EndpointConfig) -> Self {
        self.endpoint = endpoint;
        self
    }

    pub fn with_transport(mut self, transport: TransportConfig) -> Self {
        self.transport = transport;
        self
    }

    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn account(&self) -> &str {
        &self.account
    }

    pub(crate) fn auth(&self) -> &AuthConfig {
        &self.auth
    }

    pub(crate) fn session(&self) -> &SessionConfig {
        &self.session
    }

    pub(crate) fn query(&self) -> &QueryConfig {
        &self.query
    }

    pub(crate) fn endpoint(&self) -> &EndpointConfig {
        &self.endpoint
    }

    pub(crate) fn transport(&self) -> &TransportConfig {
        &self.transport
    }
}

impl SessionConfig {
    pub(crate) fn warehouse(&self) -> Option<&str> {
        self.warehouse.as_deref()
    }

    pub(crate) fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub(crate) fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub(crate) fn role(&self) -> Option<&str> {
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

    pub(crate) fn session_parameters(&self) -> &HashMap<String, serde_json::Value> {
        &self.session_parameters
    }

    pub fn with_session_parameters(mut self, params: HashMap<String, serde_json::Value>) -> Self {
        self.session_parameters = params;
        self
    }

    pub fn with_session_parameter(
        mut self,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Self {
        self.session_parameters.insert(key.into(), value);
        self
    }
}

impl QueryConfig {
    pub(crate) fn async_query_completion_timeout(&self) -> Option<Duration> {
        self.async_query_completion_timeout
    }

    pub(crate) fn collect_prefetch_concurrency(&self) -> NonZeroUsize {
        self.collect_prefetch_concurrency
    }

    pub fn with_async_query_completion_timeout(mut self, timeout: Duration) -> Self {
        self.async_query_completion_timeout = Some(timeout);
        self
    }

    /// Sets the default number of partitions fetched concurrently during collection. Defaults to `8`.
    pub fn with_collect_prefetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.collect_prefetch_concurrency = concurrency;
        self
    }
}

impl EndpointConfig {
    pub fn custom_base_url(url: Url) -> Self {
        Self::CustomBaseUrl(url)
    }

    pub(crate) fn resolve(&self, account: &str) -> Result<Url> {
        match self {
            Self::AccountDefault => Ok(Url::parse(&format!(
                "https://{account}.snowflakecomputing.com"
            ))
            .map_err(|e| ConfigError::invalid_url(e.to_string()))?),
            Self::CustomBaseUrl(url) => validate_custom_base_url(url.clone()),
        }
    }
}

const ALLOWED_ENDPOINT_SCHEMES: &[&str] = &["http", "https"];

fn validate_custom_base_url(mut url: Url) -> Result<Url> {
    if !ALLOWED_ENDPOINT_SCHEMES.contains(&url.scheme()) {
        return Err(ConfigError::invalid_url(format!(
            "unsupported custom base URL scheme '{}'; allowed: {}",
            url.scheme(),
            ALLOWED_ENDPOINT_SCHEMES.join(", "),
        ))
        .into());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(
            ConfigError::invalid_url("custom base URL must not contain query or fragment").into(),
        );
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(
            ConfigError::invalid_url("custom base URL must not contain credentials").into(),
        );
    }
    if url.path() != "/" && !url.path().is_empty() {
        return Err(ConfigError::invalid_url("custom base URL must not contain a path").into());
    }
    url.set_path("/");
    Ok(url)
}

impl TransportConfig {
    pub fn with_proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    pub(crate) fn build_http_client(&self) -> Result<reqwest::Client> {
        let builder = reqwest::ClientBuilder::new().gzip(true).use_rustls_tls();

        // Disable idle connection pooling to prevent stale-connection errors.
        //
        // This client talks to both the Snowflake REST API and S3 (for
        // downloading query-result chunks via presigned URLs). S3 closes
        // idle keep-alive connections aggressively (the exact timeout is
        // undocumented, but reported to be only a few seconds). When the
        // pool hands out a connection that S3 has already closed, hyper
        // returns `Error(IncompleteMessage)`.
        //
        // Disabling pooling is acceptable here because the bottleneck is
        // data transfer with Snowflake / S3, not establishing TCP
        // connections.
        let builder = builder.pool_max_idle_per_host(0);

        let builder = if let Some(proxy) = &self.proxy {
            builder.proxy(proxy.to_reqwest_proxy()?)
        } else {
            builder
        };

        Ok(builder
            .build()
            .map_err(ConfigError::client_builder_failure)?)
    }
}

impl ProxyConfig {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            auth: ProxyAuth::None,
        }
    }

    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = ProxyAuth::Basic {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    pub(crate) fn to_reqwest_proxy(&self) -> Result<reqwest::Proxy> {
        let url = validate_proxy_url(self.url.clone())?;
        let mut proxy =
            reqwest::Proxy::all(url.as_str()).map_err(ConfigError::client_builder_failure)?;

        if let ProxyAuth::Basic { username, password } = &self.auth {
            proxy = proxy.basic_auth(username, password);
        }

        Ok(proxy)
    }
}

const ALLOWED_PROXY_SCHEMES: &[&str] = &["http", "https"];

fn validate_proxy_url(url: Url) -> Result<Url> {
    if !ALLOWED_PROXY_SCHEMES.contains(&url.scheme()) {
        return Err(ConfigError::invalid_url(format!(
            "unsupported proxy URL scheme '{}'; allowed: {}",
            url.scheme(),
            ALLOWED_PROXY_SCHEMES.join(", "),
        ))
        .into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ConfigError::invalid_url(
            "proxy URL must not contain credentials; use ProxyConfig::with_basic_auth() instead",
        )
        .into());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(
            ConfigError::invalid_url("proxy URL must not contain query or fragment").into(),
        );
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_debug_redacts_basic_auth_password() {
        let proxy = ProxyConfig::new(Url::parse("http://proxy.example.com:8080").unwrap())
            .with_basic_auth("proxy_user", "s3cr3t-proxy-pass");
        let rendered = format!("{proxy:?}");

        assert!(
            rendered.contains("proxy_user"),
            "username should be visible: {rendered}"
        );
        assert!(
            !rendered.contains("s3cr3t-proxy-pass"),
            "password leaked into Debug: {rendered}"
        );
        assert!(
            rendered.contains("<redacted>"),
            "expected redaction marker: {rendered}"
        );
    }

    #[test]
    fn proxy_urls_with_supported_schemes_build_successfully() {
        for proxy in [
            ProxyConfig::new(Url::parse("http://proxy.example.com:8080").unwrap()),
            ProxyConfig::new(Url::parse("https://proxy.example.com:8080").unwrap()),
            ProxyConfig::new(Url::parse("http://proxy.example.com:8080").unwrap())
                .with_basic_auth("user", "pass"),
        ] {
            assert!(proxy.to_reqwest_proxy().is_ok());
        }
    }

    fn assert_proxy_rejected(url: &str, expected: &str) {
        let proxy = ProxyConfig::new(Url::parse(url).unwrap());
        let err = proxy.to_reqwest_proxy().unwrap_err();
        assert!(
            format!("{err}").contains(expected),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn proxy_urls_with_unsupported_schemes_are_rejected() {
        for url in ["socks5://proxy.example.com:1080", "ftp://proxy.example.com"] {
            assert_proxy_rejected(url, "unsupported proxy URL scheme");
        }
    }

    #[test]
    fn proxy_url_with_credentials_is_rejected() {
        assert_proxy_rejected(
            "http://user:pass@proxy.example.com:8080",
            "must not contain credentials",
        );
    }

    #[test]
    fn no_proxy_builds_client_successfully() {
        let transport = TransportConfig::default();
        assert!(transport.build_http_client().is_ok());
    }

    #[test]
    fn proxy_url_with_query_is_rejected() {
        assert_proxy_rejected(
            "http://proxy.example.com:8080?foo=bar",
            "must not contain query or fragment",
        );
    }

    #[test]
    fn custom_base_urls_with_supported_schemes_are_accepted_and_normalized() {
        for (url, expected) in [
            (
                "https://custom.snowflake.example.com",
                "https://custom.snowflake.example.com/",
            ),
            ("http://localhost:8080", "http://localhost:8080/"),
            (
                "https://snowflake.example.com",
                "https://snowflake.example.com/",
            ),
        ] {
            let result = validate_custom_base_url(Url::parse(url).unwrap()).unwrap();
            assert_eq!(result.as_str(), expected);
            assert_eq!(result.path(), "/");
        }
    }

    fn assert_custom_base_url_rejected(url: &str, expected: &str) {
        let err = validate_custom_base_url(Url::parse(url).unwrap()).unwrap_err();
        assert!(
            format!("{err}").contains(expected),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn custom_base_urls_with_unsupported_schemes_are_rejected() {
        for url in ["ftp://snowflake.example.com", "ws://snowflake.example.com"] {
            assert_custom_base_url_rejected(url, "unsupported custom base URL scheme");
        }
    }

    #[test]
    fn custom_base_urls_with_query_or_fragment_are_rejected() {
        for url in [
            "https://snowflake.example.com?foo=bar",
            "https://snowflake.example.com#section",
        ] {
            assert_custom_base_url_rejected(url, "must not contain query or fragment");
        }
    }

    #[test]
    fn custom_base_url_with_credentials_is_rejected() {
        assert_custom_base_url_rejected(
            "https://user:pass@snowflake.example.com",
            "must not contain credentials",
        );
    }

    #[test]
    fn custom_base_url_with_path_is_rejected() {
        assert_custom_base_url_rejected(
            "https://snowflake.example.com/some/path",
            "must not contain a path",
        );
    }

    #[test]
    fn endpoint_account_default_resolves() {
        let endpoint = EndpointConfig::AccountDefault;
        let url = endpoint.resolve("myaccount").unwrap();
        assert_eq!(url.as_str(), "https://myaccount.snowflakecomputing.com/");
    }

    #[test]
    fn endpoint_custom_base_url_resolves() {
        let base = Url::parse("https://custom.example.com").unwrap();
        let endpoint = EndpointConfig::custom_base_url(base);
        let url = endpoint.resolve("ignored").unwrap();
        assert_eq!(url.as_str(), "https://custom.example.com/");
    }
}
