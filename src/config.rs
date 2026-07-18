use std::{collections::HashMap, fmt, num::NonZeroUsize, time::Duration};

use url::Url;

use crate::{AuthConfig, Result, error::ConfigError, session::QueryOptions};

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
/// These values are passed as query parameters in the login request and determine the initial state of the
/// Snowflake session (active warehouse, database, schema, and role). They correspond directly to Snowflake's
/// session-level settings and do not affect client-side behavior.
#[derive(Default, Clone, Debug)]
pub struct SessionConfig {
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    session_parameters: HashMap<String, serde_json::Value>,
}

pub(crate) const DEFAULT_QUERY_RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);
pub(crate) const DEFAULT_QUERY_CANCEL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_COLLECT_PREFETCH_CONCURRENCY: NonZeroUsize =
    NonZeroUsize::new(8).expect("default concurrency is non-zero");

/// Client-side query execution policy.
///
/// Controls how this connector behaves while executing queries — for example, how long to wait for Snowflake to
/// return a query response. These settings are enforced entirely on the client side and are never sent to Snowflake.
#[derive(Clone, Debug)]
pub struct QueryConfig {
    query_response_timeout: Duration,
    query_cancel_request_timeout: Duration,
    collect_prefetch_concurrency: NonZeroUsize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            query_response_timeout: DEFAULT_QUERY_RESPONSE_TIMEOUT,
            query_cancel_request_timeout: DEFAULT_QUERY_CANCEL_REQUEST_TIMEOUT,
            collect_prefetch_concurrency: DEFAULT_COLLECT_PREFETCH_CONCURRENCY,
        }
    }
}

/// Endpoint resolution strategy for the Snowflake API base URL.
///
/// By default the base URL is derived from the account name (`https://<account>.snowflakecomputing.com`).
/// Use [`CustomBaseUrl`](Self::CustomBaseUrl) to override this — for example,
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
/// Configures how requests are physically delivered to Snowflake, independent of which endpoint they target.
#[derive(Default, Clone, Debug)]
pub struct TransportConfig {
    proxy: Option<ProxyConfig>,
}

/// Configuration for an HTTP proxy used by [`TransportConfig`].
///
/// Specifies the proxy URL and optional authentication credentials. Only HTTP and HTTPS proxy schemes are accepted.
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

    /// Compile this public builder input into the internal model held by `Client`.
    ///
    /// # Errors
    ///
    /// Returns `ErrorKind::Config` when endpoint or transport configuration is invalid.
    pub(crate) fn prepare(self) -> Result<PreparedClientConfig> {
        let base_url = self.endpoint.resolve(&self.account)?;
        let http = self.transport.build_http_client()?;

        Ok(PreparedClientConfig {
            login: ClientLoginConfig {
                username: self.username,
                account: self.account,
                auth: self.auth,
                initial_session: self.session.into(),
            },
            shared: PreparedClientShared {
                http,
                base_url,
                query: self.query.into(),
            },
        })
    }
}

impl SessionConfig {
    pub fn new() -> Self {
        Self::default()
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

    pub fn with_session_parameters(
        mut self,
        session_parameters: HashMap<String, serde_json::Value>,
    ) -> Self {
        self.session_parameters = session_parameters;
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the client-side timeout for obtaining a query response from Snowflake.
    ///
    /// This bounds how long [`Session::query()`](crate::Session::query) /
    /// [`Session::query_as()`](crate::Session::query_as) wait for Snowflake to return a query response and for this
    /// connector to build a [`ResultCursor`](crate::ResultCursor). It is not specific to async queries: it spans the
    /// initial statement submit and, when Snowflake responds asynchronously, the subsequent result polling as a single
    /// budget that is never reset once polling begins. Defaults to `300s`.
    ///
    /// This is a client-side timeout only. It does not cancel the query on Snowflake, so the statement may keep
    /// running server-side after the deadline elapses. It also does not cover chunk download or row collection
    /// performed through the returned `ResultCursor`.
    ///
    /// Snowflake can block the initial submit request for up to roughly 45 seconds before responding. Until that
    /// response arrives the connector does not know the query id, so a timeout during submit yields an error whose
    /// [`Error::query_id()`](crate::Error::query_id) is `None`. To reliably recover the query id from a timeout error,
    /// set this to more than `45s` plus network/server buffer.
    pub fn with_query_response_timeout(mut self, timeout: Duration) -> Self {
        self.query_response_timeout = timeout;
        self
    }

    /// Sets the client-side deadline for an explicit query cancellation request. Defaults to `30s`.
    ///
    /// The deadline covers abort request transport, response body reading, and bounded transport retries. It is
    /// independent of [`Self::with_query_response_timeout`].
    pub fn with_query_cancel_request_timeout(mut self, timeout: Duration) -> Self {
        self.query_cancel_request_timeout = timeout;
        self
    }

    /// Sets the default number of partitions fetched concurrently during collection. Defaults to `8`.
    pub fn with_collect_prefetch_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.collect_prefetch_concurrency = concurrency;
        self
    }
}

// Internal runtime config models: the prepared form of the public config types above, produced by `ClientConfig::prepare`.

/// Private intermediate produced by consuming a [`ClientConfig`].
pub(crate) struct PreparedClientConfig {
    pub(crate) login: ClientLoginConfig,
    pub(crate) shared: PreparedClientShared,
}

/// Prepared inputs for the connector-wide shared state.
pub(crate) struct PreparedClientShared {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: Url,
    pub(crate) query: QueryExecutionPolicy,
}

/// Login state retained by `Client` for every `create_session()` call.
pub(crate) struct ClientLoginConfig {
    username: String,
    account: String,
    auth: AuthConfig,
    initial_session: InitialSessionConfig,
}

impl ClientLoginConfig {
    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    pub(crate) fn account(&self) -> &str {
        &self.account
    }

    pub(crate) fn auth(&self) -> &AuthConfig {
        &self.auth
    }

    pub(crate) fn initial_session(&self) -> &InitialSessionConfig {
        &self.initial_session
    }
}

/// Internal login-request session context, the runtime form of [`SessionConfig`].
pub(crate) struct InitialSessionConfig {
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
    role: Option<String>,
    session_parameters: HashMap<String, serde_json::Value>,
}

impl InitialSessionConfig {
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

    pub(crate) fn session_parameters(&self) -> &HashMap<String, serde_json::Value> {
        &self.session_parameters
    }
}

impl From<SessionConfig> for InitialSessionConfig {
    fn from(config: SessionConfig) -> Self {
        Self {
            warehouse: config.warehouse,
            database: config.database,
            schema: config.schema,
            role: config.role,
            session_parameters: config.session_parameters,
        }
    }
}

/// Internal query execution policy, the runtime form of [`QueryConfig`].
#[derive(Debug)]
pub(crate) struct QueryExecutionPolicy {
    query_response_timeout: Duration,
    query_cancel_request_timeout: Duration,
    collect_prefetch_concurrency: NonZeroUsize,
}

impl QueryExecutionPolicy {
    /// Resolve per-query overrides against these prepared defaults into the concrete settings for one execution.
    pub(crate) fn resolve_options(&self, options: QueryOptions) -> QueryExecutionSettings {
        QueryExecutionSettings {
            query_response_timeout: options
                .query_response_timeout
                .unwrap_or(self.query_response_timeout),
            query_cancel_request_timeout: options
                .query_cancel_request_timeout
                .unwrap_or(self.query_cancel_request_timeout),
            collect_prefetch_concurrency: options
                .collect_prefetch_concurrency
                .unwrap_or(self.collect_prefetch_concurrency),
        }
    }
}

/// Internal concrete settings for a single statement execution, produced by [`QueryExecutionPolicy::resolve_options`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryExecutionSettings {
    pub(crate) query_response_timeout: Duration,
    pub(crate) query_cancel_request_timeout: Duration,
    pub(crate) collect_prefetch_concurrency: NonZeroUsize,
}

impl From<QueryConfig> for QueryExecutionPolicy {
    fn from(config: QueryConfig) -> Self {
        Self {
            query_response_timeout: config.query_response_timeout,
            query_cancel_request_timeout: config.query_cancel_request_timeout,
            collect_prefetch_concurrency: config.collect_prefetch_concurrency,
        }
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
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_proxy(mut self, proxy: ProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    pub(crate) fn build_http_client(&self) -> Result<reqwest::Client> {
        let builder = reqwest::ClientBuilder::new().gzip(true).use_rustls_tls();

        // Disable idle connection pooling: S3 (used for query-result chunks via presigned URLs) closes idle keep-alive
        // connections aggressively, and a reused-but-closed connection surfaces as hyper `Error(IncompleteMessage)`.
        // Acceptable because the bottleneck is data transfer, not TCP setup.
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
    fn query_config_defaults_to_300s_query_response_timeout() {
        let policy: QueryExecutionPolicy = QueryConfig::default().into();
        let settings = policy.resolve_options(QueryOptions::default());
        assert_eq!(settings.query_response_timeout, Duration::from_secs(300));
        assert_eq!(
            settings.query_cancel_request_timeout,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn with_query_response_timeout_overrides_the_default() {
        let policy: QueryExecutionPolicy = QueryConfig::default()
            .with_query_response_timeout(Duration::from_secs(60))
            .into();
        let settings = policy.resolve_options(QueryOptions::default());
        assert_eq!(settings.query_response_timeout, Duration::from_secs(60));
    }

    #[test]
    fn query_options_default_inherits_query_config_defaults() {
        let policy: QueryExecutionPolicy = QueryConfig::default()
            .with_query_response_timeout(Duration::from_secs(120))
            .with_collect_prefetch_concurrency(NonZeroUsize::new(4).unwrap())
            .into();

        let settings = policy.resolve_options(QueryOptions::default());

        assert_eq!(settings.query_response_timeout, Duration::from_secs(120));
        assert_eq!(
            settings.collect_prefetch_concurrency,
            NonZeroUsize::new(4).unwrap()
        );
    }

    #[test]
    fn query_options_with_query_response_timeout_overrides_only_timeout() {
        let policy: QueryExecutionPolicy = QueryConfig::default()
            .with_collect_prefetch_concurrency(NonZeroUsize::new(4).unwrap())
            .into();

        let settings = policy.resolve_options(
            QueryOptions::default().with_query_response_timeout(Duration::from_secs(90)),
        );

        assert_eq!(settings.query_response_timeout, Duration::from_secs(90));
        assert_eq!(
            settings.collect_prefetch_concurrency,
            NonZeroUsize::new(4).unwrap()
        );
    }

    #[test]
    fn query_options_with_query_cancel_request_timeout_overrides_only_cancel_timeout() {
        let policy: QueryExecutionPolicy = QueryConfig::default()
            .with_query_response_timeout(Duration::from_secs(120))
            .with_collect_prefetch_concurrency(NonZeroUsize::new(4).unwrap())
            .into();
        let settings = policy.resolve_options(
            QueryOptions::default().with_query_cancel_request_timeout(Duration::from_secs(7)),
        );

        assert_eq!(settings.query_response_timeout, Duration::from_secs(120));
        assert_eq!(
            settings.query_cancel_request_timeout,
            Duration::from_secs(7)
        );
        assert_eq!(
            settings.collect_prefetch_concurrency,
            NonZeroUsize::new(4).unwrap()
        );
    }

    #[test]
    fn query_options_with_collect_prefetch_concurrency_overrides_only_concurrency() {
        let policy: QueryExecutionPolicy = QueryConfig::default()
            .with_query_response_timeout(Duration::from_secs(120))
            .into();

        let settings = policy.resolve_options(
            QueryOptions::default()
                .with_collect_prefetch_concurrency(NonZeroUsize::new(2).unwrap()),
        );

        assert_eq!(settings.query_response_timeout, Duration::from_secs(120));
        assert_eq!(
            settings.collect_prefetch_concurrency,
            NonZeroUsize::new(2).unwrap()
        );
    }

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
