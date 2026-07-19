use std::{fmt, sync::Arc};

use crate::{
    ApiContext, ClientConfig, ClientLoginConfig, QueryExecutionPolicy, Result, Session,
    auth::login, runtime::QueryRuntime, session::SessionAuth,
};

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
    pub(crate) api: Arc<ApiContext>,
    pub(crate) chunk_http: reqwest::Client,
    pub(crate) query: QueryExecutionPolicy,
    pub(crate) runtime: QueryRuntime,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Prints only non-secret identity fields; credentials, the reqwest client, and the runtime are omitted.
        f.debug_struct("Client")
            .field("base_url", &self.inner.shared.api.base_url())
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

        let http = prepared.shared.http;
        let shared = Arc::new(ClientShared {
            api: Arc::new(ApiContext::new(http.clone(), prepared.shared.base_url)),
            chunk_http: http,
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
    /// Returns `ErrorKind::Config`, `ErrorKind::Auth`, `ErrorKind::Network`, `ErrorKind::Timeout`, `ErrorKind::Protocol`, or
    /// `ErrorKind::Internal` depending on how session establishment fails.
    pub async fn create_session(&self) -> Result<Session> {
        let session_token = login(&self.inner.login, Arc::clone(&self.inner.shared.api)).await?;
        Ok(Session {
            shared: Arc::clone(&self.inner.shared),
            auth: Arc::new(SessionAuth { session_token }),
        })
    }
}

#[cfg(test)]
pub(crate) use test_support::ClientSharedPartial;

#[cfg(test)]
mod test_support {
    use url::Url;

    use crate::QueryConfig;

    use super::*;

    pub(crate) struct ClientSharedPartial {
        http: reqwest::Client,
        base_url: Url,
        query: QueryExecutionPolicy,
        runtime: QueryRuntime,
    }

    impl ClientSharedPartial {
        pub(crate) fn new() -> Self {
            Self {
                http: reqwest::Client::new(),
                base_url: Url::parse("https://example.com/").expect("test base URL must be valid"),
                query: QueryConfig::new().into(),
                runtime: QueryRuntime::new(),
            }
        }

        pub(crate) fn with_http(mut self, http: reqwest::Client) -> Self {
            self.http = http;
            self
        }

        pub(crate) fn with_base_url(mut self, base_url: Url) -> Self {
            self.base_url = base_url;
            self
        }

        pub(crate) fn with_query(mut self, query: QueryExecutionPolicy) -> Self {
            self.query = query;
            self
        }

        pub(crate) fn with_runtime(mut self, runtime: QueryRuntime) -> Self {
            self.runtime = runtime;
            self
        }

        pub(crate) fn build(self) -> Arc<ClientShared> {
            Arc::new(ClientShared {
                api: Arc::new(ApiContext::new(self.http.clone(), self.base_url)),
                chunk_http: self.http,
                query: self.query,
                runtime: self.runtime,
            })
        }
    }
}
