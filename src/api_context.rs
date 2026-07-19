use http::Method;
use reqwest::{Url, header::USER_AGENT};

use crate::{Result, error::ConfigError};

pub(crate) const DEFAULT_USER_AGENT: &str =
    concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// The connector's authority to talk to one trusted Snowflake origin.
///
/// It represents what every Snowflake API request has in common regardless of endpoint: the origin it is addressed
/// to and the connector it originates from.
pub(crate) struct ApiContext {
    http: reqwest::Client,
    base_url: Url,
}

impl ApiContext {
    pub(crate) fn new(http: reqwest::Client, base_url: Url) -> Self {
        Self { http, base_url }
    }

    pub(crate) fn resolve(&self, relative: &str) -> Result<Url> {
        self.base_url
            .join(relative)
            .map_err(|e| ConfigError::invalid_url(e.to_string()).into())
    }

    pub(crate) fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.http
            .request(method, url)
            .header(USER_AGENT, DEFAULT_USER_AGENT)
    }

    pub(crate) fn base_url(&self) -> &Url {
        &self.base_url
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;

    fn context() -> ApiContext {
        ApiContext::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/").expect("test base URL must parse"),
        )
    }

    #[test]
    fn resolve_joins_relative_endpoint_against_base_url() {
        let url = context().resolve("session/v1/login-request").unwrap();
        assert_eq!(url.as_str(), "https://example.com/session/v1/login-request");
    }

    #[test]
    fn resolve_invalid_url_is_config_error() {
        // An empty host makes the join fail; it must surface as the same config error the endpoints used before.
        let err = context().resolve("https://").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Config);
    }

    #[test]
    fn request_carries_connector_user_agent() {
        let ctx = context();
        let url = ctx.resolve("queries/v1/query-request").unwrap();
        let request = ctx.request(Method::POST, url).build().unwrap();
        assert_eq!(
            request.headers().get(USER_AGENT).unwrap(),
            DEFAULT_USER_AGENT
        );
    }

    #[test]
    fn request_does_not_attach_endpoint_headers() {
        let ctx = context();
        let url = ctx.resolve("queries/v1/query-request").unwrap();
        let request = ctx.request(Method::POST, url).build().unwrap();
        assert!(request.headers().get(reqwest::header::ACCEPT).is_none());
        assert!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .is_none()
        );
        assert!(
            request
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .is_none()
        );
    }
}
