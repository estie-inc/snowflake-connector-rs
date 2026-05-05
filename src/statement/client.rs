use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use http::header::{ACCEPT, AUTHORIZATION};
use reqwest::{Client, Url};
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    Error, Result,
    error::{
        ConfigError, NetworkError, ProtocolError, QueryScopedError, QueryScopedResult,
        TimeoutError, classify_request_error,
    },
    query::QueryRequest,
};

use super::response::{
    QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE, SnowflakeResponse, parse_response,
};

pub(crate) struct StatementApiClient {
    http: Client,
    base_url: Url,
    session_token: String,
}

impl StatementApiClient {
    pub(crate) fn new(http: Client, base_url: Url, session_token: String) -> Self {
        Self {
            http,
            base_url,
            session_token,
        }
    }

    pub(crate) fn http_client(&self) -> Client {
        self.http.clone()
    }

    pub(crate) async fn submit(&self, request: &QueryRequest) -> Result<SnowflakeResponse> {
        let request_id = Uuid::new_v4();
        let mut url = self
            .base_url
            .join("queries/v1/query-request")
            .map_err(|e| ConfigError::invalid_url(e.to_string()))?;
        url.query_pairs_mut()
            .append_pair("requestId", &request_id.to_string());

        let response = self
            .http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, self.session_token),
            )
            .json(request)
            .send()
            .await
            .map_err(classify_request_error)?;

        let status = response.status();
        let body = response.bytes().await.map_err(classify_request_error)?;
        if !status.is_success() {
            return Err(NetworkError::http_status(status.as_u16(), &body).into());
        }

        parse_response(body).map_err(Error::from)
    }

    pub(crate) async fn poll_async_results(
        &self,
        poll_relative_url: &str,
        timeout: Duration,
        query_id: Arc<str>,
    ) -> QueryScopedResult<SnowflakeResponse> {
        const POLL_INTERVAL: Duration = Duration::from_secs(10);

        let poll_url = match resolve_poll_url(&self.base_url, poll_relative_url) {
            Ok(url) => url,
            Err(err) => {
                return Err(QueryScopedError::new(query_id, err));
            }
        };
        let deadline = Instant::now() + timeout;

        loop {
            let resp = self
                .http
                .get(poll_url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(
                    AUTHORIZATION,
                    format!(r#"Snowflake Token="{}""#, self.session_token),
                )
                .send()
                .await;

            let resp = match resp {
                Ok(resp) => resp,
                Err(error) => {
                    if error.is_timeout() {
                        return Err(QueryScopedError::new(
                            query_id,
                            TimeoutError::request(error),
                        ));
                    }
                    return Err(QueryScopedError::new(query_id, NetworkError::Http(error)));
                }
            };

            let status = resp.status();
            let body = match resp.bytes().await {
                Ok(body) => body,
                Err(error) => {
                    if error.is_timeout() {
                        return Err(QueryScopedError::new(
                            query_id,
                            TimeoutError::request(error),
                        ));
                    }
                    return Err(QueryScopedError::new(query_id, NetworkError::Http(error)));
                }
            };
            if !status.is_success() {
                return Err(QueryScopedError::new(
                    query_id,
                    NetworkError::http_status(status.as_u16(), &body),
                ));
            }

            match parse_response(body) {
                Ok(response)
                    if response.code.as_deref() != Some(QUERY_IN_PROGRESS_ASYNC_CODE)
                        && response.code.as_deref() != Some(QUERY_IN_PROGRESS_CODE) =>
                {
                    return Ok(response);
                }
                Ok(_) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(QueryScopedError::new(query_id, TimeoutError::query()));
                    }
                    sleep(remaining.min(POLL_INTERVAL)).await;
                }
                Err(err) => return Err(QueryScopedError::new(query_id, err)),
            }
        }
    }
}

fn resolve_poll_url(
    base_url: &Url,
    poll_relative_url: &str,
) -> std::result::Result<Url, ProtocolError> {
    const FIELD: &str = "data.getResultUrl";

    if poll_relative_url.trim().is_empty() {
        return Err(ProtocolError::invalid_field(FIELD, "must not be empty"));
    }

    if let Ok(url) = Url::parse(poll_relative_url) {
        validate_same_origin_absolute_url(base_url, &url, FIELD)?;
        return Ok(url);
    }

    let url = base_url
        .join(poll_relative_url)
        .map_err(|e| ProtocolError::invalid_response_url(FIELD, poll_relative_url, e))?;

    validate_same_origin_absolute_url(base_url, &url, FIELD)?;
    Ok(url)
}

fn validate_same_origin_absolute_url(
    base_url: &Url,
    url: &Url,
    field: &'static str,
) -> std::result::Result<(), ProtocolError> {
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ProtocolError::invalid_field(
            field,
            "must not contain credentials",
        ));
    }

    let same_origin = url.scheme() == base_url.scheme()
        && url.host_str() == base_url.host_str()
        && url.port_or_known_default() == base_url.port_or_known_default();

    if !same_origin {
        return Err(ProtocolError::invalid_field(
            field,
            "must be relative or same-origin absolute URL",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::TcpListener;

    use super::*;
    use crate::ErrorKind;

    #[test]
    fn resolve_poll_url_accepts_relative_and_same_origin_absolute_urls() {
        let base_url = Url::parse("https://example.com/").unwrap();

        let relative =
            resolve_poll_url(&base_url, "/queries/v1/query-request?requestId=abc").unwrap();
        assert_eq!(
            relative.as_str(),
            "https://example.com/queries/v1/query-request?requestId=abc"
        );

        let absolute = resolve_poll_url(
            &base_url,
            "https://example.com/queries/v1/query-request?requestId=def",
        )
        .unwrap();
        assert_eq!(
            absolute.as_str(),
            "https://example.com/queries/v1/query-request?requestId=def"
        );
    }

    #[test]
    fn resolve_poll_url_rejects_cross_origin_and_credentialed_absolute_urls() {
        let base_url = Url::parse("https://example.com/").unwrap();

        let err: Error = resolve_poll_url(&base_url, "https://attacker.example/query")
            .expect_err("cross-origin poll URL must fail")
            .into();
        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "invalid Snowflake response field data.getResultUrl: must be relative or same-origin absolute URL"
        );

        let err: Error = resolve_poll_url(&base_url, "https://user:pass@example.com/query")
            .expect_err("credentialed poll URL must fail")
            .into();
        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "invalid Snowflake response field data.getResultUrl: must not contain credentials"
        );
    }

    #[tokio::test]
    async fn submit_timeout_is_timeout_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let client = StatementApiClient::new(
            reqwest::Client::builder()
                .timeout(Duration::from_millis(50))
                .build()
                .unwrap(),
            Url::parse(&format!("http://{addr}/")).unwrap(),
            "test-token".to_string(),
        );

        let err = client
            .submit(&crate::query::QueryRequest::from("select 1"))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(err.is_timeout());

        server.abort();
        let _ = server.await;
    }
}
