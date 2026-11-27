use std::{sync::Arc, time::Duration};

use http::{
    Method,
    header::{ACCEPT, AUTHORIZATION},
};
use reqwest::Url;
use tokio::time::{sleep, timeout};

use crate::{
    error::{
        NetworkError, ProtocolError, QueryScopedError, QueryScopedResult, SessionExpiredError,
        TimeoutError,
    },
    statement::wire::response::{
        QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE, SESSION_EXPIRED, WireQueryResponse,
        parse_query_response,
    },
};

use super::{QueryApiClient, QueryResponseDeadline};

impl QueryApiClient {
    pub(crate) async fn poll_async_results(
        &self,
        poll_relative_url: &str,
        deadline: QueryResponseDeadline,
        query_id: Arc<str>,
    ) -> QueryScopedResult<WireQueryResponse> {
        let poll_url = match resolve_poll_url(self.api.base_url(), poll_relative_url) {
            Ok(url) => url,
            Err(err) => {
                return Err(QueryScopedError::new(query_id, err));
            }
        };
        let mut backoff = PollBackoff::new();
        let mut resent_after_renewal = false;

        loop {
            // The poll endpoint is a server-side long-poll, so apply the remaining query-response budget to the whole
            // GET instead of only checking the deadline between poll attempts.
            let remaining = match deadline.remaining_or_timeout() {
                Ok(remaining) => remaining,
                Err(err) => return Err(QueryScopedError::new(query_id, err)),
            };

            let tokens = self.auth.snapshot();
            let request = self
                .api
                .request(Method::GET, poll_url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(AUTHORIZATION, tokens.authorization());

            let response = match timeout(remaining, async {
                let resp = request.send().await?;
                let status = resp.status();
                let body = resp.bytes().await?;
                Ok::<_, reqwest::Error>((status, body))
            })
            .await
            {
                Ok(Ok(pair)) => pair,
                Ok(Err(error)) => {
                    if error.is_timeout() {
                        return Err(QueryScopedError::new(
                            query_id,
                            TimeoutError::request(error),
                        ));
                    }
                    return Err(QueryScopedError::new(query_id, NetworkError::Http(error)));
                }
                Err(_elapsed) => {
                    return Err(QueryScopedError::new(query_id, TimeoutError::query()));
                }
            };

            let (status, body) = response;
            if !status.is_success() {
                return Err(QueryScopedError::new(
                    query_id,
                    NetworkError::http_status(status.as_u16(), &body),
                ));
            }

            match parse_query_response(body) {
                Ok(response)
                    if response.code.as_deref() == Some(SESSION_EXPIRED)
                        && !resent_after_renewal =>
                {
                    if let Err(error) = self
                        .auth
                        .renew(&self.api, &tokens, deadline.remaining())
                        .await
                    {
                        let observed =
                            SessionExpiredError::new(response.code, response.message, None);
                        return Err(QueryScopedError::new(
                            query_id,
                            error.into_scoped(observed, TimeoutError::query()),
                        ));
                    }
                    resent_after_renewal = true;
                }
                Ok(response)
                    if response.code.as_deref() != Some(QUERY_IN_PROGRESS_ASYNC_CODE)
                        && response.code.as_deref() != Some(QUERY_IN_PROGRESS_CODE) =>
                {
                    return Ok(response);
                }
                Ok(_) => {
                    resent_after_renewal = false;
                    let remaining = deadline.remaining();
                    if remaining.is_zero() {
                        return Err(QueryScopedError::new(query_id, TimeoutError::query()));
                    }
                    sleep(remaining.min(backoff.next_delay())).await;
                }
                Err(err) => return Err(QueryScopedError::new(query_id, err)),
            }
        }
    }
}

/// Client-side backoff between consecutive poll GETs for an async query.
///
/// The poll endpoint is a server-side long-poll: each GET blocks until the query completes or the server-side
/// timeout elapses, so the client normally does not sleep at all. This backoff only takes effect when a GET
/// returns in-progress quickly, bounding how tightly the poll loop can spin in that case.
struct PollBackoff {
    step: usize,
}

impl PollBackoff {
    const STEPS: [Duration; 7] = [
        Duration::from_millis(500),
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_millis(1500),
        Duration::from_secs(2),
        Duration::from_secs(4),
        Duration::from_secs(5),
    ];

    fn new() -> Self {
        Self { step: 0 }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = Self::STEPS[self.step];
        self.step = (self.step + 1).min(Self::STEPS.len() - 1);
        delay
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
    use std::{sync::Arc, time::Duration};

    use http::StatusCode;
    use reqwest::Url;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        Error, ErrorKind,
        api_context::DEFAULT_USER_AGENT,
        statement::api::{
            QueryResponseDeadline,
            test_support::{
                QUERY_DONE_BODY, RENEWED_BODY, SESSION_EXPIRED_BODY, renewable_query_api,
                request_target, serve_script, test_query_api,
            },
        },
        test_support::http::{read_http_request, write_json_response},
    };

    #[tokio::test]
    async fn poll_get_carries_session_auth_and_user_agent() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await.unwrap();
            write_json_response(
                &mut socket,
                StatusCode::OK,
                r#"{"success":true,"data":{"queryId":"q","rowtype":[],"rowset":[],"queryResultFormat":"json"}}"#,
            )
            .await
            .unwrap();
            request
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let deadline = QueryResponseDeadline::new(Duration::from_secs(30));
        client
            .poll_async_results(
                "/queries/v1/query-request?requestId=abc",
                deadline,
                Arc::from("query-id"),
            )
            .await
            .unwrap();

        let request = server.await.unwrap();
        let request_line = request.lines().next().unwrap();
        assert!(
            request_line.starts_with("GET /queries/v1/query-request?requestId=abc HTTP/1.1"),
            "{request_line}"
        );
        let lowered = request.to_ascii_lowercase();
        assert!(lowered.contains("accept: application/snowflake"));
        assert!(lowered.contains(r#"authorization: snowflake token="test-token""#));
        assert!(lowered.contains(&format!(
            "user-agent: {}",
            DEFAULT_USER_AGENT.to_ascii_lowercase()
        )));
    }

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

    #[test]
    fn poll_backoff_steps_up_then_plateaus_at_five_seconds() {
        let mut backoff = PollBackoff::new();
        let observed = (0..9).map(|_| backoff.next_delay()).collect::<Vec<_>>();

        assert_eq!(
            observed,
            vec![
                Duration::from_millis(500),
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_millis(1500),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(5),
                // Plateaus at the 5s tail once the table is exhausted.
                Duration::from_secs(5),
                Duration::from_secs(5),
            ]
        );
    }

    const POLL_PATH: &str = "/queries/query-id/result";
    const IN_PROGRESS_BODY: &str = r#"{"success":true,"code":"333334","data":{"queryId":"query-id","getResultUrl":"/queries/query-id/result"}}"#;

    #[tokio::test]
    async fn poll_renews_expired_session_and_keeps_polling() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![SESSION_EXPIRED_BODY, RENEWED_BODY, QUERY_DONE_BODY],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let response = client
            .poll_async_results(
                POLL_PATH,
                QueryResponseDeadline::new(Duration::from_secs(30)),
                Arc::from("query-id"),
            )
            .await
            .unwrap();
        assert!(response.success);

        let (requests, quiet) = server.await.unwrap();
        assert!(quiet);
        assert_eq!(request_target(&requests[0]), POLL_PATH);
        assert!(requests[0].contains(r#"authorization: Snowflake Token="test-token""#));
        assert!(request_target(&requests[1]).starts_with("/session/token-request?"));
        assert_eq!(request_target(&requests[2]), POLL_PATH);
        assert!(requests[2].contains(r#"authorization: Snowflake Token="renewed-token""#));
    }

    #[tokio::test]
    async fn poll_renews_again_after_progress_was_observed() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![
                SESSION_EXPIRED_BODY,
                RENEWED_BODY,
                IN_PROGRESS_BODY,
                SESSION_EXPIRED_BODY,
                r#"{"success":true,"data":{"sessionToken":"renewed-again"}}"#,
                QUERY_DONE_BODY,
            ],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let response = client
            .poll_async_results(
                POLL_PATH,
                QueryResponseDeadline::new(Duration::from_secs(30)),
                Arc::from("query-id"),
            )
            .await
            .unwrap();
        assert!(response.success);

        let (requests, quiet) = server.await.unwrap();
        assert!(quiet);
        assert!(requests[4].contains(r#"authorization: Snowflake Token="renewed-master""#));
        assert!(requests[5].contains(r#"authorization: Snowflake Token="renewed-again""#));
    }

    #[tokio::test]
    async fn poll_rejected_renewal_is_session_expired_with_query_id() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![
                SESSION_EXPIRED_BODY,
                r#"{"success":false,"code":"390114","message":"master token expired"}"#,
            ],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err: Error = client
            .poll_async_results(
                POLL_PATH,
                QueryResponseDeadline::new(Duration::from_secs(30)),
                Arc::from("query-id"),
            )
            .await
            .unwrap_err()
            .into();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert_eq!(err.snowflake_code(), Some("390114"));
        assert_eq!(err.query_id(), Some("query-id"));
        let (_requests, quiet) = server.await.unwrap();
        assert!(quiet);
    }

    #[tokio::test]
    async fn poll_without_master_token_returns_expired_response() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(listener, vec![SESSION_EXPIRED_BODY]);

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err: Error = client
            .poll_async_results(
                POLL_PATH,
                QueryResponseDeadline::new(Duration::from_secs(30)),
                Arc::from("query-id"),
            )
            .await
            .unwrap_err()
            .into();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert_eq!(err.snowflake_code(), Some("390112"));
        assert_eq!(err.query_id(), Some("query-id"));
        let (requests, quiet) = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(quiet);
    }
}
