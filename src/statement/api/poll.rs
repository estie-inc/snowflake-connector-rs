use std::{sync::Arc, time::Duration};

use http::header::{ACCEPT, AUTHORIZATION};
use reqwest::Url;
use tokio::time::{sleep, timeout};

use crate::{
    error::{NetworkError, ProtocolError, QueryScopedError, QueryScopedResult, TimeoutError},
    statement::wire::response::{
        QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE, WireQueryResponse,
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
        let poll_url = match resolve_poll_url(&self.shared.base_url, poll_relative_url) {
            Ok(url) => url,
            Err(err) => {
                return Err(QueryScopedError::new(query_id, err));
            }
        };
        let mut backoff = PollBackoff::new();

        loop {
            // The poll endpoint is a server-side long-poll, so apply the remaining query-response budget to the whole
            // GET instead of only checking the deadline between poll attempts.
            let remaining = match deadline.remaining_or_timeout() {
                Ok(remaining) => remaining,
                Err(err) => return Err(QueryScopedError::new(query_id, err)),
            };

            let request = self
                .shared
                .http
                .get(poll_url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(
                    AUTHORIZATION,
                    format!(r#"Snowflake Token="{}""#, self.auth.session_token),
                );

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
                    if response.code.as_deref() != Some(QUERY_IN_PROGRESS_ASYNC_CODE)
                        && response.code.as_deref() != Some(QUERY_IN_PROGRESS_CODE) =>
                {
                    return Ok(response);
                }
                Ok(_) => {
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
    use std::time::Duration;

    use reqwest::Url;

    use super::*;
    use crate::{Error, ErrorKind};

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
}
