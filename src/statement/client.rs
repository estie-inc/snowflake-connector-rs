use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, Url};
use serde_json::from_slice;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::{
    ClientShared, Error, Result,
    error::{
        ConfigError, NetworkError, ProtocolError, QueryScopedError, QueryScopedResult, ServerError,
        SessionExpiredError, TimeoutError, classify_request_error,
    },
    session::SessionAuth,
    statement::{
        StatementParts,
        wire::{
            request::{WireAbortBody, WireQueryBody},
            response::{
                QUERY_IN_PROGRESS_ASYNC_CODE, QUERY_IN_PROGRESS_CODE, SESSION_EXPIRED,
                SnowflakeResponse, WireAbortResponse, parse_response,
            },
        },
    },
};

/// Client-side deadline for obtaining a query response from Snowflake.
///
/// Created once at the start of statement execution and shared unchanged across the initial submit and any subsequent
/// async result polling, so the whole submit-to-response path runs against a single budget.
#[derive(Clone, Copy)]
pub(crate) struct QueryResponseDeadline {
    deadline: Instant,
}

const FAR_FUTURE: Duration = Duration::from_secs(60 * 60 * 24 * 365);

impl QueryResponseDeadline {
    pub(crate) fn new(timeout: Duration) -> Self {
        let now = Instant::now();
        let deadline = now
            .checked_add(timeout)
            .or_else(|| now.checked_add(FAR_FUTURE))
            .unwrap_or(now);
        Self { deadline }
    }

    /// Time left before the deadline, saturating at zero once it has passed.
    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Remaining budget, or a timeout error when the deadline has already elapsed.
    pub(crate) fn remaining_or_timeout(&self) -> std::result::Result<Duration, TimeoutError> {
        let remaining = self.remaining();
        if remaining.is_zero() {
            Err(TimeoutError::query())
        } else {
            Ok(remaining)
        }
    }
}

#[derive(Clone)]
pub(crate) struct StatementApiClient {
    shared: Arc<ClientShared>,
    auth: Arc<SessionAuth>,
}

pub(crate) struct PreparedSubmit {
    request: reqwest::RequestBuilder,
}

impl StatementApiClient {
    pub(crate) fn new(shared: Arc<ClientShared>, auth: Arc<SessionAuth>) -> Self {
        Self { shared, auth }
    }

    pub(crate) fn http_client(&self) -> Client {
        self.shared.http.clone()
    }

    pub(crate) fn prepare_submit(
        &self,
        parts: &StatementParts,
        query_request_id: &str,
    ) -> Result<PreparedSubmit> {
        let mut url = self
            .shared
            .base_url
            .join("queries/v1/query-request")
            .map_err(|e| ConfigError::invalid_url(e.to_string()))?;
        url.query_pairs_mut()
            .append_pair("requestId", query_request_id);

        let body = Bytes::from(
            serde_json::to_vec(&WireQueryBody::from_statement_parts(parts)).map_err(|error| {
                Error::other(format!("failed to serialize query request: {error}"))
            })?,
        );

        let request = self
            .shared
            .http
            .post(url)
            .header(ACCEPT, "application/snowflake")
            .header(CONTENT_TYPE, "application/json")
            .header(
                AUTHORIZATION,
                format!(r#"Snowflake Token="{}""#, self.auth.session_token),
            )
            .body(body);

        Ok(PreparedSubmit { request })
    }

    pub(crate) async fn send_prepared_submit(
        &self,
        prepared: PreparedSubmit,
        deadline: QueryResponseDeadline,
    ) -> Result<SnowflakeResponse> {
        let remaining = deadline.remaining_or_timeout()?;

        let body = match timeout(remaining, async {
            let response = prepared
                .request
                .send()
                .await
                .map_err(classify_request_error)?;
            let status = response.status();
            let body = response.bytes().await.map_err(classify_request_error)?;
            if !status.is_success() {
                return Err(Error::from(NetworkError::http_status(
                    status.as_u16(),
                    &body,
                )));
            }
            Ok(body)
        })
        .await
        {
            Ok(result) => result?,
            Err(_elapsed) => return Err(TimeoutError::query().into()),
        };

        parse_response(body).map_err(Error::from)
    }

    #[cfg(test)]
    pub(crate) async fn submit(
        &self,
        parts: &StatementParts,
        query_request_id: &str,
        deadline: QueryResponseDeadline,
    ) -> Result<SnowflakeResponse> {
        let prepared = self.prepare_submit(parts, query_request_id)?;
        self.send_prepared_submit(prepared, deadline).await
    }

    pub(crate) async fn abort_query(
        &self,
        query_request_id: &str,
        request_timeout: Duration,
    ) -> Result<()> {
        self.abort_query_with_clock(query_request_id, &RealAbortClock::new(request_timeout))
            .await
    }

    async fn abort_query_with_clock<C: AbortClock>(
        &self,
        query_request_id: &str,
        clock: &C,
    ) -> Result<()> {
        let cancel_request_id = Uuid::new_v4().to_string();
        let mut url = self
            .shared
            .base_url
            .join("queries/v1/abort-request")
            .map_err(|e| ConfigError::invalid_url(e.to_string()))?;
        url.query_pairs_mut()
            .append_pair("requestId", &cancel_request_id);

        let body = Bytes::from(
            serde_json::to_vec(&WireAbortBody {
                request_id: query_request_id,
            })
            .map_err(|error| Error::other(format!("failed to serialize abort request: {error}")))?,
        );
        let mut backoff = AbortBackoff::new();

        for attempt in 0..MAX_ABORT_ATTEMPTS {
            let remaining = clock.remaining();
            if remaining.is_zero() {
                return Err(TimeoutError::query_cancel().into());
            }

            let request = self
                .shared
                .http
                .post(url.clone())
                .header(ACCEPT, "application/snowflake")
                .header(CONTENT_TYPE, "application/json")
                .header(
                    AUTHORIZATION,
                    format!(r#"Snowflake Token="{}""#, self.auth.session_token),
                )
                .body(body.clone());

            let attempt_result = match timeout(remaining, async {
                let response = request.send().await?;
                let status = response.status();
                let body = response.bytes().await?;
                Ok::<_, reqwest::Error>((status, body))
            })
            .await
            {
                Ok(Ok((status, body))) => {
                    if !status.is_success() {
                        let error = NetworkError::http_status(status.as_u16(), &body);
                        if is_retryable_abort_status(status.as_u16()) {
                            Err((true, Error::from(error)))
                        } else {
                            Err((false, Error::from(error)))
                        }
                    } else {
                        let response: WireAbortResponse = from_slice(&body).map_err(|error| {
                            Error::from(ProtocolError::json_parse(error, &body))
                        })?;
                        if response.code.as_deref() == Some(SESSION_EXPIRED) {
                            Err((
                                false,
                                SessionExpiredError::new(response.code, response.message, None)
                                    .into(),
                            ))
                        } else if !response.success {
                            Err((
                                false,
                                ServerError::new(response.code, response.message, None).into(),
                            ))
                        } else {
                            Ok(())
                        }
                    }
                }
                Ok(Err(error)) => Err((true, classify_request_error(error))),
                Err(_elapsed) => Err((true, TimeoutError::query_cancel().into())),
            };

            match attempt_result {
                Ok(()) => return Ok(()),
                Err((retryable, error)) if !retryable || attempt + 1 == MAX_ABORT_ATTEMPTS => {
                    return Err(error);
                }
                Err((_retryable, _error)) => {
                    let remaining = clock.remaining();
                    if remaining.is_zero() {
                        return Err(TimeoutError::query_cancel().into());
                    }
                    clock.sleep(remaining.min(backoff.next_delay())).await;
                }
            }
        }

        unreachable!("abort retry loop always returns within the attempt budget");
    }

    pub(crate) async fn poll_async_results(
        &self,
        poll_relative_url: &str,
        deadline: QueryResponseDeadline,
        query_id: Arc<str>,
    ) -> QueryScopedResult<SnowflakeResponse> {
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

            match parse_response(body) {
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

const MAX_ABORT_ATTEMPTS: usize = 8;

fn is_retryable_abort_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

pub(crate) trait AbortClock {
    fn remaining(&self) -> Duration;
    fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send;
}

struct RealAbortClock {
    deadline: Instant,
}

impl RealAbortClock {
    fn new(timeout: Duration) -> Self {
        let now = Instant::now();
        let deadline = now
            .checked_add(timeout)
            .or_else(|| now.checked_add(FAR_FUTURE))
            .unwrap_or(now);
        Self { deadline }
    }
}

impl AbortClock for RealAbortClock {
    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send {
        sleep(duration)
    }
}

struct AbortBackoff {
    step: usize,
}

impl AbortBackoff {
    const STEPS: [Duration; 4] = [
        Duration::from_millis(200),
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(2),
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::*;
    use crate::{
        ClientShared, ErrorKind, QueryConfig, Statement, runtime::QueryRuntime,
        session::SessionAuth, statement::builder::into_statement_parts,
    };

    fn test_statement_api(base_url: Url) -> StatementApiClient {
        StatementApiClient::new(
            ClientShared::for_test_with(
                reqwest::Client::new(),
                base_url,
                QueryConfig::default().into(),
                QueryRuntime::new(),
            ),
            SessionAuth::for_test("test-token"),
        )
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

    #[tokio::test]
    async fn submit_timeout_is_timeout_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let client = StatementApiClient::new(
            ClientShared::for_test_with(
                reqwest::Client::builder()
                    .timeout(Duration::from_millis(50))
                    .build()
                    .unwrap(),
                Url::parse(&format!("http://{addr}/")).unwrap(),
                QueryConfig::default().into(),
                QueryRuntime::new(),
            ),
            SessionAuth::for_test("test-token"),
        );

        let parts = into_statement_parts(Statement::from("select 1")).unwrap();
        // A generous deadline leaves the reqwest client-wide timeout as the trigger for this case.
        let deadline = QueryResponseDeadline::new(Duration::from_secs(30));
        let err = client
            .submit(&parts, "query-request-id", deadline)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(err.is_timeout());

        server.abort();
        let _ = server.await;
    }

    #[test]
    fn poll_backoff_steps_up_then_plateaus_at_five_seconds() {
        let mut backoff = PollBackoff::new();
        let observed: Vec<Duration> = (0..9).map(|_| backoff.next_delay()).collect();

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

    #[tokio::test]
    async fn abort_request_uses_separate_cancel_id_and_minimal_body() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            write_http_response(&mut socket, 200, r#"{"success":true}"#).await;
            request
        });

        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
        client
            .abort_query("query-request-id", Duration::from_secs(2))
            .await
            .unwrap();

        let request = server.await.unwrap();
        let target = request.split_whitespace().nth(1).unwrap();
        let target_url = Url::parse(&format!("http://localhost{target}")).unwrap();
        let cancel_request_id = target_url
            .query_pairs()
            .find_map(|(key, value)| (key == "requestId").then(|| value.into_owned()))
            .unwrap();
        assert_ne!(cancel_request_id, "query-request-id");
        assert_eq!(
            request.split("\r\n\r\n").nth(1).unwrap(),
            r#"{"requestId":"query-request-id"}"#
        );
        assert!(request.contains("accept: application/snowflake"));
        assert!(request.contains(r#"authorization: Snowflake Token="test-token""#));
    }

    #[tokio::test]
    async fn abort_transport_retry_reuses_identifiers() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in [
                (500, r#"{"success":false,"code":"temporary"}"#),
                (200, r#"{"success":true}"#),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                requests.push(read_http_request(&mut socket).await);
                write_http_response(&mut socket, status, body).await;
            }
            requests
        });

        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
        client
            .abort_query("query-request-id", Duration::from_secs(3))
            .await
            .unwrap();

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        let first_target = requests[0].split_whitespace().nth(1).unwrap();
        let second_target = requests[1].split_whitespace().nth(1).unwrap();
        let first_url = Url::parse(&format!("http://localhost{first_target}")).unwrap();
        let second_url = Url::parse(&format!("http://localhost{second_target}")).unwrap();
        let first_id = first_url
            .query_pairs()
            .find_map(|(key, value)| (key == "requestId").then(|| value.into_owned()))
            .unwrap();
        let second_id = second_url
            .query_pairs()
            .find_map(|(key, value)| (key == "requestId").then(|| value.into_owned()))
            .unwrap();
        assert_eq!(first_id, second_id);
        assert_eq!(
            requests[0].split("\r\n\r\n").nth(1).unwrap(),
            requests[1].split("\r\n\r\n").nth(1).unwrap()
        );
    }

    #[tokio::test]
    async fn abort_success_false_is_server_error_without_application_retry() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            write_http_response(
                &mut socket,
                200,
                r#"{"success":false,"code":"000605","message":"query is not executing"}"#,
            )
            .await;
            request
        });

        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err = client
            .abort_query("query-request-id", Duration::from_secs(2))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Server);
        assert_eq!(err.snowflake_code(), Some("000605"));
        assert_eq!(err.snowflake_message(), Some("query is not executing"));
        let _ = server.await.unwrap();
    }

    #[tokio::test]
    async fn abort_malformed_response_is_protocol_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            write_http_response(&mut socket, 200, r#"{"success":"yes"}"#).await;
            request
        });

        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err = client
            .abort_query("query-request-id", Duration::from_secs(2))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        let _ = server.await.unwrap();
    }

    #[tokio::test]
    async fn abort_zero_timeout_does_not_send_a_request() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());

        let err = client
            .abort_query("query-request-id", Duration::ZERO)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(
            timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn abort_non_retryable_http_status_is_sent_once() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            write_http_response(&mut socket, 400, r#"{"error":"bad request"}"#).await;
            request
        });

        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err = client
            .abort_query("query-request-id", Duration::from_secs(2))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Network);
        let _ = server.await.unwrap();
    }

    struct MockAbortClock {
        remaining: std::sync::Mutex<Duration>,
        sleeps: std::sync::Mutex<Vec<Duration>>,
    }

    impl MockAbortClock {
        fn new(budget: Duration) -> Self {
            Self {
                remaining: std::sync::Mutex::new(budget),
                sleeps: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn recorded_sleeps(&self) -> Vec<Duration> {
            self.sleeps.lock().unwrap().clone()
        }
    }

    impl AbortClock for MockAbortClock {
        fn remaining(&self) -> Duration {
            *self.remaining.lock().unwrap()
        }

        fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send {
            self.sleeps.lock().unwrap().push(duration);
            let mut remaining = self.remaining.lock().unwrap();
            *remaining = remaining.saturating_sub(duration);
            std::future::ready(())
        }
    }

    async fn respond_once(listener: &TcpListener, status: u16, body: &str) {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http_request(&mut socket).await;
        write_http_response(&mut socket, status, body).await;
    }

    #[tokio::test]
    async fn abort_retries_every_transient_status_then_succeeds() {
        for status in [408_u16, 429, 500, 503, 599] {
            let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                respond_once(&listener, status, r#"{"success":false}"#).await;
                respond_once(&listener, 200, r#"{"success":true}"#).await;
            });

            let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
            let clock = MockAbortClock::new(Duration::from_secs(1_000));
            client
                .abort_query_with_clock("query-request-id", &clock)
                .await
                .unwrap_or_else(|error| panic!("status {status} must retry to success: {error}"));

            server.await.unwrap();
            assert_eq!(
                clock.recorded_sleeps(),
                vec![Duration::from_millis(200)],
                "status {status} must retry exactly once before succeeding"
            );
        }
    }

    #[tokio::test]
    async fn abort_stops_after_the_maximum_attempt_budget() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..MAX_ABORT_ATTEMPTS {
                respond_once(&listener, 503, r#"{"success":false}"#).await;
            }
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        });

        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let clock = MockAbortClock::new(Duration::from_secs(1_000));
        let err = client
            .abort_query_with_clock("query-request-id", &clock)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Network);
        assert!(
            server.await.unwrap(),
            "the abort must not exceed {MAX_ABORT_ATTEMPTS} total sends"
        );
        assert_eq!(
            clock.recorded_sleeps(),
            vec![
                Duration::from_millis(200),
                Duration::from_millis(500),
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
                Duration::from_secs(2),
            ],
            "backoff must grow and then cap at 2s across the seven retries"
        );
    }

    #[tokio::test]
    async fn abort_stops_when_the_deadline_is_exhausted_between_retries() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            respond_once(&listener, 500, r#"{"success":false}"#).await;
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        });

        let client = test_statement_api(Url::parse(&format!("http://{addr}/")).unwrap());
        // The first backoff consumes the whole remaining budget, so the next iteration must fail on the deadline
        // instead of sending a second request.
        let clock = MockAbortClock::new(Duration::from_millis(200));
        let err = client
            .abort_query_with_clock("query-request-id", &clock)
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(
            server.await.unwrap(),
            "no request may be sent once the deadline is exhausted"
        );
    }

    async fn read_http_request(socket: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1024];
        let body_start;
        let body_length;

        loop {
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before request completed");
            bytes.extend_from_slice(&chunk[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                body_start = index + 4;
                let headers = String::from_utf8_lossy(&bytes[..index]);
                body_length = headers
                    .lines()
                    .find_map(|line| {
                        line.strip_prefix("content-length:")
                            .or_else(|| line.strip_prefix("Content-Length:"))
                    })
                    .and_then(|value| value.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                break;
            }
        }

        while bytes.len() < body_start + body_length {
            let read = socket.read(&mut chunk).await.unwrap();
            assert!(read > 0, "connection closed before request body completed");
            bytes.extend_from_slice(&chunk[..read]);
        }
        String::from_utf8(bytes).unwrap()
    }

    async fn write_http_response(socket: &mut tokio::net::TcpStream, status: u16, body: &str) {
        let response = format!(
            "HTTP/1.1 {status} OK\r\ncontent-length: {}\r\ncontent-type: application/json\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    }
}
