use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{
    Method,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE},
};
use serde_json::from_slice;
use tokio::time::sleep;
use uuid::Uuid;

use crate::{
    Error, Result,
    auth::{Remaining, RenewFailure, SendError},
    error::{ProtocolError, ServerError, SessionExpiredError, TimeoutError},
    statement::wire::{
        request::WireAbortBody,
        response::{SESSION_EXPIRED, WireAbortResponse},
    },
};

use super::{QueryApiClient, deadline::deadline_after, send::SendFailure};

const MAX_ABORT_ATTEMPTS: usize = 8;

impl QueryApiClient {
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
        let mut url = self.api.resolve("queries/v1/abort-request");
        url.query_pairs_mut()
            .append_pair("requestId", &cancel_request_id);

        let body = Bytes::from(
            serde_json::to_vec(&WireAbortBody {
                request_id: query_request_id,
            })
            .map_err(|error| Error::other(format!("failed to serialize abort request: {error}")))?,
        );
        let mut backoff = AbortBackoff::new();
        let mut attempt = 0;

        loop {
            // Renewing and resending happens inside one attempt, so it does not count against the abort attempt budget.
            let sent: std::result::Result<WireAbortResponse, AbortAttemptError> = self
                .auth
                .send_with_renewal(&self.api, clock, TimeoutError::query_cancel(), |tokens| {
                    let request = self
                        .api
                        .request(Method::POST, url.clone())
                        .header(ACCEPT, "application/snowflake")
                        .header(CONTENT_TYPE, "application/json")
                        .header(AUTHORIZATION, tokens.authorization())
                        .body(body.clone());
                    async move {
                        let remaining = clock.remaining();
                        if remaining.is_zero() {
                            return Err(SendError::Other(AbortAttemptError::fatal(
                                TimeoutError::query_cancel(),
                            )));
                        }
                        let body = self
                            .send_snowflake(request, remaining, TimeoutError::query_cancel())
                            .await
                            .map_err(|failure| SendError::Other(failure.into()))?;

                        let response: WireAbortResponse = from_slice(&body).map_err(|error| {
                            SendError::Other(AbortAttemptError::fatal(ProtocolError::json_parse(
                                error, &body,
                            )))
                        })?;
                        if response.code.as_deref() == Some(SESSION_EXPIRED) {
                            return Err(SendError::Expired(SessionExpiredError::new(
                                response.code,
                                response.message,
                                None,
                            )));
                        }
                        Ok(response)
                    }
                })
                .await;

            let attempt_result = match sent {
                Ok(response) if !response.success => Err(AbortAttemptError::fatal(
                    ServerError::new(response.code, response.message, None),
                )),
                Ok(_) => Ok(()),
                Err(error) => Err(error),
            };

            attempt += 1;
            match attempt_result {
                Ok(()) => return Ok(()),
                Err(AbortAttemptError { retryable, error })
                    if !retryable || attempt == MAX_ABORT_ATTEMPTS =>
                {
                    return Err(error);
                }
                Err(_) => {
                    let remaining = clock.remaining();
                    if remaining.is_zero() {
                        return Err(TimeoutError::query_cancel().into());
                    }
                    clock.sleep(remaining.min(backoff.next_delay())).await;
                }
            }
        }
    }
}

struct AbortAttemptError {
    retryable: bool,
    error: Error,
}

impl AbortAttemptError {
    fn fatal(error: impl Into<Error>) -> Self {
        Self {
            retryable: false,
            error: error.into(),
        }
    }
}

impl From<SendFailure> for AbortAttemptError {
    fn from(failure: SendFailure) -> Self {
        Self {
            retryable: failure.is_retryable(),
            error: failure.into(),
        }
    }
}

impl From<RenewFailure> for AbortAttemptError {
    fn from(failure: RenewFailure) -> Self {
        Self::fatal(failure)
    }
}

trait AbortClock: Remaining {
    fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send;
}

struct RealAbortClock {
    deadline: Instant,
}

impl RealAbortClock {
    fn new(timeout: Duration) -> Self {
        Self {
            deadline: deadline_after(timeout),
        }
    }
}

impl Remaining for RealAbortClock {
    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

impl AbortClock for RealAbortClock {
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
    use std::{sync::Mutex as StdMutex, time::Duration};

    use http::StatusCode;
    use reqwest::Url;
    use tokio::{net::TcpListener, time::timeout};

    use super::*;
    use crate::{
        ErrorKind,
        api_context::DEFAULT_USER_AGENT,
        statement::api::test_support::{
            RENEWED_BODY, SESSION_EXPIRED_BODY, renewable_query_api, request_target, respond_once,
            serve_script, test_query_api,
        },
        test_support::http::{read_http_request, write_json_response},
    };

    #[tokio::test]
    async fn abort_request_uses_separate_cancel_id_and_minimal_body() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await.unwrap();
            write_json_response(&mut socket, StatusCode::OK, r#"{"success":true}"#)
                .await
                .unwrap();
            request
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
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
        assert!(request.to_ascii_lowercase().contains(&format!(
            "user-agent: {}",
            DEFAULT_USER_AGENT.to_ascii_lowercase()
        )));
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
                requests.push(read_http_request(&mut socket).await.unwrap());
                write_json_response(&mut socket, StatusCode::from_u16(status).unwrap(), body)
                    .await
                    .unwrap();
            }
            requests
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
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
            let request = read_http_request(&mut socket).await.unwrap();
            write_json_response(
                &mut socket,
                StatusCode::OK,
                r#"{"success":false,"code":"000605","message":"query is not executing"}"#,
            )
            .await
            .unwrap();
            request
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
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
            let request = read_http_request(&mut socket).await.unwrap();
            write_json_response(&mut socket, StatusCode::OK, r#"{"success":"yes"}"#)
                .await
                .unwrap();
            request
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
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
        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());

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
            let request = read_http_request(&mut socket).await.unwrap();
            write_json_response(
                &mut socket,
                StatusCode::BAD_REQUEST,
                r#"{"error":"bad request"}"#,
            )
            .await
            .unwrap();
            request
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err = client
            .abort_query("query-request-id", Duration::from_secs(2))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Network);
        let _ = server.await.unwrap();
    }

    struct MockAbortClock {
        remaining: StdMutex<Duration>,
        sleeps: StdMutex<Vec<Duration>>,
    }

    impl MockAbortClock {
        fn new(budget: Duration) -> Self {
            Self {
                remaining: StdMutex::new(budget),
                sleeps: StdMutex::new(Vec::new()),
            }
        }

        fn recorded_sleeps(&self) -> Vec<Duration> {
            self.sleeps.lock().unwrap().clone()
        }
    }

    impl Remaining for MockAbortClock {
        fn remaining(&self) -> Duration {
            *self.remaining.lock().unwrap()
        }
    }

    impl AbortClock for MockAbortClock {
        fn sleep(&self, duration: Duration) -> impl std::future::Future<Output = ()> + Send {
            self.sleeps.lock().unwrap().push(duration);
            let mut remaining = self.remaining.lock().unwrap();
            *remaining = remaining.saturating_sub(duration);
            std::future::ready(())
        }
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

            let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
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

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
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

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
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

    #[tokio::test]
    async fn abort_renews_expired_session_and_resends_same_request() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![SESSION_EXPIRED_BODY, RENEWED_BODY, r#"{"success":true}"#],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        client
            .abort_query("query-request-id", Duration::from_secs(5))
            .await
            .unwrap();

        let (requests, quiet) = server.await.unwrap();
        assert!(quiet);
        assert!(requests[0].contains(r#"authorization: Snowflake Token="test-token""#));
        assert!(request_target(&requests[1]).starts_with("/session/token-request?"));
        assert!(requests[2].contains(r#"authorization: Snowflake Token="renewed-token""#));
        assert_eq!(request_target(&requests[0]), request_target(&requests[2]));
        assert_eq!(
            requests[0].split("\r\n\r\n").nth(1).unwrap(),
            requests[2].split("\r\n\r\n").nth(1).unwrap()
        );
    }

    #[tokio::test]
    async fn abort_renewal_does_not_consume_the_attempt_budget() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..MAX_ABORT_ATTEMPTS - 1 {
                respond_once(&listener, 503, "unavailable").await;
            }
            respond_once(&listener, 200, SESSION_EXPIRED_BODY).await;
            respond_once(&listener, 200, RENEWED_BODY).await;
            respond_once(&listener, 200, r#"{"success":true}"#).await;
        });

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let clock = MockAbortClock::new(Duration::from_secs(60));
        client
            .abort_query_with_clock("query-request-id", &clock)
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn abort_renews_again_after_another_answer_was_observed() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            respond_once(&listener, 200, SESSION_EXPIRED_BODY).await;
            respond_once(&listener, 200, RENEWED_BODY).await;
            // A transient failure separates the two expiries, so the second one is renewed as well.
            respond_once(&listener, 503, "unavailable").await;
            respond_once(&listener, 200, SESSION_EXPIRED_BODY).await;
            respond_once(
                &listener,
                200,
                r#"{"success":true,"data":{"sessionToken":"renewed-again"}}"#,
            )
            .await;
            respond_once(&listener, 200, r#"{"success":true}"#).await;
        });

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let clock = MockAbortClock::new(Duration::from_secs(60));
        client
            .abort_query_with_clock("query-request-id", &clock)
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn abort_still_expired_after_renewal_is_session_expired() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![SESSION_EXPIRED_BODY, RENEWED_BODY, SESSION_EXPIRED_BODY],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err = client
            .abort_query("query-request-id", Duration::from_secs(5))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert_eq!(err.snowflake_code(), Some("390112"));
        let (requests, quiet) = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(quiet, "a second renewal must not be attempted");
    }

    #[tokio::test]
    async fn abort_rejected_renewal_is_session_expired_with_renewal_code() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_script(
            listener,
            vec![
                SESSION_EXPIRED_BODY,
                r#"{"success":false,"code":"390115","message":"master token invalid"}"#,
            ],
        );

        let client = renewable_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let err = client
            .abort_query("query-request-id", Duration::from_secs(5))
            .await
            .unwrap_err();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert_eq!(err.snowflake_code(), Some("390115"));
        let (_requests, quiet) = server.await.unwrap();
        assert!(quiet);
    }
}
