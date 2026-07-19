use std::time::{Duration, Instant};

use bytes::Bytes;
use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde_json::from_slice;
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::{
    Error, Result,
    error::{
        ConfigError, NetworkError, ProtocolError, ServerError, SessionExpiredError, TimeoutError,
        classify_request_error,
    },
    statement::wire::{
        request::WireAbortBody,
        response::{SESSION_EXPIRED, WireAbortResponse},
    },
};

use super::{QueryApiClient, deadline::deadline_after};

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
}

fn is_retryable_abort_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500..=599)
}

trait AbortClock {
    fn remaining(&self) -> Duration;
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
    use std::{sync::Mutex as StdMutex, time::Duration};

    use reqwest::Url;
    use tokio::{net::TcpListener, time::timeout};

    use super::*;
    use crate::{
        ErrorKind,
        statement::api::test_support::{
            read_http_request, respond_once, test_query_api, write_http_response,
        },
    };

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
            let request = read_http_request(&mut socket).await;
            write_http_response(
                &mut socket,
                200,
                r#"{"success":false,"code":"000605","message":"query is not executing"}"#,
            )
            .await;
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
            let request = read_http_request(&mut socket).await;
            write_http_response(&mut socket, 200, r#"{"success":"yes"}"#).await;
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
            let request = read_http_request(&mut socket).await;
            write_http_response(&mut socket, 400, r#"{"error":"bad request"}"#).await;
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
}
