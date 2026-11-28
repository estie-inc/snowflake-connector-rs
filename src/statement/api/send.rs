use std::time::Duration;

use bytes::Bytes;
use http::StatusCode;
use reqwest::RequestBuilder;
use tokio::time::timeout;

use crate::{
    Error,
    error::{NetworkError, QueryScopedRepr, TimeoutError, classify_request_error},
};

use super::QueryApiClient;

#[derive(Debug)]
pub(crate) enum SendFailure {
    Request(reqwest::Error),
    Elapsed(TimeoutError),
    Status { status: StatusCode, body: Bytes },
}

impl SendFailure {
    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Request(_) | Self::Elapsed(_) => true,
            Self::Status { status, .. } => matches!(status.as_u16(), 408 | 429 | 500..=599),
        }
    }
}

impl From<SendFailure> for Error {
    fn from(failure: SendFailure) -> Self {
        match failure {
            SendFailure::Request(error) => classify_request_error(error),
            SendFailure::Elapsed(error) => error.into(),
            SendFailure::Status { status, body } => {
                NetworkError::http_status(status.as_u16(), &body).into()
            }
        }
    }
}

impl From<SendFailure> for QueryScopedRepr {
    fn from(failure: SendFailure) -> Self {
        match failure {
            SendFailure::Request(error) if error.is_timeout() => {
                TimeoutError::request(error).into()
            }
            SendFailure::Request(error) => NetworkError::request(error).into(),
            SendFailure::Elapsed(error) => error.into(),
            SendFailure::Status { status, body } => {
                NetworkError::http_status(status.as_u16(), &body).into()
            }
        }
    }
}

impl QueryApiClient {
    // The caller rejects a zero `remaining`. The body is read before the status is checked, so a body read failure on
    // an error status is a request failure.
    pub(super) async fn send_snowflake(
        &self,
        request: RequestBuilder,
        remaining: Duration,
        timeout_error: TimeoutError,
    ) -> Result<Bytes, SendFailure> {
        let (status, body) = match timeout(remaining, async {
            let response = request.send().await?;
            let status = response.status();
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, body))
        })
        .await
        {
            Ok(Ok(pair)) => pair,
            Ok(Err(error)) => return Err(SendFailure::Request(error)),
            Err(_elapsed) => return Err(SendFailure::Elapsed(timeout_error)),
        };

        if !status.is_success() {
            return Err(SendFailure::Status { status, body });
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use reqwest::Url;
    use tokio::net::TcpListener;

    use super::*;
    use crate::{
        ErrorKind,
        error::QueryScopedError,
        statement::api::test_support::{respond_once, test_query_api},
    };

    async fn connection_refused_error() -> reqwest::Error {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        reqwest::Client::new()
            .get(format!("http://{addr}/"))
            .send()
            .await
            .unwrap_err()
    }

    fn status_failure(status: u16) -> SendFailure {
        SendFailure::Status {
            status: StatusCode::from_u16(status).unwrap(),
            body: Bytes::from_static(b"busy"),
        }
    }

    #[tokio::test]
    async fn retryable_failures() {
        for status in [408, 429, 500, 503] {
            assert!(status_failure(status).is_retryable(), "{status}");
        }
        for status in [400, 403, 404] {
            assert!(!status_failure(status).is_retryable(), "{status}");
        }
        assert!(SendFailure::Elapsed(TimeoutError::query()).is_retryable());
        assert!(SendFailure::Request(connection_refused_error().await).is_retryable());
    }

    #[tokio::test]
    async fn failures_convert_to_matching_errors() {
        let err = Error::from(status_failure(503));
        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(
            Error::from(status_failure(503)).to_string(),
            Error::from(NetworkError::http_status(503, "busy")).to_string()
        );

        let err = Error::from(SendFailure::Elapsed(TimeoutError::query_cancel()));
        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert_eq!(
            err.to_string(),
            Error::from(TimeoutError::query_cancel()).to_string()
        );

        let err = Error::from(SendFailure::Request(connection_refused_error().await));
        assert_eq!(err.kind(), ErrorKind::Network);
    }

    #[tokio::test]
    async fn timed_out_request_converts_to_timeout_in_both_forms() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut sockets = Vec::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                sockets.push(socket);
            }
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap();
        // `reqwest::Error` is not `Clone`, so each conversion gets its own timed-out request.
        let mut timed_out = Vec::new();
        for _ in 0..2 {
            let error = client
                .get(format!("http://{addr}/"))
                .send()
                .await
                .unwrap_err();
            assert!(error.is_timeout());
            timed_out.push(error);
        }
        let scoped = timed_out.pop().unwrap();
        let plain = timed_out.pop().unwrap();

        let err = Error::from(SendFailure::Request(plain));
        assert_eq!(err.kind(), ErrorKind::Timeout);

        let err = Error::from(QueryScopedError::new(
            Arc::from("q"),
            SendFailure::Request(scoped),
        ));
        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert_eq!(err.query_id(), Some("q"));
        server.abort();
    }

    #[tokio::test]
    async fn scoped_failure_carries_the_query_id() {
        let err = Error::from(QueryScopedError::new(Arc::from("q"), status_failure(500)));
        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("q"));
    }

    #[tokio::test]
    async fn error_status_keeps_the_body() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            respond_once(&listener, 503, r#"{"busy":true}"#).await;
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let request = client
            .api
            .request(http::Method::GET, client.api.resolve("x"));
        let failure = client
            .send_snowflake(request, Duration::from_secs(5), TimeoutError::query())
            .await
            .unwrap_err();

        match failure {
            SendFailure::Status { status, body } => {
                assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(&body[..], br#"{"busy":true}"#);
            }
            other => panic!("expected status failure, got {other:?}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn exhausted_budget_is_the_given_timeout_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let client = test_query_api(Url::parse(&format!("http://{addr}/")).unwrap());
        let request = client
            .api
            .request(http::Method::GET, client.api.resolve("x"));
        let failure = client
            .send_snowflake(
                request,
                Duration::from_millis(50),
                TimeoutError::query_cancel(),
            )
            .await
            .unwrap_err();

        assert!(matches!(failure, SendFailure::Elapsed(_)), "{failure:?}");
        assert_eq!(
            Error::from(failure).to_string(),
            Error::from(TimeoutError::query_cancel()).to_string()
        );
        server.abort();
    }
}
