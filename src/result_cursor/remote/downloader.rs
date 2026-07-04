use std::{fmt, result::Result as StdResult, sync::Arc, time::Duration};

use bytes::Bytes;
use http::HeaderMap;
use reqwest::StatusCode;
use tokio::time::sleep;

use crate::{
    error::{NetworkError, QueryScopedError, QueryScopedResult, TimeoutError},
    result_table::{ResultTable, Schema},
    rowset::{ParseWorkload, parser::parse_remote_chunk_result_table_async},
    runtime::BlockingParseLimiter,
};

const MAX_RETRIES: usize = 7;
const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(16);

/// Everything the downloader needs to fetch and parse a single remote partition.
pub(super) struct DownloadRequest {
    pub(super) url: String,
    pub(super) headers: Arc<HeaderMap>,
    pub(super) query_id: Arc<str>,
    pub(super) row_count: i64,
    pub(super) compressed_size: i64,
    pub(super) uncompressed_size: i64,
    pub(super) blocking_parse_limiter: Option<BlockingParseLimiter>,
}

/// Internal download error used for retry classification.
enum DownloadFailure {
    Retryable(QueryScopedError),
    Fatal(QueryScopedError),
}

struct DownloadedChunk {
    body: Bytes,
    workload: ParseWorkload,
}

impl DownloadFailure {
    fn into_inner(self) -> QueryScopedError {
        match self {
            Self::Retryable(e) | Self::Fatal(e) => e,
        }
    }
}

/// Downloads remote partitions from S3/blob storage with retry and gzip support.
#[derive(Clone)]
pub(super) struct RemotePartitionDownloader {
    client: reqwest::Client,
}

impl RemotePartitionDownloader {
    pub(super) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub(super) async fn download_table(
        &self,
        request: DownloadRequest,
        schema: Arc<Schema>,
    ) -> QueryScopedResult<ResultTable> {
        let mut retries = 0;
        let column_count = schema.len();
        loop {
            match self.download_once(&request, column_count).await {
                Ok(chunk) => {
                    return parse_remote_chunk_result_table_async(
                        schema,
                        request.query_id,
                        chunk.body,
                        chunk.workload,
                        request.blocking_parse_limiter,
                    )
                    .await;
                }
                Err(DownloadFailure::Retryable(_)) if retries < MAX_RETRIES => {
                    sleep(retry_delay(retries)).await;
                    retries += 1;
                }
                Err(e) => return Err(e.into_inner()),
            }
        }
    }

    async fn download_once(
        &self,
        request: &DownloadRequest,
        column_count: usize,
    ) -> StdResult<DownloadedChunk, DownloadFailure> {
        let response = match self
            .client
            .get(&request.url)
            .headers((*request.headers).clone())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                if error.is_timeout() {
                    return Err(DownloadFailure::Retryable(QueryScopedError::new(
                        Arc::clone(&request.query_id),
                        TimeoutError::request(error),
                    )));
                }
                return Err(DownloadFailure::Retryable(QueryScopedError::new(
                    Arc::clone(&request.query_id),
                    NetworkError::Http(error),
                )));
            }
        };

        let status = response.status();
        if !status.is_success() {
            return Err(classify_failed_status(
                status,
                response.bytes().await,
                Arc::clone(&request.query_id),
            ));
        }

        let body = match response.bytes().await {
            Ok(body) => body,
            Err(error) => {
                if error.is_timeout() {
                    return Err(DownloadFailure::Retryable(QueryScopedError::new(
                        Arc::clone(&request.query_id),
                        TimeoutError::request(error),
                    )));
                }
                return Err(DownloadFailure::Retryable(QueryScopedError::new(
                    Arc::clone(&request.query_id),
                    NetworkError::Http(error),
                )));
            }
        };

        let gzip_encoded = body.len() >= 2 && body[0] == 0x1f && body[1] == 0x8b;
        let workload = ParseWorkload::remote_chunk(
            body.len(),
            request.row_count,
            column_count,
            gzip_encoded,
            request.compressed_size,
            request.uncompressed_size,
        );

        Ok(DownloadedChunk { body, workload })
    }
}

fn classify_failed_status<E>(
    status: StatusCode,
    body: StdResult<Bytes, E>,
    query_id: Arc<str>,
) -> DownloadFailure
where
    E: fmt::Display,
{
    let error = match body {
        Ok(body) => QueryScopedError::new(
            query_id,
            NetworkError::chunk_download(status.as_u16(), body),
        ),
        Err(err) => QueryScopedError::new(
            query_id,
            NetworkError::chunk_download(
                status.as_u16(),
                format!("<failed to read response body: {err}>"),
            ),
        ),
    };

    if is_retryable_status(status) {
        DownloadFailure::Retryable(error)
    } else {
        DownloadFailure::Fatal(error)
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
}

fn retry_delay(retries: usize) -> Duration {
    let factor = 1u32.checked_shl(retries as u32).unwrap_or(u32::MAX);
    MIN_RETRY_DELAY.saturating_mul(factor).min(MAX_RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::ErrorKind;

    fn qid() -> Arc<str> {
        Arc::from("query-id")
    }

    #[test]
    fn retryable_status_codes() {
        assert!(is_retryable_status(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(is_retryable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(is_retryable_status(StatusCode::GATEWAY_TIMEOUT));
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(is_retryable_status(StatusCode::REQUEST_TIMEOUT));

        assert!(!is_retryable_status(StatusCode::OK));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
        assert!(!is_retryable_status(StatusCode::FORBIDDEN));
    }

    #[test]
    fn retry_delay_exponential_backoff() {
        assert_eq!(retry_delay(0), Duration::from_secs(1));
        assert_eq!(retry_delay(1), Duration::from_secs(2));
        assert_eq!(retry_delay(2), Duration::from_secs(4));
        assert_eq!(retry_delay(3), Duration::from_secs(8));
        assert_eq!(retry_delay(4), Duration::from_secs(16));
        assert_eq!(retry_delay(5), Duration::from_secs(16));
        assert_eq!(retry_delay(6), Duration::from_secs(16));
    }

    #[test]
    fn body_read_failure_keeps_non_retryable_status_fatal() {
        match classify_failed_status(StatusCode::FORBIDDEN, Err("body read failed"), qid()) {
            DownloadFailure::Fatal(error) => {
                let error: crate::Error = error.into();
                assert_eq!(error.kind(), ErrorKind::Network);
                assert_eq!(
                    error.to_string(),
                    "chunk download failed with HTTP 403: <failed to read response body: body read failed>"
                );
            }
            DownloadFailure::Retryable(error) => {
                panic!("expected fatal classification, got retryable: {error:?}")
            }
        }
    }

    #[test]
    fn body_read_failure_keeps_retryable_status_retryable() {
        match classify_failed_status(StatusCode::BAD_GATEWAY, Err("body read failed"), qid()) {
            DownloadFailure::Retryable(error) => {
                let error: crate::Error = error.into();
                assert_eq!(error.kind(), ErrorKind::Network);
                assert_eq!(
                    error.to_string(),
                    "chunk download failed with HTTP 502: <failed to read response body: body read failed>"
                );
            }
            DownloadFailure::Fatal(error) => {
                panic!("expected retryable classification, got fatal: {error:?}")
            }
        }
    }

    #[test]
    fn non_utf8_body_keeps_lossy_preview() {
        match classify_failed_status(
            StatusCode::FORBIDDEN,
            Ok::<Bytes, &str>(Bytes::from_static(b"\xfffoo")),
            qid(),
        ) {
            DownloadFailure::Fatal(error) => {
                let error: crate::Error = error.into();
                assert_eq!(error.kind(), ErrorKind::Network);
                assert!(error.to_string().contains("foo"));
                assert!(!error.to_string().contains("<failed to read response body"));
            }
            DownloadFailure::Retryable(error) => {
                panic!("expected fatal classification, got retryable: {error:?}")
            }
        }
    }
}
