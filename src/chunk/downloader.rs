use std::{io, sync::Arc, time::Duration};

use http::HeaderMap;
use reqwest::StatusCode;
use tokio::time::sleep;

use crate::{
    Error, Result,
    query_result::partition_source::FetchContext,
    result::{ResultTable, Schema},
    rowset::{ParseWorkload, parser::parse_remote_chunk_result_table_async},
};

const MAX_RETRIES: usize = 7;
const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(16);

/// Internal download error used for retry classification.
enum DownloadFailure {
    Retryable(Error),
    Fatal(Error),
}

impl DownloadFailure {
    fn into_inner(self) -> Error {
        match self {
            Self::Retryable(e) | Self::Fatal(e) => e,
        }
    }
}

impl From<reqwest::Error> for DownloadFailure {
    fn from(e: reqwest::Error) -> Self {
        Self::Retryable(Error::Reqwest(e))
    }
}

impl From<io::Error> for DownloadFailure {
    fn from(e: io::Error) -> Self {
        Self::Retryable(Error::IO(e))
    }
}

/// Downloads remote partitions from S3/blob storage with retry and gzip support.
#[derive(Clone)]
pub(crate) struct ChunkDownloader {
    client: reqwest::Client,
}

impl ChunkDownloader {
    pub(crate) fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub(crate) async fn download_table(
        &self,
        url: &str,
        headers: &HeaderMap,
        schema: Arc<Schema>,
        ctx: FetchContext,
    ) -> Result<ResultTable> {
        let mut retries = 0;
        loop {
            match self
                .download_once(url, headers.clone(), Arc::clone(&schema), ctx.clone())
                .await
            {
                Ok(t) => return Ok(t),
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
        url: &str,
        headers: HeaderMap,
        schema: Arc<Schema>,
        ctx: FetchContext,
    ) -> std::result::Result<ResultTable, DownloadFailure> {
        let response = self.client.get(url).headers(headers).send().await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let error = Error::ChunkDownload(body);
            return if is_retryable_status(status) {
                Err(DownloadFailure::Retryable(error))
            } else {
                Err(DownloadFailure::Fatal(error))
            };
        }

        let body = response.bytes().await?;
        let gzip_encoded = body.len() >= 2 && body[0] == 0x1f && body[1] == 0x8b;
        let workload = ParseWorkload::remote_chunk(
            body.len(),
            ctx.row_count,
            schema.len(),
            gzip_encoded,
            ctx.compressed_size,
            ctx.uncompressed_size,
        );

        parse_remote_chunk_result_table_async(
            schema,
            ctx.query_id,
            body,
            workload,
            ctx.blocking_parse_limiter,
        )
        .await
        .map_err(DownloadFailure::Fatal)
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
    use super::*;

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
}
