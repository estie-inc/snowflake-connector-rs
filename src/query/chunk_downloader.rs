use std::io::Read;
use std::time::Duration;

use flate2::bufread::GzDecoder;
use http::HeaderMap;
use reqwest::StatusCode;
use tokio::time::sleep;

use crate::{Error, Result};

use super::snapshot::RawPartitionRows;

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

impl From<std::io::Error> for DownloadFailure {
    fn from(e: std::io::Error) -> Self {
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

    pub(crate) async fn download(
        &self,
        url: &str,
        headers: &HeaderMap,
    ) -> Result<RawPartitionRows> {
        let mut retries = 0;
        loop {
            match self.download_once(url, headers.clone()).await {
                Ok(rows) => return Ok(rows),
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
    ) -> std::result::Result<RawPartitionRows, DownloadFailure> {
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
        if body.len() < 2 {
            return Err(DownloadFailure::Fatal(Error::ChunkDownload(
                "invalid chunk format".into(),
            )));
        }

        let bytes = if body[0] == 0x1f && body[1] == 0x8b {
            let mut d = GzDecoder::new(&body[..]);
            let mut buf = vec![];
            d.read_to_end(&mut buf)?;
            buf
        } else {
            body.to_vec()
        };

        let mut buf = vec![b'['];
        buf.extend(bytes);
        buf.push(b']');

        serde_json::from_slice(&buf).map_err(|e| {
            DownloadFailure::Fatal(Error::Json(e, String::from_utf8_lossy(&buf).into_owned()))
        })
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
