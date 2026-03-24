use std::io::Read;
use std::time::Duration;

use flate2::bufread::GzDecoder;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use tokio::time::sleep;

use crate::{Error, Result};

const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
const AES256: &str = "AES256";

const MAX_RETRIES: usize = 7;
const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(16);

/// Private error type for classifying retry behavior within chunk downloads,
/// without exposing transient-error details in the public `Error` enum.
enum DownloadError {
    Retryable(Error),
    Fatal(Error),
}

impl DownloadError {
    fn into_inner(self) -> Error {
        match self {
            Self::Retryable(e) | Self::Fatal(e) => e,
        }
    }
}

impl From<reqwest::Error> for DownloadError {
    fn from(e: reqwest::Error) -> Self {
        Self::Retryable(Error::Reqwest(e))
    }
}

impl From<std::io::Error> for DownloadError {
    fn from(e: std::io::Error) -> Self {
        Self::Retryable(Error::IO(e))
    }
}

pub(crate) async fn download_chunk(
    client: reqwest::Client,
    chunk_url: String,
    mut headers: HeaderMap,
    qrmk: String,
) -> Result<Vec<Vec<Option<String>>>> {
    if headers.is_empty() {
        headers.append(HEADER_SSE_C_ALGORITHM, AES256.parse()?);
        headers.append(HEADER_SSE_C_KEY, qrmk.parse()?);
    }

    let mut retries = 0;
    loop {
        match download_chunk_once(&client, &chunk_url, &headers).await {
            Ok(rows) => return Ok(rows),
            Err(DownloadError::Retryable(_)) if retries < MAX_RETRIES => {
                sleep(retry_delay(retries)).await;
                retries += 1;
            }
            Err(e) => return Err(e.into_inner()),
        }
    }
}

async fn download_chunk_once(
    client: &reqwest::Client,
    chunk_url: &str,
    headers: &HeaderMap,
) -> std::result::Result<Vec<Vec<Option<String>>>, DownloadError> {
    let response = client
        .get(chunk_url)
        .headers(headers.clone())
        .send()
        .await?;
    let status = response.status();

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let error = Error::ChunkDownload(body);
        return if is_retryable_status(status) {
            Err(DownloadError::Retryable(error))
        } else {
            Err(DownloadError::Fatal(error))
        };
    }

    let body = response.bytes().await?;
    if body.len() < 2 {
        return Err(DownloadError::Fatal(Error::ChunkDownload(
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
    let rows: Vec<Vec<Option<String>>> = match serde_json::from_slice(&buf) {
        Ok(rows) => rows,
        Err(e) => {
            return Err(DownloadError::Fatal(Error::Json(
                e,
                String::from_utf8_lossy(&buf).into_owned(),
            )));
        }
    };
    Ok(rows)
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
