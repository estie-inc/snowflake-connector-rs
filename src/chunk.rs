use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

use chrono::{NaiveDateTime, Utc};
use flate2::bufread::GzDecoder;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use tokio::time::sleep;
use url::Url;

use crate::{Error, Result};

/// Buffer before actual expiry to avoid the URL expiring mid-download.
const EXPIRY_BUFFER_SECS: i64 = 300;

const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
const AES256: &str = "AES256";

const MAX_RETRIES: usize = 7;
const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(16);

/// Internal chunk download error used for retry classification.
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
    if is_presigned_url_expired(&chunk_url) {
        return Err(Error::ChunkUrlExpired);
    }

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

/// Check whether an S3 V4 presigned URL has expired or will expire within
/// [`EXPIRY_BUFFER_SECS`] by parsing `X-Amz-Date` and `X-Amz-Expires`.
/// Returns `false` when the URL cannot be parsed so that the download is
/// attempted normally.
fn is_presigned_url_expired(url: &str) -> bool {
    let Some(parsed) = Url::parse(url).ok() else {
        return false;
    };
    let pairs: HashMap<_, _> = parsed.query_pairs().collect();
    let expiry = (|| {
        let signed_at =
            NaiveDateTime::parse_from_str(pairs.get("X-Amz-Date")?, "%Y%m%dT%H%M%SZ").ok()?;
        let secs: i64 = pairs.get("X-Amz-Expires")?.parse().ok()?;
        Some(signed_at.and_utc() + chrono::Duration::seconds(secs))
    })();
    expiry.is_some_and(|exp| Utc::now() + chrono::Duration::seconds(EXPIRY_BUFFER_SECS) >= exp)
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

    #[test]
    fn presigned_url_expired_in_the_past() {
        let url =
            "https://bucket.s3.amazonaws.com/key?X-Amz-Date=20200101T000000Z&X-Amz-Expires=3600";
        assert!(is_presigned_url_expired(url));
    }

    #[test]
    fn presigned_url_far_future_not_expired() {
        let url =
            "https://bucket.s3.amazonaws.com/key?X-Amz-Date=20990101T000000Z&X-Amz-Expires=86400";
        assert!(!is_presigned_url_expired(url));
    }

    #[test]
    fn presigned_url_realistic_snowflake_format() {
        // Mirrors the actual URL shape from Snowflake error logs.
        // X-Amz-Date=20260326T163806Z + X-Amz-Expires=21599 (≈6h)
        // Expired in the past relative to 2026-03-27, so this should be true.
        let url = "https://bucket.s3.us-west-2.amazonaws.com/data?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA&X-Amz-Date=20260326T163806Z&X-Amz-Expires=21599&X-Amz-SignedHeaders=host&X-Amz-Signature=abc";
        assert!(is_presigned_url_expired(url));
    }

    #[test]
    fn presigned_url_no_params_returns_false() {
        assert!(!is_presigned_url_expired(
            "https://bucket.s3.amazonaws.com/key"
        ));
    }

    #[test]
    fn presigned_url_invalid_date_returns_false() {
        let url = "https://bucket.s3.amazonaws.com/key?X-Amz-Date=invalid&X-Amz-Expires=3600";
        assert!(!is_presigned_url_expired(url));
    }

    #[tokio::test]
    async fn download_chunk_returns_chunk_url_expired_for_expired_url() {
        let client = reqwest::Client::new();
        let expired_url =
            "https://bucket.s3.amazonaws.com/key?X-Amz-Date=20200101T000000Z&X-Amz-Expires=3600"
                .to_string();
        let result = download_chunk(client, expired_url, HeaderMap::new(), String::new()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, crate::Error::ChunkUrlExpired),
            "expected ChunkUrlExpired, got: {err:?}"
        );
    }
}
