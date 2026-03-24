use std::io::Read;
use std::time::Duration;

use flate2::bufread::GzDecoder;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;

use crate::{Error, Result};

const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
const AES256: &str = "AES256";

/// Maximum number of retries for transient errors (network, 5xx, 429, 408).
const MAX_RETRIES: u32 = 7;

/// Initial backoff duration (doubles on each retry: 1s, 2s, 4s, 8s, 16s, 32s, 64s).
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);

/// Upper bound for the backoff duration.
const MAX_BACKOFF: Duration = Duration::from_secs(128);

/// Download a single result chunk from S3, retrying on transient errors.
///
/// Returns [`Error::ChunkUrlExpired`] when the presigned URL has expired (HTTP 403),
/// signalling the caller to refresh URLs via the Snowflake API and retry.
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

    let mut last_error: Option<Error> = None;

    for attempt in 0..=MAX_RETRIES {
        if attempt > 0 {
            let backoff_secs = INITIAL_BACKOFF
                .as_secs()
                .saturating_mul(2u64.pow(attempt - 1));
            let backoff = Duration::from_secs(backoff_secs.min(MAX_BACKOFF.as_secs()));
            tokio::time::sleep(backoff).await;
        }

        // Send the HTTP GET request to S3.
        let response = match client.get(&chunk_url).headers(headers.clone()).send().await {
            Ok(resp) => resp,
            Err(e) if is_retryable_reqwest_error(&e) => {
                last_error = Some(Error::Reqwest(e));
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        let status = response.status();

        // HTTP 403 from S3 means the presigned URL has expired.
        // Return immediately so the caller can refresh URLs and retry.
        if status == StatusCode::FORBIDDEN {
            return Err(Error::ChunkUrlExpired);
        }

        // Retryable server/throttle errors.
        if is_retryable_status(status) {
            let body = response.text().await.unwrap_or_default();
            last_error = Some(Error::ChunkDownload(format!("{status}: {body}")));
            continue;
        }

        // Other non-success codes are not retryable.
        if !status.is_success() {
            let body = response.text().await?;
            return Err(Error::ChunkDownload(body));
        }

        // Read the full response body.
        let body = match response.bytes().await {
            Ok(b) => b,
            Err(e) if is_retryable_reqwest_error(&e) => {
                last_error = Some(Error::Reqwest(e));
                continue;
            }
            Err(e) => return Err(e.into()),
        };

        if body.len() < 2 {
            return Err(Error::ChunkDownload("invalid chunk format".into()));
        }

        // Decompress gzip if needed (magic bytes 0x1f 0x8b).
        let bytes = if body[0] == 0x1f && body[1] == 0x8b {
            let mut d = GzDecoder::new(&body[..]);
            let mut buf = vec![];
            match d.read_to_end(&mut buf) {
                Ok(_) => buf,
                Err(e) => {
                    // Decompression failure may indicate a corrupted/truncated download.
                    last_error = Some(Error::IO(e));
                    continue;
                }
            }
        } else {
            body.to_vec()
        };

        // Parse JSON rows. Snowflake sends chunk data without the outer array brackets.
        let mut buf = vec![b'['];
        buf.extend(bytes);
        buf.push(b']');
        let rows: Vec<Vec<Option<String>>> = match serde_json::from_slice(&buf) {
            Ok(rows) => rows,
            Err(e) => {
                return Err(Error::Json(e, String::from_utf8_lossy(&buf).into_owned()));
            }
        };
        return Ok(rows);
    }

    Err(last_error.unwrap_or_else(|| Error::ChunkDownload("max retries exceeded".into())))
}

fn is_retryable_reqwest_error(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request() || e.is_body()
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
}
