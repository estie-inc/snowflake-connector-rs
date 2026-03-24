use std::io::Read;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use flate2::bufread::GzDecoder;
use reqwest::StatusCode;
use reqwest::header::HeaderMap;

use crate::{Error, Result};

const HEADER_SSE_C_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
const HEADER_SSE_C_KEY: &str = "x-amz-server-side-encryption-customer-key";
const AES256: &str = "AES256";

/// Download a single result chunk from cloud storage with retry on transient errors.
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

    (|| async { download_chunk_once(&client, &chunk_url, &headers).await })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_secs(1))
                .with_max_delay(Duration::from_secs(16))
                .with_max_times(7)
                .with_jitter(),
        )
        .when(is_retryable)
        .await
}

/// Single attempt to download and parse a chunk.
async fn download_chunk_once(
    client: &reqwest::Client,
    chunk_url: &str,
    headers: &HeaderMap,
) -> Result<Vec<Vec<Option<String>>>> {
    let response = client
        .get(chunk_url)
        .headers(headers.clone())
        .send()
        .await?;
    let status = response.status();

    if is_retryable_status(status) {
        let body = response.text().await.unwrap_or_default();
        return Err(Error::ChunkDownloadTransient {
            status: status.as_u16(),
            body,
        });
    }

    if !status.is_success() {
        let body = response.text().await?;
        return Err(Error::ChunkDownload(body));
    }

    let body = response.bytes().await?;
    if body.len() < 2 {
        return Err(Error::ChunkDownload("invalid chunk format".into()));
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
            return Err(Error::Json(e, String::from_utf8_lossy(&buf).into_owned()));
        }
    };
    Ok(rows)
}

fn is_retryable(e: &Error) -> bool {
    matches!(
        e,
        Error::ChunkDownloadTransient { .. } | Error::IO(_) | Error::Reqwest(_)
    )
}

/// 5xx, 408, 429, 403 are retryable.
/// 403 can be a transient S3 error (rate limiting, clock skew), not just URL expiry.
fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::FORBIDDEN
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
        assert!(is_retryable_status(StatusCode::FORBIDDEN));

        assert!(!is_retryable_status(StatusCode::OK));
        assert!(!is_retryable_status(StatusCode::NOT_FOUND));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
    }
}
