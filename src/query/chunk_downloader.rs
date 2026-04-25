use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use flate2::bufread::GzDecoder;
use http::HeaderMap;
use reqwest::StatusCode;
use tokio::time::sleep;

use crate::Error;

use super::snapshot::{DownloadLocator, RawPartitionRows};

const MAX_RETRIES: usize = 7;
const MIN_RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(16);

/// Structured failure from a single download attempt. Each variant carries the
/// `DownloadLocator` under which the failure occurred so a refresh-aware
/// classifier can reason about the surrounding state.
#[derive(Debug)]
pub(crate) enum DownloadFailure {
    PredictedExpired {
        #[allow(dead_code)]
        locator: DownloadLocator,
        reason: Arc<str>,
    },
    Http {
        #[allow(dead_code)]
        locator: DownloadLocator,
        status: StatusCode,
        #[allow(dead_code)]
        headers: HeaderMap,
        body_excerpt: String,
    },
    Transport {
        #[allow(dead_code)]
        locator: DownloadLocator,
        error: Error,
    },
    Decode {
        #[allow(dead_code)]
        locator: DownloadLocator,
        error: Error,
    },
}

impl From<DownloadFailure> for Error {
    fn from(failure: DownloadFailure) -> Self {
        match failure {
            DownloadFailure::PredictedExpired { reason, .. } => {
                Error::ChunkDownload(reason.to_string())
            }
            DownloadFailure::Http { body_excerpt, .. } => Error::ChunkDownload(body_excerpt),
            DownloadFailure::Transport { error, .. } => error,
            DownloadFailure::Decode { error, .. } => error,
        }
    }
}

fn is_retryable_failure(failure: &DownloadFailure) -> bool {
    match failure {
        DownloadFailure::Transport { .. } => true,
        DownloadFailure::Http { status, .. } => is_retryable_status(*status),
        DownloadFailure::PredictedExpired { .. } | DownloadFailure::Decode { .. } => false,
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
        locator: &DownloadLocator,
    ) -> std::result::Result<RawPartitionRows, DownloadFailure> {
        let mut retries = 0;
        loop {
            match self.download_once(locator.clone()).await {
                Ok(rows) => return Ok(rows),
                Err(failure) if retries < MAX_RETRIES && is_retryable_failure(&failure) => {
                    sleep(retry_delay(retries)).await;
                    retries += 1;
                }
                Err(failure) => return Err(failure),
            }
        }
    }

    async fn download_once(
        &self,
        locator: DownloadLocator,
    ) -> std::result::Result<RawPartitionRows, DownloadFailure> {
        let response = match self
            .client
            .get(&locator.url)
            .headers((*locator.headers).clone())
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return Err(DownloadFailure::Transport {
                    locator,
                    error: Error::Reqwest(e),
                });
            }
        };
        let status = response.status();

        if !status.is_success() {
            let headers = response.headers().clone();
            let body_excerpt = response.text().await.unwrap_or_default();
            return Err(DownloadFailure::Http {
                locator,
                status,
                headers,
                body_excerpt,
            });
        }

        let body = match response.bytes().await {
            Ok(b) => b,
            Err(e) => {
                return Err(DownloadFailure::Transport {
                    locator,
                    error: Error::Reqwest(e),
                });
            }
        };

        if body.len() < 2 {
            return Err(DownloadFailure::Decode {
                locator,
                error: Error::ChunkDownload("invalid chunk format".into()),
            });
        }

        let bytes = if body[0] == 0x1f && body[1] == 0x8b {
            let mut d = GzDecoder::new(&body[..]);
            let mut buf = vec![];
            if let Err(e) = d.read_to_end(&mut buf) {
                return Err(DownloadFailure::Decode {
                    locator,
                    error: Error::IO(e),
                });
            }
            buf
        } else {
            body.to_vec()
        };

        let mut buf = vec![b'['];
        buf.extend(bytes);
        buf.push(b']');

        serde_json::from_slice(&buf).map_err(|e| DownloadFailure::Decode {
            locator,
            error: Error::Json(e, String::from_utf8_lossy(&buf).into_owned()),
        })
    }
}

pub(crate) fn is_retryable_status(status: StatusCode) -> bool {
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

    fn dummy_locator() -> DownloadLocator {
        DownloadLocator {
            url: "https://example.invalid/chunk".into(),
            headers: Arc::new(HeaderMap::new()),
        }
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
    fn retryable_failure_truth_table() {
        let locator = dummy_locator();

        let transport = DownloadFailure::Transport {
            locator: locator.clone(),
            error: Error::ChunkDownload("socket closed".into()),
        };
        assert!(is_retryable_failure(&transport));

        let retryable_http = DownloadFailure::Http {
            locator: locator.clone(),
            status: StatusCode::SERVICE_UNAVAILABLE,
            headers: HeaderMap::new(),
            body_excerpt: String::new(),
        };
        assert!(is_retryable_failure(&retryable_http));

        let non_retryable_http = DownloadFailure::Http {
            locator: locator.clone(),
            status: StatusCode::FORBIDDEN,
            headers: HeaderMap::new(),
            body_excerpt: String::new(),
        };
        assert!(!is_retryable_failure(&non_retryable_http));

        let decode = DownloadFailure::Decode {
            locator: locator.clone(),
            error: Error::ChunkDownload("bad format".into()),
        };
        assert!(!is_retryable_failure(&decode));

        let predicted = DownloadFailure::PredictedExpired {
            locator,
            reason: Arc::from("classifier said so"),
        };
        assert!(!is_retryable_failure(&predicted));
    }

    #[test]
    fn download_failure_maps_to_error() {
        let locator = dummy_locator();

        let predicted: Error = DownloadFailure::PredictedExpired {
            locator: locator.clone(),
            reason: Arc::from("predicted expired reason"),
        }
        .into();
        match predicted {
            Error::ChunkDownload(msg) => assert_eq!(msg, "predicted expired reason"),
            other => panic!("unexpected: {other:?}"),
        }

        let http: Error = DownloadFailure::Http {
            locator: locator.clone(),
            status: StatusCode::FORBIDDEN,
            headers: HeaderMap::new(),
            body_excerpt: "s3 said no".into(),
        }
        .into();
        match http {
            Error::ChunkDownload(msg) => assert_eq!(msg, "s3 said no"),
            other => panic!("unexpected: {other:?}"),
        }

        let transport: Error = DownloadFailure::Transport {
            locator: locator.clone(),
            error: Error::ChunkDownload("wrapped transport".into()),
        }
        .into();
        match transport {
            Error::ChunkDownload(msg) => assert_eq!(msg, "wrapped transport"),
            other => panic!("unexpected: {other:?}"),
        }

        let decode: Error = DownloadFailure::Decode {
            locator,
            error: Error::Decode("bad json".into()),
        }
        .into();
        match decode {
            Error::Decode(msg) => assert_eq!(msg, "bad json"),
            other => panic!("unexpected: {other:?}"),
        }
    }

}
