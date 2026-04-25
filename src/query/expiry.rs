use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, Utc};
use url::Url;

use super::chunk_downloader::{DownloadFailure, is_retryable_status};
use super::snapshot::DownloadLocator;

/// Treat a presigned URL as expired when its expiry is within this much of
/// "now". Covers client/server clock skew plus time spent in the refresh
/// request itself.
const S3_EXPIRY_SKEW_BUFFER: Duration = Duration::from_secs(300);

const S3_PREDICTED_EXPIRED_REASON: &str = "predicted expired s3 presigned url";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeaseDisposition {
    ExpiredLease,
    RetryableTransport,
    Fatal,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LeaseExpiryClassifier;

impl LeaseExpiryClassifier {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn classify(&self, failure: &DownloadFailure) -> LeaseDisposition {
        match failure {
            DownloadFailure::PredictedExpired { .. } => LeaseDisposition::ExpiredLease,
            DownloadFailure::Transport { .. } => LeaseDisposition::RetryableTransport,
            DownloadFailure::Http { status, .. } => {
                if is_retryable_status(*status) {
                    LeaseDisposition::RetryableTransport
                } else {
                    LeaseDisposition::Fatal
                }
            }
            DownloadFailure::Decode { .. } => LeaseDisposition::Fatal,
        }
    }
}

/// Decide whether a locator's presigned URL is effectively expired before
/// issuing any HTTP call. Returns `Some(reason)` when the URL looks like an
/// S3 presigned URL whose `X-Amz-Date` + `X-Amz-Expires` put its expiry
/// within `S3_EXPIRY_SKEW_BUFFER` of the current clock.
///
/// Unrecognized shapes (missing fields, parse failures, overflow) return
/// `None` so the caller proceeds with a normal download attempt.
pub(crate) fn predict_expired_locator(locator: &DownloadLocator) -> Option<Arc<str>> {
    predict_expired_locator_at(locator, SystemTime::now(), S3_EXPIRY_SKEW_BUFFER)
}

/// Stricter variant of [`predict_expired_locator`] with no skew buffer:
/// returns `true` only when the URL's encoded expiry is at or before the
/// current clock. Used by the same-URL retry guard to distinguish "we have
/// no evidence the locator was rotated but it may still have a few minutes
/// of real validity" (buffer window, retryable) from "the URL itself says
/// it is already expired" (strictly past, must surface as
/// [`Error::ResultSetExpired`]).
pub(crate) fn is_strictly_expired_locator(locator: &DownloadLocator) -> bool {
    predict_expired_locator_at(locator, SystemTime::now(), Duration::ZERO).is_some()
}

fn predict_expired_locator_at(
    locator: &DownloadLocator,
    now: SystemTime,
    buffer: Duration,
) -> Option<Arc<str>> {
    let url = Url::parse(&locator.url).ok()?;

    let mut amz_date: Option<String> = None;
    let mut amz_expires: Option<String> = None;
    for (key, value) in url.query_pairs() {
        if key == "X-Amz-Date" {
            amz_date = Some(value.into_owned());
        } else if key == "X-Amz-Expires" {
            amz_expires = Some(value.into_owned());
        }
    }

    let amz_date = amz_date?;
    let amz_expires = amz_expires?;

    let expires_secs: i64 = amz_expires.parse().ok()?;
    let signed_naive = NaiveDateTime::parse_from_str(&amz_date, "%Y%m%dT%H%M%SZ").ok()?;
    let signed_at: DateTime<Utc> = signed_naive.and_utc();

    let expires_at_ts = signed_at.timestamp().checked_add(expires_secs)?;
    let now_ts: i64 = now
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_secs()
        .try_into()
        .ok()?;
    let buffer_secs: i64 = buffer.as_secs().try_into().ok()?;
    let threshold = now_ts.checked_add(buffer_secs)?;

    if expires_at_ts <= threshold {
        Some(Arc::from(S3_PREDICTED_EXPIRED_REASON))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use http::{HeaderMap, StatusCode};

    use crate::Error;

    fn locator(url: &str) -> DownloadLocator {
        DownloadLocator {
            url: url.to_string(),
            headers: Arc::new(HeaderMap::new()),
        }
    }

    /// An arbitrary fixed "now" so tests don't depend on `SystemTime::now()`.
    const NOW_TS: i64 = 1_700_000_000;

    fn fixed_now() -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(NOW_TS as u64)
    }

    fn format_amz_date(ts_secs: i64) -> String {
        DateTime::<Utc>::from_timestamp(ts_secs, 0)
            .expect("valid timestamp")
            .format("%Y%m%dT%H%M%SZ")
            .to_string()
    }

    #[test]
    fn predict_returns_expired_for_s3_url_past_buffer() {
        // Signed 1h ago with 1h expiry → expires_at == NOW, which is <= NOW + 300.
        let signed = format_amz_date(NOW_TS - 3_600);
        let url = format!(
            "https://bucket.s3.amazonaws.com/chunk?X-Amz-Date={signed}\
             &X-Amz-Expires=3600&X-Amz-Signature=deadbeef",
        );
        let got = predict_expired_locator_at(&locator(&url), fixed_now(), S3_EXPIRY_SKEW_BUFFER);
        assert_eq!(got.as_deref(), Some(S3_PREDICTED_EXPIRED_REASON));
    }

    #[test]
    fn predict_returns_expired_when_within_skew_buffer() {
        // Signed so that expires_at == NOW + 299s, which is <= NOW + 300.
        let signed = format_amz_date(NOW_TS - 3_301);
        let url = format!(
            "https://bucket.s3.amazonaws.com/chunk?X-Amz-Date={signed}\
             &X-Amz-Expires=3600",
        );
        let got = predict_expired_locator_at(&locator(&url), fixed_now(), S3_EXPIRY_SKEW_BUFFER);
        assert_eq!(got.as_deref(), Some(S3_PREDICTED_EXPIRED_REASON));
    }

    #[test]
    fn predict_returns_none_for_valid_s3_url() {
        // Signed at NOW with 1h expiry → 3600s > 300s buffer, not expired.
        let signed = format_amz_date(NOW_TS);
        let url = format!(
            "https://bucket.s3.amazonaws.com/chunk?X-Amz-Date={signed}\
             &X-Amz-Expires=3600",
        );
        assert_eq!(
            predict_expired_locator_at(&locator(&url), fixed_now(), S3_EXPIRY_SKEW_BUFFER),
            None,
        );
    }

    #[test]
    fn predict_returns_none_when_amz_date_missing() {
        let url = "https://bucket.s3.amazonaws.com/chunk?X-Amz-Expires=3600";
        assert_eq!(
            predict_expired_locator_at(&locator(url), fixed_now(), S3_EXPIRY_SKEW_BUFFER),
            None,
        );
    }

    #[test]
    fn predict_returns_none_when_amz_expires_missing() {
        let signed = format_amz_date(NOW_TS - 3_600);
        let url = format!("https://bucket.s3.amazonaws.com/chunk?X-Amz-Date={signed}");
        assert_eq!(
            predict_expired_locator_at(&locator(&url), fixed_now(), S3_EXPIRY_SKEW_BUFFER),
            None,
        );
    }

    #[test]
    fn predict_returns_none_on_parse_failure() {
        let url = "https://bucket.s3.amazonaws.com/chunk?X-Amz-Date=NOT-A-DATE&X-Amz-Expires=3600";
        assert_eq!(
            predict_expired_locator_at(&locator(url), fixed_now(), S3_EXPIRY_SKEW_BUFFER),
            None,
        );

        let url = "https://bucket.s3.amazonaws.com/chunk?X-Amz-Date=20250101T000000Z&X-Amz-Expires=notanumber";
        assert_eq!(
            predict_expired_locator_at(&locator(url), fixed_now(), S3_EXPIRY_SKEW_BUFFER),
            None,
        );
    }

    #[test]
    fn strict_predict_matches_on_past_expiry_but_not_on_buffer_window() {
        // Strictly past: signed 2h ago, 1h expiry → expires_at == NOW - 3600.
        let strict_past = format!(
            "https://bucket.s3.amazonaws.com/chunk?X-Amz-Date={signed}\
             &X-Amz-Expires=3600",
            signed = format_amz_date(NOW_TS - 7_200),
        );
        assert_eq!(
            predict_expired_locator_at(&locator(&strict_past), fixed_now(), Duration::ZERO)
                .as_deref(),
            Some(S3_PREDICTED_EXPIRED_REASON),
        );

        // Within buffer but not strictly past: expires_at == NOW + 200 (future).
        let buffer_window = format!(
            "https://bucket.s3.amazonaws.com/chunk?X-Amz-Date={signed}\
             &X-Amz-Expires=3600",
            signed = format_amz_date(NOW_TS - 3_400),
        );
        assert_eq!(
            predict_expired_locator_at(&locator(&buffer_window), fixed_now(), Duration::ZERO),
            None,
        );
    }

    #[test]
    fn classifier_truth_table() {
        let c = LeaseExpiryClassifier::new();
        let loc = locator("https://example.invalid/chunk");

        assert_eq!(
            c.classify(&DownloadFailure::PredictedExpired {
                locator: loc.clone(),
                reason: Arc::from("x"),
            }),
            LeaseDisposition::ExpiredLease,
        );

        assert_eq!(
            c.classify(&DownloadFailure::Transport {
                locator: loc.clone(),
                error: Error::ChunkDownload("socket".into()),
            }),
            LeaseDisposition::RetryableTransport,
        );

        assert_eq!(
            c.classify(&DownloadFailure::Http {
                locator: loc.clone(),
                status: StatusCode::SERVICE_UNAVAILABLE,
                headers: HeaderMap::new(),
                body_excerpt: String::new(),
            }),
            LeaseDisposition::RetryableTransport,
        );

        assert_eq!(
            c.classify(&DownloadFailure::Http {
                locator: loc.clone(),
                status: StatusCode::TOO_MANY_REQUESTS,
                headers: HeaderMap::new(),
                body_excerpt: String::new(),
            }),
            LeaseDisposition::RetryableTransport,
        );

        assert_eq!(
            c.classify(&DownloadFailure::Http {
                locator: loc.clone(),
                status: StatusCode::FORBIDDEN,
                headers: HeaderMap::new(),
                body_excerpt: String::new(),
            }),
            LeaseDisposition::Fatal,
        );

        assert_eq!(
            c.classify(&DownloadFailure::Decode {
                locator: loc,
                error: Error::Decode("bad json".into()),
            }),
            LeaseDisposition::Fatal,
        );
    }
}
