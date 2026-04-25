use std::string::FromUtf8Error;

use reqwest::header::InvalidHeaderValue;
use tokio::task::JoinError;

/// An error that can occur when interacting with Snowflake.
///
/// Note: Errors may include sensitive information from Snowflake.
#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("HTTP client error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("communication error: {0}")]
    Communication(String),

    #[error("invalid header value: {0}")]
    InvalidHeader(#[from] InvalidHeaderValue),

    #[error("http error: {0}")]
    Http(#[from] http::Error),

    #[error("session expired")]
    SessionExpired,

    #[error("chunk download error: {0}")]
    ChunkDownload(String),

    #[error("io error: {0}")]
    IO(#[from] std::io::Error),

    #[error("json parse error: {0} {1}")]
    Json(serde_json::Error, String),

    #[error("utf-8 error: {0}")]
    Utf8Error(#[from] FromUtf8Error),

    #[error("future join error: {0}")]
    FutureJoin(#[from] JoinError),

    #[error("decode error: {0}")]
    Decode(String),

    #[error("decrypt error: {0}")]
    Decryption(#[from] pkcs8::Error),

    #[error("der error: {0}")]
    Der(#[from] pkcs8::spki::Error),

    #[error("jwt error: {0}")]
    JWT(#[from] jsonwebtoken::errors::Error),

    #[error("url error: {0}")]
    Url(String),

    #[error("unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("async response doesn't contain a URL to poll for results")]
    NoPollingUrlAsyncQuery,

    #[error("timed out waiting for query results")]
    TimedOut,

    /// The remaining result set can no longer be fetched: the backing lease
    /// expired and a fresh one could not be reconciled with the snapshot
    /// taken when the query was first executed. Callers should re-run the
    /// query to get a new `ResultSet`.
    ///
    /// Only surfaces when the experimental result-chunk refresh feature is
    /// enabled via
    /// [`SnowflakeQueryConfig::with_result_chunk_refresh`](crate::SnowflakeQueryConfig::with_result_chunk_refresh).
    /// With the default configuration this variant is never emitted.
    #[error("result set expired")]
    ResultSetExpired,
}

/// A `Result` alias where the `Err` case is `snowflake::Error`.
pub type Result<T> = std::result::Result<T, Error>;

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Error::Url(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_set_expired_display() {
        assert_eq!(Error::ResultSetExpired.to_string(), "result set expired");
    }

    #[test]
    fn existing_variant_display_messages_are_unchanged() {
        assert_eq!(Error::SessionExpired.to_string(), "session expired");
        assert_eq!(
            Error::TimedOut.to_string(),
            "timed out waiting for query results",
        );
        assert_eq!(
            Error::ChunkDownload("s3 said no".into()).to_string(),
            "chunk download error: s3 said no",
        );
    }
}
