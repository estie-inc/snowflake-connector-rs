use std::string::FromUtf8Error;

use reqwest::header::InvalidHeaderValue;
use tokio::task::JoinError;

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

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),

    #[error("utf-8 error: {0}")]
    Utf8Error(#[from] FromUtf8Error),

    #[error("future join error: {0}")]
    FutureJoin(#[from] JoinError),
}

pub type Result<T> = std::result::Result<T, Error>;
