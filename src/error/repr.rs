use std::{error::Error as StdError, sync::Arc};

use reqwest::header::InvalidHeaderValue;
use tokio::task::JoinError;

use super::{CellDecodeError, RowsetParseError, SchemaError};

#[derive(Debug)]
pub(crate) enum Repr {
    Config(ConfigError),
    Auth(AuthError),
    Network {
        error: NetworkError,
        query_id: Option<Arc<str>>,
    },
    Server(ServerError),
    SessionExpired(SessionExpiredError),
    Timeout {
        error: TimeoutError,
        query_id: Option<Arc<str>>,
    },
    Protocol {
        error: ProtocolError,
        query_id: Option<Arc<str>>,
    },
    Internal {
        error: InternalError,
        query_id: Option<Arc<str>>,
    },
    Other(Box<str>),
    Schema(SchemaError),
    CellDecode(CellDecodeError),
}

#[derive(Debug)]
pub(crate) enum ConfigError {
    InvalidUrl(Box<str>),
    HttpClientBuild(reqwest::Error),
}

#[derive(Debug)]
pub(crate) enum AuthError {
    LoginRejected {
        message: Option<Box<str>>,
    },
    KeyParse(Box<dyn StdError + Send + Sync>),
    DerParse(Box<dyn StdError + Send + Sync>),
    JwtSign(Box<dyn StdError + Send + Sync>),
    #[cfg(feature = "external-browser-sso")]
    ExternalBrowser {
        message: Box<str>,
        source: Option<Box<dyn StdError + Send + Sync>>,
    },
}

#[derive(Debug)]
pub(crate) enum NetworkError {
    Http(reqwest::Error),
    HttpStatus { status: u16, body: Box<str> },
    ChunkDownload { status: u16, body: Box<str> },
}

#[derive(Debug)]
pub(crate) struct ServerError {
    pub(crate) code: Option<Box<str>>,
    pub(crate) message: Option<Box<str>>,
    pub(crate) query_id: Option<Arc<str>>,
}

#[derive(Debug)]
pub(crate) struct SessionExpiredError {
    pub(crate) code: Option<Box<str>>,
    pub(crate) message: Option<Box<str>>,
    pub(crate) query_id: Option<Arc<str>>,
}

#[derive(Debug)]
pub(crate) enum TimeoutError {
    Request(reqwest::Error),
    Query,
    #[cfg(feature = "external-browser-sso")]
    BrowserCallback,
}

#[derive(Debug)]
pub(crate) enum ProtocolError {
    JsonParse {
        source: Box<dyn StdError + Send + Sync>,
        body_preview: Box<str>,
    },
    InvalidResponseUrl {
        path: &'static str,
        value_preview: Box<str>,
        source: url::ParseError,
    },
    InvalidField {
        path: &'static str,
        reason: Box<str>,
    },
    MissingField {
        field: &'static str,
    },
    HeaderConversion(http::Error),
    InvalidResponseHeaderValue(InvalidHeaderValue),
    NoPollingUrlAsyncQuery,
    UnsupportedResultFormat(Box<str>),
    InvalidChunkFormat {
        message: Box<str>,
        source: Option<Box<dyn StdError + Send + Sync>>,
    },
    RowsetParse(RowsetParseError),
}

#[derive(Debug)]
pub(crate) enum InternalError {
    FutureJoin(JoinError),
}
