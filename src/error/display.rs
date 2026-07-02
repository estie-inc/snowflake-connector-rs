use std::{error::Error as StdError, fmt};

use super::Error;
use super::repr::{
    AuthError, ConfigError, InternalError, NetworkError, ProtocolError, Repr, ServerError,
    TimeoutError,
};

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.repr {
            Repr::Config(ConfigError::InvalidUrl(message)) => write!(f, "invalid URL: {message}"),
            Repr::Config(ConfigError::HttpClientBuild(_source)) => {
                f.write_str("HTTP client build error")
            }
            Repr::Auth(AuthError::LoginRejected {
                message: Some(message),
            }) => write!(f, "authentication rejected: {message}"),
            Repr::Auth(AuthError::LoginRejected { message: None }) => {
                f.write_str("authentication rejected")
            }
            #[cfg(feature = "key-pair-auth")]
            Repr::Auth(AuthError::KeyParse(_source)) => f.write_str("private key error"),
            #[cfg(feature = "key-pair-auth")]
            Repr::Auth(AuthError::DerParse(_source)) => f.write_str("DER error"),
            #[cfg(feature = "key-pair-auth")]
            Repr::Auth(AuthError::JwtSign(_source)) => f.write_str("JWT error"),
            #[cfg(feature = "external-browser-sso")]
            Repr::Auth(AuthError::ExternalBrowser { message, .. }) => {
                write!(f, "external browser authentication error: {message}")
            }
            Repr::Network {
                error: NetworkError::Http(_source),
                ..
            } => f.write_str("network error"),
            Repr::Network {
                error:
                    NetworkError::HttpStatus {
                        status,
                        body_preview,
                    },
                ..
            } => {
                write!(f, "HTTP {status}: {body_preview}")
            }
            Repr::Network {
                error:
                    NetworkError::ChunkDownload {
                        status,
                        body_preview,
                    },
                ..
            } => {
                write!(
                    f,
                    "chunk download failed with HTTP {status}: {body_preview}"
                )
            }
            Repr::Server(ServerError {
                code,
                message,
                query_id,
            }) => {
                match (code.as_deref(), message.as_deref()) {
                    (Some(code), Some(message)) => {
                        write!(f, "Snowflake server error {code}: {message}")?
                    }
                    (Some(code), None) => write!(f, "Snowflake server error {code}")?,
                    (None, Some(message)) => write!(f, "Snowflake server error: {message}")?,
                    (None, None) => f.write_str("Snowflake server error")?,
                }
                if let Some(query_id) = query_id {
                    write!(f, " (query id: {query_id})")?;
                }
                Ok(())
            }
            Repr::SessionExpired(_) => f.write_str("session expired"),
            Repr::Timeout {
                error: TimeoutError::Request(_),
                ..
            } => f.write_str("network request timed out"),
            Repr::Timeout {
                error: TimeoutError::Query,
                ..
            } => f.write_str("timed out waiting for query results"),
            #[cfg(feature = "external-browser-sso")]
            Repr::Timeout {
                error: TimeoutError::BrowserCallback,
                ..
            } => f.write_str("timed out waiting for external browser callback"),
            Repr::Protocol {
                error:
                    ProtocolError::JsonParse {
                        source: _source,
                        body_preview,
                    },
                ..
            } => write!(f, "JSON parse error; body preview: {body_preview:?}"),
            Repr::Protocol {
                error:
                    ProtocolError::InvalidResponseUrl {
                        path,
                        value_preview,
                        source: _source,
                    },
                ..
            } => write!(
                f,
                "invalid URL in Snowflake response field {path}; value: {value_preview}"
            ),
            Repr::Protocol {
                error: ProtocolError::InvalidField { path, reason },
                ..
            } => {
                write!(f, "invalid Snowflake response field {path}: {reason}")
            }
            Repr::Protocol {
                error: ProtocolError::MissingField { field },
                ..
            } => {
                write!(f, "missing required field in Snowflake response: {field}")
            }
            Repr::Protocol {
                error: ProtocolError::HeaderConversion(_source),
                ..
            } => f.write_str("header conversion error"),
            Repr::Protocol {
                error: ProtocolError::InvalidResponseHeaderValue(_source),
                ..
            } => f.write_str("invalid header value in Snowflake response"),
            Repr::Protocol {
                error: ProtocolError::NoPollingUrlAsyncQuery,
                ..
            } => f.write_str("async response doesn't contain a URL to poll for results"),
            Repr::Protocol {
                error: ProtocolError::UnsupportedResultFormat(message),
                ..
            } => {
                write!(f, "unsupported result format: {message}")
            }
            Repr::Protocol {
                error:
                    ProtocolError::InvalidChunkFormat {
                        message,
                        source: Some(_source),
                    },
                ..
            } => {
                write!(f, "chunk download error: {message}")
            }
            Repr::Protocol {
                error:
                    ProtocolError::InvalidChunkFormat {
                        message,
                        source: None,
                    },
                ..
            } => {
                write!(f, "chunk download error: {message}")
            }
            Repr::Protocol {
                error: ProtocolError::RowsetParse(error),
                ..
            } => fmt::Display::fmt(error, f),
            Repr::Internal {
                error: InternalError::FutureJoin(_source),
                ..
            } => f.write_str("future join error"),
            Repr::BindEncode { message, .. } => write!(f, "bind encode error: {message}"),
            Repr::Other(message) => write!(f, "snowflake connector error: {message}"),
            Repr::Schema(error) => fmt::Display::fmt(error, f),
            Repr::CellDecode(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &*self.repr {
            Repr::Config(ConfigError::HttpClientBuild(source)) => Some(source),
            #[cfg(feature = "key-pair-auth")]
            Repr::Auth(AuthError::KeyParse(source)) => Some(source.as_ref()),
            #[cfg(feature = "key-pair-auth")]
            Repr::Auth(AuthError::DerParse(source)) => Some(source.as_ref()),
            #[cfg(feature = "key-pair-auth")]
            Repr::Auth(AuthError::JwtSign(source)) => Some(source.as_ref()),
            #[cfg(feature = "external-browser-sso")]
            Repr::Auth(AuthError::ExternalBrowser {
                source: Some(source),
                ..
            }) => Some(source.as_ref()),
            Repr::Timeout {
                error: TimeoutError::Request(source),
                ..
            } => Some(source),
            Repr::Network {
                error: NetworkError::Http(source),
                ..
            } => Some(source),
            Repr::Protocol {
                error: ProtocolError::JsonParse { source, .. },
                ..
            } => Some(source.as_ref()),
            Repr::Protocol {
                error: ProtocolError::InvalidResponseUrl { source, .. },
                ..
            } => Some(source),
            Repr::Protocol {
                error: ProtocolError::HeaderConversion(source),
                ..
            } => Some(source),
            Repr::Protocol {
                error: ProtocolError::InvalidResponseHeaderValue(source),
                ..
            } => Some(source),
            Repr::Protocol {
                error:
                    ProtocolError::InvalidChunkFormat {
                        source: Some(source),
                        ..
                    },
                ..
            } => Some(source.as_ref()),
            Repr::Internal {
                error: InternalError::FutureJoin(source),
                ..
            } => Some(source),
            Repr::BindEncode {
                source: Some(source),
                ..
            } => Some(source.as_ref()),
            Repr::Protocol {
                error: ProtocolError::RowsetParse(_),
                ..
            } => None,
            Repr::Schema(_) => None,
            Repr::CellDecode(error) => StdError::source(error),
            _ => None,
        }
    }
}
