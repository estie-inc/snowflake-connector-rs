pub(crate) mod decode;
mod display;
mod parse;
mod query_scoped;
mod repr;
mod schema;

use std::{error::Error as StdError, fmt, sync::Arc};

use reqwest::header::InvalidHeaderValue;
use tokio::task::JoinError;

pub use decode::{
    CellConversionError, CellConversionErrorBuilder, CellDecodeError, CellDecodeResult,
};
pub(crate) use parse::RowsetParseError;
pub(crate) use query_scoped::{QueryScopedError, QueryScopedRepr, QueryScopedResult};
use repr::Repr;
pub(crate) use repr::{
    AuthError, ConfigError, InternalError, NetworkError, ProtocolError, ServerError,
    SessionExpiredError, TimeoutError,
};
pub use schema::{
    AmbiguousColumnError, ColumnCountMismatchError, DuplicateColumnNameError,
    InvalidColumnIndexError, MissingColumnError, SchemaError,
};

const VALUE_PREVIEW_MAX_CHARS: usize = 128;
const JSON_BODY_PREVIEW_MAX_BYTES: usize = 1024;

/// An error that can occur when interacting with Snowflake.
///
/// Note: Errors may include sensitive information from Snowflake.
///
/// Use [`Error::kind`] for stable categorization, and [`std::error::Error::source`]
/// to inspect the underlying cause chain when one exists. The concrete source
/// type returned by downcasting is not covered by semver guarantees.
pub struct Error {
    repr: Box<Repr>,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct SourceDebug<'a>(Option<&'a (dyn StdError + 'static)>);

        impl fmt::Debug for SourceDebug<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    Some(source) => f.debug_tuple("Some").field(&source.to_string()).finish(),
                    None => f.write_str("None"),
                }
            }
        }

        f.debug_struct("Error")
            .field("kind", &self.kind())
            .field("message", &self.to_string())
            .field("source", &SourceDebug(StdError::source(self)))
            .finish()
    }
}

/// Semantic categories for [`Error`].
///
/// Variants are partitioned by the action a caller would take, not by the
/// internal type that produced the error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Client configuration or URL construction failed.
    Config,
    /// Authentication or session creation failed.
    Auth,
    /// HTTP or IO failures occurred while communicating with remote services.
    Network,
    /// Snowflake rejected the request and returned a server-side error message.
    Server,
    /// The current session token is no longer valid.
    SessionExpired,
    /// The connector timed out while waiting for a response.
    Timeout,
    /// The Snowflake protocol payload was malformed or unsupported.
    ///
    /// Includes the connector's internal `RowsetParseError` failures (chunk parser
    /// limits, malformed payload tokens). These are surfaced as `Protocol`
    /// rather than a separate kind because callers cannot recover from them.
    Protocol,
    /// Connector-internal runtime work failed, such as a cancelled or panicked task join.
    Internal,
    /// Client-side bind encoding or validation failed before the request was sent.
    BindEncode,
    /// Caller-supplied fallback error created via [`Error::other`].
    Other,
    /// The result schema or a cell value did not match the caller's
    /// expectations. Use [`Error::as_schema_error`] for column-lookup level
    /// mismatches and [`Error::as_cell_decode_error`] for cell decoding failures.
    Decode,
}

/// A `Result` alias where the `Err` case is [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    fn new(repr: Repr) -> Self {
        Self {
            repr: Box::new(repr),
        }
    }

    pub fn kind(&self) -> ErrorKind {
        match &*self.repr {
            Repr::Config(_) => ErrorKind::Config,
            Repr::Auth(_) => ErrorKind::Auth,
            Repr::Network { .. } => ErrorKind::Network,
            Repr::Server(_) => ErrorKind::Server,
            Repr::SessionExpired(_) => ErrorKind::SessionExpired,
            Repr::Timeout { .. } => ErrorKind::Timeout,
            Repr::Protocol { .. } => ErrorKind::Protocol,
            Repr::Internal { .. } => ErrorKind::Internal,
            Repr::BindEncode { .. } => ErrorKind::BindEncode,
            Repr::Other(_) => ErrorKind::Other,
            Repr::Schema(_) | Repr::CellDecode(_) => ErrorKind::Decode,
        }
    }

    /// Returns the Snowflake-provided message when one is available.
    pub fn snowflake_message(&self) -> Option<&str> {
        match &*self.repr {
            Repr::Auth(AuthError::LoginRejected { message }) => message.as_deref(),
            Repr::SessionExpired(SessionExpiredError { message, .. }) => message.as_deref(),
            Repr::Server(ServerError { message, .. }) => message.as_deref(),
            _ => None,
        }
    }

    /// Returns the Snowflake error code when one is available.
    pub fn snowflake_code(&self) -> Option<&str> {
        match &*self.repr {
            Repr::SessionExpired(SessionExpiredError { code, .. }) => code.as_deref(),
            Repr::Server(ServerError { code, .. }) => code.as_deref(),
            _ => None,
        }
    }

    /// Returns the related Snowflake query ID when the connector could extract
    /// one from the failure path.
    ///
    /// `None` does not mean "Snowflake did not assign a query ID". The
    /// accessor may return `None` when, for example, response parsing failed
    /// before the query ID could be extracted, or when another access path
    /// (a live `ResultCursor` / `ResultTable` receiver) was available to the caller.
    ///
    /// If an API that reliably exposes the query ID is available (e.g.
    /// `ResultCursor::query_id`), prefer that. Use this accessor as a best-effort fallback.
    pub fn query_id(&self) -> Option<&str> {
        match &*self.repr {
            Repr::Network { query_id, .. }
            | Repr::Timeout { query_id, .. }
            | Repr::Protocol { query_id, .. }
            | Repr::Internal { query_id, .. } => query_id.as_deref(),
            Repr::Server(ServerError { query_id, .. }) => query_id.as_deref(),
            Repr::SessionExpired(SessionExpiredError { query_id, .. }) => query_id.as_deref(),
            _ => None,
        }
    }

    pub fn is_session_expired(&self) -> bool {
        matches!(&*self.repr, Repr::SessionExpired(_))
    }

    pub fn is_timeout(&self) -> bool {
        matches!(&*self.repr, Repr::Timeout { .. })
    }

    pub fn is_server_error(&self) -> bool {
        matches!(&*self.repr, Repr::Server(_))
    }

    pub fn as_cell_decode_error(&self) -> Option<&CellDecodeError> {
        match &*self.repr {
            Repr::CellDecode(error) => Some(error),
            _ => None,
        }
    }

    pub fn as_schema_error(&self) -> Option<&SchemaError> {
        match &*self.repr {
            Repr::Schema(error) => Some(error),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn as_rowset_parse_error(&self) -> Option<&RowsetParseError> {
        match &*self.repr {
            Repr::Protocol {
                error: ProtocolError::RowsetParse(error),
                ..
            } => Some(error),
            _ => None,
        }
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self::new(Repr::Other(message.into().into_boxed_str()))
    }

    pub(crate) fn bind_encode(message: impl Into<String>) -> Self {
        Self::new(Repr::BindEncode {
            message: message.into().into_boxed_str(),
            source: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn bind_encode_with_source(
        message: impl Into<String>,
        source: impl Into<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self::new(Repr::BindEncode {
            message: message.into().into_boxed_str(),
            source: Some(source.into()),
        })
    }
}

pub(crate) fn classify_request_error(source: reqwest::Error) -> Error {
    if source.is_timeout() {
        TimeoutError::request(source).into()
    } else {
        NetworkError::request(source).into()
    }
}

impl From<ConfigError> for Error {
    fn from(error: ConfigError) -> Self {
        Self::new(Repr::Config(error))
    }
}

impl From<AuthError> for Error {
    fn from(error: AuthError) -> Self {
        Self::new(Repr::Auth(error))
    }
}

impl From<NetworkError> for Error {
    fn from(error: NetworkError) -> Self {
        Self::new(Repr::Network {
            error,
            query_id: None,
        })
    }
}

impl From<ServerError> for Error {
    fn from(error: ServerError) -> Self {
        Self::new(Repr::Server(error))
    }
}

impl From<SessionExpiredError> for Error {
    fn from(error: SessionExpiredError) -> Self {
        Self::new(Repr::SessionExpired(error))
    }
}

impl From<TimeoutError> for Error {
    fn from(error: TimeoutError) -> Self {
        Self::new(Repr::Timeout {
            error,
            query_id: None,
        })
    }
}

impl From<ProtocolError> for Error {
    fn from(error: ProtocolError) -> Self {
        Self::new(Repr::Protocol {
            error,
            query_id: None,
        })
    }
}

impl From<InternalError> for Error {
    fn from(error: InternalError) -> Self {
        Self::new(Repr::Internal {
            error,
            query_id: None,
        })
    }
}

impl From<CellDecodeError> for Error {
    fn from(error: CellDecodeError) -> Self {
        Self::new(Repr::CellDecode(error))
    }
}

impl From<SchemaError> for Error {
    fn from(error: SchemaError) -> Self {
        Self::new(Repr::Schema(error))
    }
}

impl From<RowsetParseError> for Error {
    fn from(error: RowsetParseError) -> Self {
        ProtocolError::RowsetParse(error).into()
    }
}

impl ConfigError {
    pub(crate) fn invalid_url(message: impl Into<String>) -> Self {
        Self::InvalidUrl(message.into().into_boxed_str())
    }

    pub(crate) fn client_builder_failure(source: reqwest::Error) -> Self {
        Self::HttpClientBuild(source)
    }
}

impl AuthError {
    pub(crate) fn login_rejected(message: Option<String>) -> Self {
        Self::LoginRejected {
            message: message.map(String::into_boxed_str),
        }
    }

    #[cfg(feature = "key-pair-auth")]
    pub(crate) fn key_parse(source: pkcs8::Error) -> Self {
        Self::KeyParse(Box::new(source))
    }

    #[cfg(feature = "key-pair-auth")]
    pub(crate) fn der_parse(source: pkcs8::spki::Error) -> Self {
        Self::DerParse(Box::new(source))
    }

    #[cfg(feature = "key-pair-auth")]
    pub(crate) fn jwt_sign(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::JwtSign(Box::new(source))
    }

    #[cfg(feature = "external-browser-sso")]
    pub(crate) fn external_browser(message: impl Into<String>) -> Self {
        Self::ExternalBrowser {
            message: message.into().into_boxed_str(),
            source: None,
        }
    }

    #[cfg(feature = "external-browser-sso")]
    pub(crate) fn external_browser_with_source(
        message: impl Into<String>,
        source: impl Into<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self::ExternalBrowser {
            message: message.into().into_boxed_str(),
            source: Some(source.into()),
        }
    }
}

impl NetworkError {
    pub(crate) fn request(source: reqwest::Error) -> Self {
        debug_assert!(
            !source.is_timeout(),
            "timeouts should be converted to TimeoutError before NetworkError",
        );
        Self::Http(source)
    }

    pub(crate) fn http_status(status: u16, body: impl AsRef<[u8]>) -> Self {
        Self::HttpStatus {
            status,
            body: truncate_preview_lossy_bytes(body.as_ref(), JSON_BODY_PREVIEW_MAX_BYTES),
        }
    }

    pub(crate) fn chunk_download(status: u16, body: impl AsRef<[u8]>) -> Self {
        Self::ChunkDownload {
            status,
            body: truncate_preview_lossy_bytes(body.as_ref(), JSON_BODY_PREVIEW_MAX_BYTES),
        }
    }
}

impl ServerError {
    pub(crate) fn new(
        code: Option<String>,
        message: Option<String>,
        query_id: Option<Arc<str>>,
    ) -> Self {
        Self {
            code: code.map(String::into_boxed_str),
            message: message.map(String::into_boxed_str),
            query_id,
        }
    }
}

impl SessionExpiredError {
    pub(crate) fn new(
        code: Option<String>,
        message: Option<String>,
        query_id: Option<Arc<str>>,
    ) -> Self {
        Self {
            code: code.map(String::into_boxed_str),
            message: message.map(String::into_boxed_str),
            query_id,
        }
    }
}

impl TimeoutError {
    pub(crate) fn request(source: reqwest::Error) -> Self {
        debug_assert!(
            source.is_timeout(),
            "only timeout errors should be converted to TimeoutError::Request"
        );
        Self::Request(source)
    }

    pub(crate) fn query() -> Self {
        Self::Query
    }

    #[cfg(feature = "external-browser-sso")]
    pub(crate) fn browser_callback() -> Self {
        Self::BrowserCallback
    }
}

impl ProtocolError {
    pub(crate) fn json_parse(source: serde_json::Error, body: impl AsRef<[u8]>) -> Self {
        Self::JsonParse {
            source: Box::new(source),
            body_preview: truncate_preview_lossy_bytes(body.as_ref(), JSON_BODY_PREVIEW_MAX_BYTES),
        }
    }

    pub(crate) fn invalid_response_url(
        path: &'static str,
        value: impl AsRef<str>,
        source: url::ParseError,
    ) -> Self {
        Self::InvalidResponseUrl {
            path,
            value_preview: truncate_preview_bytes(value.as_ref(), JSON_BODY_PREVIEW_MAX_BYTES),
            source,
        }
    }

    pub(crate) fn invalid_field(path: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidField {
            path,
            reason: reason.into().into_boxed_str(),
        }
    }

    pub(crate) fn missing_field(field: &'static str) -> Self {
        Self::MissingField { field }
    }

    pub(crate) fn header_conversion(source: http::Error) -> Self {
        Self::HeaderConversion(source)
    }

    pub(crate) fn invalid_response_header_value(source: InvalidHeaderValue) -> Self {
        Self::InvalidResponseHeaderValue(source)
    }

    pub(crate) fn no_polling_url() -> Self {
        Self::NoPollingUrlAsyncQuery
    }

    pub(crate) fn unsupported_result_format(message: impl Into<String>) -> Self {
        Self::UnsupportedResultFormat(message.into().into_boxed_str())
    }

    pub(crate) fn chunk_format(message: impl Into<String>) -> Self {
        Self::InvalidChunkFormat {
            message: message.into().into_boxed_str(),
            source: None,
        }
    }

    pub(crate) fn gzip_decode(source: std::io::Error) -> Self {
        Self::InvalidChunkFormat {
            message: Box::from("gzip decompression failed"),
            source: Some(Box::new(source)),
        }
    }
}

impl InternalError {
    pub(crate) fn future_join(source: JoinError) -> Self {
        Self::FutureJoin(source)
    }
}

pub(crate) fn truncate_preview_chars(input: &str, max_chars: usize) -> Box<str> {
    match input.char_indices().nth(max_chars) {
        Some((end, _)) => truncate_preview_at_byte(input, end),
        None => Box::from(input),
    }
}

fn truncate_preview_bytes(input: &str, max_bytes: usize) -> Box<str> {
    if input.len() <= max_bytes {
        return Box::from(input);
    }

    let mut end = max_bytes;
    while !input.is_char_boundary(end) {
        end -= 1;
    }

    truncate_preview_at_byte(input, end)
}

fn truncate_preview_at_byte(input: &str, end: usize) -> Box<str> {
    let mut out = String::with_capacity(end.saturating_add(3));
    out.push_str(&input[..end]);
    out.push_str("...");
    out.into_boxed_str()
}

fn truncate_preview_lossy_bytes(input: &[u8], max_bytes: usize) -> Box<str> {
    if input.len() <= max_bytes {
        return String::from_utf8_lossy(input).into_owned().into_boxed_str();
    }

    let mut out = String::from_utf8_lossy(&input[..max_bytes]).into_owned();
    out.push_str("...");
    out.into_boxed_str()
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<Error>();
    assert_send_sync::<CellDecodeError>();
    assert_send_sync::<SchemaError>();
};

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::net::TcpListener;

    use crate::result::{
        CellConversionError, CellDecodeResult, CellRef, ColumnType, FromCell,
        test_data::{make_result_table_from_rows, make_schema},
    };

    use super::*;

    #[derive(Debug)]
    struct NoSourceDecode;

    impl FromCell for NoSourceDecode {
        fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
            let _ = cell.required_raw()?;
            Err(CellConversionError::builder("bad value").build())
        }
    }

    #[derive(Debug)]
    struct WithSourceDecode;

    impl FromCell for WithSourceDecode {
        fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
            let raw = cell.required_raw()?;
            raw.parse::<u32>()
                .map(|_| Self)
                .map_err(|e| CellConversionError::builder("bad value").source(e).build())
        }
    }

    fn decode_error<T: FromCell>(value: &str) -> Error {
        let schema = make_schema(vec![(
            "COL".to_string(),
            ColumnType::Text { length: None },
            true,
        )]);
        let table =
            make_result_table_from_rows(schema, vec![vec![Some(value.to_string())]]).unwrap();
        match table.rows::<(T,)>().unwrap().next().unwrap() {
            Ok(_) => panic!("decode_error helper should produce an error"),
            Err(err) => err,
        }
    }

    #[test]
    fn error_accessors_expose_structured_details() {
        let err: Error = ServerError::new(
            Some("12345".to_string()),
            Some("statement failed".to_string()),
            Some(Arc::from("query-id")),
        )
        .into();

        assert_eq!(err.kind(), ErrorKind::Server);
        assert_eq!(err.snowflake_code(), Some("12345"));
        assert_eq!(err.snowflake_message(), Some("statement failed"));
        assert_eq!(err.query_id(), Some("query-id"));
        assert!(err.is_server_error());
        assert!(!err.is_timeout());
    }

    #[test]
    fn error_accessors_preserve_missing_message_fields() {
        let auth_err: Error = AuthError::login_rejected(None).into();
        assert_eq!(auth_err.kind(), ErrorKind::Auth);
        assert_eq!(auth_err.snowflake_message(), None);
        assert_eq!(auth_err.to_string(), "authentication rejected");

        let server_err: Error = ServerError::new(Some("390100".to_string()), None, None).into();
        assert_eq!(server_err.kind(), ErrorKind::Server);
        assert_eq!(server_err.snowflake_code(), Some("390100"));
        assert_eq!(server_err.snowflake_message(), None);
        assert_eq!(server_err.to_string(), "Snowflake server error 390100");
    }

    #[test]
    fn session_expired_preserves_snowflake_fields() {
        let err: Error = SessionExpiredError::new(
            Some("390112".to_string()),
            Some("Your session has expired. Please login again.".to_string()),
            None,
        )
        .into();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert!(err.is_session_expired());
        assert_eq!(err.snowflake_code(), Some("390112"));
        assert_eq!(
            err.snowflake_message(),
            Some("Your session has expired. Please login again.")
        );
        assert_eq!(err.query_id(), None);
        assert_eq!(err.to_string(), "session expired");
    }

    #[test]
    fn query_scoped_error_decorates_contextual_variants() {
        let err = Error::from(QueryScopedError::new(
            std::sync::Arc::from("query-id"),
            NetworkError::chunk_download(500, "boom"),
        ));
        assert_eq!(err.kind(), ErrorKind::Network);
        assert_eq!(err.query_id(), Some("query-id"));
    }

    #[test]
    fn query_scoped_error_keeps_intrinsic_server_query_id() {
        let err = Error::from(QueryScopedError::new(
            std::sync::Arc::from("outer-query-id"),
            ServerError::new(
                Some("390001".to_string()),
                Some("server boom".to_string()),
                Some(Arc::from("intrinsic-query-id")),
            ),
        ));

        assert_eq!(err.kind(), ErrorKind::Server);
        assert_eq!(err.query_id(), Some("intrinsic-query-id"));
    }

    #[test]
    fn session_expired_query_id_is_exposed_when_present() {
        let err: Error = SessionExpiredError::new(
            Some("390112".to_string()),
            Some("Your session has expired. Please login again.".to_string()),
            Some(Arc::from("query-id")),
        )
        .into();

        assert_eq!(err.kind(), ErrorKind::SessionExpired);
        assert_eq!(err.query_id(), Some("query-id"));
    }

    #[test]
    fn json_parse_truncates_large_body_preview() {
        let body = "x".repeat(JSON_BODY_PREVIEW_MAX_BYTES + 10);
        let err: Error = ProtocolError::json_parse(
            serde_json::from_str::<serde_json::Value>("{").unwrap_err(),
            body.as_bytes(),
        )
        .into();

        let rendered = err.to_string();
        assert!(rendered.contains("JSON parse error"));
        assert!(rendered.contains("..."));
    }

    #[test]
    fn bytes_preview_truncates_large_http_and_json_bodies_without_full_string_input() {
        let body = vec![b'x'; JSON_BODY_PREVIEW_MAX_BYTES + 10];

        let json_err: Error = ProtocolError::json_parse(
            serde_json::from_str::<serde_json::Value>("{").unwrap_err(),
            &body,
        )
        .into();
        assert!(json_err.to_string().contains("..."));

        let http_err: Error = NetworkError::http_status(500, &body).into();
        assert!(http_err.to_string().contains("..."));

        let chunk_err: Error = NetworkError::chunk_download(500, &body).into();
        assert!(chunk_err.to_string().contains("..."));
    }

    #[test]
    fn chunk_download_preview_handles_non_utf8_body_bytes() {
        let err: Error = NetworkError::chunk_download(500, [0xff, b'x']).into();

        assert_eq!(err.kind(), ErrorKind::Network);
        assert!(err.to_string().contains("x"));
        assert!(!err.to_string().contains("<failed to read response body"));
    }

    #[test]
    fn body_preview_truncates_by_bytes_on_utf8_boundary() {
        let body = "あ".repeat(JSON_BODY_PREVIEW_MAX_BYTES);
        let preview = truncate_preview_bytes(&body, JSON_BODY_PREVIEW_MAX_BYTES);
        let prefix = preview.strip_suffix("...").expect("truncated preview");

        assert_eq!(
            prefix.len(),
            JSON_BODY_PREVIEW_MAX_BYTES - (JSON_BODY_PREVIEW_MAX_BYTES % 'あ'.len_utf8())
        );
        assert!(preview.ends_with("..."));
    }

    #[test]
    fn other_errors_use_dedicated_kind_and_display_prefix() {
        let err = Error::other("boom");

        assert_eq!(err.kind(), ErrorKind::Other);
        assert_eq!(err.to_string(), "snowflake connector error: boom");
    }

    #[test]
    fn bind_encode_errors_have_dedicated_kind_display_and_optional_source() {
        let err = Error::bind_encode("bad bind");
        assert_eq!(err.kind(), ErrorKind::BindEncode);
        assert_eq!(err.to_string(), "bind encode error: bad bind");
        assert!(StdError::source(&err).is_none());

        let err = Error::bind_encode_with_source("bad bind", std::io::Error::other("boom"));
        assert_eq!(err.kind(), ErrorKind::BindEncode);
        assert_eq!(err.to_string(), "bind encode error: bad bind");
        assert!(StdError::source(&err).is_some());
    }

    #[test]
    fn chunk_format_is_protocol_kind() {
        let err: Error = ProtocolError::chunk_format("invalid chunk format").into();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "chunk download error: invalid chunk format"
        );
    }

    #[test]
    fn invalid_response_header_value_is_protocol_kind() {
        let source = "bad\nvalue"
            .parse::<reqwest::header::HeaderValue>()
            .unwrap_err();
        let err: Error = ProtocolError::invalid_response_header_value(source).into();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert!(StdError::source(&err).is_some());
    }

    #[test]
    fn invalid_response_url_preview_is_truncated() {
        let source = url::Url::parse("http://[::1").unwrap_err();
        let value = "x".repeat(JSON_BODY_PREVIEW_MAX_BYTES + 10);
        let err: Error =
            ProtocolError::invalid_response_url("data.getResultUrl", &value, source).into();

        let rendered = err.to_string();
        assert!(rendered.contains("data.getResultUrl"));
        assert!(rendered.contains("..."));
    }

    #[test]
    fn invalid_field_is_protocol_kind() {
        let err: Error = ProtocolError::invalid_field("data.ssoUrl", "must not be empty").into();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert_eq!(
            err.to_string(),
            "invalid Snowflake response field data.ssoUrl: must not be empty"
        );
    }

    #[test]
    fn schema_decode_and_parse_errors_do_not_repeat_as_sources() {
        let schema_error: Error = SchemaError::MissingColumn(MissingColumnError::new("col")).into();
        assert!(StdError::source(&schema_error).is_none());

        let decode_error = decode_error::<NoSourceDecode>("x");
        assert!(StdError::source(&decode_error).is_none());

        let parse_error: Error = RowsetParseError::CapacityOverflow.into();
        assert_eq!(parse_error.kind(), ErrorKind::Protocol);
        assert!(StdError::source(&parse_error).is_none());
    }

    #[test]
    fn schema_and_cell_decode_share_decode_kind() {
        let schema_error: Error = SchemaError::MissingColumn(MissingColumnError::new("col")).into();
        assert_eq!(schema_error.kind(), ErrorKind::Decode);
        assert!(schema_error.as_schema_error().is_some());
        assert!(schema_error.as_cell_decode_error().is_none());

        let decode_error = decode_error::<NoSourceDecode>("x");
        assert_eq!(decode_error.kind(), ErrorKind::Decode);
        assert!(decode_error.as_cell_decode_error().is_some());
        assert!(decode_error.as_schema_error().is_none());
    }

    #[test]
    fn cell_decode_errors_expose_issue_then_underlying_source() {
        let err = decode_error::<WithSourceDecode>("x");
        let decode = err
            .as_cell_decode_error()
            .expect("typed row decode should expose a CellDecodeError");

        assert_eq!(decode.issue().reason(), "bad value");

        let issue =
            StdError::source(decode).expect("decode error should expose CellConversionError");
        assert_eq!(issue.to_string(), "bad value");
        assert!(StdError::source(issue).is_some());

        let top =
            StdError::source(&err).expect("top-level Error should expose CellConversionError");
        assert_eq!(top.to_string(), "bad value");
        assert!(StdError::source(top).is_some());
        assert_eq!(
            StdError::source(&err).map(|source| source.to_string()),
            StdError::source(decode).map(|source| source.to_string())
        );
    }

    #[test]
    fn parse_error_is_classified_as_protocol() {
        let err: Error = RowsetParseError::CapacityOverflow.into();

        assert_eq!(err.kind(), ErrorKind::Protocol);
        assert!(err.as_rowset_parse_error().is_some());
    }

    #[tokio::test]
    async fn future_join_uses_internal_kind() {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();

        let err: Error = InternalError::future_join(handle.await.unwrap_err()).into();
        assert_eq!(err.kind(), ErrorKind::Internal);
    }

    #[tokio::test]
    async fn classify_request_error_uses_timeout_kind() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(50))
            .build()
            .unwrap();
        let err = client
            .get(format!("http://{addr}/"))
            .send()
            .await
            .unwrap_err();

        assert!(err.is_timeout());

        let err = classify_request_error(err);
        assert_eq!(err.kind(), ErrorKind::Timeout);
        assert!(err.is_timeout());
        assert!(StdError::source(&err).is_some());
        assert_eq!(err.to_string(), "network request timed out");

        server.abort();
        let _ = server.await;
    }
}
