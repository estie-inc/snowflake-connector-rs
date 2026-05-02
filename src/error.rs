use std::{borrow::Cow, string::FromUtf8Error};

use reqwest::header::InvalidHeaderValue;
use tokio::task::JoinError;

use crate::result::{ColumnIndex, ColumnType};

/// An error that can occur when interacting with Snowflake.
///
/// Note: Errors may include sensitive information from Snowflake.
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
    Decode(DecodeError),

    #[error("schema error: {0}")]
    Schema(SchemaError),

    #[error("parse error: {0}")]
    Parse(ParseError),

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
}

/// A `Result` alias where the `Err` case is `snowflake::Error`.
pub type Result<T> = std::result::Result<T, Error>;

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Error::Url(err.to_string())
    }
}

impl From<DecodeError> for Error {
    fn from(err: DecodeError) -> Self {
        Error::Decode(err)
    }
}

impl From<SchemaError> for Error {
    fn from(err: SchemaError) -> Self {
        Error::Schema(err)
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::Parse(err)
    }
}

const VALUE_PREVIEW_MAX_CHARS: usize = 128;

/// Cell decode failure with row/column context.
#[derive(Debug, Clone)]
pub struct DecodeError {
    row: usize,
    column: ColumnIndex,
    column_name: Box<str>,
    expected: Cow<'static, str>,
    actual: ColumnType,
    value_preview: Option<Box<str>>,
    reason: Box<str>,
}

impl DecodeError {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        row: usize,
        column: ColumnIndex,
        column_name: impl Into<Box<str>>,
        expected: impl Into<Cow<'static, str>>,
        actual: ColumnType,
        value_preview: Option<&str>,
        reason: impl Into<Box<str>>,
    ) -> Self {
        Self {
            row,
            column,
            column_name: column_name.into(),
            expected: expected.into(),
            actual,
            value_preview: value_preview.map(truncate_preview),
            reason: reason.into(),
        }
    }

    pub fn row(&self) -> usize {
        self.row
    }
    pub fn column(&self) -> ColumnIndex {
        self.column
    }
    pub fn column_name(&self) -> &str {
        &self.column_name
    }
    pub fn expected(&self) -> &str {
        &self.expected
    }
    pub fn actual(&self) -> &ColumnType {
        &self.actual
    }
    pub fn value_preview(&self) -> Option<&str> {
        self.value_preview.as_deref()
    }
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row {} column {:?} ({}): expected {}, found {:?}",
            self.row, self.column, self.column_name, self.expected, self.actual
        )?;
        if let Some(p) = &self.value_preview {
            write!(f, ", value: {p:?}")?;
        }
        if !self.reason.is_empty() {
            write!(f, " ({})", self.reason)?;
        }
        Ok(())
    }
}

fn truncate_preview(s: &str) -> Box<str> {
    if s.chars().count() <= VALUE_PREVIEW_MAX_CHARS {
        return Box::from(s);
    }
    let mut out = String::with_capacity(VALUE_PREVIEW_MAX_CHARS * 4 + 3);
    for ch in s.chars().take(VALUE_PREVIEW_MAX_CHARS) {
        out.push(ch);
    }
    out.push_str("...");
    out.into_boxed_str()
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SchemaError {
    #[error("missing column: {name}")]
    MissingColumn { name: Box<str> },
    #[error("ambiguous column: {name}")]
    AmbiguousColumn {
        name: Box<str>,
        candidates: Box<[ColumnIndex]>,
    },
    #[error("invalid column index {index:?} for schema with {len} columns")]
    InvalidColumnIndex { index: ColumnIndex, len: usize },
    #[error("duplicate column name in result: {name}")]
    DuplicateColumnName { name: Box<str> },
    #[error("column count mismatch (expected {expected}, actual {actual})")]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error("schema mismatch")]
    SchemaMismatch,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ParseError {
    #[error("unexpected token at offset {offset}: expected {expected}")]
    UnexpectedToken {
        offset: usize,
        expected: &'static str,
    },
    #[error("invalid string at offset {offset}: {reason}")]
    InvalidString { offset: usize, reason: Box<str> },
    #[error("invalid unicode escape at offset {offset}")]
    InvalidUnicodeEscape { offset: usize },
    #[error("row length mismatch at row {row} (expected {expected}, actual {actual})")]
    RowLengthMismatch {
        row: usize,
        expected: usize,
        actual: usize,
    },
    #[error("span overflow: {scope} exceeded {limit} bytes (actual {actual})")]
    SpanOverflow {
        limit: u64,
        actual: u64,
        scope: &'static str,
    },
    #[error("capacity overflow")]
    CapacityOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(value: usize) -> ColumnIndex {
        ColumnIndex::new(value).unwrap()
    }

    #[test]
    fn schema_error_display_formats_missing_and_ambiguous_variants() {
        assert_eq!(
            SchemaError::MissingColumn {
                name: Box::from("value"),
            }
            .to_string(),
            "missing column: value"
        );
        assert_eq!(
            SchemaError::AmbiguousColumn {
                name: Box::from("value"),
                candidates: vec![index(0), index(1)].into_boxed_slice(),
            }
            .to_string(),
            "ambiguous column: value"
        );
    }

    #[test]
    fn schema_error_display_formats_remaining_variants() {
        assert_eq!(
            SchemaError::InvalidColumnIndex {
                index: index(7),
                len: 3,
            }
            .to_string(),
            "invalid column index ColumnIndex(7) for schema with 3 columns"
        );
        assert_eq!(
            SchemaError::DuplicateColumnName {
                name: Box::from("id"),
            }
            .to_string(),
            "duplicate column name in result: id"
        );
        assert_eq!(
            SchemaError::ColumnCountMismatch {
                expected: 4,
                actual: 2,
            }
            .to_string(),
            "column count mismatch (expected 4, actual 2)"
        );
        assert_eq!(SchemaError::SchemaMismatch.to_string(), "schema mismatch");
    }
}
