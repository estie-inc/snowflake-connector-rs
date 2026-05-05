use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RowsetParseError {
    UnexpectedToken {
        offset: usize,
        expected: &'static str,
    },
    InvalidString {
        offset: usize,
        reason: Box<str>,
    },
    InvalidUnicodeEscape {
        offset: usize,
    },
    RowLengthMismatch {
        row: usize,
        expected: usize,
        actual: usize,
    },
    SpanOverflow {
        limit: u64,
        actual: u64,
        scope: &'static str,
    },
    CapacityOverflow,
}

impl fmt::Display for RowsetParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedToken { offset, expected } => {
                write!(
                    f,
                    "unexpected token at offset {offset}: expected {expected}"
                )
            }
            Self::InvalidString { offset, reason } => {
                write!(f, "invalid string at offset {offset}: {reason}")
            }
            Self::InvalidUnicodeEscape { offset } => {
                write!(f, "invalid unicode escape at offset {offset}")
            }
            Self::RowLengthMismatch {
                row,
                expected,
                actual,
            } => write!(
                f,
                "row length mismatch at row {row} (expected {expected}, actual {actual})"
            ),
            Self::SpanOverflow {
                limit,
                actual,
                scope,
            } => write!(
                f,
                "span overflow: {scope} exceeded {limit} bytes (actual {actual})"
            ),
            Self::CapacityOverflow => f.write_str("capacity overflow"),
        }
    }
}

impl std::error::Error for RowsetParseError {}
