use std::borrow::Cow;

use crate::result::{ColumnIndex, ColumnType};

use super::{VALUE_PREVIEW_MAX_CHARS, truncate_preview_chars};

/// Cell decode failure with row/column context.
#[derive(Debug, Clone)]
pub struct CellDecodeError {
    row: usize,
    column: ColumnIndex,
    column_name: Box<str>,
    expected: Cow<'static, str>,
    actual: ColumnType,
    value_preview: Option<Box<str>>,
    reason: Box<str>,
}

impl CellDecodeError {
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
            value_preview: value_preview
                .map(|preview| truncate_preview_chars(preview, VALUE_PREVIEW_MAX_CHARS)),
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

impl std::fmt::Display for CellDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row {} column {:?} ({}): expected {}, found {:?}",
            self.row, self.column, self.column_name, self.expected, self.actual
        )?;
        if let Some(preview) = &self.value_preview {
            write!(f, ", value: {preview:?}")?;
        }
        if !self.reason.is_empty() {
            write!(f, " ({})", self.reason)?;
        }
        Ok(())
    }
}

impl std::error::Error for CellDecodeError {}
