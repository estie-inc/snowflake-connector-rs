use std::borrow::Cow;

use crate::result::{ColumnIndex, ColumnType};

use super::{VALUE_PREVIEW_MAX_CHARS, truncate_preview_chars};

/// Result alias for cell-local decode failures.
pub type CellDecodeResult<T> = std::result::Result<T, CellDecodeIssue>;

/// Cell-local reason why decoding a value failed.
///
/// This describes only the local conversion problem. Row, column, and
/// value context live on [`CellDecodeError`].
#[derive(Debug)]
pub struct CellDecodeIssue {
    reason: Box<str>,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl CellDecodeIssue {
    pub fn builder(reason: impl Into<Box<str>>) -> CellDecodeIssueBuilder {
        CellDecodeIssueBuilder {
            reason: reason.into(),
            source: None,
        }
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn source(&self) -> Option<&(dyn std::error::Error + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}

impl std::fmt::Display for CellDecodeIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for CellDecodeIssue {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

#[derive(Debug)]
pub struct CellDecodeIssueBuilder {
    reason: Box<str>,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl CellDecodeIssueBuilder {
    pub fn source(
        mut self,
        source: impl Into<Box<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn build(self) -> CellDecodeIssue {
        CellDecodeIssue {
            reason: self.reason,
            source: self.source,
        }
    }
}

/// Cell decode failure with row and column context.
#[derive(Debug)]
pub struct CellDecodeError {
    row_index: usize,
    column_index: ColumnIndex,
    column_name: Box<str>,
    target_type_name: Cow<'static, str>,
    actual_column_type: ColumnType,
    raw_value_preview: Option<Box<str>>,
    issue: CellDecodeIssue,
}

impl CellDecodeError {
    pub(crate) fn new(
        row_index: usize,
        column_index: ColumnIndex,
        column_name: impl Into<Box<str>>,
        target_type_name: impl Into<Cow<'static, str>>,
        actual_column_type: ColumnType,
        raw_value_preview: Option<&str>,
        issue: CellDecodeIssue,
    ) -> Self {
        Self {
            row_index,
            column_index,
            column_name: column_name.into(),
            target_type_name: target_type_name.into(),
            actual_column_type,
            raw_value_preview: raw_value_preview
                .map(|preview| truncate_preview_chars(preview, VALUE_PREVIEW_MAX_CHARS)),
            issue,
        }
    }

    pub fn row_index(&self) -> usize {
        self.row_index
    }

    pub fn column_index(&self) -> ColumnIndex {
        self.column_index
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn target_type_name(&self) -> &str {
        &self.target_type_name
    }

    pub fn actual_column_type(&self) -> &ColumnType {
        &self.actual_column_type
    }

    pub fn raw_value_preview(&self) -> Option<&str> {
        self.raw_value_preview.as_deref()
    }

    pub fn issue(&self) -> &CellDecodeIssue {
        &self.issue
    }
}

impl std::fmt::Display for CellDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "row_index {} column_index {:?} ({}): target_type {}, found {:?}",
            self.row_index,
            self.column_index,
            self.column_name,
            self.target_type_name,
            self.actual_column_type
        )?;
        if let Some(preview) = &self.raw_value_preview {
            write!(f, ", value: {preview:?}")?;
        }
        if !self.issue.reason().is_empty() {
            write!(f, " ({})", self.issue.reason())?;
        }
        Ok(())
    }
}

impl std::error::Error for CellDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.issue
            .source()
            .map(|_| &self.issue as &(dyn std::error::Error + 'static))
    }
}
