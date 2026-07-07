use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{self, Display},
};

use crate::result_table::{ColumnIndex, ColumnType};

use super::{SchemaError, VALUE_PREVIEW_MAX_CHARS, truncate_preview_chars};

/// Result alias for cell-local decode failures.
pub type CellDecodeResult<T> = std::result::Result<T, CellConversionError>;

/// Cell-local reason why decoding a value failed.
///
/// This describes only the local conversion problem. Row, column, and value context live on [`CellDecodeError`].
#[derive(Debug)]
pub struct CellConversionError {
    reason: Box<str>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl CellConversionError {
    /// Build a cell error with just a reason, equivalent to `builder(reason).build()`.
    pub fn new(reason: impl Into<Box<str>>) -> Self {
        Self::builder(reason).build()
    }

    pub fn builder(reason: impl Into<Box<str>>) -> CellConversionErrorBuilder {
        CellConversionErrorBuilder {
            reason: reason.into(),
            source: None,
        }
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn source(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.source.as_deref()
    }
}

impl Display for CellConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl StdError for CellConversionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

#[derive(Debug)]
pub struct CellConversionErrorBuilder {
    reason: Box<str>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl CellConversionErrorBuilder {
    pub fn source(mut self, source: impl Into<Box<dyn StdError + Send + Sync + 'static>>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn build(self) -> CellConversionError {
        CellConversionError {
            reason: self.reason,
            source: self.source,
        }
    }
}

/// Plan-time decode failure raised by a hand-written [`FromCell::build_plan`](crate::FromCell::build_plan)
/// or [`FromRow::build_plan`](crate::FromRow::build_plan).
///
/// Use this for custom validation of column metadata or row shape. The connector's structured schema mismatches still
/// return [`SchemaError`](crate::error::SchemaError).
///
/// Callers provide only a reason and optional source. The connector adds column context for failures returned through
/// [`CellPlan::new`](crate::CellPlan); row-level plan failures keep no column context.
#[derive(Debug)]
pub struct CustomPlanError {
    reason: Box<str>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    column_index: Option<ColumnIndex>,
    column_name: Option<Box<str>>,
}

impl CustomPlanError {
    /// Build a plan-time error with just a reason, equivalent to `builder(reason).build()`.
    pub fn new(reason: impl Into<Box<str>>) -> Self {
        Self::builder(reason).build()
    }

    pub fn builder(reason: impl Into<Box<str>>) -> CustomPlanErrorBuilder {
        CustomPlanErrorBuilder {
            reason: reason.into(),
            source: None,
        }
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn source(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.source.as_deref()
    }

    /// The column this plan was being built for, filled in by the connector for
    /// [`FromCell::build_plan`](crate::FromCell::build_plan) failures.
    pub fn column_index(&self) -> Option<ColumnIndex> {
        self.column_index
    }

    pub fn column_name(&self) -> Option<&str> {
        self.column_name.as_deref()
    }

    /// Fill in the column context if it has not already been set.
    ///
    /// Nested plan construction resolves the innermost column first, so later enclosing plans do not overwrite it.
    pub(crate) fn set_column_context(
        &mut self,
        column_index: ColumnIndex,
        column_name: impl Into<Box<str>>,
    ) {
        if self.column_index.is_some() {
            return;
        }
        self.column_index = Some(column_index);
        self.column_name = Some(column_name.into());
    }
}

impl Display for CustomPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.column_index, &self.column_name) {
            (Some(column_index), Some(column_name)) => write!(
                f,
                "decode plan error at column_index {column_index} ({column_name}): {}",
                self.reason
            ),
            _ => write!(f, "decode plan error: {}", self.reason),
        }
    }
}

impl StdError for CustomPlanError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

#[derive(Debug)]
pub struct CustomPlanErrorBuilder {
    reason: Box<str>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl CustomPlanErrorBuilder {
    pub fn source(mut self, source: impl Into<Box<dyn StdError + Send + Sync + 'static>>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn build(self) -> CustomPlanError {
        CustomPlanError {
            reason: self.reason,
            source: self.source,
            column_index: None,
            column_name: None,
        }
    }
}

/// Row-level conversion failure raised by a hand-written
/// [`FromRow::from_row_with_plan`](crate::FromRow::from_row_with_plan).
///
/// Use this when decoded cells are individually valid but fail a row-level domain rule. Cell-local failures should stay
/// [`CellConversionError`].
///
/// Callers provide only a reason and optional source. The connector adds `row_index` when iteration yields the failure.
#[derive(Debug)]
pub struct RowConversionError {
    reason: Box<str>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
    row_index: Option<usize>,
}

impl RowConversionError {
    /// Build a row-level error with just a reason, equivalent to `builder(reason).build()`.
    pub fn new(reason: impl Into<Box<str>>) -> Self {
        Self::builder(reason).build()
    }

    pub fn builder(reason: impl Into<Box<str>>) -> RowConversionErrorBuilder {
        RowConversionErrorBuilder {
            reason: reason.into(),
            source: None,
        }
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }

    pub fn source(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.source.as_deref()
    }

    /// Zero-based index of the failing row, filled in by the connector's decode loop.
    pub fn row_index(&self) -> Option<usize> {
        self.row_index
    }

    /// Fill in the failing row's index if it has not already been set.
    pub(crate) fn set_row_index(&mut self, row_index: usize) {
        if self.row_index.is_none() {
            self.row_index = Some(row_index);
        }
    }
}

impl Display for RowConversionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.row_index {
            Some(row_index) => {
                write!(
                    f,
                    "row conversion error at row_index {row_index}: {}",
                    self.reason
                )
            }
            None => write!(f, "row conversion error: {}", self.reason),
        }
    }
}

impl StdError for RowConversionError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source.as_deref().map(|source| source as _)
    }
}

#[derive(Debug)]
pub struct RowConversionErrorBuilder {
    reason: Box<str>,
    source: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl RowConversionErrorBuilder {
    pub fn source(mut self, source: impl Into<Box<dyn StdError + Send + Sync + 'static>>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn build(self) -> RowConversionError {
        RowConversionError {
            reason: self.reason,
            source: self.source,
            row_index: None,
        }
    }
}

/// Cell decode failure with row and column context.
#[derive(Debug)]
pub struct CellDecodeError {
    inner: Box<CellDecodeErrorInner>,
}

#[derive(Debug)]
struct CellDecodeErrorInner {
    row_index: usize,
    column_index: ColumnIndex,
    column_name: Box<str>,
    target_type_name: Cow<'static, str>,
    actual_column_type: ColumnType,
    raw_value_preview: Option<Box<str>>,
    issue: CellConversionError,
}

impl CellDecodeError {
    pub(crate) fn new(
        row_index: usize,
        column_index: ColumnIndex,
        column_name: impl Into<Box<str>>,
        target_type_name: impl Into<Cow<'static, str>>,
        actual_column_type: ColumnType,
        raw_value_preview: Option<&str>,
        issue: CellConversionError,
    ) -> Self {
        Self {
            inner: Box::new(CellDecodeErrorInner {
                row_index,
                column_index,
                column_name: column_name.into(),
                target_type_name: target_type_name.into(),
                actual_column_type,
                raw_value_preview: raw_value_preview
                    .map(|preview| truncate_preview_chars(preview, VALUE_PREVIEW_MAX_CHARS)),
                issue,
            }),
        }
    }

    pub fn row_index(&self) -> usize {
        self.inner.row_index
    }

    pub fn column_index(&self) -> ColumnIndex {
        self.inner.column_index
    }

    pub fn column_name(&self) -> &str {
        &self.inner.column_name
    }

    pub fn target_type_name(&self) -> &str {
        &self.inner.target_type_name
    }

    pub fn actual_column_type(&self) -> &ColumnType {
        &self.inner.actual_column_type
    }

    pub fn raw_value_preview(&self) -> Option<&str> {
        self.inner.raw_value_preview.as_deref()
    }

    pub fn conversion_error(&self) -> &CellConversionError {
        &self.inner.issue
    }
}

impl Display for CellDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = &*self.inner;
        write!(
            f,
            "row_index {} column_index {} ({}): target_type {}, found {}",
            inner.row_index,
            inner.column_index,
            inner.column_name,
            inner.target_type_name,
            inner.actual_column_type
        )?;
        if let Some(preview) = &inner.raw_value_preview {
            write!(f, ", value: {preview:?}")?;
        }
        if !inner.issue.reason().is_empty() {
            write!(f, " ({})", inner.issue.reason())?;
        }
        Ok(())
    }
}

impl StdError for CellDecodeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .issue
            .source()
            .map(|_| &self.inner.issue as &(dyn StdError + 'static))
    }
}

/// Error union returned by the plan-time decode hooks ([`FromCell::build_plan`](crate::FromCell::build_plan),
/// [`FromRow::build_plan`](crate::FromRow::build_plan), and [`CellPlan`](crate::CellPlan) resolution).
///
/// This keeps plan hooks limited to schema failures and custom validation failures. At the connector boundary, each
/// variant maps to [`crate::Error`] with `ErrorKind::Decode`.
///
/// Like [`SchemaError`], this enum is non-exhaustive: downstream crates can build variants and convert through the
/// `From` impls, but cannot match it exhaustively.
#[derive(Debug)]
#[non_exhaustive]
pub enum PlanBuildError {
    /// A structured schema mismatch, from column resolution or a built-in column-type check.
    Schema(SchemaError),
    /// A hand-written plan's free-form validation failure.
    Custom(CustomPlanError),
}

/// Error union returned by [`FromRow::from_row_with_plan`](crate::FromRow::from_row_with_plan).
///
/// The [`Schema`](RowDecodeError::Schema) variant lets a hand-written row decoder propagate a dynamic
/// [`RowRef::cell_at`](crate::RowRef::cell_at) lookup failure with `?`.
#[derive(Debug)]
#[non_exhaustive]
pub enum RowDecodeError {
    /// A dynamic column lookup failed, e.g. from [`RowRef::cell_at`](crate::RowRef::cell_at).
    Schema(SchemaError),
    /// A cell failed to decode, surfaced through [`RowRef::get_with_plan`](crate::RowRef::get_with_plan).
    Cell(CellDecodeError),
    /// A hand-written row decoder rejected an otherwise-decoded row.
    Conversion(RowConversionError),
}

/// Result alias for the plan-time decode hooks.
pub type PlanBuildResult<T> = std::result::Result<T, PlanBuildError>;

/// Result alias for [`FromRow::from_row_with_plan`](crate::FromRow::from_row_with_plan).
pub type RowDecodeResult<T> = std::result::Result<T, RowDecodeError>;

impl Display for PlanBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(error) => Display::fmt(error, f),
            Self::Custom(error) => Display::fmt(error, f),
        }
    }
}

impl StdError for PlanBuildError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Schema(error) => StdError::source(error),
            Self::Custom(error) => StdError::source(error),
        }
    }
}

impl Display for RowDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Schema(error) => Display::fmt(error, f),
            Self::Cell(error) => Display::fmt(error, f),
            Self::Conversion(error) => Display::fmt(error, f),
        }
    }
}

impl StdError for RowDecodeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Schema(error) => StdError::source(error),
            Self::Cell(error) => StdError::source(error),
            Self::Conversion(error) => StdError::source(error),
        }
    }
}

impl From<SchemaError> for PlanBuildError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<CustomPlanError> for PlanBuildError {
    fn from(error: CustomPlanError) -> Self {
        Self::Custom(error)
    }
}

impl From<SchemaError> for RowDecodeError {
    fn from(error: SchemaError) -> Self {
        Self::Schema(error)
    }
}

impl From<CellDecodeError> for RowDecodeError {
    fn from(error: CellDecodeError) -> Self {
        Self::Cell(error)
    }
}

impl From<RowConversionError> for RowDecodeError {
    fn from(error: RowConversionError) -> Self {
        Self::Conversion(error)
    }
}
