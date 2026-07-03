use std::{
    borrow::Cow,
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

use crate::result_table::{ColumnIndex, ColumnType};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaError {
    MissingColumn(MissingColumnError),
    AmbiguousColumn(AmbiguousColumnError),
    InvalidColumnIndex(InvalidColumnIndexError),
    DuplicateColumnName(DuplicateColumnNameError),
    ColumnCountMismatch(ColumnCountMismatchError),
    IncompatibleColumnType(IncompatibleColumnTypeError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingColumnError {
    name: Box<str>,
}

impl MissingColumnError {
    pub(crate) fn new(name: impl Into<Box<str>>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Display for MissingColumnError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "missing column: {}", self.name)
    }
}

impl StdError for MissingColumnError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousColumnError {
    name: Box<str>,
    candidates: Box<[ColumnIndex]>,
}

impl AmbiguousColumnError {
    pub(crate) fn new(
        name: impl Into<Box<str>>,
        candidates: impl Into<Box<[ColumnIndex]>>,
    ) -> Self {
        Self {
            name: name.into(),
            candidates: candidates.into(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn candidates(&self) -> &[ColumnIndex] {
        &self.candidates
    }
}

impl Display for AmbiguousColumnError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ambiguous column: {}", self.name)
    }
}

impl StdError for AmbiguousColumnError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidColumnIndexError {
    index: ColumnIndex,
    column_count: usize,
}

impl InvalidColumnIndexError {
    pub(crate) fn new(index: ColumnIndex, column_count: usize) -> Self {
        Self {
            index,
            column_count,
        }
    }

    pub fn index(&self) -> ColumnIndex {
        self.index
    }

    pub fn column_count(&self) -> usize {
        self.column_count
    }
}

impl Display for InvalidColumnIndexError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid column index {:?} for schema with {} columns",
            self.index, self.column_count
        )
    }
}

impl StdError for InvalidColumnIndexError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicateColumnNameError {
    name: Box<str>,
}

impl DuplicateColumnNameError {
    pub(crate) fn new(name: impl Into<Box<str>>) -> Self {
        Self { name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Display for DuplicateColumnNameError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "duplicate column name in result: {}", self.name)
    }
}

impl StdError for DuplicateColumnNameError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCountMismatchError {
    expected: usize,
    actual: usize,
}

impl ColumnCountMismatchError {
    // Must stay `pub`: the `FromRow` derive emits a call to this constructor in downstream crates.
    pub fn new(expected: usize, actual: usize) -> Self {
        Self { expected, actual }
    }

    pub fn expected(&self) -> usize {
        self.expected
    }

    pub fn actual(&self) -> usize {
        self.actual
    }
}

impl Display for ColumnCountMismatchError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "column count mismatch (expected {}, actual {})",
            self.expected, self.actual
        )
    }
}

impl StdError for ColumnCountMismatchError {}

/// A column's Snowflake type cannot be decoded into the requested Rust type.
///
/// Raised while building a row decode plan, before any row is read, so a type mismatch fails the whole
/// [`rows`](crate::ResultTable::rows) call rather than surfacing per cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncompatibleColumnTypeError {
    column_index: ColumnIndex,
    column_name: Box<str>,
    target_type_name: Cow<'static, str>,
    actual_column_type: ColumnType,
    detail: Option<Box<str>>,
}

impl IncompatibleColumnTypeError {
    pub(crate) fn new(
        column_index: ColumnIndex,
        column_name: impl Into<Box<str>>,
        target_type_name: impl Into<Cow<'static, str>>,
        actual_column_type: ColumnType,
        detail: Option<Box<str>>,
    ) -> Self {
        Self {
            column_index,
            column_name: column_name.into(),
            target_type_name: target_type_name.into(),
            actual_column_type,
            detail,
        }
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

    /// Extra context beyond the type tags, such as an out-of-range scale.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
}

impl Display for IncompatibleColumnTypeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "column {:?} ({}) of type {:?} cannot decode into {}",
            self.column_index, self.column_name, self.actual_column_type, self.target_type_name
        )?;
        if let Some(detail) = &self.detail {
            write!(f, " ({detail})")?;
        }
        Ok(())
    }
}

impl StdError for IncompatibleColumnTypeError {}

impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingColumn(error) => error.fmt(f),
            Self::AmbiguousColumn(error) => error.fmt(f),
            Self::InvalidColumnIndex(error) => error.fmt(f),
            Self::DuplicateColumnName(error) => error.fmt(f),
            Self::ColumnCountMismatch(error) => error.fmt(f),
            Self::IncompatibleColumnType(error) => error.fmt(f),
        }
    }
}

impl StdError for SchemaError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_error_display_formats_missing_and_ambiguous_variants() {
        assert_eq!(
            SchemaError::MissingColumn(MissingColumnError::new("value")).to_string(),
            "missing column: value"
        );
        assert_eq!(
            SchemaError::AmbiguousColumn(AmbiguousColumnError::new(
                "value",
                vec![ColumnIndex::new(0), ColumnIndex::new(1)].into_boxed_slice(),
            ))
            .to_string(),
            "ambiguous column: value"
        );
    }

    #[test]
    fn schema_error_accessors_expose_structured_fields() {
        let ambiguous = AmbiguousColumnError::new(
            "value",
            vec![ColumnIndex::new(0), ColumnIndex::new(1)].into_boxed_slice(),
        );
        assert_eq!(ambiguous.name(), "value");
        assert_eq!(
            ambiguous.candidates(),
            &[ColumnIndex::new(0), ColumnIndex::new(1)]
        );

        let invalid = InvalidColumnIndexError::new(ColumnIndex::new(7), 3);
        assert_eq!(invalid.index(), ColumnIndex::new(7));
        assert_eq!(invalid.column_count(), 3);

        let duplicate = DuplicateColumnNameError::new("id");
        assert_eq!(duplicate.name(), "id");

        let mismatch = ColumnCountMismatchError::new(4, 2);
        assert_eq!(mismatch.expected(), 4);
        assert_eq!(mismatch.actual(), 2);
    }

    #[test]
    fn schema_error_display_formats_remaining_variants() {
        assert_eq!(
            SchemaError::InvalidColumnIndex(InvalidColumnIndexError::new(ColumnIndex::new(7), 3))
                .to_string(),
            "invalid column index ColumnIndex(7) for schema with 3 columns"
        );
        assert_eq!(
            SchemaError::DuplicateColumnName(DuplicateColumnNameError::new("id")).to_string(),
            "duplicate column name in result: id"
        );
        assert_eq!(
            SchemaError::ColumnCountMismatch(ColumnCountMismatchError::new(4, 2)).to_string(),
            "column count mismatch (expected 4, actual 2)"
        );
    }

    #[test]
    fn incompatible_column_type_exposes_fields_and_appends_detail() {
        let without_detail = IncompatibleColumnTypeError::new(
            ColumnIndex::new(2),
            "TS",
            "chrono::NaiveDateTime",
            ColumnType::Boolean,
            None,
        );
        assert_eq!(without_detail.column_index(), ColumnIndex::new(2));
        assert_eq!(without_detail.column_name(), "TS");
        assert_eq!(without_detail.target_type_name(), "chrono::NaiveDateTime");
        assert_eq!(without_detail.actual_column_type(), &ColumnType::Boolean);
        assert_eq!(without_detail.detail(), None);
        assert_eq!(
            without_detail.to_string(),
            "column ColumnIndex(2) (TS) of type Boolean cannot decode into chrono::NaiveDateTime"
        );

        let with_detail = IncompatibleColumnTypeError::new(
            ColumnIndex::new(0),
            "T",
            "chrono::NaiveTime",
            ColumnType::Time { scale: Some(12) },
            Some(Box::from("invalid time scale: 12")),
        );
        assert_eq!(with_detail.detail(), Some("invalid time scale: 12"));
        assert!(
            with_detail
                .to_string()
                .ends_with("(invalid time scale: 12)"),
            "actual: {with_detail}"
        );
    }
}
