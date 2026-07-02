use std::fmt;

use crate::result::ColumnIndex;

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaError {
    MissingColumn(MissingColumnError),
    AmbiguousColumn(AmbiguousColumnError),
    InvalidColumnIndex(InvalidColumnIndexError),
    DuplicateColumnName(DuplicateColumnNameError),
    ColumnCountMismatch(ColumnCountMismatchError),
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

impl fmt::Display for MissingColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "missing column: {}", self.name)
    }
}

impl std::error::Error for MissingColumnError {}

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

impl fmt::Display for AmbiguousColumnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ambiguous column: {}", self.name)
    }
}

impl std::error::Error for AmbiguousColumnError {}

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

impl fmt::Display for InvalidColumnIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid column index {:?} for schema with {} columns",
            self.index, self.column_count
        )
    }
}

impl std::error::Error for InvalidColumnIndexError {}

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

impl fmt::Display for DuplicateColumnNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "duplicate column name in result: {}", self.name)
    }
}

impl std::error::Error for DuplicateColumnNameError {}

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

impl fmt::Display for ColumnCountMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "column count mismatch (expected {}, actual {})",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for ColumnCountMismatchError {}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingColumn(error) => error.fmt(f),
            Self::AmbiguousColumn(error) => error.fmt(f),
            Self::InvalidColumnIndex(error) => error.fmt(f),
            Self::DuplicateColumnName(error) => error.fmt(f),
            Self::ColumnCountMismatch(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SchemaError {}

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
}
