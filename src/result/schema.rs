use std::{collections::HashMap, sync::Arc};

use crate::{AmbiguousColumnError, MissingColumnError, SchemaError, error::RowsetParseError};

/// Position of a column inside a result-set schema.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnIndex(u32);

impl ColumnIndex {
    pub(crate) const fn new(index: u32) -> Self {
        Self(index)
    }

    /// Returns the index as `usize` for slice-style access.
    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

/// Snowflake column type as reported by the result-set metadata.
///
/// Variants mirror the wire-level type tags Snowflake returns for each result column.
/// A handful of variants carry the type's parameters (precision/scale/length) when the server provides them.
///
/// Unknown or unrecognized server-side types fall through to [`ColumnType::Unknown`] rather than failing — this
/// keeps the connector usable when Snowflake introduces new types.
///
/// # Example
///
/// ```
/// use snowflake_connector_rs::result::ColumnType;
///
/// // `ColumnType` values come from a query result's schema (`column.ty()`).
/// // Read the reported metadata through its accessors:
/// fn describe(ty: &ColumnType) -> String {
///     format!(
///         "{} precision={:?} scale={:?} length={:?}",
///         ty.as_str(),
///         ty.precision(),
///         ty.scale(),
///         ty.length(),
///     )
/// }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnType {
    /// `NUMBER` / `DECIMAL` / `NUMERIC` / `INT` / etc. — exact-precision numbers.
    /// `precision` and `scale` come from the column definition when reported by Snowflake.
    #[non_exhaustive]
    Fixed {
        /// Total digit count (1..=38), when reported.
        precision: Option<u8>,
        /// Digits to the right of the decimal point (0..=37). Zero means integer.
        scale: Option<u8>,
    },
    /// `FLOAT` / `DOUBLE` / `REAL` — IEEE 754 floating-point.
    Real,
    /// `TEXT` / `VARCHAR` / `CHAR` / `STRING` — variable-length string.
    #[non_exhaustive]
    Text {
        /// Maximum length in characters, when reported by Snowflake.
        /// This is the character count, not the byte length Snowflake also reports.
        length: Option<u32>,
    },
    /// `BOOLEAN`.
    Boolean,
    /// `DATE` (calendar day, no time-of-day component).
    Date,
    /// `TIME` (time-of-day, no date component).
    #[non_exhaustive]
    Time {
        /// Fractional-seconds precision (digits, 0..=9).
        scale: Option<u8>,
    },
    /// `TIMESTAMP_NTZ` — timestamp without time zone.
    #[non_exhaustive]
    TimestampNtz {
        /// Fractional-seconds precision (digits, 0..=9).
        scale: Option<u8>,
    },
    /// `TIMESTAMP_LTZ` — timestamp with local-time-zone semantics.
    #[non_exhaustive]
    TimestampLtz {
        /// Fractional-seconds precision (digits, 0..=9).
        scale: Option<u8>,
    },
    /// `TIMESTAMP_TZ` — timestamp with explicit offset.
    #[non_exhaustive]
    TimestampTz {
        /// Fractional-seconds precision (digits, 0..=9).
        scale: Option<u8>,
    },
    /// `VARIANT` — semi-structured value.
    Variant,
    /// `OBJECT` — semi-structured map.
    Object,
    /// `ARRAY` — semi-structured array.
    Array,
    /// `BINARY` — byte buffer.
    #[non_exhaustive]
    Binary {
        /// Maximum length in bytes, when reported by Snowflake.
        length: Option<u32>,
    },
    /// `GEOGRAPHY`.
    Geography,
    /// `GEOMETRY`.
    Geometry,
    /// `VECTOR`.
    Vector,
    /// Type tag the connector did not recognize. The original (lowercased) server-side tag is preserved for diagnostics;
    /// values come through as raw text.
    Unknown {
        /// Original lowercased Snowflake type tag.
        snowflake_type: Box<str>,
    },
}

impl ColumnType {
    pub(crate) fn from_driver_metadata(
        snowflake_type: &str,
        length: Option<u32>,
        precision: Option<u8>,
        scale: Option<u8>,
    ) -> Self {
        let lower = snowflake_type.to_ascii_lowercase();
        match lower.as_str() {
            "fixed" => ColumnType::Fixed { precision, scale },
            "real" | "float" | "double" | "double precision" => ColumnType::Real,
            "text" | "varchar" | "char" | "string" => ColumnType::Text { length },
            "boolean" => ColumnType::Boolean,
            "date" => ColumnType::Date,
            "time" => ColumnType::Time { scale },
            "timestamp_ntz" => ColumnType::TimestampNtz { scale },
            "timestamp_ltz" => ColumnType::TimestampLtz { scale },
            "timestamp_tz" => ColumnType::TimestampTz { scale },
            "variant" => ColumnType::Variant,
            "object" => ColumnType::Object,
            "array" => ColumnType::Array,
            "binary" => ColumnType::Binary { length },
            "geography" => ColumnType::Geography,
            "geometry" => ColumnType::Geometry,
            "vector" => ColumnType::Vector,
            _ => ColumnType::Unknown {
                snowflake_type: Box::from(lower),
            },
        }
    }

    /// Returns the lowercase Snowflake type tag (matching the
    /// `rowtype.type` field).
    ///
    pub fn as_str(&self) -> &str {
        match self {
            ColumnType::Fixed { .. } => "fixed",
            ColumnType::Real => "real",
            ColumnType::Text { .. } => "text",
            ColumnType::Boolean => "boolean",
            ColumnType::Date => "date",
            ColumnType::Time { .. } => "time",
            ColumnType::TimestampNtz { .. } => "timestamp_ntz",
            ColumnType::TimestampLtz { .. } => "timestamp_ltz",
            ColumnType::TimestampTz { .. } => "timestamp_tz",
            ColumnType::Variant => "variant",
            ColumnType::Object => "object",
            ColumnType::Array => "array",
            ColumnType::Binary { .. } => "binary",
            ColumnType::Geography => "geography",
            ColumnType::Geometry => "geometry",
            ColumnType::Vector => "vector",
            ColumnType::Unknown { snowflake_type } => snowflake_type,
        }
    }

    /// Numeric precision for [`ColumnType::Fixed`]; `None` for any other
    /// variant.
    pub fn precision(&self) -> Option<u8> {
        if let ColumnType::Fixed { precision, .. } = self {
            *precision
        } else {
            None
        }
    }

    /// Scale for variants that carry one; `None` for any other variant.
    ///
    /// The meaning depends on the variant: for [`ColumnType::Fixed`] it is the decimal scale (digits after the point, 0..=37);
    /// for [`ColumnType::Time`] and the `Timestamp*` family it is the fractional-seconds precision (0..=9).
    /// Snowflake delivers both in the same wire field.
    pub fn scale(&self) -> Option<u8> {
        match self {
            ColumnType::Fixed { scale, .. }
            | ColumnType::Time { scale }
            | ColumnType::TimestampNtz { scale }
            | ColumnType::TimestampLtz { scale }
            | ColumnType::TimestampTz { scale } => *scale,
            _ => None,
        }
    }

    /// Maximum character length for [`ColumnType::Text`]; `None` for any other variant.
    pub fn length(&self) -> Option<u32> {
        match self {
            ColumnType::Text { length } => *length,
            _ => None,
        }
    }
}

/// Metadata for a single result-set column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    name: Arc<str>,
    index: ColumnIndex,
    nullable: bool,
    ty: ColumnType,
}

impl Column {
    pub(crate) fn new(
        name: impl Into<Arc<str>>,
        index: u32,
        nullable: bool,
        ty: ColumnType,
    ) -> Self {
        Self {
            name: name.into(),
            index: ColumnIndex(index),
            nullable,
            ty,
        }
    }

    /// Raw column name as Snowflake reported it (case-sensitive).
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Zero-based position of this column within the schema.
    pub fn index(&self) -> ColumnIndex {
        self.index
    }
    /// Whether the column may carry SQL `NULL`.
    pub fn nullable(&self) -> bool {
        self.nullable
    }
    /// The column's Snowflake type.
    pub fn ty(&self) -> &ColumnType {
        &self.ty
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LookupEntry {
    Unique(ColumnIndex),
    Ambiguous(Box<[ColumnIndex]>),
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct ColumnIndexMap {
    map: HashMap<Arc<str>, LookupEntry>,
}

impl ColumnIndexMap {
    fn build(columns: &[Column]) -> Self {
        let mut map: HashMap<Arc<str>, Vec<ColumnIndex>> = HashMap::with_capacity(columns.len());
        for col in columns {
            map.entry(Arc::clone(&col.name))
                .or_default()
                .push(col.index());
        }

        let entries = map
            .into_iter()
            .map(|(k, mut v)| {
                let entry = if v.len() == 1 {
                    LookupEntry::Unique(v.pop().expect("exactly one match"))
                } else {
                    LookupEntry::Ambiguous(v.into_boxed_slice())
                };
                (k, entry)
            })
            .collect();

        Self { map: entries }
    }

    fn get(&self, name: &str) -> Option<&LookupEntry> {
        self.map.get(name)
    }
}

/// Ordered metadata describing the columns of a result set.
///
/// Lookups are case-sensitive and match the raw label Snowflake reported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    columns: Box<[Column]>,
    indices: ColumnIndexMap,
}

impl Schema {
    pub(crate) fn from_columns(
        columns: Vec<Column>,
    ) -> std::result::Result<Self, RowsetParseError> {
        if columns.len() > u32::MAX as usize {
            return Err(RowsetParseError::CapacityOverflow);
        }

        let indices = ColumnIndexMap::build(&columns);
        Ok(Self {
            columns: columns.into_boxed_slice(),
            indices,
        })
    }

    /// Borrows the columns in declaration order.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the number of columns.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns `true` when the result set has no columns.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Borrow column metadata by index.
    pub fn column_at(&self, index: ColumnIndex) -> Option<&Column> {
        self.columns.get(index.as_usize())
    }

    /// Resolve a column name to its [`ColumnIndex`].
    ///
    /// Matching is case-sensitive against the raw label Snowflake reported
    /// (a quoted `"Id"` and an unquoted `ID` are distinct).
    ///
    /// # Errors
    ///
    /// - [`SchemaError::MissingColumn`] when no column carries the name.
    /// - [`SchemaError::AmbiguousColumn`] when several columns share it.
    pub fn column(&self, name: &str) -> std::result::Result<ColumnIndex, SchemaError> {
        lookup_result(name, self.indices.get(name))
    }
}

fn lookup_result(
    name: &str,
    entry: Option<&LookupEntry>,
) -> std::result::Result<ColumnIndex, SchemaError> {
    match entry {
        Some(LookupEntry::Unique(idx)) => Ok(*idx),
        Some(LookupEntry::Ambiguous(candidates)) => Err(SchemaError::AmbiguousColumn(
            AmbiguousColumnError::new(name, candidates.clone()),
        )),
        None => Err(SchemaError::MissingColumn(MissingColumnError::new(name))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_label_lookup_returns_exact_match() {
        let schema = Schema::from_columns(vec![
            Column::new(
                "ID",
                0,
                false,
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
            ),
            Column::new(
                "id",
                1,
                false,
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
            ),
        ])
        .unwrap();
        assert_eq!(schema.column("ID").unwrap().as_usize(), 0);
        assert_eq!(schema.column("id").unwrap().as_usize(), 1);
        assert!(matches!(
            schema.column("Id"),
            Err(SchemaError::MissingColumn(error)) if error.name() == "Id"
        ));
    }

    #[test]
    fn schema_label_lookup_reports_duplicate_raw_labels() {
        let schema = Schema::from_columns(vec![
            Column::new(
                "id",
                0,
                false,
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
            ),
            Column::new(
                "id",
                1,
                false,
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
            ),
        ])
        .unwrap();
        let err = schema.column("id").unwrap_err();
        match err {
            SchemaError::AmbiguousColumn(error) => {
                assert_eq!(error.name(), "id");
                assert_eq!(
                    error
                        .candidates()
                        .iter()
                        .map(|candidate| candidate.as_usize())
                        .collect::<Vec<_>>(),
                    vec![0, 1]
                );
            }
            other => panic!("expected label ambiguity, got {other:?}"),
        }
    }

    #[test]
    fn column_type_from_driver_metadata() {
        assert_eq!(
            ColumnType::from_driver_metadata("FIXED", None, Some(38), Some(0)),
            ColumnType::Fixed {
                precision: Some(38),
                scale: Some(0),
            }
        );
        assert_eq!(
            ColumnType::from_driver_metadata("text", Some(255), None, None),
            ColumnType::Text { length: Some(255) }
        );
        let unknown = ColumnType::from_driver_metadata("Frob", None, None, None);
        assert!(matches!(unknown, ColumnType::Unknown { .. }));
    }
}
