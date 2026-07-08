use std::{collections::HashMap, fmt, result::Result as StdResult, sync::Arc};

use crate::error::{AmbiguousColumnError, MissingColumnError, SchemaError};

/// Snowflake column type as reported by the result-set metadata.
///
/// Variants mirror the wire-level type tags Snowflake returns for each result column.
/// A handful of variants carry the type's parameters (precision/scale/length) when the server provides them.
///
/// Unknown or unrecognized server-side types fall through to [`ColumnType::Unknown`] rather than failing — this
/// keeps the connector usable when Snowflake introduces new types.
///
/// # GEOGRAPHY / GEOMETRY
///
/// Driver API query results normally report GEOGRAPHY / GEOMETRY values according to the session's
/// `GEOGRAPHY_OUTPUT_FORMAT` / `GEOMETRY_OUTPUT_FORMAT`: GeoJSON as [`Object`](ColumnType::Object), WKT/EWKT as
/// [`Text`](ColumnType::Text), and WKB/EWKB as [`Binary`](ColumnType::Binary). The connector does not parse or validate
/// the geo payload itself.
///
/// # Example
///
/// ```
/// use snowflake_connector_rs::ColumnType;
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
    /// `GEOGRAPHY`, when reported directly as a logical type.
    Geography,
    /// `GEOMETRY`, when reported directly as a logical type.
    Geometry,
    /// `VECTOR`.
    ///
    /// SELECT result metadata reports `VECTOR`, but not the element type or dimension. Use
    /// [`Vector<i32>`](crate::decode::Vector) / [`Vector<f32>`](crate::decode::Vector) when decoding the expected element type.
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

    /// Returns the lowercase Snowflake type tag (matching the `rowtype.type` field).
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

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())?;
        match self {
            ColumnType::Fixed {
                precision: Some(precision),
                scale: Some(scale),
            } => write!(f, "({precision},{scale})"),
            ColumnType::Fixed {
                precision: Some(precision),
                scale: None,
            } => write!(f, "({precision})"),
            ColumnType::Text {
                length: Some(length),
            }
            | ColumnType::Binary {
                length: Some(length),
            } => write!(f, "({length})"),
            ColumnType::Time { scale: Some(scale) }
            | ColumnType::TimestampNtz { scale: Some(scale) }
            | ColumnType::TimestampLtz { scale: Some(scale) }
            | ColumnType::TimestampTz { scale: Some(scale) } => write!(f, "({scale})"),
            _ => Ok(()),
        }
    }
}

/// Metadata for a single result-set column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    name: Arc<str>,
    index: usize,
    nullable: bool,
    ty: ColumnType,
}

impl Column {
    pub(crate) fn new(
        name: impl Into<Arc<str>>,
        index: usize,
        nullable: bool,
        ty: ColumnType,
    ) -> Self {
        Self {
            name: name.into(),
            index,
            nullable,
            ty,
        }
    }

    /// Raw column name as Snowflake reported it (case-sensitive).
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Zero-based position of this column within the schema.
    pub fn index(&self) -> usize {
        self.index
    }
    /// Whether the column may carry SQL `NULL`.
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
    /// The column's Snowflake type.
    pub fn ty(&self) -> &ColumnType {
        &self.ty
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LookupEntry {
    Unique(usize),
    Ambiguous(Box<[usize]>),
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) struct ColumnIndexMap {
    map: HashMap<Arc<str>, LookupEntry>,
}

impl ColumnIndexMap {
    fn build(columns: &[Column]) -> Self {
        let mut map: HashMap<Arc<str>, Vec<usize>> = HashMap::with_capacity(columns.len());
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
    pub(crate) fn from_columns(columns: Vec<Column>) -> Self {
        let indices = ColumnIndexMap::build(&columns);
        Self {
            columns: columns.into_boxed_slice(),
            indices,
        }
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
    pub fn column_at(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Resolve a column name to its zero-based index.
    ///
    /// Matching is case-sensitive against the raw label Snowflake reported, so `Id` and `ID` are treated as distinct names.
    ///
    /// # Errors
    ///
    /// - [`SchemaError::MissingColumn`] when no column carries the name.
    /// - [`SchemaError::AmbiguousColumn`] when several columns share it.
    pub fn column_index(&self, name: &str) -> StdResult<usize, SchemaError> {
        match self.indices.get(name) {
            Some(LookupEntry::Unique(idx)) => Ok(*idx),
            Some(LookupEntry::Ambiguous(candidates)) => Err(SchemaError::AmbiguousColumn(
                AmbiguousColumnError::new(name, candidates.clone()),
            )),
            None => Err(SchemaError::MissingColumn(MissingColumnError::new(name))),
        }
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
        ]);
        assert_eq!(schema.column_index("ID").unwrap(), 0);
        assert_eq!(schema.column_index("id").unwrap(), 1);
        assert!(matches!(
            schema.column_index("Id"),
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
        ]);
        let err = schema.column_index("id").unwrap_err();
        match err {
            SchemaError::AmbiguousColumn(error) => {
                assert_eq!(error.name(), "id");
                assert_eq!(error.candidates(), &[0, 1]);
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
