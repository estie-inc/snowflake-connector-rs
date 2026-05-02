use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{Error, IdentifierError, LookupKind, ParseError, Result, SchemaError};

/// Index into a result-set column list.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnIndex(u32);

impl ColumnIndex {
    pub fn new(index: usize) -> Result<Self> {
        if index > u32::MAX as usize {
            return Err(Error::Parse(ParseError::CapacityOverflow));
        }
        Ok(Self(index as u32))
    }

    pub fn as_usize(self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Fixed {
        precision: Option<i64>,
        scale: Option<i64>,
    },
    Real,
    Text {
        length: Option<i64>,
    },
    Boolean,
    Date,
    Time {
        scale: Option<i64>,
    },
    TimestampNtz {
        scale: Option<i64>,
    },
    TimestampLtz {
        scale: Option<i64>,
    },
    TimestampTz {
        scale: Option<i64>,
    },
    Variant,
    Object,
    Array,
    Binary,
    Geography,
    Geometry,
    Vector,
    Unknown {
        snowflake_type: Box<str>,
    },
}

impl ColumnType {
    /// Build a `ColumnType` from the raw rowtype metadata returned by Snowflake.
    pub fn from_driver_metadata(
        snowflake_type: &str,
        length: Option<i64>,
        precision: Option<i64>,
        scale: Option<i64>,
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
            "binary" => ColumnType::Binary,
            "geography" => ColumnType::Geography,
            "geometry" => ColumnType::Geometry,
            "vector" => ColumnType::Vector,
            _ => ColumnType::Unknown {
                snowflake_type: Box::from(lower),
            },
        }
    }

    /// Wire-level type tag (lowercase Snowflake `rowtype.type`).
    pub fn wire_type(&self) -> &str {
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
            ColumnType::Binary => "binary",
            ColumnType::Geography => "geography",
            ColumnType::Geometry => "geometry",
            ColumnType::Vector => "vector",
            ColumnType::Unknown { snowflake_type } => snowflake_type,
        }
    }

    pub fn precision(&self) -> Option<i64> {
        if let ColumnType::Fixed { precision, .. } = self {
            *precision
        } else {
            None
        }
    }

    pub fn scale(&self) -> Option<i64> {
        match self {
            ColumnType::Fixed { scale, .. }
            | ColumnType::Time { scale }
            | ColumnType::TimestampNtz { scale }
            | ColumnType::TimestampLtz { scale }
            | ColumnType::TimestampTz { scale } => *scale,
            _ => None,
        }
    }

    pub fn length(&self) -> Option<i64> {
        match self {
            ColumnType::Text { length } => *length,
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    name: Arc<str>,
    index: ColumnIndex,
    nullable: bool,
    ty: ColumnType,
}

impl Column {
    pub fn new(name: impl Into<Arc<str>>, index: u32, nullable: bool, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            index: ColumnIndex(index),
            nullable,
            ty,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn index(&self) -> ColumnIndex {
        self.index
    }
    pub fn nullable(&self) -> bool {
        self.nullable
    }
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
            map.entry(Arc::clone(&col.name)).or_default().push(col.index());
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

    fn lookup_label(&self, name: &str) -> Option<&LookupEntry> {
        self.map.get(name)
    }

    fn lookup_identifier(
        &self,
        name: &str,
    ) -> std::result::Result<Option<&LookupEntry>, IdentifierError> {
        let canonical = canonicalize_unquoted(name)?;
        Ok(self.map.get(canonical.as_ref()))
    }
}

pub(crate) fn canonicalize_unquoted(
    name: &str,
) -> std::result::Result<Cow<'_, str>, IdentifierError> {
    if name.is_empty() {
        return Err(IdentifierError::Empty);
    }

    let len = name.chars().count();
    if len > 255 {
        return Err(IdentifierError::TooLong { len, max: 255 });
    }

    let mut canonical = None::<String>;
    for (offset, ch) in name.char_indices() {
        if offset == 0 {
            if !(ch.is_ascii_alphabetic() || ch == '_') {
                return Err(IdentifierError::InvalidStart { ch });
            }
        } else if !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '$')) {
            return Err(IdentifierError::InvalidChar { offset, ch });
        }

        let upper = ch.to_ascii_uppercase();
        if let Some(buf) = &mut canonical {
            buf.push(upper);
        } else if upper != ch {
            let mut buf = String::with_capacity(name.len());
            buf.push_str(&name[..offset]);
            buf.push(upper);
            canonical = Some(buf);
        }
    }

    Ok(match canonical {
        Some(canonical) => Cow::Owned(canonical),
        None => Cow::Borrowed(name),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    columns: Box<[Column]>,
    indices: ColumnIndexMap,
}

impl Schema {
    pub(crate) fn from_columns(columns: Vec<Column>) -> Result<Self> {
        if columns.len() > u32::MAX as usize {
            return Err(Error::Parse(ParseError::CapacityOverflow));
        }

        let indices = ColumnIndexMap::build(&columns);
        Ok(Self {
            columns: columns.into_boxed_slice(),
            indices,
        })
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    pub fn column_at(&self, index: ColumnIndex) -> Option<&Column> {
        self.columns.get(index.as_usize())
    }

    pub fn column_by_label(&self, name: &str) -> std::result::Result<ColumnIndex, SchemaError> {
        lookup_result(LookupKind::Label, name, self.indices.lookup_label(name))
    }

    pub fn column_by_identifier(
        &self,
        name: &str,
    ) -> std::result::Result<ColumnIndex, SchemaError> {
        let entry = self
            .indices
            .lookup_identifier(name)
            .map_err(|reason| SchemaError::InvalidIdentifier {
                input: Box::from(name),
                reason,
            })?;
        lookup_result(LookupKind::Identifier, name, entry)
    }
}

fn lookup_result(
    lookup: LookupKind,
    name: &str,
    entry: Option<&LookupEntry>,
) -> std::result::Result<ColumnIndex, SchemaError> {
    match entry {
        Some(LookupEntry::Unique(idx)) => Ok(*idx),
        Some(LookupEntry::Ambiguous(candidates)) => Err(SchemaError::AmbiguousColumn {
            lookup,
            name: Box::from(name),
            candidates: candidates.clone(),
        }),
        None => Err(SchemaError::MissingColumn {
            lookup,
            name: Box::from(name),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_lookup_normalizes_case() {
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
            Column::new("Name", 1, true, ColumnType::Text { length: None }),
        ])
        .unwrap();
        assert_eq!(schema.column("ID").unwrap().as_usize(), 0);
        assert_eq!(schema.column("name").unwrap().as_usize(), 1);
    }

    #[test]
    fn schema_ambiguous_quoted_columns() {
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
                "ID",
                1,
                false,
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
            ),
        ])
        .unwrap();
        assert!(schema.column("id").is_none());
        let err = schema.require_column("id").unwrap_err();
        assert!(matches!(
            err,
            crate::Error::Schema(SchemaError::AmbiguousColumn { .. })
        ));
        assert_eq!(schema.column_exact("id").unwrap().as_usize(), 0);
        assert_eq!(schema.column_exact("ID").unwrap().as_usize(), 1);
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
