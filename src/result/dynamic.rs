use std::sync::Arc;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use crate::result::{
    FromRow, RowPlanContext,
    cell::CellRef,
    decode::{
        decode_hex, parse_time_seconds_and_nanos, parse_timestamp_epoch,
        parse_timestamp_tz_with_offset,
    },
    row::RowRef,
    schema::{ColumnIndex, ColumnType, Schema},
};
use crate::{Result, SchemaError};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecimalValue {
    raw: Box<str>,
    precision: Option<i64>,
    scale: Option<i64>,
}

impl DecimalValue {
    pub fn new(raw: impl Into<Box<str>>, precision: Option<i64>, scale: Option<i64>) -> Self {
        Self {
            raw: raw.into(),
            precision,
            scale,
        }
    }

    pub fn raw(&self) -> &str {
        &self.raw
    }

    pub fn precision(&self) -> Option<i64> {
        self.precision
    }

    pub fn scale(&self) -> Option<i64> {
        self.scale
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum SnowflakeValue {
    Null,
    Boolean(bool),
    Integer(i128),
    Float(f64),
    Decimal(DecimalValue),
    String(String),
    Date(NaiveDate),
    Time(NaiveTime),
    TimestampNtz(NaiveDateTime),
    TimestampLtz(DateTime<Utc>),
    TimestampTz(DateTime<FixedOffset>),
    Json(serde_json::Value),
    Binary(Vec<u8>),
}

impl SnowflakeValue {
    pub fn is_null(&self) -> bool {
        matches!(self, SnowflakeValue::Null)
    }
}

#[derive(Clone, Debug)]
pub struct DynamicRow {
    schema: Arc<Schema>,
    values: Box<[SnowflakeValue]>,
}

impl DynamicRow {
    /// Returns the shared schema metadata for this row.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Returns all decoded values in column order.
    pub fn values(&self) -> &[SnowflakeValue] {
        &self.values
    }

    /// Borrows a value by resolved column index.
    pub fn at(
        &self,
        index: ColumnIndex,
    ) -> std::result::Result<&SnowflakeValue, SchemaError> {
        self.schema
            .column_at(index)
            .ok_or_else(|| SchemaError::InvalidColumnIndex {
                index,
                len: self.schema.len(),
            })?;
        Ok(&self.values[index.as_usize()])
    }

    /// Resolves an exact result label and borrows the corresponding value.
    pub fn value_by_label(
        &self,
        name: &str,
    ) -> std::result::Result<&SnowflakeValue, SchemaError> {
        let idx = self.schema.column_by_label(name)?;
        self.at(idx)
    }

    /// Resolves an unquoted identifier and borrows the corresponding value.
    pub fn value_by_identifier(
        &self,
        name: &str,
    ) -> std::result::Result<&SnowflakeValue, SchemaError> {
        let idx = self.schema.column_by_identifier(name)?;
        self.at(idx)
    }

    /// Moves a decoded value out of the row, replacing the slot with `Null`.
    pub fn take(
        &mut self,
        index: ColumnIndex,
    ) -> std::result::Result<SnowflakeValue, SchemaError> {
        self.schema
            .column_at(index)
            .ok_or_else(|| SchemaError::InvalidColumnIndex {
                index,
                len: self.schema.len(),
            })?;
        Ok(std::mem::replace(
            &mut self.values[index.as_usize()],
            SnowflakeValue::Null,
        ))
    }

    /// Consumes the row and returns its schema and decoded values.
    pub fn into_parts(self) -> (Arc<Schema>, Box<[SnowflakeValue]>) {
        (self.schema, self.values)
    }

    /// Consumes the row into a JSON object keyed by raw result labels.
    pub fn into_json_object(
        self,
    ) -> std::result::Result<serde_json::Map<String, serde_json::Value>, SchemaError> {
        let DynamicRow { schema, values } = self;

        let mut map = serde_json::Map::new();
        for (col, value) in schema.columns().iter().zip(values.into_vec()) {
            if map.contains_key(col.name()) {
                return Err(SchemaError::DuplicateColumnName {
                    name: Box::from(col.name()),
                });
            }
            map.insert(col.name().to_string(), value.into_json_value());
        }

        Ok(map)
    }
}

impl SnowflakeValue {
    pub fn into_json_value(self) -> serde_json::Value {
        match self {
            SnowflakeValue::Null => serde_json::Value::Null,
            SnowflakeValue::Boolean(b) => serde_json::Value::Bool(b),
            SnowflakeValue::Integer(i) => match serde_json::Number::from_i128(i) {
                Some(n) => serde_json::Value::Number(n),
                None => serde_json::Value::String(i.to_string()),
            },
            SnowflakeValue::Float(f) => match serde_json::Number::from_f64(f) {
                Some(n) => serde_json::Value::Number(n),
                None => serde_json::Value::String(f.to_string()),
            },
            SnowflakeValue::Decimal(d) => serde_json::Value::String(d.raw().to_string()),
            SnowflakeValue::String(s) => serde_json::Value::String(s),
            SnowflakeValue::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
            SnowflakeValue::Time(t) => {
                serde_json::Value::String(t.format("%H:%M:%S%.f").to_string())
            }
            SnowflakeValue::TimestampNtz(dt) => {
                serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
            }
            SnowflakeValue::TimestampLtz(dt) => serde_json::Value::String(dt.to_rfc3339()),
            SnowflakeValue::TimestampTz(dt) => serde_json::Value::String(dt.to_rfc3339()),
            SnowflakeValue::Json(v) => v,
            SnowflakeValue::Binary(bytes) => {
                use base64::Engine as _;
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(&bytes))
            }
        }
    }
}

impl FromRow for DynamicRow {
    type Plan = Arc<Schema>;

    fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan> {
        Ok(ctx.clone_schema())
    }

    fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> Result<Self> {
        let mut values = Vec::with_capacity(plan.len());
        for col in plan.columns() {
            let cell = row.cell(col.index())?;
            values.push(decode_dynamic(cell)?);
        }

        Ok(DynamicRow {
            schema: Arc::clone(plan),
            values: values.into_boxed_slice(),
        })
    }
}

fn decode_dynamic(cell: CellRef<'_>) -> Result<SnowflakeValue> {
    if cell.is_null() {
        return Ok(SnowflakeValue::Null);
    }

    let raw = cell.raw().expect("non-null checked above");
    let ty = cell.column_type();

    match ty {
        ColumnType::Boolean => {
            if raw.eq_ignore_ascii_case("1") || raw.eq_ignore_ascii_case("true") {
                Ok(SnowflakeValue::Boolean(true))
            } else if raw.eq_ignore_ascii_case("0") || raw.eq_ignore_ascii_case("false") {
                Ok(SnowflakeValue::Boolean(false))
            } else {
                Err(cell.decode_error("bool", format!("'{raw}' is not bool")))
            }
        }
        ColumnType::Fixed { precision, scale } => {
            if scale.unwrap_or(0) == 0 {
                if let Ok(v) = raw.parse::<i128>() {
                    return Ok(SnowflakeValue::Integer(v));
                }
            }
            Ok(SnowflakeValue::Decimal(DecimalValue::new(
                raw, *precision, *scale,
            )))
        }
        ColumnType::Real => raw
            .parse::<f64>()
            .map(SnowflakeValue::Float)
            .map_err(|e| cell.decode_error("f64", format!("parse error: {e}"))),
        ColumnType::Text { .. } => Ok(SnowflakeValue::String(raw.to_string())),
        ColumnType::Date => {
            let days = raw
                .parse::<i64>()
                .map_err(|_| cell.decode_error("Date", format!("'{raw}' not Date")))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let date = if days >= 0 {
                epoch.checked_add_days(chrono::Days::new(days as u64))
            } else {
                epoch.checked_sub_days(chrono::Days::new(days.unsigned_abs()))
            }
            .ok_or_else(|| cell.decode_error("Date", format!("'{raw}' not Date")))?;
            Ok(SnowflakeValue::Date(date))
        }
        ColumnType::Time { scale } => {
            let scale = match scale {
                None => 0usize,
                Some(s) if (0..=9).contains(s) => *s as usize,
                Some(s) => {
                    return Err(cell.decode_error("Time", format!("invalid time scale: {s}")));
                }
            };
            let (secs, nsec) = parse_time_seconds_and_nanos(raw, scale)
                .map_err(|m| cell.decode_error("Time", m))?;
            let t = NaiveTime::from_num_seconds_from_midnight_opt(secs, nsec)
                .ok_or_else(|| cell.decode_error("Time", format!("invalid time: {raw}")))?;
            Ok(SnowflakeValue::Time(t))
        }
        ColumnType::TimestampNtz { scale } => {
            let scale = scale.unwrap_or(9);
            let dt = parse_timestamp_epoch(raw, scale)
                .map_err(|m| cell.decode_error("TimestampNtz", m))?;
            Ok(SnowflakeValue::TimestampNtz(dt.naive_utc()))
        }
        ColumnType::TimestampLtz { scale } => {
            let scale = scale.unwrap_or(9);
            let dt = parse_timestamp_epoch(raw, scale)
                .map_err(|m| cell.decode_error("TimestampLtz", m))?;
            Ok(SnowflakeValue::TimestampLtz(dt))
        }
        ColumnType::TimestampTz { scale } => {
            let scale = scale.unwrap_or(9);
            let dt = parse_timestamp_tz_with_offset(raw, scale)
                .map_err(|m| cell.decode_error("TimestampTz", m))?;
            Ok(SnowflakeValue::TimestampTz(dt))
        }
        ColumnType::Variant | ColumnType::Object | ColumnType::Array => {
            let v = serde_json::from_str(raw)
                .map_err(|e| cell.decode_error("json", format!("invalid json: {e}")))?;
            Ok(SnowflakeValue::Json(v))
        }
        ColumnType::Binary => {
            let bytes = decode_hex(raw).map_err(|m| cell.decode_error("binary", m))?;
            Ok(SnowflakeValue::Binary(bytes))
        }
        ColumnType::Geography
        | ColumnType::Geometry
        | ColumnType::Vector
        | ColumnType::Unknown { .. } => Ok(SnowflakeValue::String(raw.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::result::{
        ColumnType,
        test_data::{make_result_table_from_rows, make_schema},
    };

    fn one_cell_row(ty: ColumnType, value: &str) -> DynamicRow {
        let schema = make_schema(vec![("PAYLOAD".to_string(), ty, true)]);
        let table =
            make_result_table_from_rows(schema, vec![vec![Some(value.to_string())]]).unwrap();
        table.dynamic_rows().unwrap().next().unwrap().unwrap()
    }

    #[test]
    fn dynamic_row_keeps_text_cells_as_strings() {
        for value in [r#"{"a":1}"#, "plain text"] {
            let row = one_cell_row(ColumnType::Text { length: None }, value);
            match row.value_by_label("PAYLOAD").unwrap() {
                SnowflakeValue::String(actual) => assert_eq!(actual, value),
                other => panic!("expected String, got {other:?}"),
            }
        }
    }

    #[test]
    fn dynamic_row_decodes_variant_cells_as_json() {
        let row = one_cell_row(ColumnType::Variant, r#"{"a":1}"#);
        match row.value_by_label("PAYLOAD").unwrap() {
            SnowflakeValue::Json(value) => assert_eq!(value["a"], 1),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn dynamic_row_at_rejects_invalid_indices() {
        let row = one_cell_row(ColumnType::Text { length: None }, "value");
        let index = ColumnIndex::new(1).unwrap();
        assert!(matches!(
            row.at(index),
            Err(SchemaError::InvalidColumnIndex { index: actual, len: 1 }) if actual == index
        ));
    }

    #[test]
    fn dynamic_row_value_lookup_distinguishes_labels_and_identifiers() {
        let schema = make_schema(vec![
            (
                "ID".to_string(),
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
                false,
            ),
            (
                "id".to_string(),
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
                false,
            ),
        ]);
        let table = make_result_table_from_rows(
            schema,
            vec![vec![Some("1".to_string()), Some("2".to_string())]],
        )
        .unwrap();
        let row = table.dynamic_rows().unwrap().next().unwrap().unwrap();

        assert_eq!(row.value_by_label("ID").unwrap(), &SnowflakeValue::Integer(1));
        assert_eq!(row.value_by_label("id").unwrap(), &SnowflakeValue::Integer(2));
        assert_eq!(
            row.value_by_identifier("id").unwrap(),
            &SnowflakeValue::Integer(1)
        );
    }

    #[test]
    fn dynamic_row_take_replaces_slots_with_null() {
        let mut row = one_cell_row(ColumnType::Text { length: None }, "value");
        let index = row.schema().column_by_label("PAYLOAD").unwrap();

        assert_eq!(row.take(index).unwrap(), SnowflakeValue::String("value".into()));
        assert_eq!(row.at(index).unwrap(), &SnowflakeValue::Null);
        assert_eq!(row.take(index).unwrap(), SnowflakeValue::Null);
    }

    #[test]
    fn dynamic_row_into_parts_preserves_schema_and_values() {
        let schema = make_schema(vec![(
            "PAYLOAD".to_string(),
            ColumnType::Text { length: None },
            true,
        )]);
        let table =
            make_result_table_from_rows(schema, vec![vec![Some("value".to_string())]]).unwrap();
        let row = table.dynamic_rows().unwrap().next().unwrap().unwrap();

        let (schema, values) = row.into_parts();
        assert!(ptr::eq(schema.as_ref(), table.schema()));
        assert_eq!(values.as_ref(), &[SnowflakeValue::String("value".to_string())]);
    }

    #[test]
    fn dynamic_row_into_json_object_rejects_duplicate_labels() {
        let schema = make_schema(vec![
            (
                "id".to_string(),
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
                false,
            ),
            (
                "id".to_string(),
                ColumnType::Fixed {
                    precision: None,
                    scale: Some(0),
                },
                false,
            ),
        ]);
        let table = make_result_table_from_rows(
            schema,
            vec![vec![Some("1".to_string()), Some("2".to_string())]],
        )
        .unwrap();
        let row = table.dynamic_rows().unwrap().next().unwrap().unwrap();

        assert!(matches!(
            row.into_json_object(),
            Err(SchemaError::DuplicateColumnName { name }) if name.as_ref() == "id"
        ));
    }

    #[test]
    fn dynamic_row_paths_share_the_table_schema() {
        let schema = make_schema(vec![(
            "PAYLOAD".to_string(),
            ColumnType::Text { length: None },
            true,
        )]);
        let table =
            make_result_table_from_rows(schema, vec![vec![Some("value".to_string())]]).unwrap();

        let generic = table.rows::<DynamicRow>().unwrap().next().unwrap().unwrap();
        let alias = table.dynamic_rows().unwrap().next().unwrap().unwrap();

        assert!(ptr::eq(generic.schema(), table.schema()));
        assert!(ptr::eq(alias.schema(), table.schema()));
        assert!(ptr::eq(generic.schema(), alias.schema()));
    }
}
