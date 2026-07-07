use std::{any::type_name, fmt, mem, result::Result as StdResult, sync::Arc};

use base64::Engine as _;
use bytes::Bytes;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use crate::{
    error::{
        CellDecodeError, DuplicateColumnNameError, InvalidColumnIndexError, PlanBuildResult,
        RowDecodeResult, SchemaError,
    },
    result_table::{
        CellConversionError, CellDecodeResult, FromRow, RowPlanContext,
        cell::CellRef,
        decode::{
            Vector, decode_hex, decode_json_payload, parse_time_seconds_and_nanos,
            parse_timestamp_epoch, parse_timestamp_tz_with_offset, parse_vector_f32_payload,
            parse_vector_i32_payload,
        },
        row::RowRef,
        schema::{ColumnIndex, ColumnType, Schema},
    },
};

/// Result-side representation of a Snowflake `NUMBER` / `DECIMAL` cell with non-zero scale.
///
/// Snowflake decimals can carry up to 38 digits of precision, which does not fit in any native Rust numeric type.
/// `DecimalValue` therefore preserves the value as the exact string Snowflake delivered.
/// The column's declared precision and scale are available on its [`ColumnType`](crate::ColumnType).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecimalValue {
    raw: Box<str>,
}

impl DecimalValue {
    pub(crate) fn new(raw: impl Into<Box<str>>) -> Self {
        Self { raw: raw.into() }
    }

    /// Borrow the underlying decimal string.
    pub fn raw(&self) -> &str {
        &self.raw
    }
}

impl fmt::Display for DecimalValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

/// Result-side representation of a Snowflake `BINARY` cell.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BinaryValue(Bytes);

impl BinaryValue {
    pub(crate) fn new(bytes: impl Into<Bytes>) -> Self {
        Self(bytes.into())
    }

    /// Borrow the decoded bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume the wrapper, returning the underlying [`bytes::Bytes`].
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
}

impl From<Bytes> for BinaryValue {
    fn from(bytes: Bytes) -> Self {
        Self(bytes)
    }
}

impl From<Vec<u8>> for BinaryValue {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes.into())
    }
}

impl AsRef<[u8]> for BinaryValue {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Decoded Snowflake `VECTOR` value on the dynamic-typing path.
///
/// Snowflake result metadata does not report a `VECTOR`'s element type, so the payload determines the variant:
/// an integer-only payload decodes as [`Int`](VectorValue::Int), and a payload carrying a decimal point or a
/// lowercase `inf` / `-inf` / `nan` token decodes as [`Float`](VectorValue::Float). Snowflake cannot produce an empty
/// `VECTOR`, so an empty payload is rejected rather than being treated as an ambiguous element type.
#[derive(Clone, Debug, PartialEq)]
pub enum VectorValue {
    /// `VECTOR(INT)` — 32-bit signed integer elements.
    Int(Vector<i32>),
    /// `VECTOR(FLOAT)` — 32-bit IEEE 754 float elements.
    Float(Vector<f32>),
}

impl VectorValue {
    /// Convert this vector into a [`serde_json::Value`] array.
    ///
    /// `Int` elements become JSON numbers. Finite `Float` elements become JSON numbers; the non-finite `f32` values
    /// have no JSON representation and fall back to the strings `"inf"`, `"-inf"`, and `"nan"`, matching Snowflake's
    /// lowercase `VECTOR` wire token spelling.
    fn into_json_value(self) -> serde_json::Value {
        match self {
            VectorValue::Int(vector) => {
                serde_json::Value::Array(vector.into_vec().into_iter().map(Into::into).collect())
            }
            VectorValue::Float(vector) => serde_json::Value::Array(
                vector
                    .into_vec()
                    .into_iter()
                    .map(f32_element_to_json_value)
                    .collect(),
            ),
        }
    }
}

fn f32_element_to_json_value(value: f32) -> serde_json::Value {
    if value.is_nan() {
        serde_json::Value::String("nan".to_string())
    } else if value.is_infinite() {
        let token = if value.is_sign_positive() {
            "inf"
        } else {
            "-inf"
        };
        serde_json::Value::String(token.to_string())
    } else {
        match serde_json::Number::from_f64(f64::from(value)) {
            Some(number) => serde_json::Value::Number(number),
            None => serde_json::Value::String(value.to_string()),
        }
    }
}

/// Decoded value of a single cell on the dynamic-typing path.
///
/// Each variant corresponds to a Snowflake result type.
///
/// # Example
///
/// ```
/// use snowflake_connector_rs::CellValue;
///
/// let value = CellValue::Integer(42);
/// match value {
///     CellValue::Integer(n) => assert_eq!(n, 42),
///     CellValue::Null => unreachable!(),
///     other => panic!("unexpected variant: {other:?}"),
/// }
/// ```
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum CellValue {
    /// SQL `NULL`.
    Null,
    /// `BOOLEAN`.
    Boolean(bool),
    /// Integer-shaped `NUMBER` (scale 0) that fits in `i128`.
    Integer(i128),
    /// `FLOAT` / `DOUBLE` / `REAL`.
    Float(f64),
    /// `NUMBER` with non-zero scale — preserved as text via [`DecimalValue`].
    Decimal(DecimalValue),
    /// `TEXT` / `VARCHAR` / `CHAR` / `STRING`.
    String(String),
    /// `DATE`.
    Date(NaiveDate),
    /// `TIME`.
    Time(NaiveTime),
    /// `TIMESTAMP_NTZ`.
    TimestampNtz(NaiveDateTime),
    /// `TIMESTAMP_LTZ`, anchored to UTC.
    TimestampLtz(DateTime<Utc>),
    /// `TIMESTAMP_TZ`, with the wire-reported offset preserved.
    TimestampTz(DateTime<FixedOffset>),
    /// `VARIANT` / `OBJECT` / `ARRAY` payloads represented as JSON.
    ///
    /// Snowflake non-finite float tokens (`Infinity`, `-Infinity`, `NaN`) are rejected during decoding because JSON has
    /// no representation for them.
    Json(serde_json::Value),
    /// `BINARY`, decoded from Snowflake's hex representation.
    Binary(BinaryValue),
    /// `VECTOR`, with the element type inferred from the payload.
    Vector(VectorValue),
}

impl CellValue {
    /// Returns `true` for [`CellValue::Null`].
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::CellValue;
    ///
    /// assert!(CellValue::Null.is_null());
    /// assert!(!CellValue::Integer(0).is_null());
    /// ```
    pub fn is_null(&self) -> bool {
        matches!(self, CellValue::Null)
    }
}

/// A row decoded into the dynamic [`CellValue`] vocabulary.
///
/// Values are stored in column order and can be accessed by [`ColumnIndex`] or by raw column name.
#[derive(Clone, Debug, PartialEq)]
pub struct DynamicRow {
    schema: Arc<Schema>,
    values: Box<[CellValue]>,
}

impl DynamicRow {
    /// Borrow the shared schema this row was decoded against.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Borrow all decoded values in column order.
    pub fn values(&self) -> &[CellValue] {
        &self.values
    }

    /// Borrow the value of a column resolved by raw label (case-sensitive).
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::MissingColumn`] when no column carries the name, or [`SchemaError::AmbiguousColumn`] when several do.
    pub fn value(&self, name: &str) -> StdResult<&CellValue, SchemaError> {
        let idx = self.schema.column_index(name)?;
        self.value_at(idx)
    }

    /// Borrow the value at a resolved column index.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InvalidColumnIndex`] when `index` is out of range for this row's schema.
    pub fn value_at(&self, index: ColumnIndex) -> StdResult<&CellValue, SchemaError> {
        self.schema.column_at(index).ok_or_else(|| {
            SchemaError::InvalidColumnIndex(InvalidColumnIndexError::new(index, self.schema.len()))
        })?;
        Ok(&self.values[index.as_usize()])
    }

    /// Move a value out of the row by raw label, replacing the slot with
    /// [`CellValue::Null`].
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::MissingColumn`] when no column carries the name, or [`SchemaError::AmbiguousColumn`] when several do.
    pub fn take(&mut self, name: &str) -> StdResult<CellValue, SchemaError> {
        let idx = self.schema.column_index(name)?;
        self.take_at(idx)
    }

    /// Move a value out of the row by resolved index, replacing the slot
    /// with [`CellValue::Null`].
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InvalidColumnIndex`] when `index` is out of range for this row's schema.
    pub fn take_at(&mut self, index: ColumnIndex) -> StdResult<CellValue, SchemaError> {
        self.schema.column_at(index).ok_or_else(|| {
            SchemaError::InvalidColumnIndex(InvalidColumnIndexError::new(index, self.schema.len()))
        })?;

        Ok(mem::replace(
            &mut self.values[index.as_usize()],
            CellValue::Null,
        ))
    }

    /// Consume the row, returning the shared schema and the decoded values in column order.
    pub fn into_parts(self) -> (Arc<Schema>, Box<[CellValue]>) {
        (self.schema, self.values)
    }

    /// Consume the row into a `serde_json::Map` keyed by raw column label.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::DuplicateColumnName`] when two columns share the same raw label — JSON object keys must be unique.
    pub fn into_json_object(
        self,
    ) -> StdResult<serde_json::Map<String, serde_json::Value>, SchemaError> {
        let DynamicRow { schema, values } = self;

        let mut map = serde_json::Map::new();
        for (col, value) in schema.columns().iter().zip(values.into_vec()) {
            if map.contains_key(col.name()) {
                return Err(SchemaError::DuplicateColumnName(
                    DuplicateColumnNameError::new(col.name()),
                ));
            }
            map.insert(col.name().to_string(), value.into_json_value());
        }

        Ok(map)
    }
}

impl CellValue {
    /// Convert this value into a [`serde_json::Value`].
    ///
    /// Lossy in the cases where Snowflake's value range exceeds JSON's numeric range: `i128` integers outside `i64`/`u64` range and
    /// non-finite floats fall back to string. `Decimal` values render as their original decimal text. Dates/times/timestamps render
    /// in their canonical string form, and `Binary` is base64-encoded.
    pub fn into_json_value(self) -> serde_json::Value {
        match self {
            CellValue::Null => serde_json::Value::Null,
            CellValue::Boolean(b) => serde_json::Value::Bool(b),
            CellValue::Integer(i) => match serde_json::Number::from_i128(i) {
                Some(n) => serde_json::Value::Number(n),
                None => serde_json::Value::String(i.to_string()),
            },
            CellValue::Float(f) => match serde_json::Number::from_f64(f) {
                Some(n) => serde_json::Value::Number(n),
                None => serde_json::Value::String(f.to_string()),
            },
            CellValue::Decimal(d) => serde_json::Value::String(d.raw().to_string()),
            CellValue::String(s) => serde_json::Value::String(s),
            CellValue::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
            CellValue::Time(t) => serde_json::Value::String(t.format("%H:%M:%S%.f").to_string()),
            CellValue::TimestampNtz(dt) => {
                serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
            }
            CellValue::TimestampLtz(dt) => serde_json::Value::String(dt.to_rfc3339()),
            CellValue::TimestampTz(dt) => serde_json::Value::String(dt.to_rfc3339()),
            CellValue::Json(v) => v,
            CellValue::Binary(bytes) => serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(bytes.as_bytes()),
            ),
            CellValue::Vector(vector) => vector.into_json_value(),
        }
    }
}

impl FromRow for DynamicRow {
    type Plan = Arc<Schema>;

    fn build_plan(ctx: RowPlanContext<'_>) -> PlanBuildResult<Self::Plan> {
        Ok(ctx.shared_schema())
    }

    fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> RowDecodeResult<Self> {
        let mut values = Vec::with_capacity(plan.len());
        for (offset, col) in plan.columns().iter().enumerate() {
            let cell = row.cell_at_offset(col, offset);
            values.push(decode_dynamic(cell).map_err(|issue| {
                CellDecodeError::new(
                    cell.row_index(),
                    cell.column().index(),
                    cell.column().name(),
                    type_name::<CellValue>(),
                    cell.column().ty().clone(),
                    cell.raw(),
                    issue,
                )
            })?);
        }

        Ok(DynamicRow {
            schema: Arc::clone(plan),
            values: values.into_boxed_slice(),
        })
    }
}

fn decode_dynamic(cell: CellRef<'_>) -> CellDecodeResult<CellValue> {
    if cell.is_null() {
        return Ok(CellValue::Null);
    }

    let raw = cell.raw().expect("non-null checked above");
    let ty = cell.column().ty();

    match ty {
        ColumnType::Boolean => {
            if raw == "1" || raw.eq_ignore_ascii_case("true") {
                Ok(CellValue::Boolean(true))
            } else if raw == "0" || raw.eq_ignore_ascii_case("false") {
                Ok(CellValue::Boolean(false))
            } else {
                Err(CellConversionError::builder(format!("'{raw}' is not bool")).build())
            }
        }
        ColumnType::Fixed { scale, .. } => {
            if scale.unwrap_or(0) == 0 {
                if let Ok(v) = raw.parse::<i128>() {
                    return Ok(CellValue::Integer(v));
                }
            }
            Ok(CellValue::Decimal(DecimalValue::new(raw)))
        }
        ColumnType::Real => raw.parse::<f64>().map(CellValue::Float).map_err(|e| {
            CellConversionError::builder(format!("parse error: {e}"))
                .source(e)
                .build()
        }),
        ColumnType::Text { .. } => Ok(CellValue::String(raw.to_string())),
        ColumnType::Date => {
            let days = raw.parse::<i64>().map_err(|e| {
                CellConversionError::builder(format!("'{raw}' not Date"))
                    .source(e)
                    .build()
            })?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
            let date = if days >= 0 {
                epoch.checked_add_days(chrono::Days::new(days as u64))
            } else {
                epoch.checked_sub_days(chrono::Days::new(days.unsigned_abs()))
            }
            .ok_or_else(|| CellConversionError::builder(format!("'{raw}' not Date")).build())?;
            Ok(CellValue::Date(date))
        }
        ColumnType::Time { scale } => {
            let scale = match scale {
                None => 0usize,
                Some(s) if (0..=9).contains(s) => *s as usize,
                Some(s) => {
                    return Err(
                        CellConversionError::builder(format!("invalid time scale: {s}")).build(),
                    );
                }
            };
            let (secs, nsec) = parse_time_seconds_and_nanos(raw, scale)
                .map_err(|m| CellConversionError::builder(m).build())?;
            let t = NaiveTime::from_num_seconds_from_midnight_opt(secs, nsec).ok_or_else(|| {
                CellConversionError::builder(format!("invalid time: {raw}")).build()
            })?;
            Ok(CellValue::Time(t))
        }
        ColumnType::TimestampNtz { scale } => {
            let scale = scale.unwrap_or(9);
            let dt = parse_timestamp_epoch(raw, scale)
                .map_err(|m| CellConversionError::builder(m).build())?;
            Ok(CellValue::TimestampNtz(dt.naive_utc()))
        }
        ColumnType::TimestampLtz { scale } => {
            let scale = scale.unwrap_or(9);
            let dt = parse_timestamp_epoch(raw, scale)
                .map_err(|m| CellConversionError::builder(m).build())?;
            Ok(CellValue::TimestampLtz(dt))
        }
        ColumnType::TimestampTz { scale } => {
            let scale = scale.unwrap_or(9);
            let dt = parse_timestamp_tz_with_offset(raw, scale)
                .map_err(|m| CellConversionError::builder(m).build())?;
            Ok(CellValue::TimestampTz(dt))
        }
        ColumnType::Variant | ColumnType::Object | ColumnType::Array => {
            Ok(CellValue::Json(decode_json_payload(raw)?))
        }
        ColumnType::Binary { .. } => {
            let bytes = decode_hex(raw).map_err(|m| CellConversionError::builder(m).build())?;
            Ok(CellValue::Binary(BinaryValue::new(bytes)))
        }
        ColumnType::Vector => decode_vector_dynamic(raw).map(CellValue::Vector),
        ColumnType::Geography | ColumnType::Geometry | ColumnType::Unknown { .. } => {
            Ok(CellValue::String(raw.to_string()))
        }
    }
}

/// Decode a `VECTOR` payload, inferring the element type.
///
/// A `VECTOR(INT)` payload parses as [`VectorValue::Int`]. If integer parsing fails, the float parser handles decimal
/// and lowercase non-finite tokens and returns [`VectorValue::Float`].
fn decode_vector_dynamic(raw: &str) -> CellDecodeResult<VectorValue> {
    if let Ok(ints) = parse_vector_i32_payload(raw) {
        return Ok(VectorValue::Int(Vector::from_vec(ints)));
    }
    parse_vector_f32_payload(raw)
        .map(|floats| VectorValue::Float(Vector::from_vec(floats)))
        .map_err(|m| CellConversionError::builder(m).build())
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::result_table::{
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
            match row.value("PAYLOAD").unwrap() {
                CellValue::String(actual) => assert_eq!(actual, value),
                other => panic!("expected String, got {other:?}"),
            }
        }
    }

    #[test]
    fn dynamic_row_decodes_variant_cells_as_json() {
        let row = one_cell_row(ColumnType::Variant, r#"{"a":1}"#);
        match row.value("PAYLOAD").unwrap() {
            CellValue::Json(value) => assert_eq!(value["a"], 1),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn dynamic_row_decode_failure_reports_contextual_error() {
        let schema = make_schema(vec![("PAYLOAD".to_string(), ColumnType::Boolean, false)]);
        let table =
            make_result_table_from_rows(schema, vec![vec![Some("maybe".to_string())]]).unwrap();

        let err = table.dynamic_rows().unwrap().next().unwrap().unwrap_err();
        let decode = err
            .as_cell_decode_error()
            .expect("dynamic row decode should yield CellDecodeError");

        assert_eq!(decode.row_index(), 0);
        assert_eq!(decode.column_name(), "PAYLOAD");
        assert_eq!(decode.conversion_error().reason(), "'maybe' is not bool");
        assert!(decode.target_type_name().ends_with("CellValue"));
        assert_eq!(decode.raw_value_preview(), Some("maybe"));
    }

    #[test]
    fn dynamic_row_decodes_integer_vector() {
        let row = one_cell_row(ColumnType::Vector, "[1,2,3]");
        match row.value("PAYLOAD").unwrap() {
            CellValue::Vector(VectorValue::Int(vector)) => {
                assert_eq!(vector.as_slice(), &[1, 2, 3]);
            }
            other => panic!("expected Vector(Int), got {other:?}"),
        }
    }

    #[test]
    fn dynamic_row_decodes_float_vector_from_decimal_payload() {
        let row = one_cell_row(ColumnType::Vector, "[1.500000,-2.250000,0.000000]");
        match row.value("PAYLOAD").unwrap() {
            CellValue::Vector(VectorValue::Float(vector)) => {
                assert_eq!(vector.as_slice(), &[1.5f32, -2.25, 0.0]);
            }
            other => panic!("expected Vector(Float), got {other:?}"),
        }
    }

    #[test]
    fn dynamic_row_decodes_float_vector_from_non_finite_payload() {
        let row = one_cell_row(ColumnType::Vector, "[1.000000,nan,-inf]");
        match row.value("PAYLOAD").unwrap() {
            CellValue::Vector(VectorValue::Float(vector)) => {
                let slice = vector.as_slice();
                assert_eq!(slice[0], 1.0f32);
                assert!(slice[1].is_nan());
                assert!(slice[2].is_infinite() && slice[2].is_sign_negative());
            }
            other => panic!("expected Vector(Float), got {other:?}"),
        }
    }

    #[test]
    fn dynamic_row_malformed_vector_reports_contextual_error() {
        let schema = make_schema(vec![("PAYLOAD".to_string(), ColumnType::Vector, true)]);
        let table =
            make_result_table_from_rows(schema, vec![vec![Some("[abc]".to_string())]]).unwrap();

        let err = table.dynamic_rows().unwrap().next().unwrap().unwrap_err();
        let decode = err
            .as_cell_decode_error()
            .expect("malformed vector should yield CellDecodeError");
        assert_eq!(decode.column_name(), "PAYLOAD");
        assert!(
            decode
                .conversion_error()
                .reason()
                .contains("invalid VECTOR(FLOAT) element"),
            "actual: {}",
            decode.conversion_error().reason()
        );
    }

    #[test]
    fn float_vector_into_json_value_stringifies_non_finite_elements() {
        let value = CellValue::Vector(VectorValue::Float(Vector::from_vec(vec![
            1.5,
            f32::INFINITY,
            f32::NEG_INFINITY,
            f32::NAN,
        ])));
        assert_eq!(
            value.into_json_value(),
            serde_json::json!([1.5, "inf", "-inf", "nan"])
        );
    }

    #[test]
    fn integer_vector_into_json_value_is_number_array() {
        let value = CellValue::Vector(VectorValue::Int(Vector::from_vec(vec![1, -2, 3])));
        assert_eq!(value.into_json_value(), serde_json::json!([1, -2, 3]));
    }

    #[test]
    fn dynamic_row_value_at_rejects_invalid_indices() {
        let row = one_cell_row(ColumnType::Text { length: None }, "value");
        let index = ColumnIndex::new(1);
        assert!(matches!(
            row.value_at(index),
            Err(SchemaError::InvalidColumnIndex(error))
                if error.index() == index && error.column_count() == 1
        ));
    }

    #[test]
    fn dynamic_row_value_returns_exact_label_match() {
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

        assert_eq!(row.value("ID").unwrap(), &CellValue::Integer(1));
        assert_eq!(row.value("id").unwrap(), &CellValue::Integer(2));
    }

    #[test]
    fn dynamic_row_take_at_replaces_slots_with_null() {
        let mut row = one_cell_row(ColumnType::Text { length: None }, "value");
        let index = row.schema().column_index("PAYLOAD").unwrap();

        assert_eq!(
            row.take_at(index).unwrap(),
            CellValue::String("value".into())
        );
        assert_eq!(row.value_at(index).unwrap(), &CellValue::Null);
        assert_eq!(row.take_at(index).unwrap(), CellValue::Null);
    }

    #[test]
    fn dynamic_row_take_at_rejects_invalid_indices() {
        let mut row = one_cell_row(ColumnType::Text { length: None }, "value");
        let index = ColumnIndex::new(1);
        assert!(matches!(
            row.take_at(index),
            Err(SchemaError::InvalidColumnIndex(error))
                if error.index() == index && error.column_count() == 1
        ));
    }

    #[test]
    fn dynamic_row_take_resolves_label_and_replaces_slot() {
        let mut row = one_cell_row(ColumnType::Text { length: None }, "value");

        assert_eq!(
            row.take("PAYLOAD").unwrap(),
            CellValue::String("value".into())
        );
        assert_eq!(row.value("PAYLOAD").unwrap(), &CellValue::Null,);
        assert_eq!(row.take("PAYLOAD").unwrap(), CellValue::Null);
    }

    #[test]
    fn dynamic_row_take_reports_missing_column_for_unknown_label() {
        let mut row = one_cell_row(ColumnType::Text { length: None }, "value");
        assert!(matches!(
            row.take("missing"),
            Err(SchemaError::MissingColumn(error)) if error.name() == "missing"
        ));
    }

    #[test]
    fn dynamic_row_into_parts_preserves_schema_and_supports_walk() {
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
                "PAYLOAD".to_string(),
                ColumnType::Text { length: None },
                true,
            ),
        ]);
        let table = make_result_table_from_rows(
            schema,
            vec![vec![Some("1".to_string()), Some("value".to_string())]],
        )
        .unwrap();
        let row = table.dynamic_rows().unwrap().next().unwrap().unwrap();

        let (schema, values) = row.into_parts();
        assert!(ptr::eq(schema.as_ref(), table.schema()));
        let walked = schema
            .columns()
            .iter()
            .zip(values.into_vec())
            .map(|(column, value)| (column.name().to_string(), value))
            .collect::<Vec<_>>();
        assert_eq!(
            walked,
            vec![
                ("ID".to_string(), CellValue::Integer(1)),
                (
                    "PAYLOAD".to_string(),
                    CellValue::String("value".to_string())
                ),
            ]
        );
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
            Err(SchemaError::DuplicateColumnName(error)) if error.name() == "id"
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
