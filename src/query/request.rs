use std::collections::HashMap;
use std::fmt::Write;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use serde::Serialize;

/// Snowflake bind parameter type.
///
/// See <https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#using-bind-variables-in-a-statement>
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub enum BindingType {
    #[serde(rename = "FIXED")]
    Fixed,
    #[serde(rename = "REAL")]
    Real,
    #[serde(rename = "TEXT")]
    Text,
    #[serde(rename = "BOOLEAN")]
    Boolean,
    #[serde(rename = "DATE")]
    Date,
    #[serde(rename = "TIME")]
    Time,
    #[serde(rename = "TIMESTAMP_NTZ")]
    TimestampNtz,
    #[serde(rename = "TIMESTAMP_LTZ")]
    TimestampLtz,
    #[serde(rename = "TIMESTAMP_TZ")]
    TimestampTz,
    #[serde(rename = "BINARY")]
    Binary,
}

mod sealed {
    pub trait Sealed {}
}

/// Types accepted by [`Binding::fixed`] (Snowflake `FIXED` / exact numeric).
pub trait SnowflakeFixedType: sealed::Sealed + ToString {}

/// Types accepted by [`Binding::real`] (Snowflake `REAL` / floating-point).
pub trait SnowflakeRealType: sealed::Sealed + ToString {}

macro_rules! impl_sealed_binding {
    ($trait:ident => $($t:ty),*) => { $(
        impl sealed::Sealed for $t {}
        impl $trait for $t {}
    )* };
}
impl_sealed_binding!(SnowflakeFixedType => i8, i16, i32, i64, i128, u8, u16, u32, u64, u128);
impl_sealed_binding!(SnowflakeRealType => f32, f64);

/// A single bind parameter for a Snowflake query.
///
/// Use [`Binding::null`] to represent SQL NULL.
#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct Binding {
    #[serde(rename = "type")]
    binding_type: BindingType,
    value: Option<String>,
}

impl Binding {
    pub(crate) fn new(binding_type: BindingType, value: impl Into<String>) -> Self {
        Self {
            binding_type,
            value: Some(value.into()),
        }
    }

    /// NULL bind parameter with the given type.
    pub fn null(binding_type: BindingType) -> Self {
        Self {
            binding_type,
            value: None,
        }
    }

    pub fn fixed(value: impl SnowflakeFixedType) -> Self {
        Self::new(BindingType::Fixed, value.to_string())
    }

    /// Snowflake's REST API rejects `"inf"` / `"-inf"` for REAL bind type
    /// but accepts `"Infinity"` / `"-Infinity"`. `"NaN"` is accepted as-is.
    pub fn real(value: impl SnowflakeRealType) -> Self {
        let s = value.to_string();
        let s = match s.as_str() {
            "inf" => "Infinity".to_string(),
            "-inf" => "-Infinity".to_string(),
            _ => s,
        };
        Self::new(BindingType::Real, s)
    }

    pub fn text(value: impl Into<String>) -> Self {
        Self::new(BindingType::Text, value)
    }

    pub fn boolean(value: bool) -> Self {
        Self::new(BindingType::Boolean, value.to_string())
    }

    /// Snowflake REST API expects milliseconds since the Unix epoch for DATE.
    pub fn date(value: NaiveDate) -> Self {
        let ms = value
            .and_hms_opt(0, 0, 0)
            .expect("and_hms_opt(0, 0, 0) is always valid")
            .and_utc()
            .timestamp_millis();
        Self::new(BindingType::Date, ms.to_string())
    }

    /// Snowflake REST API expects nanoseconds since midnight for TIME.
    pub fn time(value: NaiveTime) -> Self {
        let total_nanos = u64::from(value.num_seconds_from_midnight()) * 1_000_000_000
            + u64::from(value.nanosecond() % 1_000_000_000);
        Self::new(BindingType::Time, total_nanos.to_string())
    }

    /// Snowflake REST API expects nanoseconds since the Unix epoch for TIMESTAMP_NTZ.
    pub fn timestamp_ntz(value: NaiveDateTime) -> Self {
        Self::new(BindingType::TimestampNtz, format_epoch_nanos(value))
    }

    /// Snowflake REST API expects nanoseconds since the Unix epoch for TIMESTAMP_LTZ.
    pub fn timestamp_ltz(value: DateTime<Utc>) -> Self {
        Self::new(
            BindingType::TimestampLtz,
            format_epoch_nanos(value.naive_utc()),
        )
    }

    /// Snowflake REST API expects nanoseconds since the Unix epoch followed by
    /// a space and the timezone offset encoded as `1440 + offset_minutes`.
    pub fn timestamp_tz(value: DateTime<FixedOffset>) -> Self {
        let nanos = format_epoch_nanos(value.naive_utc());
        let offset_minutes = value.offset().local_minus_utc() / 60;
        let sf_tz = 1440 + offset_minutes;
        Self::new(BindingType::TimestampTz, format!("{nanos} {sf_tz}"))
    }

    pub fn binary(value: &[u8]) -> Self {
        let hex = value
            .iter()
            .fold(String::with_capacity(value.len() * 2), |mut s, b| {
                let _ = write!(s, "{b:02X}");
                s
            });
        Self::new(BindingType::Binary, hex)
    }
}

/// Falls back to manual calculation when `timestamp_nanos_opt()` returns None
/// (dates outside ~1678–2262 overflow i64 nanoseconds).
fn format_epoch_nanos(value: NaiveDateTime) -> String {
    let ts = value.and_utc();
    match ts.timestamp_nanos_opt() {
        Some(nanos) => nanos.to_string(),
        None => {
            let secs = ts.timestamp();
            let total_nanos =
                i128::from(secs) * 1_000_000_000 + i128::from(ts.timestamp_subsec_nanos());
            total_nanos.to_string()
        }
    }
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    /// SQL statement to execute against Snowflake.
    pub sql_text: String,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    bindings: HashMap<String, Binding>,
}

impl QueryRequest {
    pub fn bindings(&self) -> &HashMap<String, Binding> {
        &self.bindings
    }

    /// Build a query with positional bind parameters.
    /// The iterator index order maps to `"1"`, `"2"`, … (1-origin keys).
    pub fn with_bindings(
        sql_text: impl Into<String>,
        bindings: impl IntoIterator<Item = Binding>,
    ) -> Self {
        let map = bindings
            .into_iter()
            .enumerate()
            .map(|(i, b)| ((i + 1).to_string(), b))
            .collect::<HashMap<_, _>>();
        Self {
            sql_text: sql_text.into(),
            bindings: map,
        }
    }
}

impl From<&str> for QueryRequest {
    fn from(sql_text: &str) -> Self {
        Self {
            sql_text: sql_text.to_string(),
            bindings: HashMap::new(),
        }
    }
}
impl From<&QueryRequest> for QueryRequest {
    fn from(request: &QueryRequest) -> Self {
        request.clone()
    }
}

impl From<String> for QueryRequest {
    fn from(sql_text: String) -> Self {
        Self {
            sql_text,
            bindings: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_request_with_bindings_serializes_correctly() {
        let request = QueryRequest::with_bindings(
            "INSERT INTO t (c1, c2) VALUES (?, ?)",
            vec![Binding::fixed(123), Binding::text("hello")],
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["sqlText"], "INSERT INTO t (c1, c2) VALUES (?, ?)");
        assert_eq!(json["bindings"]["1"]["type"], "FIXED");
        assert_eq!(json["bindings"]["1"]["value"], "123");
        assert_eq!(json["bindings"]["2"]["type"], "TEXT");
        assert_eq!(json["bindings"]["2"]["value"], "hello");
    }

    #[test]
    fn test_query_request_without_bindings_omits_field() {
        let request = QueryRequest::from("SELECT 1");
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["sqlText"], "SELECT 1");
        assert!(json.get("bindings").is_none());
    }

    #[test]
    fn test_from_str_has_no_bindings() {
        let request = QueryRequest::from("SELECT 1");
        assert!(request.bindings().is_empty());
    }

    #[test]
    fn test_from_string_has_no_bindings() {
        let request = QueryRequest::from("SELECT 1".to_string());
        assert!(request.bindings().is_empty());
    }

    #[test]
    fn test_with_bindings_indices_are_1_origin() {
        let request = QueryRequest::with_bindings(
            "SELECT ?, ?, ?",
            vec![
                Binding::fixed(1),
                Binding::text("two"),
                Binding::boolean(true),
            ],
        );
        let json = serde_json::to_value(&request).unwrap();
        let bindings = json["bindings"].as_object().unwrap();
        assert_eq!(bindings.len(), 3);
        assert_eq!(json["bindings"]["1"]["type"], "FIXED");
        assert_eq!(json["bindings"]["1"]["value"], "1");
        assert_eq!(json["bindings"]["2"]["type"], "TEXT");
        assert_eq!(json["bindings"]["2"]["value"], "two");
        assert_eq!(json["bindings"]["3"]["type"], "BOOLEAN");
        assert_eq!(json["bindings"]["3"]["value"], "true");
    }

    #[test]
    fn test_null_binding_serializes_correctly() {
        let request = QueryRequest::with_bindings(
            "INSERT INTO t (c1) VALUES (?)",
            vec![Binding::null(BindingType::Text)],
        );
        let json = serde_json::to_value(&request).unwrap();
        assert_eq!(json["bindings"]["1"]["type"], "TEXT");
        assert!(json["bindings"]["1"]["value"].is_null());
    }

    #[test]
    fn test_with_bindings_empty_vec_produces_empty_map() {
        let request = QueryRequest::with_bindings("SELECT 1", vec![]);
        assert!(request.bindings().is_empty());
    }

    #[test]
    fn test_binding_constructors() {
        let cases: Vec<(Binding, &str, &str)> = vec![
            (Binding::fixed(42), "FIXED", "42"),
            (Binding::real(1.5), "REAL", "1.5"),
            (Binding::text("hi"), "TEXT", "hi"),
            (Binding::boolean(true), "BOOLEAN", "true"),
            (Binding::boolean(false), "BOOLEAN", "false"),
            (
                Binding::date(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
                "DATE",
                "1718409600000",
            ),
            (
                Binding::time(NaiveTime::from_hms_opt(12, 34, 56).unwrap()),
                "TIME",
                "45296000000000",
            ),
            (
                Binding::timestamp_ntz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap(),
                ),
                "TIMESTAMP_NTZ",
                "1718454645000000000",
            ),
            (
                Binding::timestamp_ltz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap()
                        .and_utc(),
                ),
                "TIMESTAMP_LTZ",
                "1718454645000000000",
            ),
            (
                Binding::timestamp_tz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap()
                        .and_utc()
                        .fixed_offset(),
                ),
                "TIMESTAMP_TZ",
                "1718454645000000000 1440",
            ),
            (Binding::binary(b"Hello"), "BINARY", "48656C6C6F"),
        ];
        for (binding, expected_type, expected_value) in cases {
            let json = serde_json::to_value(&binding).unwrap();
            assert_eq!(json["type"], expected_type);
            assert_eq!(json["value"], expected_value);
        }
    }

    #[test]
    fn test_timestamp_tz_non_utc_offsets() {
        // UTC+9 (e.g. JST): 2024-06-15 21:30:45+09:00 = 2024-06-15 12:30:45 UTC
        let offset_east = FixedOffset::east_opt(9 * 3600).unwrap();
        let dt_east = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(21, 30, 45)
            .unwrap()
            .and_local_timezone(offset_east)
            .unwrap();
        let json = serde_json::to_value(Binding::timestamp_tz(dt_east)).unwrap();
        assert_eq!(json["type"], "TIMESTAMP_TZ");
        // UTC nanos for 2024-06-15 12:30:45 UTC, offset = 1440 + 540 = 1980
        assert_eq!(json["value"], "1718454645000000000 1980");

        // UTC-5 (e.g. EST): 2024-06-15 07:30:45-05:00 = 2024-06-15 12:30:45 UTC
        let offset_west = FixedOffset::west_opt(5 * 3600).unwrap();
        let dt_west = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(7, 30, 45)
            .unwrap()
            .and_local_timezone(offset_west)
            .unwrap();
        let json = serde_json::to_value(Binding::timestamp_tz(dt_west)).unwrap();
        assert_eq!(json["type"], "TIMESTAMP_TZ");
        // offset = 1440 + (-300) = 1140
        assert_eq!(json["value"], "1718454645000000000 1140");
    }

    #[test]
    fn test_real_nan_uses_real_type() {
        let json = serde_json::to_value(Binding::real(f64::NAN)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "NaN");
    }

    #[test]
    fn test_real_infinity_rewrites_to_full_word() {
        let json = serde_json::to_value(Binding::real(f64::INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "Infinity");

        let json = serde_json::to_value(Binding::real(f64::NEG_INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "-Infinity");

        let json = serde_json::to_value(Binding::real(f32::INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "Infinity");

        let json = serde_json::to_value(Binding::real(f32::NEG_INFINITY)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "-Infinity");
    }

    #[test]
    fn test_fixed_boundary_values() {
        let cases: Vec<(Binding, String)> = vec![
            (Binding::fixed(-1_i64), "-1".to_string()),
            (Binding::fixed(0_i64), "0".to_string()),
            (Binding::fixed(i64::MAX), i64::MAX.to_string()),
            (Binding::fixed(i64::MIN), i64::MIN.to_string()),
            (Binding::fixed(u64::MAX), u64::MAX.to_string()),
        ];
        for (binding, expected_value) in cases {
            let json = serde_json::to_value(&binding).unwrap();
            assert_eq!(json["type"], "FIXED");
            assert_eq!(json["value"], expected_value);
        }
    }

    #[test]
    fn test_real_zero() {
        let json = serde_json::to_value(Binding::real(0.0_f64)).unwrap();
        assert_eq!(json["type"], "REAL");
        assert_eq!(json["value"], "0");
    }
}
