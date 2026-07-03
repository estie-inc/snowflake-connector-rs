use std::fmt::{self, Display, Formatter};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use indexmap::IndexMap;
use serde::{Serialize, Serializer, ser::SerializeMap as _};

use crate::statement::{
    bind::{Bind, BindName, BindValue},
    builder::{StatementParts, StatementPartsRepr},
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireQueryBody<'a> {
    sql_text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bindings: Option<WireBindings<'a>>,
}

impl<'a> WireQueryBody<'a> {
    pub(crate) fn from_statement_parts(parts: &'a StatementParts) -> Self {
        let bindings = match parts.repr() {
            StatementPartsRepr::Unbound { .. } => None,
            StatementPartsRepr::Positional { bindings, .. } if bindings.is_empty() => None,
            StatementPartsRepr::Positional { bindings, .. } => {
                Some(WireBindings::Positional(bindings))
            }
            StatementPartsRepr::Named { bindings, .. } if bindings.is_empty() => None,
            StatementPartsRepr::Named { bindings, .. } => Some(WireBindings::Keyed(bindings)),
        };
        Self {
            sql_text: parts.sql(),
            bindings,
        }
    }
}

#[derive(Debug)]
enum WireBindings<'a> {
    Positional(&'a [Bind]),
    Keyed(&'a IndexMap<BindName, Bind>),
}

impl Serialize for WireBindings<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Positional(binds) => {
                let mut map = serializer.serialize_map(Some(binds.len()))?;
                for (index, bind) in binds.iter().enumerate() {
                    map.serialize_entry(&PositionalKey(index + 1), &WireBinding { bind })?;
                }
                map.end()
            }
            Self::Keyed(binds) => {
                let mut map = serializer.serialize_map(Some(binds.len()))?;
                for (name, bind) in binds.iter() {
                    map.serialize_entry(name.as_str(), &WireBinding { bind })?;
                }
                map.end()
            }
        }
    }
}

struct PositionalKey(usize);

impl Serialize for PositionalKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&self.0)
    }
}

#[derive(Debug)]
struct WireBinding<'a> {
    bind: &'a Bind,
}

impl Serialize for WireBinding<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("type", self.bind.ty().as_wire_str())?;

        match self.bind.value() {
            None => map.serialize_entry("value", &Option::<&str>::None)?,
            Some(value) => map.serialize_entry("value", &WireValue(value))?,
        }

        map.end()
    }
}

/// Per-variant wire-format encoder for `BindValue`.
struct WireValue<'a>(&'a BindValue);

impl Serialize for WireValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            BindValue::Fixed(value) => serializer.collect_str(value),
            BindValue::Real32(value) => serializer.collect_str(&Real32Adapter(*value)),
            BindValue::Real64(value) => serializer.collect_str(&Real64Adapter(*value)),
            BindValue::Bool(value) => {
                serializer.serialize_str(if *value { "true" } else { "false" })
            }
            BindValue::Text(value) => serializer.serialize_str(value.as_ref()),
            BindValue::Date(value) => serializer.collect_str(&DateMillisAdapter(*value)),
            BindValue::Time(value) => serializer.collect_str(&TimeNanosAdapter(*value)),
            BindValue::TimestampNtz(value) => serializer.collect_str(&EpochNanosAdapter(*value)),
            BindValue::TimestampLtz(value) => {
                serializer.collect_str(&EpochNanosAdapter(value.naive_utc()))
            }
            BindValue::TimestampTz(value) => serializer.collect_str(&TimestampTzAdapter(*value)),
            BindValue::Binary(value) => serializer.collect_str(&HexAdapter(value)),
            BindValue::Raw(value) => serializer.serialize_str(value.as_ref()),
        }
    }
}

struct HexAdapter<'a>(&'a [u8]);

impl Display for HexAdapter<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02X}")?;
        }
        Ok(())
    }
}

struct Real32Adapter(f32);

impl Display for Real32Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.0.is_infinite() {
            f.write_str(if self.0.is_sign_negative() {
                "-Infinity"
            } else {
                "Infinity"
            })
        } else {
            write!(f, "{}", self.0)
        }
    }
}

struct Real64Adapter(f64);

impl Display for Real64Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.0.is_infinite() {
            f.write_str(if self.0.is_sign_negative() {
                "-Infinity"
            } else {
                "Infinity"
            })
        } else {
            write!(f, "{}", self.0)
        }
    }
}

struct DateMillisAdapter(NaiveDate);

impl Display for DateMillisAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let ms = self
            .0
            .and_hms_opt(0, 0, 0)
            .expect("and_hms_opt(0, 0, 0) is always valid")
            .and_utc()
            .timestamp_millis();
        write!(f, "{ms}")
    }
}

struct TimeNanosAdapter(NaiveTime);

impl Display for TimeNanosAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let total_nanos = u64::from(self.0.num_seconds_from_midnight()) * 1_000_000_000
            + u64::from(self.0.nanosecond());
        write!(f, "{total_nanos}")
    }
}

struct EpochNanosAdapter(NaiveDateTime);

impl Display for EpochNanosAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let ts = self.0.and_utc();
        let total_nanos =
            i128::from(ts.timestamp()) * 1_000_000_000 + i128::from(ts.timestamp_subsec_nanos());
        write!(f, "{total_nanos}")
    }
}

struct TimestampTzAdapter(DateTime<FixedOffset>);

impl Display for TimestampTzAdapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let nanos = EpochNanosAdapter(self.0.naive_utc());
        let offset_minutes = self.0.offset().local_minus_utc() / 60;
        let sf_tz = 1440 + offset_minutes;
        write!(f, "{nanos} {sf_tz}")
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use chrono::{FixedOffset, NaiveDate, NaiveTime};
    use serde_json::json;

    use super::*;
    use crate::{
        IntoStatement, Statement,
        bind::{Binary, BindType, RawBind, Time, TimestampLtz, TimestampNtz, TimestampTz},
        statement::builder::into_statement_parts,
    };

    fn wire_body(statement: impl IntoStatement) -> serde_json::Value {
        let parts = into_statement_parts(statement);
        serde_json::to_value(WireQueryBody::from_statement_parts(&parts)).unwrap()
    }

    #[test]
    fn empty_bindings_are_omitted_from_wire_body() {
        assert_eq!(
            wire_body(Statement::new("SELECT 1")),
            json!({ "sqlText": "SELECT 1" }),
        );
        assert_eq!(
            wire_body(Statement::new_positional("SELECT 1")),
            json!({ "sqlText": "SELECT 1" }),
        );
        assert_eq!(
            wire_body(Statement::new_named("SELECT 1")),
            json!({ "sqlText": "SELECT 1" }),
        );
    }

    #[test]
    fn positional_bindings_use_one_origin_string_keys() {
        assert_eq!(
            wire_body(Statement::new("SELECT ?, ?").bind(1_i64).bind("two")),
            json!({
                "sqlText": "SELECT ?, ?",
                "bindings": {
                    "1": { "type": "FIXED", "value": "1" },
                    "2": { "type": "TEXT", "value": "two" },
                },
            }),
        );
    }

    #[test]
    fn named_bindings_preserve_caller_keys() {
        assert_eq!(
            wire_body(
                Statement::new("SELECT :id OR :1")
                    .bind_named("id", 1_i64)
                    .bind_named("1", 2_i64),
            ),
            json!({
                "sqlText": "SELECT :id OR :1",
                "bindings": {
                    "id": { "type": "FIXED", "value": "1" },
                    "1": { "type": "FIXED", "value": "2" },
                },
            }),
        );
    }

    #[test]
    fn named_bindings_do_not_normalize_keys() {
        assert_eq!(
            wire_body(Statement::new("SELECT :id").bind_named(":id", 1_i64)),
            json!({
                "sqlText": "SELECT :id",
                "bindings": {
                    ":id": { "type": "FIXED", "value": "1" },
                },
            }),
        );
    }

    #[test]
    fn raw_bind_decfloat_serializes_as_typed_value_and_null() {
        assert_eq!(
            wire_body(
                Statement::new("SELECT ?").bind(RawBind::new(BindType::DecFloat, "1.23e-40")),
            ),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "DECFLOAT", "value": "1.23e-40" },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(RawBind::null(BindType::DecFloat))),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "DECFLOAT", "value": null },
                },
            }),
        );
    }

    #[test]
    fn scalar_bindings_serialize_to_expected_wire_values() {
        assert_eq!(
            wire_body(
                Statement::new("SELECT ?").bind(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
            ),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "DATE", "value": "1718409600000" },
                },
            }),
        );

        assert_eq!(
            wire_body(
                Statement::new("SELECT ?").bind(
                    Time::try_from(NaiveTime::from_hms_nano_opt(12, 34, 56, 123_000_000).unwrap())
                        .unwrap(),
                )
            ),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "TIME", "value": "45296123000000" },
                },
            }),
        );

        assert_eq!(
            wire_body(
                Statement::new("SELECT ?").bind(
                    TimestampNtz::try_from(
                        NaiveDate::from_ymd_opt(2024, 6, 15)
                            .unwrap()
                            .and_hms_opt(12, 30, 45)
                            .unwrap(),
                    )
                    .unwrap(),
                ),
            ),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "TIMESTAMP_NTZ", "value": "1718454645000000000" },
                },
            }),
        );

        assert_eq!(
            wire_body(
                Statement::new("SELECT ?").bind(
                    TimestampLtz::try_from(
                        NaiveDate::from_ymd_opt(2024, 6, 15)
                            .unwrap()
                            .and_hms_opt(12, 30, 45)
                            .unwrap()
                            .and_utc(),
                    )
                    .unwrap(),
                ),
            ),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "TIMESTAMP_LTZ", "value": "1718454645000000000" },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(Binary::new(b"Hello".as_slice()))),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "BINARY", "value": "48656C6C6F" },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(true)),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "BOOLEAN", "value": "true" },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(false)),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "BOOLEAN", "value": "false" },
                },
            }),
        );
    }

    #[test]
    fn binary_bindings_stream_uppercase_hex_for_full_byte_range() {
        let payload = (0u8..=255u8).collect::<Vec<_>>();
        let mut expected_hex = String::with_capacity(512);
        for byte in 0u8..=255u8 {
            write!(expected_hex, "{byte:02X}").unwrap();
        }

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(Binary::new(payload))),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "BINARY", "value": expected_hex },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(Binary::new(Vec::<u8>::new()))),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "BINARY", "value": "" },
                },
            }),
        );
    }

    #[test]
    fn timestamp_tz_bindings_include_snowflake_offset_suffix() {
        assert_eq!(
            wire_body(
                Statement::new("SELECT ?").bind(
                    TimestampTz::try_from(
                        NaiveDate::from_ymd_opt(2024, 6, 15)
                            .unwrap()
                            .and_hms_opt(21, 30, 45)
                            .unwrap()
                            .and_local_timezone(FixedOffset::east_opt(9 * 3600).unwrap())
                            .unwrap(),
                    )
                    .unwrap(),
                ),
            ),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "TIMESTAMP_TZ", "value": "1718454645000000000 1980" },
                },
            }),
        );

        assert_eq!(
            wire_body(
                Statement::new("SELECT ?").bind(
                    TimestampTz::try_from(
                        NaiveDate::from_ymd_opt(2024, 6, 15)
                            .unwrap()
                            .and_hms_opt(7, 30, 45)
                            .unwrap()
                            .and_local_timezone(FixedOffset::west_opt(5 * 3600).unwrap())
                            .unwrap(),
                    )
                    .unwrap(),
                ),
            ),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "TIMESTAMP_TZ", "value": "1718454645000000000 1140" },
                },
            }),
        );
    }

    #[test]
    fn real_special_values_use_snowflake_spelling() {
        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(f64::INFINITY)),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "REAL", "value": "Infinity" },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(f32::NEG_INFINITY)),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "REAL", "value": "-Infinity" },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(f64::NAN)),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "REAL", "value": "NaN" },
                },
            }),
        );
    }

    #[test]
    fn typed_nulls_preserve_their_wire_type() {
        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(None::<&'static str>)),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "TEXT", "value": null },
                },
            }),
        );

        assert_eq!(
            wire_body(Statement::new("SELECT ?").bind(None::<NaiveDate>)),
            json!({
                "sqlText": "SELECT ?",
                "bindings": {
                    "1": { "type": "DATE", "value": null },
                },
            }),
        );
    }
}
