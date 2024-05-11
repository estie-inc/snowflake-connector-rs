use std::{collections::HashMap, ops::Deref, sync::Arc};

use chrono::{DateTime, Days, NaiveDate, NaiveDateTime};

use crate::{Error, Result};

#[derive(Debug)]
pub struct SnowflakeColumn {
    pub name: String,
    pub column_type: SnowflakeColumnType,
}

#[derive(Debug, Clone)]
pub struct SnowflakeColumnType {
    /// The index of the column in the row
    pub index: usize,
    /// Data type of the column in Snowflake
    pub snowflake_type: String,
    /// Whether the column is nullable
    pub nullable: bool,
}

#[derive(Debug)]
pub struct SnowflakeRow {
    pub(crate) row: Vec<Option<String>>,
    pub(crate) column_types: Arc<HashMap<String, SnowflakeColumnType>>,
}

impl SnowflakeRow {
    pub fn get<T: SnowflakeDecode>(&self, column_name: &str) -> Result<T> {
        let column_type = self
            .column_types
            .get(&column_name.to_ascii_uppercase())
            .ok_or_else(|| Error::Decode(format!("column not found: {}", column_name)))?;
        self.row[column_type.index].try_get()
    }
    pub fn column_names(&self) -> Vec<&str> {
        self.column_types.iter().map(|(k, _)| k.as_str()).collect()
    }
    pub fn column_types(&self) -> Vec<SnowflakeColumn> {
        let column_types = self.column_types.deref();
        let mut v: Vec<_> = column_types
            .iter()
            .map(|(k, v)| SnowflakeColumn {
                name: k.clone(),
                column_type: v.clone(),
            })
            .collect();
        // sort by column index
        v.sort_by_key(|c| c.column_type.index);
        v
    }
}

pub trait SnowflakeDecode: Sized {
    fn try_decode(value: &Option<String>) -> Result<Self>;
}

impl SnowflakeDecode for u64 {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not u64")))
    }
}
impl SnowflakeDecode for i64 {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not i64")))
    }
}
impl SnowflakeDecode for i32 {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not i32")))
    }
}

impl SnowflakeDecode for f64 {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not f64")))
    }
}

impl SnowflakeDecode for i8 {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not i8")))
    }
}

impl SnowflakeDecode for String {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        Ok(value.to_string())
    }
}

impl SnowflakeDecode for bool {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        if let Ok(v) = value.parse::<u16>() {
            return Ok(v > 0);
        }
        if let Ok(v) = value.parse::<bool>() {
            return Ok(v);
        }
        Err(Error::Decode(format!("'{value}' is not bool")))
    }
}

impl SnowflakeDecode for NaiveDateTime {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        if let Ok(v) = value.parse::<f64>() {
            let secs = v.trunc() as i64;
            let nsec = (v.fract() * 1_000_000_000.0) as u32;
            let dt = DateTime::from_timestamp(secs, nsec)
                .ok_or_else(|| Error::Decode(format!("invalid datetime: {}", value)))?;
            return Ok(dt.naive_utc());
        }
        if let Ok(v) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
            return Ok(v);
        }
        Err(Error::Decode(format!("'{value}' is not datetime")))
    }
}
impl SnowflakeDecode for chrono::NaiveDate {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        let days_since_epoch = value
            .parse::<u64>()
            .map_err(|_| Error::Decode(format!("'{value}' is not Date type")))?;
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap_or_default()
            .checked_add_days(Days::new(days_since_epoch))
            .ok_or(Error::Decode(format!("'{value}' is not a valid date")))
    }
}

impl SnowflakeDecode for serde_json::Value {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        let value = unwrap(value)?;
        serde_json::from_str(value).map_err(|_| Error::Decode(format!("'{value}' is not json")))
    }
}

impl<T: SnowflakeDecode> SnowflakeDecode for Option<T> {
    fn try_decode(value: &Option<String>) -> Result<Self> {
        if value.is_none() {
            return Ok(None);
        }
        T::try_decode(value).map(|v| Some(v))
    }
}

trait TryGet {
    fn try_get<T: SnowflakeDecode>(&self) -> Result<T>;
}

impl TryGet for Option<String> {
    fn try_get<T: SnowflakeDecode>(&self) -> Result<T> {
        T::try_decode(self)
    }
}

fn unwrap(value: &Option<String>) -> Result<&String> {
    value
        .as_ref()
        .ok_or_else(|| Error::Decode("value is null".into()))
}
