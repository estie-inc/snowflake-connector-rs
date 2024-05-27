use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Days, NaiveDate, NaiveDateTime, NaiveTime};

use crate::{Error, Result};

#[derive(Debug)]
pub struct SnowflakeColumn {
    pub(super) name: String,
    pub(super) index: usize,
    pub(super) column_type: SnowflakeColumnType,
}

impl SnowflakeColumn {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn column_type(&self) -> &SnowflakeColumnType {
        &self.column_type
    }

    pub fn index(&self) -> usize {
        self.index
    }
}

#[derive(Debug, Clone)]
pub struct SnowflakeColumnType {
    pub(super) snowflake_type: String,
    pub(super) nullable: bool,
    pub(super) length: Option<i64>,
    pub(super) precision: Option<i64>,
    pub(super) scale: Option<i64>,
}

impl SnowflakeColumnType {
    pub fn snowflake_type(&self) -> &str {
        &self.snowflake_type
    }
    pub fn nullable(&self) -> bool {
        self.nullable
    }
    pub fn length(&self) -> Option<i64> {
        self.length
    }
    pub fn precision(&self) -> Option<i64> {
        self.precision
    }
    pub fn scale(&self) -> Option<i64> {
        self.scale
    }
}

#[derive(Debug)]
pub struct SnowflakeRow {
    pub(crate) row: Vec<Option<String>>,
    pub(crate) column_types: Arc<Vec<SnowflakeColumnType>>,
    pub(crate) column_indices: Arc<HashMap<String, usize>>,
}

impl SnowflakeRow {
    pub fn get<T: SnowflakeDecode>(&self, column_name: &str) -> Result<T> {
        let idx = self
            .column_indices
            .get(&column_name.to_ascii_uppercase())
            .ok_or_else(|| Error::Decode(format!("column not found: {}", column_name)))?;
        let ty = &self.column_types[*idx];
        (&self.row[*idx], ty).try_get()
    }
    pub fn at<T: SnowflakeDecode>(&self, column_index: usize) -> Result<T> {
        let ty = &self.column_types[column_index];
        (&self.row[column_index], ty).try_get()
    }
    pub fn column_names(&self) -> Vec<&str> {
        let mut names: Vec<(_, usize)> = self.column_indices.iter().map(|(k, v)| (k, *v)).collect();
        names.sort_by_key(|(_, v)| *v);
        names.into_iter().map(|(name, _)| name.as_str()).collect()
    }
    pub fn column_types(&self) -> Vec<SnowflakeColumn> {
        let mut names: Vec<(String, usize)> = self
            .column_indices
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        names.sort_by_key(|(_, v)| *v);
        names
            .into_iter()
            .map(|(name, index)| SnowflakeColumn {
                name,
                index,
                column_type: self.column_types[index].clone(),
            })
            .collect()
    }
}

pub trait SnowflakeDecode: Sized {
    fn try_decode(value: &Option<String>, ty: &SnowflakeColumnType) -> Result<Self>;
}

impl SnowflakeDecode for u64 {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not u64")))
    }
}
impl SnowflakeDecode for i64 {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not i64")))
    }
}
impl SnowflakeDecode for i32 {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not i32")))
    }
}

impl SnowflakeDecode for f64 {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not f64")))
    }
}

impl SnowflakeDecode for i8 {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        value
            .parse()
            .map_err(|_| Error::Decode(format!("'{value}' is not i8")))
    }
}

impl SnowflakeDecode for String {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        Ok(value.to_string())
    }
}

impl SnowflakeDecode for bool {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        match value.to_uppercase().as_str() {
            "1" | "TRUE" => Ok(true),
            "0" | "FALSE" => Ok(false),
            _ => Err(Error::Decode(format!("'{value}' is not bool"))),
        }
    }
}

impl SnowflakeDecode for NaiveDateTime {
    fn try_decode(value: &Option<String>, ty: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        let scale = ty.scale.unwrap_or(0) as i32;
        if let Ok(mut v) = value.parse::<f64>() {
            v = v * 10_f64.powi(scale);
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

impl SnowflakeDecode for NaiveTime {
    fn try_decode(value: &Option<String>, ty: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        let scale = ty.scale.unwrap_or(0) as i32;
        if let Ok(mut v) = value.parse::<f64>() {
            v = v * 10_f64.powi(scale);
            let secs = v.trunc() as u32;
            let nsec = (v.fract() * 1_000_000_000.0) as u32;
            let t = NaiveTime::from_num_seconds_from_midnight_opt(secs, nsec)
                .ok_or_else(|| Error::Decode(format!("invalid time: {}", value)))?;
            return Ok(t);
        }
        Err(Error::Decode(format!("'{value}' is not Time type")))
    }
}

impl SnowflakeDecode for NaiveDate {
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
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
    fn try_decode(value: &Option<String>, _: &SnowflakeColumnType) -> Result<Self> {
        let value = unwrap(value)?;
        serde_json::from_str(value).map_err(|_| Error::Decode(format!("'{value}' is not json")))
    }
}

impl<T: SnowflakeDecode> SnowflakeDecode for Option<T> {
    fn try_decode(value: &Option<String>, ty: &SnowflakeColumnType) -> Result<Self> {
        if value.is_none() {
            return Ok(None);
        }
        T::try_decode(value, ty).map(|v| Some(v))
    }
}

trait TryGet {
    fn try_get<T: SnowflakeDecode>(&self) -> Result<T>;
}

impl TryGet for (&Option<String>, &SnowflakeColumnType) {
    fn try_get<T: SnowflakeDecode>(&self) -> Result<T> {
        T::try_decode(&self.0, &self.1)
    }
}

fn unwrap(value: &Option<String>) -> Result<&String> {
    value
        .as_ref()
        .ok_or_else(|| Error::Decode("value is null".into()))
}
