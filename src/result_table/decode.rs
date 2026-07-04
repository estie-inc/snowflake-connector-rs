use std::{any::type_name, result::Result as StdResult};

use chrono::{DateTime, Days, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use crate::result_table::{
    CellConversionError, CellDecodeResult,
    dynamic::DecimalValue,
    plan::RowPlanContext,
    row::RowRef,
    schema::{Column, ColumnIndex, ColumnType, Schema},
};
use crate::{ColumnCountMismatchError, Error, IncompatibleColumnTypeError, Result, SchemaError};

const LEGACY_TIMESTAMP_TZ_SHIFT: i128 = 16_384;

/// Decode a single result-set cell into a Rust value.
///
/// Implementations exist for primitive Rust types, [`DecimalValue`](crate::DecimalValue), and `Option<T>`.
/// Implement it yourself when adapting Snowflake values into domain types.
///
/// Decoding is split in two so schema-dependent work happens once per result schema rather than once per cell:
/// [`build_plan`](FromCell::build_plan) validates the column and precomputes any state, and
/// [`from_cell_with_plan`](FromCell::from_cell_with_plan) converts the raw cell text using that state.
///
/// # Example
///
/// A custom decoder that wraps `f64` for type safety and does not care about the column type:
///
/// ```
/// use snowflake_connector_rs::{
///     CellConversionError, CellDecodeResult, Column, FromCell, Result,
/// };
///
/// struct CelsiusTemp(f64);
///
/// impl FromCell for CelsiusTemp {
///     type Plan = ();
///
///     fn build_plan(_column: &Column) -> Result<Self::Plan> {
///         Ok(())
///     }
///
///     fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
///         let raw = raw.ok_or_else(|| CellConversionError::builder("value is NULL").build())?;
///         raw.parse::<f64>().map(CelsiusTemp).map_err(|e| {
///             CellConversionError::builder("not a valid temperature")
///                 .source(e)
///                 .build()
///         })
///     }
/// }
/// ```
pub trait FromCell: Sized {
    /// Per-column state prepared once and reused for every cell in that column.
    ///
    /// `Send + Sync` so the plan can be shared across threads when an iterator is moved between tasks.
    type Plan: Send + Sync;

    /// Validate the column and precompute schema-dependent state.
    ///
    /// # Errors
    ///
    /// Return [`SchemaError::IncompatibleColumnType`](crate::SchemaError::IncompatibleColumnType) when the column
    /// cannot be decoded into `Self`. This runs before any row is read, so a mismatch fails the whole
    /// [`rows`](crate::ResultTable::rows) call rather than surfacing per cell.
    fn build_plan(column: &Column) -> Result<Self::Plan>;

    /// Decode a single cell's raw text into `Self`.
    ///
    /// `raw` is `None` for SQL `NULL`. Implementations should reject `NULL` for non-`Option` targets and describe
    /// conversion failures with [`CellConversionError`].
    ///
    /// # Errors
    ///
    /// Implementations should return only cell-local issues here.
    fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self>;
}

/// A resolved column paired with a prepared [`FromCell::Plan`].
///
/// Built by [`FromRow::build_plan`], then handed to [`RowRef::get_with_plan`] for each row, avoiding
/// per-row schema lookups and per-cell type matching.
pub struct CellPlan<T: FromCell> {
    pub(crate) column: Column,
    pub(crate) offset: usize,
    pub(crate) decode_plan: T::Plan,
}

impl<T: FromCell> CellPlan<T> {
    /// Build a cell plan for an already-resolved column.
    ///
    /// # Errors
    ///
    /// Propagates [`FromCell::build_plan`] failures for `T`.
    pub fn new(column: &Column) -> Result<Self> {
        Ok(Self {
            column: column.clone(),
            offset: column.index().as_usize(),
            decode_plan: T::build_plan(column)?,
        })
    }

    /// Resolve a column by raw label and build its cell plan.
    ///
    /// # Errors
    ///
    /// - [`SchemaError::MissingColumn`](crate::SchemaError::MissingColumn) / [`SchemaError::AmbiguousColumn`](crate::SchemaError::AmbiguousColumn)
    ///   from the label lookup.
    /// - Any [`FromCell::build_plan`] failure for `T`.
    pub fn by_name(schema: &Schema, name: &str) -> Result<Self> {
        let index = schema.column_index(name)?;
        let column = schema
            .column_at(index)
            .expect("column_index returns a valid index");
        Self::new(column)
    }

    /// Resolve a column by position and build its cell plan.
    ///
    /// # Errors
    ///
    /// - [`SchemaError::ColumnCountMismatch`](crate::SchemaError::ColumnCountMismatch) when `position` is out of range.
    /// - Any [`FromCell::build_plan`] failure for `T`.
    pub fn by_position(schema: &Schema, position: usize) -> Result<Self> {
        let index = ColumnIndex::new(position as u32);
        let column = schema.column_at(index).ok_or_else(|| {
            SchemaError::ColumnCountMismatch(ColumnCountMismatchError::new(
                position + 1,
                schema.len(),
            ))
        })?;
        Self::new(column)
    }
}

/// Decode an entire row into a Rust type.
///
/// Implementations may use an associated plan to cache schema-dependent state before decoding individual rows.
///
/// # Example
///
/// Hand-written equivalent of `#[derive(FromRow)]` for a two-column row:
///
/// ```
/// use snowflake_connector_rs::{
///     CellPlan, FromRow, Result, RowPlanContext, RowRef,
/// };
///
/// struct UserRow {
///     id: i64,
///     name: String,
/// }
///
/// struct UserRowPlan {
///     id: CellPlan<i64>,
///     name: CellPlan<String>,
/// }
///
/// impl FromRow for UserRow {
///     type Plan = UserRowPlan;
///
///     fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan> {
///         let schema = ctx.schema();
///         Ok(UserRowPlan {
///             id: CellPlan::by_name(schema, "ID")?,
///             name: CellPlan::by_name(schema, "NAME")?,
///         })
///     }
///
///     fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> Result<Self> {
///         Ok(UserRow {
///             id: row.get_with_plan(&plan.id)?,
///             name: row.get_with_plan(&plan.name)?,
///         })
///     }
/// }
/// ```
pub trait FromRow: Sized {
    /// Per-result-schema state reused while decoding rows.
    ///
    /// `Send + Sync` so the plan can be shared across threads when an iterator is moved between tasks.
    type Plan: Send + Sync;

    /// Build the per-result-schema decode plan from the result-set metadata.
    ///
    /// # Errors
    ///
    /// Implementations may return any [`Error`](crate::Error) appropriate for schema validation or planning.
    /// Conventional errors include [`SchemaError::MissingColumn`](crate::SchemaError::MissingColumn)
    /// and [`SchemaError::ColumnCountMismatch`](crate::SchemaError::ColumnCountMismatch).
    fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan>;

    /// Decode a single row using the associated plan.
    ///
    /// # Errors
    ///
    /// Implementations may return any [`Error`](crate::Error) appropriate for row conversion.
    fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> Result<Self>;
}

impl FromRow for () {
    type Plan = ();

    fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan> {
        let schema = ctx.schema();
        if !schema.is_empty() {
            return Err(
                SchemaError::ColumnCountMismatch(ColumnCountMismatchError::new(0, schema.len()))
                    .into(),
            );
        }
        Ok(())
    }

    fn from_row_with_plan(_: RowRef<'_>, _: &Self::Plan) -> Result<Self> {
        Ok(())
    }
}

impl<T: FromCell> FromCell for Option<T> {
    type Plan = T::Plan;

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        T::build_plan(column)
    }

    fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
        match raw {
            None => Ok(None),
            Some(_) => T::from_cell_with_plan(raw, plan).map(Some),
        }
    }
}

/// Borrow the raw text, reporting SQL `NULL` as a cell error for non-`Option` targets.
fn required_raw(raw: Option<&str>) -> CellDecodeResult<&str> {
    raw.ok_or_else(|| CellConversionError::builder("value is NULL").build())
}

/// Build the plan-time error for a column that cannot be decoded as `T`.
fn incompatible_column<T>(column: &Column, detail: Option<String>) -> Error {
    SchemaError::IncompatibleColumnType(IncompatibleColumnTypeError::new(
        column.index(),
        column.name(),
        type_name::<T>(),
        column.ty().clone(),
        detail.map(String::into_boxed_str),
    ))
    .into()
}

macro_rules! impl_int_from_cell {
    ($t:ty) => {
        impl FromCell for $t {
            type Plan = ();

            fn build_plan(column: &Column) -> Result<Self::Plan> {
                // Integers are only valid for `FIXED` with `scale = 0`.
                match column.ty() {
                    ColumnType::Fixed { scale, .. } if scale.unwrap_or(0) == 0 => Ok(()),
                    ColumnType::Fixed { scale, .. } => Err(incompatible_column::<$t>(
                        column,
                        Some(format!(
                            "integer cannot decode FIXED with scale {}",
                            scale.unwrap_or(0)
                        )),
                    )),
                    _ => Err(incompatible_column::<$t>(column, None)),
                }
            }

            fn from_cell_with_plan(
                raw: Option<&str>,
                _plan: &Self::Plan,
            ) -> CellDecodeResult<Self> {
                let raw = required_raw(raw)?;
                raw.parse::<$t>().map_err(|e| {
                    CellConversionError::builder(format!("parse error: {e}"))
                        .source(e)
                        .build()
                })
            }
        }
    };
}

impl_int_from_cell!(i8);
impl_int_from_cell!(i16);
impl_int_from_cell!(i32);
impl_int_from_cell!(i64);
impl_int_from_cell!(i128);
impl_int_from_cell!(u8);
impl_int_from_cell!(u16);
impl_int_from_cell!(u32);
impl_int_from_cell!(u64);
impl_int_from_cell!(u128);

macro_rules! impl_float_from_cell {
    ($t:ty) => {
        impl FromCell for $t {
            type Plan = ();

            fn build_plan(column: &Column) -> Result<Self::Plan> {
                match column.ty() {
                    ColumnType::Real | ColumnType::Fixed { .. } => Ok(()),
                    _ => Err(incompatible_column::<$t>(column, None)),
                }
            }

            fn from_cell_with_plan(
                raw: Option<&str>,
                _plan: &Self::Plan,
            ) -> CellDecodeResult<Self> {
                let raw = required_raw(raw)?;
                raw.parse::<$t>().map_err(|e| {
                    CellConversionError::builder(format!("parse error: {e}"))
                        .source(e)
                        .build()
                })
            }
        }
    };
}

impl_float_from_cell!(f32);
impl_float_from_cell!(f64);

impl FromCell for String {
    type Plan = ();

    fn build_plan(_column: &Column) -> Result<Self::Plan> {
        Ok(())
    }

    fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
        required_raw(raw).map(|s| s.to_owned())
    }
}

impl FromCell for DecimalValue {
    type Plan = ();

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::Fixed { .. } => Ok(()),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
        required_raw(raw).map(DecimalValue::new)
    }
}

impl FromCell for bool {
    type Plan = ();

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::Boolean => Ok(()),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        if raw == "1" || raw.eq_ignore_ascii_case("true") {
            Ok(true)
        } else if raw == "0" || raw.eq_ignore_ascii_case("false") {
            Ok(false)
        } else {
            Err(CellConversionError::builder(format!("'{raw}' is not bool")).build())
        }
    }
}

impl FromCell for NaiveDate {
    type Plan = ();

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::Date => Ok(()),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        let days = raw.parse::<i64>().map_err(|e| {
            CellConversionError::builder(format!("'{raw}' is not Date"))
                .source(e)
                .build()
        })?;
        let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");

        if days >= 0 {
            unix_epoch
                .checked_add_days(Days::new(days as u64))
                .ok_or_else(|| {
                    CellConversionError::builder(format!("'{raw}' is not a valid date")).build()
                })
        } else {
            unix_epoch
                .checked_sub_days(Days::new(days.unsigned_abs()))
                .ok_or_else(|| {
                    CellConversionError::builder(format!("'{raw}' is not a valid date")).build()
                })
        }
    }
}

impl FromCell for NaiveTime {
    type Plan = TimePlan;

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::Time { scale } => {
                let scale = match scale {
                    None => 0usize,
                    Some(s) if (0..=9).contains(s) => *s as usize,
                    Some(s) => {
                        return Err(incompatible_column::<Self>(
                            column,
                            Some(format!("invalid time scale: {s}")),
                        ));
                    }
                };
                Ok(TimePlan::new(scale))
            }
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        let (secs, nsec) =
            parse_time_seconds_and_nanos_with(raw, plan.scale, plan.nanos_multiplier)
                .map_err(|m| CellConversionError::builder(m).build())?;

        NaiveTime::from_num_seconds_from_midnight_opt(secs, nsec)
            .ok_or_else(|| CellConversionError::builder(format!("invalid time: {raw}")).build())
    }
}

impl FromCell for NaiveDateTime {
    type Plan = TimestampPlan;

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::TimestampNtz { scale } => TimestampPlan::from_metadata_scale(*scale)
                .map_err(|detail| incompatible_column::<Self>(column, Some(detail))),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        parse_timestamp_epoch_with(raw, plan)
            .map(|dt| dt.naive_utc())
            .map_err(|m| CellConversionError::builder(m).build())
    }
}

/// Which UTC-anchored decode path a [`DateTime<Utc>`] column uses.
pub enum UtcTimestampPlan {
    /// `TIMESTAMP_LTZ` — stored as a UTC epoch.
    Ltz(TimestampPlan),
    /// `TIMESTAMP_TZ` — wire value carries an offset that is normalized to UTC.
    Tz(TimestampPlan),
}

impl FromCell for DateTime<Utc> {
    type Plan = UtcTimestampPlan;

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::TimestampLtz { scale } => TimestampPlan::from_metadata_scale(*scale)
                .map(UtcTimestampPlan::Ltz)
                .map_err(|detail| incompatible_column::<Self>(column, Some(detail))),
            ColumnType::TimestampTz { scale } => TimestampPlan::from_metadata_scale(*scale)
                .map(UtcTimestampPlan::Tz)
                .map_err(|detail| incompatible_column::<Self>(column, Some(detail))),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        match plan {
            UtcTimestampPlan::Ltz(plan) => parse_timestamp_epoch_with(raw, plan),
            UtcTimestampPlan::Tz(plan) => parse_timestamp_tz_as_utc_with(raw, plan),
        }
        .map_err(|m| CellConversionError::builder(m).build())
    }
}

impl FromCell for DateTime<FixedOffset> {
    type Plan = TimestampPlan;

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::TimestampTz { scale } => TimestampPlan::from_metadata_scale(*scale)
                .map_err(|detail| incompatible_column::<Self>(column, Some(detail))),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        parse_timestamp_tz_with_offset_with(raw, plan)
            .map_err(|m| CellConversionError::builder(m).build())
    }
}

impl FromCell for serde_json::Value {
    type Plan = ();

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::Variant | ColumnType::Object | ColumnType::Array => Ok(()),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        serde_json::from_str(raw).map_err(|e| {
            CellConversionError::builder(format!("invalid json: {e}"))
                .source(e)
                .build()
        })
    }
}

impl FromCell for Vec<u8> {
    type Plan = ();

    fn build_plan(column: &Column) -> Result<Self::Plan> {
        match column.ty() {
            ColumnType::Binary { .. } => Ok(()),
            _ => Err(incompatible_column::<Self>(column, None)),
        }
    }

    fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
        let raw = required_raw(raw)?;
        decode_hex(raw).map_err(|m| CellConversionError::builder(m).build())
    }
}

/// Precomputed fractional-seconds state for a `TIME` column.
pub struct TimePlan {
    scale: usize,
    /// `10^(9 - scale)`, scaling truncated fractional digits up to nanoseconds.
    nanos_multiplier: u32,
}

impl TimePlan {
    /// `scale` must already be validated to `0..=9`.
    fn new(scale: usize) -> Self {
        Self {
            scale,
            nanos_multiplier: 10u32.pow((9 - scale) as u32),
        }
    }
}

/// Precomputed scale factors for a timestamp column, shared by the `NTZ` / `LTZ` / `TZ` decoders.
#[derive(Clone, Copy)]
pub struct TimestampPlan {
    scale: usize,
    /// `10^scale`, converting between the scaled integer wire value and whole seconds.
    scale_factor: i128,
    /// `10^(9 - scale)`, scaling the fractional remainder up to nanoseconds.
    nanos_multiplier: u64,
}

impl TimestampPlan {
    const SCALE_ZERO: TimestampPlan = TimestampPlan {
        scale: 0,
        scale_factor: 1,
        nanos_multiplier: 1_000_000_000,
    };

    fn from_metadata_scale(scale: Option<u8>) -> StdResult<Self, String> {
        Self::new(scale.unwrap_or(9))
    }

    fn new(scale: u8) -> StdResult<Self, String> {
        let scale = validate_ts_scale(scale)?;
        // `scale` is `0..=9`, so neither power can overflow.
        Ok(Self {
            scale,
            scale_factor: 10i128.pow(scale as u32),
            nanos_multiplier: 10u64.pow((9 - scale) as u32),
        })
    }
}

fn validate_ts_scale(scale: u8) -> StdResult<usize, String> {
    if scale > 9 {
        return Err(format!("invalid timestamp scale: {scale} (expected 0..=9)"));
    }
    Ok(usize::from(scale))
}

fn parse_scaled_decimal_i128(
    s: &str,
    plan: &TimestampPlan,
    kind: &'static str,
) -> StdResult<i128, String> {
    let scale = plan.scale;
    let s = s.trim();
    if s.is_empty() {
        return Err(format!("Could not decode {kind}: {s}"));
    }

    let bytes = s.as_bytes();
    let mut i = 0usize;
    let negative = match bytes[0] {
        b'-' => {
            i = 1;
            true
        }
        b'+' => {
            i = 1;
            false
        }
        _ => false,
    };
    if i == bytes.len() {
        return Err(format!("Could not decode {kind}: {s}"));
    }

    let mut whole = 0i128;
    let whole_start = i;
    while i < bytes.len() {
        let b = bytes[i];
        if !b.is_ascii_digit() {
            break;
        }
        whole = whole
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(b - b'0')))
            .ok_or_else(|| format!("Could not decode {kind}: {s}"))?;
        i += 1;
    }
    if i == whole_start {
        return Err(format!("Could not decode {kind}: {s}"));
    }

    let mut scaled = whole
        .checked_mul(plan.scale_factor)
        .ok_or_else(|| format!("Could not decode {kind}: {s}"))?;

    if i < bytes.len() {
        if bytes[i] != b'.' {
            return Err(format!("Could not decode {kind}: {s}"));
        }
        i += 1;
        if i == bytes.len() {
            return Err(format!("Could not decode {kind}: {s}"));
        }
        if scale == 0 {
            return Err(format!("Could not decode {kind}: {s}"));
        }

        let mut frac = 0i128;
        let mut frac_len = 0usize;
        while i < bytes.len() {
            let b = bytes[i];
            if !b.is_ascii_digit() || frac_len >= scale {
                return Err(format!("Could not decode {kind}: {s}"));
            }
            frac = frac
                .checked_mul(10)
                .and_then(|value| value.checked_add(i128::from(b - b'0')))
                .ok_or_else(|| format!("Could not decode {kind}: {s}"))?;
            frac_len += 1;
            i += 1;
        }
        for _ in frac_len..scale {
            frac = frac
                .checked_mul(10)
                .ok_or_else(|| format!("Could not decode {kind}: {s}"))?;
        }
        scaled = scaled
            .checked_add(frac)
            .ok_or_else(|| format!("Could not decode {kind}: {s}"))?;
    }

    if negative {
        scaled = scaled
            .checked_neg()
            .ok_or_else(|| format!("Could not decode {kind}: {s}"))?;
    }
    Ok(scaled)
}

fn parse_timestamp_epoch_scaled(
    scaled: i128,
    plan: &TimestampPlan,
    original: &str,
    kind: &'static str,
) -> StdResult<DateTime<Utc>, String> {
    let secs = scaled.div_euclid(plan.scale_factor);
    let frac = scaled.rem_euclid(plan.scale_factor);

    let secs = i64::try_from(secs).map_err(|_| format!("Could not decode {kind}: {original}"))?;
    let frac = u64::try_from(frac).map_err(|_| format!("Could not decode {kind}: {original}"))?;

    let nsec = if plan.scale == 0 {
        0u32
    } else {
        frac.checked_mul(plan.nanos_multiplier)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| format!("Could not decode {kind}: {original}"))?
    };

    DateTime::from_timestamp(secs, nsec)
        .ok_or_else(|| format!("Could not decode {kind}: {original}"))
}

fn parse_unsigned_timestamp_epoch_fast(
    s: &str,
    plan: &TimestampPlan,
    original: &str,
    kind: &'static str,
) -> Option<StdResult<DateTime<Utc>, String>> {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.is_empty() || matches!(bytes[0], b'-' | b'+') {
        return None;
    }

    let mut i = 0usize;
    let mut secs = 0i64;
    while i < bytes.len() {
        let b = bytes[i];
        if !b.is_ascii_digit() {
            break;
        }
        secs = match secs
            .checked_mul(10)
            .and_then(|value| value.checked_add(i64::from(b - b'0')))
        {
            Some(value) => value,
            None => return Some(Err(format!("Could not decode {kind}: {s}"))),
        };
        i += 1;
    }
    if i == 0 {
        return Some(Err(format!("Could not decode {kind}: {s}")));
    }

    let nsec = if i == bytes.len() {
        0u32
    } else {
        if bytes[i] != b'.' {
            return Some(Err(format!("Could not decode {kind}: {s}")));
        }
        i += 1;
        if i == bytes.len() || plan.scale == 0 {
            return Some(Err(format!("Could not decode {kind}: {s}")));
        }

        let mut frac = 0u64;
        let mut frac_len = 0usize;
        while i < bytes.len() {
            let b = bytes[i];
            if !b.is_ascii_digit() || frac_len >= plan.scale {
                return Some(Err(format!("Could not decode {kind}: {s}")));
            }
            frac = frac * 10 + u64::from(b - b'0');
            frac_len += 1;
            i += 1;
        }
        for _ in frac_len..plan.scale {
            frac *= 10;
        }
        match frac
            .checked_mul(plan.nanos_multiplier)
            .and_then(|value| u32::try_from(value).ok())
        {
            Some(value) => value,
            None => return Some(Err(format!("Could not decode {kind}: {s}"))),
        }
    };

    Some(
        DateTime::from_timestamp(secs, nsec)
            .ok_or_else(|| format!("Could not decode {kind}: {original}")),
    )
}

pub(crate) fn parse_timestamp_epoch_with(
    s: &str,
    plan: &TimestampPlan,
) -> StdResult<DateTime<Utc>, String> {
    if let Some(result) = parse_unsigned_timestamp_epoch_fast(s, plan, s, "timestamp") {
        return result;
    }
    let scaled = parse_scaled_decimal_i128(s, plan, "timestamp")?;
    parse_timestamp_epoch_scaled(scaled, plan, s, "timestamp")
}

pub(crate) fn parse_timestamp_epoch(s: &str, scale: u8) -> StdResult<DateTime<Utc>, String> {
    parse_timestamp_epoch_with(s, &TimestampPlan::new(scale)?)
}

struct TimestampTzWire {
    utc: DateTime<Utc>,
    tz_index: i32,
}

fn parse_timestamp_tz_index(tz_str: &str, original: &str) -> StdResult<i32, String> {
    let tz_index = tz_str
        .parse::<i32>()
        .map_err(|_| format!("invalid timestamp_tz: {original}"))?;
    if !(0..=2880).contains(&tz_index) {
        return Err(format!("invalid timezone for timestamp_tz: {original}"));
    }
    Ok(tz_index)
}

fn fixed_offset_from_tz_index(tz_index: i32, original: &str) -> StdResult<FixedOffset, String> {
    let offset_minutes = tz_index
        .checked_sub(1440)
        .ok_or_else(|| format!("invalid timezone for timestamp_tz: {original}"))?;

    let offset_seconds = offset_minutes
        .checked_mul(60)
        .ok_or_else(|| format!("invalid timezone for timestamp_tz: {original}"))?;

    FixedOffset::east_opt(offset_seconds).ok_or_else(|| {
        format!("timezone offset out of range for DateTime<FixedOffset>: {original}")
    })
}

fn parse_legacy_timestamp_tz_wire(
    s: &str,
    plan: &TimestampPlan,
) -> StdResult<TimestampTzWire, String> {
    let packed = parse_scaled_decimal_i128(s, &TimestampPlan::SCALE_ZERO, "timestamp_tz")?;
    let epoch_scaled = packed.div_euclid(LEGACY_TIMESTAMP_TZ_SHIFT);
    let tz_index = packed.rem_euclid(LEGACY_TIMESTAMP_TZ_SHIFT);
    let utc = parse_timestamp_epoch_scaled(epoch_scaled, plan, s, "timestamp_tz")?;

    let tz_index =
        i32::try_from(tz_index).map_err(|_| format!("invalid timezone for timestamp_tz: {s}"))?;
    if !(0..=2880).contains(&tz_index) {
        return Err(format!("invalid timezone for timestamp_tz: {s}"));
    }

    Ok(TimestampTzWire { utc, tz_index })
}

fn parse_timestamp_tz_wire(s: &str, plan: &TimestampPlan) -> StdResult<TimestampTzWire, String> {
    let s = s.trim();
    let mut parts = s.split_whitespace();
    let first = parts
        .next()
        .ok_or_else(|| format!("invalid timestamp_tz: {s}"))?;

    if let Some(tz_str) = parts.next() {
        if parts.next().is_some() {
            return Err(format!("invalid timestamp_tz: {s}"));
        }
        let utc = parse_timestamp_epoch_with(first, plan)?;
        let tz_index = parse_timestamp_tz_index(tz_str, s)?;
        Ok(TimestampTzWire { utc, tz_index })
    } else {
        parse_legacy_timestamp_tz_wire(s, plan)
    }
}

pub(crate) fn parse_timestamp_tz_as_utc_with(
    s: &str,
    plan: &TimestampPlan,
) -> StdResult<DateTime<Utc>, String> {
    parse_timestamp_tz_wire(s, plan).map(|wire| wire.utc)
}

pub(crate) fn parse_timestamp_tz_with_offset_with(
    s: &str,
    plan: &TimestampPlan,
) -> StdResult<DateTime<FixedOffset>, String> {
    let wire = parse_timestamp_tz_wire(s, plan)?;
    let offset = fixed_offset_from_tz_index(wire.tz_index, s)?;
    Ok(wire.utc.with_timezone(&offset))
}

pub(crate) fn parse_timestamp_tz_with_offset(
    s: &str,
    scale: u8,
) -> StdResult<DateTime<FixedOffset>, String> {
    parse_timestamp_tz_with_offset_with(s, &TimestampPlan::new(scale)?)
}

fn parse_time_seconds_and_nanos_with(
    value: &str,
    scale: usize,
    nanos_multiplier: u32,
) -> StdResult<(u32, u32), String> {
    let value = value.trim();
    let (secs_str, frac_str) = match value.split_once('.') {
        Some((secs, frac)) if !frac.is_empty() => (secs, Some(frac)),
        Some(_) => return Err(format!("invalid time: {value}")),
        None => (value, None),
    };
    let secs = secs_str
        .parse::<u32>()
        .map_err(|_| format!("'{value}' is not Time type"))?;

    let frac_str = frac_str.unwrap_or("");
    if !frac_str.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return Err(format!("invalid time: {value}"));
    }
    if scale == 0 {
        return Ok((secs, 0));
    }

    let mut frac_scaled = 0u32;
    let used_digits = frac_str.len().min(scale);
    for b in frac_str.bytes().take(used_digits) {
        frac_scaled = frac_scaled
            .checked_mul(10)
            .and_then(|value| value.checked_add(u32::from(b - b'0')))
            .ok_or_else(|| format!("invalid time: {value}"))?;
    }
    for _ in used_digits..scale {
        frac_scaled = frac_scaled
            .checked_mul(10)
            .ok_or_else(|| format!("invalid time: {value}"))?;
    }
    let nsec = frac_scaled
        .checked_mul(nanos_multiplier)
        .ok_or_else(|| format!("invalid time: {value}"))?;

    Ok((secs, nsec))
}

pub(crate) fn parse_time_seconds_and_nanos(
    value: &str,
    scale: usize,
) -> StdResult<(u32, u32), String> {
    if scale > 9 {
        return Err(format!("invalid time scale: {scale}"));
    }
    parse_time_seconds_and_nanos_with(value, scale, 10u32.pow((9 - scale) as u32))
}

pub(crate) fn decode_hex(s: &str) -> StdResult<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err(format!("invalid hex length: {}", s.len()));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> StdResult<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex digit: {}", b as char)),
    }
}

macro_rules! impl_tuple_from_row {
    ($len:expr; $($t:ident => $idx:tt),*) => {
        impl<$($t: FromCell),*> FromRow for ($($t,)*) {
            type Plan = ($(CellPlan<$t>,)*);

            fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan> {
                let schema = ctx.schema();
                if schema.len() != $len {
                    return Err(SchemaError::ColumnCountMismatch(
                        ColumnCountMismatchError::new($len, schema.len()),
                    )
                    .into());
                }
                Ok(($(CellPlan::<$t>::by_position(schema, $idx)?,)*))
            }

            fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> Result<Self> {
                Ok(($(row.get_with_plan(&plan.$idx)?,)*))
            }
        }
    };
}

impl_tuple_from_row!(1; T0 => 0);
impl_tuple_from_row!(2; T0 => 0, T1 => 1);
impl_tuple_from_row!(3; T0 => 0, T1 => 1, T2 => 2);
impl_tuple_from_row!(4; T0 => 0, T1 => 1, T2 => 2, T3 => 3);
impl_tuple_from_row!(5; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4);
impl_tuple_from_row!(6; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5);
impl_tuple_from_row!(7; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6);
impl_tuple_from_row!(8; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7);
impl_tuple_from_row!(9; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8);
impl_tuple_from_row!(10; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8, T9 => 9);
impl_tuple_from_row!(11; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8, T9 => 9, T10 => 10);
impl_tuple_from_row!(12; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8, T9 => 9, T10 => 10, T11 => 11);
impl_tuple_from_row!(13; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8, T9 => 9, T10 => 10, T11 => 11, T12 => 12);
impl_tuple_from_row!(14; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8, T9 => 9, T10 => 10, T11 => 11, T12 => 12, T13 => 13);
impl_tuple_from_row!(15; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8, T9 => 9, T10 => 10, T11 => 11, T12 => 12, T13 => 13, T14 => 14);
impl_tuple_from_row!(16; T0 => 0, T1 => 1, T2 => 2, T3 => 3, T4 => 4, T5 => 5, T6 => 6, T7 => 7, T8 => 8, T9 => 9, T10 => 10, T11 => 11, T12 => 12, T13 => 13, T14 => 14, T15 => 15);

#[cfg(test)]
mod tests {
    use std::{error::Error as StdError, sync::Arc};

    use bytes::Bytes;

    use super::*;
    use crate::{
        ResultTable,
        result_table::test_data::{make_result_table_from_rows, make_schema},
        rowset::parser::parse_inline_result_table,
    };

    fn timestamp_scale(ty: &ColumnType) -> Option<u8> {
        match ty {
            ColumnType::TimestampNtz { scale }
            | ColumnType::TimestampLtz { scale }
            | ColumnType::TimestampTz { scale } => *scale,
            _ => None,
        }
    }

    fn parse_timestamp_tz_as_utc(s: &str, scale: u8) -> StdResult<DateTime<Utc>, String> {
        parse_timestamp_tz_as_utc_with(s, &TimestampPlan::new(scale)?)
    }

    /// Pre-1970 timestamps with a fractional part: `-1.1` is 1969-12-31T23:59:58.900Z (= secs -2, nsec 900_000_000), not `.100`.
    #[test]
    fn parse_timestamp_epoch_negative_with_fraction() {
        let dt = parse_timestamp_epoch("-1.1", 9).unwrap();
        assert_eq!(dt.timestamp(), -2);
        assert_eq!(dt.timestamp_subsec_nanos(), 900_000_000);
    }

    /// `-1.5` happens to be symmetric and was already correct, but keep the
    /// explicit case so future regressions show up next to the asymmetric one.
    #[test]
    fn parse_timestamp_epoch_negative_half() {
        let dt = parse_timestamp_epoch("-1.5", 9).unwrap();
        assert_eq!(dt.timestamp(), -2);
        assert_eq!(dt.timestamp_subsec_nanos(), 500_000_000);
    }

    /// `-0.1` straddles the negative-zero boundary: secs_str = "-0" parses to
    /// 0 but the value is still negative. Result must be 1969-12-31T23:59:59.900Z.
    #[test]
    fn parse_timestamp_epoch_negative_zero_subsecond() {
        let dt = parse_timestamp_epoch("-0.1", 9).unwrap();
        assert_eq!(dt.timestamp(), -1);
        assert_eq!(dt.timestamp_subsec_nanos(), 900_000_000);
    }

    /// Positive subsecond must NOT be complemented.
    #[test]
    fn parse_timestamp_epoch_positive_fraction() {
        let dt = parse_timestamp_epoch("0.1", 9).unwrap();
        assert_eq!(dt.timestamp(), 0);
        assert_eq!(dt.timestamp_subsec_nanos(), 100_000_000);
    }

    /// scale=3 (millis) negative.
    #[test]
    fn parse_timestamp_epoch_negative_fraction_scale_3() {
        let dt = parse_timestamp_epoch("-1.250", 3).unwrap();
        assert_eq!(dt.timestamp(), -2);
        assert_eq!(dt.timestamp_subsec_millis(), 750);
    }

    #[test]
    fn parse_timestamp_epoch_rejects_fraction_when_scale_zero() {
        let err = parse_timestamp_epoch("0.1", 0).unwrap_err();
        assert!(err.contains("Could not decode timestamp"), "actual: {err}");
    }

    #[test]
    fn parse_timestamp_epoch_rejects_too_many_fraction_digits_for_scale() {
        let err = parse_timestamp_epoch("1.2345", 3).unwrap_err();
        assert!(err.contains("Could not decode timestamp"), "actual: {err}");
    }

    #[test]
    fn parse_timestamp_epoch_rejects_trailing_garbage() {
        let err = parse_timestamp_epoch("0.1xyz", 1).unwrap_err();
        assert!(err.contains("Could not decode timestamp"), "actual: {err}");
    }

    #[test]
    fn parse_timestamp_epoch_rejects_extreme_negative_subsecond_without_overflow() {
        let err = parse_timestamp_epoch("-9223372036854775808.1", 9).unwrap_err();
        assert!(err.contains("Could not decode timestamp"), "actual: {err}");
    }

    /// Snowflake documents timestamp scales in the `0..=9` range.
    #[test]
    fn parse_timestamp_epoch_rejects_out_of_range_scale() {
        for scale in [10, u8::MAX] {
            let err = parse_timestamp_epoch("0.1", scale).unwrap_err();
            assert!(err.contains("invalid timestamp scale"), "actual: {err}");
        }
    }

    #[test]
    fn timestamp_scale_ignores_fixed_scale() {
        assert_eq!(
            timestamp_scale(&ColumnType::Fixed {
                precision: Some(10),
                scale: Some(2),
            }),
            None
        );
        assert_eq!(
            timestamp_scale(&ColumnType::TimestampTz { scale: Some(4) }),
            Some(4)
        );
    }

    #[test]
    fn parse_timestamp_tz_as_utc_accepts_legacy_numeric_only_format() {
        let dt = parse_timestamp_tz_as_utc("1440", 0).unwrap();
        assert_eq!(dt.timestamp(), 0);
        assert_eq!(dt.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn parse_timestamp_tz_with_offset_accepts_legacy_numeric_only_format() {
        let dt = parse_timestamp_tz_with_offset("1440", 0).unwrap();
        assert_eq!(dt.timestamp(), 0);
        assert_eq!(dt.offset().local_minus_utc(), 0);
    }

    #[test]
    fn parse_timestamp_tz_as_utc_accepts_legacy_numeric_only_format_with_subseconds() {
        let epoch_scaled = 1_718_429_445_123_456_789i128;
        let packed = epoch_scaled * LEGACY_TIMESTAMP_TZ_SHIFT + 1980;
        let dt = parse_timestamp_tz_as_utc(&packed.to_string(), 9).unwrap();
        assert_eq!(dt.timestamp(), 1_718_429_445);
        assert_eq!(dt.timestamp_subsec_nanos(), 123_456_789);
    }

    #[test]
    fn parse_timestamp_tz_with_offset_accepts_legacy_numeric_only_format_with_subseconds() {
        let epoch_scaled = 1_718_429_445_123_456_789i128;
        let packed = epoch_scaled * LEGACY_TIMESTAMP_TZ_SHIFT + 1980;
        let dt = parse_timestamp_tz_with_offset(&packed.to_string(), 9).unwrap();
        assert_eq!(dt.timestamp(), 1_718_429_445);
        assert_eq!(dt.timestamp_subsec_nanos(), 123_456_789);
        assert_eq!(dt.offset().local_minus_utc(), 9 * 60 * 60);
    }

    #[test]
    fn parse_timestamp_tz_as_utc_accepts_boundary_timezone_indices() {
        let far_west = parse_timestamp_tz_as_utc("0 0", 9).unwrap();
        assert_eq!(far_west.timestamp(), 0);

        let far_east = parse_timestamp_tz_as_utc("0 2880", 9).unwrap();
        assert_eq!(far_east.timestamp(), 0);
    }

    #[test]
    fn parse_timestamp_tz_with_offset_rejects_trailing_tokens() {
        let err = parse_timestamp_tz_with_offset("0 1440 trailing", 9).unwrap_err();
        assert!(err.contains("invalid timestamp_tz"), "actual: {err}");
    }

    #[test]
    fn parse_timestamp_tz_with_offset_accepts_full_fixed_offset_minute_range() {
        let far_west = parse_timestamp_tz_with_offset("0 1", 9).unwrap();
        assert_eq!(
            far_west.offset().local_minus_utc(),
            -(23 * 60 * 60 + 59 * 60)
        );

        let plus_14_01 = parse_timestamp_tz_with_offset("0 2281", 9).unwrap();
        assert_eq!(plus_14_01.offset().local_minus_utc(), 14 * 60 * 60 + 60);

        let far_east = parse_timestamp_tz_with_offset("0 2879", 9).unwrap();
        assert_eq!(far_east.offset().local_minus_utc(), 23 * 60 * 60 + 59 * 60);
    }

    #[test]
    fn parse_timestamp_tz_with_offset_rejects_unrepresentable_boundary_offsets() {
        for raw in ["0 0", "0 2880"] {
            let err = parse_timestamp_tz_with_offset(raw, 9).unwrap_err();
            assert!(
                err.contains("out of range for DateTime<FixedOffset>"),
                "actual: {err}"
            );
        }
    }

    #[test]
    fn parse_time_seconds_and_nanos_truncates_numeric_fraction_when_scale_zero() {
        let (secs, nsec) = parse_time_seconds_and_nanos("45296.123", 0).unwrap();
        assert_eq!(secs, 45_296);
        assert_eq!(nsec, 0);
    }

    #[test]
    fn parse_time_seconds_and_nanos_rejects_fraction_when_scale_zero() {
        let err = parse_time_seconds_and_nanos("1.xyz", 0).unwrap_err();
        assert!(err.contains("invalid time"), "actual: {err}");
    }

    #[test]
    fn parse_time_seconds_and_nanos_truncates_too_many_fraction_digits_for_scale() {
        let (secs, nsec) = parse_time_seconds_and_nanos("1.999", 2).unwrap();
        assert_eq!(secs, 1);
        assert_eq!(nsec, 990_000_000);
    }

    fn one_cell_table(ty: ColumnType, value: &str) -> ResultTable {
        let schema = make_schema(vec![("X".to_string(), ty, true)]);
        make_result_table_from_rows(schema, vec![vec![Some(value.to_string())]]).unwrap()
    }

    fn one_nullable_cell_table(ty: ColumnType, value: Option<&str>) -> ResultTable {
        let schema = make_schema(vec![("X".to_string(), ty, true)]);
        make_result_table_from_rows(
            schema,
            vec![vec![value.map(std::string::ToString::to_string)]],
        )
        .unwrap()
    }

    fn zero_cell_table(row_count: usize) -> ResultTable {
        let schema = make_schema(vec![]);
        make_result_table_from_rows(schema, (0..row_count).map(|_| Vec::new()).collect()).unwrap()
    }

    #[derive(Debug)]
    struct CelsiusTemp;

    impl FromCell for CelsiusTemp {
        type Plan = ();

        fn build_plan(_column: &Column) -> Result<Self::Plan> {
            Ok(())
        }

        fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
            let raw = required_raw(raw)?;
            raw.parse::<f64>().map(|_| CelsiusTemp).map_err(|e| {
                CellConversionError::builder("not a valid temperature")
                    .source(e)
                    .build()
            })
        }
    }

    #[derive(Debug)]
    struct AlwaysFails;

    impl FromCell for AlwaysFails {
        type Plan = ();

        fn build_plan(_column: &Column) -> Result<Self::Plan> {
            Ok(())
        }

        fn from_cell_with_plan(raw: Option<&str>, _plan: &Self::Plan) -> CellDecodeResult<Self> {
            let _ = required_raw(raw)?;
            Err(CellConversionError::builder("always fails").build())
        }
    }

    #[test]
    fn unit_tuple_decodes_zero_column_rows() {
        let table = zero_cell_table(2);
        let rows = table
            .rows::<()>()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(rows, vec![(), ()]);
    }

    #[test]
    fn unit_tuple_rejects_non_empty_schema() {
        let table = one_cell_table(ColumnType::Text { length: None }, "x");
        let err = match table.rows::<()>() {
            Ok(_) => panic!("expected unit tuple plan build to reject non-empty schema"),
            Err(err) => err,
        };
        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::ColumnCountMismatch(error))
                if error.expected() == 0 && error.actual() == 1
        ));
    }

    #[test]
    fn option_preserves_null_as_ok_none() {
        let table = one_nullable_cell_table(ColumnType::Text { length: None }, None);
        let value = table
            .rows::<(Option<String>,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(value, None);
    }

    #[test]
    fn custom_from_cell_parse_failure_is_contextualized_and_preserves_source() {
        let err = one_cell_table(ColumnType::Text { length: None }, "abc")
            .rows::<(CelsiusTemp,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();

        let decode = err
            .as_cell_decode_error()
            .expect("custom decode failure should be contextualized");

        assert_eq!(decode.row_index(), 0);
        assert_eq!(decode.column_index(), ColumnIndex::new(0));
        assert_eq!(decode.column_name(), "X");
        assert_eq!(
            decode.conversion_error().reason(),
            "not a valid temperature"
        );
        assert_eq!(
            decode.actual_column_type(),
            &ColumnType::Text { length: None }
        );
        assert_eq!(decode.raw_value_preview(), Some("abc"));
        assert!(decode.target_type_name().ends_with("CelsiusTemp"));
        assert!(StdError::source(decode).is_some());
        assert!(decode.conversion_error().source().is_some());
        assert!(StdError::source(&err).is_some());
    }

    #[test]
    fn built_in_parse_failure_preserves_source() {
        let err = one_cell_table(ColumnType::Real, "abc")
            .rows::<(f64,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();

        let decode = err
            .as_cell_decode_error()
            .expect("built-in parse failure should be contextualized");

        assert!(decode.conversion_error().reason().contains("parse error"));
        assert!(StdError::source(decode).is_some());
        assert!(decode.conversion_error().source().is_some());
        assert!(StdError::source(&err).is_some());
    }

    #[test]
    fn long_raw_value_preview_is_truncated() {
        let raw = "x".repeat(256);
        let err = one_cell_table(ColumnType::Text { length: None }, &raw)
            .rows::<(AlwaysFails,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();

        let decode = err.as_cell_decode_error().expect("decode failure expected");
        let preview = decode
            .raw_value_preview()
            .expect("preview should be present for non-null values");

        assert!(preview.ends_with("..."), "preview: {preview}");
        assert!(preview.len() <= 131, "preview length: {}", preview.len());
    }

    #[test]
    fn decimal_value_decodes_fixed_column() {
        let t = one_cell_table(
            ColumnType::Fixed {
                precision: Some(10),
                scale: Some(2),
            },
            "12.34",
        );
        let value = t
            .rows::<(DecimalValue,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(value, DecimalValue::new("12.34"));
    }

    #[test]
    fn decimal_value_rejects_text_column() {
        let t = one_cell_table(ColumnType::Text { length: None }, "12.34");
        let err = match t.rows::<(DecimalValue,)>() {
            Ok(_) => panic!("expected plan build to reject text column for DecimalValue"),
            Err(err) => err,
        };
        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::IncompatibleColumnType(_))
        ));
    }

    /// Integers only accept `FIXED(scale=0)` columns; nearby shapes must fail during plan build.
    #[test]
    fn integer_rejects_non_integer_column_shapes() {
        for (label, ty) in [
            ("text", ColumnType::Text { length: None }),
            ("boolean", ColumnType::Boolean),
            (
                "scaled fixed",
                ColumnType::Fixed {
                    precision: Some(10),
                    scale: Some(2),
                },
            ),
        ] {
            let table = one_cell_table(ty, "42");
            let err = match table.rows::<(i64,)>() {
                Ok(_) => panic!("{label} unexpectedly planned as i64"),
                Err(err) => err,
            };
            assert!(
                matches!(
                    err.as_schema_error(),
                    Some(SchemaError::IncompatibleColumnType(_))
                ),
                "{label} unexpectedly decoded as i64: {err:?}"
            );
        }
    }

    /// `TEXT "true"` must NOT decode as `bool`.
    #[test]
    fn bool_rejects_text_column() {
        let t = one_cell_table(ColumnType::Text { length: None }, "true");
        let err = match t.rows::<(bool,)>() {
            Ok(_) => panic!("expected plan build to reject text column for bool"),
            Err(err) => err,
        };
        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::IncompatibleColumnType(_))
        ));
    }

    #[test]
    fn real_f64_accepts_documented_special_values_case_insensitively() {
        let nan = one_cell_table(ColumnType::Real, "nAn")
            .rows::<(f64,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert!(nan.is_nan());

        let inf = one_cell_table(ColumnType::Real, "InFiNiTy")
            .rows::<(f64,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert!(inf.is_infinite() && inf.is_sign_positive());

        let neg_inf = one_cell_table(ColumnType::Real, "-iNf")
            .rows::<(f64,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert!(neg_inf.is_infinite() && neg_inf.is_sign_negative());
    }

    #[test]
    fn real_f32_accepts_documented_special_values_case_insensitively() {
        let nan = one_cell_table(ColumnType::Real, "nAn")
            .rows::<(f32,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert!(nan.is_nan());

        let inf = one_cell_table(ColumnType::Real, "iNf")
            .rows::<(f32,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert!(inf.is_infinite() && inf.is_sign_positive());

        let neg_inf = one_cell_table(ColumnType::Real, "-InFiNiTy")
            .rows::<(f32,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert!(neg_inf.is_infinite() && neg_inf.is_sign_negative());
    }

    /// `TEXT` cells are plain strings; semi-structured JSON must come from `VARIANT` / `OBJECT` / `ARRAY`.
    #[test]
    fn json_rejects_text_column() {
        let t = one_cell_table(ColumnType::Text { length: None }, r#"{"a":1}"#);
        let err = match t.rows::<(serde_json::Value,)>() {
            Ok(_) => panic!("expected plan build to reject text column for json"),
            Err(err) => err,
        };
        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::IncompatibleColumnType(_))
        ));
    }

    /// The outer rowset parser should unescape once, leaving the inner JSON document escapes intact for `serde_json::from_str`.
    #[test]
    fn json_variant_decode_preserves_inner_escapes_after_parser_unescape() {
        let body = Bytes::from_static(br#"[[ "{\"message\":\"line\\n\\\\path\"}" ]]"#);
        let schema = make_schema(vec![("X".to_string(), ColumnType::Variant, true)]);
        let table = parse_inline_result_table(schema, Arc::from("test"), body).unwrap();

        let value = table
            .rows::<(serde_json::Value,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;

        assert_eq!(value["message"], "line\n\\path");
    }

    /// `TEXT` hex must NOT decode as `Vec<u8>`.
    #[test]
    fn binary_rejects_text_column() {
        let t = one_cell_table(ColumnType::Text { length: None }, "48656C6C6F");
        let err = match t.rows::<(Vec<u8>,)>() {
            Ok(_) => panic!("expected plan build to reject text column for Vec<u8>"),
            Err(err) => err,
        };
        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::IncompatibleColumnType(_))
        ));
    }

    /// `TIMESTAMP_LTZ` must NOT decode as `NaiveDateTime` — semantics differ.
    #[test]
    fn naive_datetime_rejects_timestamp_ltz() {
        let t = one_cell_table(ColumnType::TimestampLtz { scale: Some(9) }, "0");
        let err = match t.rows::<(NaiveDateTime,)>() {
            Ok(_) => panic!("expected plan build to reject TIMESTAMP_LTZ for NaiveDateTime"),
            Err(err) => err,
        };
        assert!(matches!(
            err.as_schema_error(),
            Some(SchemaError::IncompatibleColumnType(_))
        ));
    }

    #[test]
    fn datetime_utc_accepts_legacy_timestamp_tz_wire() {
        let t = one_cell_table(ColumnType::TimestampTz { scale: Some(0) }, "1440");
        let dt = t
            .rows::<(DateTime<Utc>,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(dt.timestamp(), 0);
    }

    #[test]
    fn datetime_utc_accepts_legacy_timestamp_tz_wire_with_subseconds() {
        let epoch_scaled = 1_718_429_445_123_456_789i128;
        let packed = (epoch_scaled * LEGACY_TIMESTAMP_TZ_SHIFT + 1980).to_string();
        let t = one_cell_table(ColumnType::TimestampTz { scale: Some(9) }, &packed);
        let dt = t
            .rows::<(DateTime<Utc>,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(dt.timestamp(), 1_718_429_445);
        assert_eq!(dt.timestamp_subsec_nanos(), 123_456_789);
    }
}
