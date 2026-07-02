use std::iter::repeat_n;

use chrono::{DateTime, Days, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use crate::result::{
    CellConversionError, CellDecodeResult,
    cell::CellRef,
    dynamic::DecimalValue,
    plan::RowPlanContext,
    row::RowRef,
    schema::{ColumnIndex, ColumnType},
};
use crate::{ColumnCountMismatchError, Result, SchemaError};

const LEGACY_TIMESTAMP_TZ_SHIFT: i128 = 16_384;

/// Decode a single result-set cell into a Rust value.
///
/// Implementations exist for primitive Rust types,
/// [`CellValue`](crate::CellValue),
/// [`DecimalValue`](crate::DecimalValue), and `Option<T>`.
/// Implement it yourself when adapting Snowflake values into domain types.
///
/// # Example
///
/// A custom decoder that wraps `f64` for type safety:
///
/// ```
/// use snowflake_connector_rs::{
///     CellConversionError, CellDecodeResult, CellRef, FromCell,
/// };
///
/// struct CelsiusTemp(f64);
///
/// impl FromCell for CelsiusTemp {
///     fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
///         let raw = cell.required_raw()?;
///         raw.parse::<f64>().map(CelsiusTemp).map_err(|e| {
///             CellConversionError::builder("not a valid temperature")
///                 .source(e)
///                 .build()
///         })
///     }
/// }
/// ```
pub trait FromCell: Sized {
    /// Decode a single cell into `Self`.
    ///
    /// Implementations should reject `NULL` for non-`Option` targets and
    /// describe conversion failures with [`CellConversionError`].
    ///
    /// # Errors
    ///
    /// Implementations should return only cell-local issues here.
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self>;
}

/// Decode an entire row into a Rust type.
///
/// Implementations may use an associated plan to cache schema-dependent
/// state before decoding individual rows.
///
/// # Example
///
/// Hand-written equivalent of `#[derive(FromRow)]` for a two-column row:
///
/// ```
/// use snowflake_connector_rs::{
///     FromRow, Result,
///     {ColumnIndex, RowPlanContext, RowRef},
/// };
///
/// struct UserRow {
///     id: i64,
///     name: String,
/// }
///
/// struct UserRowPlan {
///     id: ColumnIndex,
///     name: ColumnIndex,
/// }
///
/// impl FromRow for UserRow {
///     type Plan = UserRowPlan;
///
///     fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan> {
///         let schema = ctx.schema();
///         Ok(UserRowPlan {
///             id: schema.column_index("ID")?,
///             name: schema.column_index("NAME")?,
///         })
///     }
///
///     fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> Result<Self> {
///         Ok(UserRow {
///             id: row.get(plan.id)?,
///             name: row.get(plan.name)?,
///         })
///     }
/// }
/// ```
pub trait FromRow: Sized {
    /// Per-table state reused while decoding rows.
    ///
    /// `Send + Sync` so the plan can be shared across threads when an
    /// iterator is moved between tasks.
    type Plan: Send + Sync;

    /// Build the per-table decode plan from the result-set metadata.
    ///
    /// # Errors
    ///
    /// Implementations may return any [`Error`](crate::Error) appropriate
    /// for schema validation or planning. Conventional errors include
    /// [`SchemaError::MissingColumn`](crate::SchemaError::MissingColumn)
    /// and
    /// [`SchemaError::ColumnCountMismatch`](crate::SchemaError::ColumnCountMismatch).
    fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan>;

    /// Decode a single row using the associated plan.
    ///
    /// # Errors
    ///
    /// Implementations may return any [`Error`](crate::Error) appropriate
    /// for row conversion.
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
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        if cell.is_null() {
            Ok(None)
        } else {
            T::from_cell(cell).map(Some)
        }
    }
}

fn ensure_column_type(cell: CellRef<'_>, ok: bool) -> CellDecodeResult<()> {
    if ok {
        Ok(())
    } else {
        Err(CellConversionError::builder(format!(
            "incompatible column type: {:?}",
            cell.column().ty()
        ))
        .build())
    }
}

macro_rules! impl_int_from_cell {
    ($t:ty) => {
        impl FromCell for $t {
            fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
                // Integers are only valid for `FIXED` with `scale = 0`.
                match cell.column().ty() {
                    ColumnType::Fixed { scale, .. } => {
                        let s = scale.unwrap_or(0);
                        if s != 0 {
                            return Err(CellConversionError::builder(format!(
                                "integer cannot decode FIXED with scale {s}"
                            ))
                            .build());
                        }
                    }
                    _ => {
                        ensure_column_type(cell, false)?;
                        unreachable!()
                    }
                }
                let raw = cell.required_raw()?;
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

impl FromCell for f64 {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(
            cell,
            matches!(
                cell.column().ty(),
                ColumnType::Real | ColumnType::Fixed { .. }
            ),
        )?;

        let raw = cell.required_raw()?;
        raw.parse::<f64>().map_err(|e| {
            CellConversionError::builder(format!("parse error: {e}"))
                .source(e)
                .build()
        })
    }
}

impl FromCell for f32 {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(
            cell,
            matches!(
                cell.column().ty(),
                ColumnType::Real | ColumnType::Fixed { .. }
            ),
        )?;

        let raw = cell.required_raw()?;
        raw.parse::<f32>().map_err(|e| {
            CellConversionError::builder(format!("parse error: {e}"))
                .source(e)
                .build()
        })
    }
}

impl FromCell for String {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        cell.required_raw().map(|s| s.to_owned())
    }
}

impl FromCell for DecimalValue {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(cell, matches!(cell.column().ty(), ColumnType::Fixed { .. }))?;
        let raw = cell.required_raw()?;
        Ok(DecimalValue::new(raw))
    }
}

impl FromCell for bool {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(cell, matches!(cell.column().ty(), ColumnType::Boolean))?;

        let raw = cell.required_raw()?;
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
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(cell, matches!(cell.column().ty(), ColumnType::Date))?;

        let raw = cell.required_raw()?;
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
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(cell, matches!(cell.column().ty(), ColumnType::Time { .. }))?;

        let raw = cell.required_raw()?;
        let scale = match cell.column().ty() {
            ColumnType::Time { scale } => match *scale {
                None => 0usize,
                Some(s) if (0..=9).contains(&s) => s as usize,
                Some(s) => {
                    return Err(
                        CellConversionError::builder(format!("invalid time scale: {s}")).build(),
                    );
                }
            },
            _ => 0,
        };
        let (secs, nsec) = parse_time_seconds_and_nanos(raw, scale)
            .map_err(|m| CellConversionError::builder(m).build())?;

        NaiveTime::from_num_seconds_from_midnight_opt(secs, nsec)
            .ok_or_else(|| CellConversionError::builder(format!("invalid time: {raw}")).build())
    }
}

impl FromCell for NaiveDateTime {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(
            cell,
            matches!(cell.column().ty(), ColumnType::TimestampNtz { .. }),
        )?;

        let raw = cell.required_raw()?;
        let scale = timestamp_scale(cell.column().ty()).unwrap_or(9);

        parse_timestamp_epoch(raw, scale)
            .map(|dt| dt.naive_utc())
            .map_err(|m| CellConversionError::builder(m).build())
    }
}

impl FromCell for DateTime<Utc> {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        let raw = cell.required_raw()?;
        let scale = timestamp_scale(cell.column().ty()).unwrap_or(9);

        match cell.column().ty() {
            ColumnType::TimestampLtz { .. } => parse_timestamp_epoch(raw, scale)
                .map_err(|m| CellConversionError::builder(m).build()),
            ColumnType::TimestampTz { .. } => parse_timestamp_tz_as_utc(raw, scale)
                .map_err(|m| CellConversionError::builder(m).build()),
            other => Err(CellConversionError::builder(format!(
                "unsupported column type for DateTime<Utc>: {other:?}"
            ))
            .build()),
        }
    }
}

impl FromCell for DateTime<FixedOffset> {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        let raw = cell.required_raw()?;
        let scale = timestamp_scale(cell.column().ty()).unwrap_or(9);

        match cell.column().ty() {
            ColumnType::TimestampTz { .. } => parse_timestamp_tz_with_offset(raw, scale)
                .map_err(|m| CellConversionError::builder(m).build()),
            other => Err(CellConversionError::builder(format!(
                "unsupported column type for DateTime<FixedOffset>: {other:?}"
            ))
            .build()),
        }
    }
}

impl FromCell for serde_json::Value {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(
            cell,
            matches!(
                cell.column().ty(),
                ColumnType::Variant | ColumnType::Object | ColumnType::Array
            ),
        )?;

        let raw = cell.required_raw()?;
        serde_json::from_str(raw).map_err(|e| {
            CellConversionError::builder(format!("invalid json: {e}"))
                .source(e)
                .build()
        })
    }
}

impl FromCell for Vec<u8> {
    fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
        ensure_column_type(
            cell,
            matches!(cell.column().ty(), ColumnType::Binary { .. }),
        )?;

        let raw = cell.required_raw()?;
        decode_hex(raw).map_err(|m| CellConversionError::builder(m).build())
    }
}

fn timestamp_scale(ty: &ColumnType) -> Option<i64> {
    match ty {
        ColumnType::TimestampNtz { scale }
        | ColumnType::TimestampLtz { scale }
        | ColumnType::TimestampTz { scale } => scale.map(i64::from),
        _ => None,
    }
}

fn validate_ts_scale(scale: i64) -> std::result::Result<i64, String> {
    if !(0..=9).contains(&scale) {
        return Err(format!("invalid timestamp scale: {scale} (expected 0..=9)"));
    }
    Ok(scale)
}

fn pow10_i128(scale: i64, original: &str, kind: &'static str) -> std::result::Result<i128, String> {
    10i128
        .checked_pow(scale as u32)
        .ok_or_else(|| format!("Could not decode {kind}: {original}"))
}

fn parse_scaled_decimal_i128(
    s: &str,
    scale: i64,
    kind: &'static str,
) -> std::result::Result<i128, String> {
    let scale = validate_ts_scale(scale)?;
    let s = s.trim();
    if s.is_empty() {
        return Err(format!("Could not decode {kind}: {s}"));
    }

    let (negative, digits) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    };
    if digits.is_empty() {
        return Err(format!("Could not decode {kind}: {s}"));
    }

    let (whole_str, frac_str) = match digits.split_once('.') {
        Some((whole, frac)) if !frac.is_empty() => (whole, Some(frac)),
        Some(_) => return Err(format!("Could not decode {kind}: {s}")),
        None => (digits, None),
    };
    if whole_str.is_empty() || !whole_str.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return Err(format!("Could not decode {kind}: {s}"));
    }

    let scale_factor = pow10_i128(scale, s, kind)?;
    let whole = whole_str
        .parse::<i128>()
        .map_err(|_| format!("Could not decode {kind}: {s}"))?;
    let mut scaled = whole
        .checked_mul(scale_factor)
        .ok_or_else(|| format!("Could not decode {kind}: {s}"))?;

    if let Some(frac_str) = frac_str {
        if scale == 0 {
            return Err(format!("Could not decode {kind}: {s}"));
        }
        if !frac_str.as_bytes().iter().all(|b| b.is_ascii_digit()) {
            return Err(format!("Could not decode {kind}: {s}"));
        }
        if frac_str.len() > scale as usize {
            return Err(format!("Could not decode {kind}: {s}"));
        }

        let mut digits = frac_str.bytes().collect::<Vec<_>>();
        if digits.len() < scale as usize {
            digits.resize(scale as usize, b'0');
        }

        let frac = std::str::from_utf8(&digits)
            .map_err(|_| format!("Could not decode {kind}: {s}"))?
            .parse::<i128>()
            .map_err(|_| format!("Could not decode {kind}: {s}"))?;
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
    scale: i64,
    original: &str,
    kind: &'static str,
) -> std::result::Result<DateTime<Utc>, String> {
    let scale = validate_ts_scale(scale)?;
    let scale_factor = pow10_i128(scale, original, kind)?;

    let secs = scaled.div_euclid(scale_factor);
    let frac = scaled.rem_euclid(scale_factor);

    let secs = i64::try_from(secs).map_err(|_| format!("Could not decode {kind}: {original}"))?;
    let frac = u64::try_from(frac).map_err(|_| format!("Could not decode {kind}: {original}"))?;

    let nsec = if scale == 0 {
        0u32
    } else {
        let frac_scale = 10u64.pow((9 - scale) as u32);
        frac.checked_mul(frac_scale)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| format!("Could not decode {kind}: {original}"))?
    };

    DateTime::from_timestamp(secs, nsec)
        .ok_or_else(|| format!("Could not decode {kind}: {original}"))
}

pub(crate) fn parse_timestamp_epoch(
    s: &str,
    scale: i64,
) -> std::result::Result<DateTime<Utc>, String> {
    let scaled = parse_scaled_decimal_i128(s, scale, "timestamp")?;
    parse_timestamp_epoch_scaled(scaled, scale, s, "timestamp")
}

struct TimestampTzWire {
    utc: DateTime<Utc>,
    tz_index: i32,
}

fn parse_timestamp_tz_index(tz_str: &str, original: &str) -> std::result::Result<i32, String> {
    let tz_index = tz_str
        .parse::<i32>()
        .map_err(|_| format!("invalid timestamp_tz: {original}"))?;
    if !(0..=2880).contains(&tz_index) {
        return Err(format!("invalid timezone for timestamp_tz: {original}"));
    }
    Ok(tz_index)
}

fn fixed_offset_from_tz_index(
    tz_index: i32,
    original: &str,
) -> std::result::Result<FixedOffset, String> {
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
    scale: i64,
) -> std::result::Result<TimestampTzWire, String> {
    let packed = parse_scaled_decimal_i128(s, 0, "timestamp_tz")?;
    let epoch_scaled = packed.div_euclid(LEGACY_TIMESTAMP_TZ_SHIFT);
    let tz_index = packed.rem_euclid(LEGACY_TIMESTAMP_TZ_SHIFT);
    let utc = parse_timestamp_epoch_scaled(epoch_scaled, scale, s, "timestamp_tz")?;

    let tz_index =
        i32::try_from(tz_index).map_err(|_| format!("invalid timezone for timestamp_tz: {s}"))?;
    let tz_index = parse_timestamp_tz_index(&tz_index.to_string(), s)?;

    Ok(TimestampTzWire { utc, tz_index })
}

fn parse_timestamp_tz_wire(s: &str, scale: i64) -> std::result::Result<TimestampTzWire, String> {
    let s = s.trim();
    let mut parts = s.split_whitespace();
    let first = parts
        .next()
        .ok_or_else(|| format!("invalid timestamp_tz: {s}"))?;

    if let Some(tz_str) = parts.next() {
        if parts.next().is_some() {
            return Err(format!("invalid timestamp_tz: {s}"));
        }
        let utc = parse_timestamp_epoch(first, scale)?;
        let tz_index = parse_timestamp_tz_index(tz_str, s)?;
        Ok(TimestampTzWire { utc, tz_index })
    } else {
        parse_legacy_timestamp_tz_wire(s, scale)
    }
}

fn parse_timestamp_tz_as_utc(s: &str, scale: i64) -> std::result::Result<DateTime<Utc>, String> {
    parse_timestamp_tz_wire(s, scale).map(|wire| wire.utc)
}

pub(crate) fn parse_timestamp_tz_with_offset(
    s: &str,
    scale: i64,
) -> std::result::Result<DateTime<FixedOffset>, String> {
    let wire = parse_timestamp_tz_wire(s, scale)?;
    let offset = fixed_offset_from_tz_index(wire.tz_index, s)?;
    Ok(wire.utc.with_timezone(&offset))
}

pub(crate) fn parse_time_seconds_and_nanos(
    value: &str,
    scale: usize,
) -> std::result::Result<(u32, u32), String> {
    if scale > 9 {
        return Err(format!("invalid time scale: {scale}"));
    }

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

    let mut frac_digits = frac_str.as_bytes().to_vec();
    if frac_digits.len() > scale {
        frac_digits.truncate(scale);
    }
    if frac_digits.len() < scale {
        frac_digits.extend(repeat_n(b'0', scale - frac_digits.len()));
    }

    let frac_scaled = {
        let s = std::str::from_utf8(&frac_digits).map_err(|_| format!("invalid time: {value}"))?;
        s.parse::<u32>()
            .map_err(|_| format!("invalid time: {value}"))?
    };
    let nsec = frac_scaled
        .checked_mul(10u32.pow((9 - scale) as u32))
        .ok_or_else(|| format!("invalid time: {value}"))?;

    Ok((secs, nsec))
}

pub(crate) fn decode_hex(s: &str) -> std::result::Result<Vec<u8>, String> {
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

fn hex_nibble(b: u8) -> std::result::Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(format!("invalid hex digit: {}", b as char)),
    }
}

macro_rules! impl_tuple_from_row {
    ($len:expr; $($t:ident => $idx:tt),*) => {
        #[allow(non_snake_case, unused_assignments)]
        impl<$($t: FromCell),*> FromRow for ($($t,)*) {
            type Plan = [ColumnIndex; $len];

            fn build_plan(ctx: RowPlanContext<'_>) -> Result<Self::Plan> {
                let schema = ctx.schema();
                if schema.len() != $len {
                    return Err(SchemaError::ColumnCountMismatch(
                        ColumnCountMismatchError::new($len, schema.len()),
                    )
                    .into());
                }
                Ok([$(ColumnIndex::new($idx as u32)),*])
            }

            fn from_row_with_plan(row: RowRef<'_>, plan: &Self::Plan) -> Result<Self> {
                Ok(($(row.get::<$t>(plan[$idx])?,)*))
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
    use std::sync::Arc;

    use bytes::Bytes;

    use super::*;
    use crate::{
        ResultTable,
        result::test_data::{make_result_table_from_rows, make_schema},
        rowset::parser::parse_inline_result_table,
    };

    /// Pre-1970 timestamps with a fractional part: `-1.1` is 1969-12-31T23:59:58.900Z
    /// (= secs -2, nsec 900_000_000), not `.100`.
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
        for scale in [-1, 10] {
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
        fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
            let raw = cell.required_raw()?;
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
        fn from_cell(cell: CellRef<'_>) -> CellDecodeResult<Self> {
            let _ = cell.required_raw()?;
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
        assert!(std::error::Error::source(decode).is_some());
        assert!(decode.conversion_error().source().is_some());
        assert!(std::error::Error::source(&err).is_some());
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
        assert!(std::error::Error::source(decode).is_some());
        assert!(decode.conversion_error().source().is_some());
        assert!(std::error::Error::source(&err).is_some());
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
        let err = t
            .rows::<(DecimalValue,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();
        assert!(err.as_cell_decode_error().is_some());
    }

    /// Integers only accept `FIXED(scale=0)` columns; nearby shapes must fail.
    #[test]
    fn integer_rejects_non_integer_column_shapes() {
        for (label, ty, raw) in [
            ("text", ColumnType::Text { length: None }, "42"),
            ("boolean", ColumnType::Boolean, "1"),
            (
                "scaled fixed",
                ColumnType::Fixed {
                    precision: Some(10),
                    scale: Some(2),
                },
                "12",
            ),
        ] {
            let err = one_cell_table(ty, raw)
                .rows::<(i64,)>()
                .unwrap()
                .next()
                .unwrap()
                .unwrap_err();
            assert!(
                err.as_cell_decode_error().is_some(),
                "{label} unexpectedly decoded as i64: {err:?}"
            );
        }
    }

    /// `TEXT "true"` must NOT decode as `bool`.
    #[test]
    fn bool_rejects_text_column() {
        let t = one_cell_table(ColumnType::Text { length: None }, "true");
        let e = t.rows::<(bool,)>().unwrap().next().unwrap().unwrap_err();
        assert!(e.as_cell_decode_error().is_some());
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

    /// `TEXT` cells are plain strings; semi-structured JSON must come from
    /// `VARIANT` / `OBJECT` / `ARRAY`.
    #[test]
    fn json_rejects_text_column() {
        let t = one_cell_table(ColumnType::Text { length: None }, r#"{"a":1}"#);
        let err = t
            .rows::<(serde_json::Value,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();
        assert!(err.as_cell_decode_error().is_some());
    }

    /// The outer rowset parser should unescape once, leaving the inner JSON
    /// document escapes intact for `serde_json::from_str`.
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
        let e = t.rows::<(Vec<u8>,)>().unwrap().next().unwrap().unwrap_err();
        assert!(e.as_cell_decode_error().is_some());
    }

    /// `TIMESTAMP_LTZ` must NOT decode as `NaiveDateTime` — semantics differ.
    #[test]
    fn naive_datetime_rejects_timestamp_ltz() {
        let t = one_cell_table(ColumnType::TimestampLtz { scale: Some(9) }, "0");
        let e = t
            .rows::<(NaiveDateTime,)>()
            .unwrap()
            .next()
            .unwrap()
            .unwrap_err();
        assert!(e.as_cell_decode_error().is_some());
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
