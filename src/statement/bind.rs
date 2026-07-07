// `pub(crate)` `Bind` is exposed through the sealed (unnameable) `IntoBind` supertrait; callers cannot name it,
// so this leak is intentional.
#![allow(private_interfaces)]

use std::{borrow::Cow, fmt};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};

/// Snowflake server-side bind type, used with [`RawBind`](crate::bind::RawBind) to pick the wire type explicitly.
///
/// The typed wrappers select the right variant for you; reach for this
/// enum only when going through [`RawBind`](crate::bind::RawBind).
///
/// # Example
///
/// ```
/// use snowflake_connector_rs::bind::{RawBind, BindType};
///
/// let _ = RawBind::new(BindType::DecFloat, "1.23e-40");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum BindType {
    /// `FIXED` — integer NUMBER.
    Fixed,
    /// `REAL` — IEEE 754 floating-point.
    Real,
    /// `TEXT` — variable-length string.
    Text,
    /// `BOOLEAN`.
    Boolean,
    /// `DATE`.
    Date,
    /// `TIME`.
    Time,
    /// `TIMESTAMP_NTZ` — naive (timezone-less) timestamp.
    TimestampNtz,
    /// `TIMESTAMP_LTZ` — UTC-anchored timestamp.
    TimestampLtz,
    /// `TIMESTAMP_TZ` — timestamp with whole-minute offset.
    TimestampTz,
    /// `BINARY` — byte buffer.
    Binary,
    /// `DECFLOAT` (decimal floating-point). No typed wrapper is provided;
    /// bind via [`RawBind`] with the documented decimal-string payload (e.g. `"1.23e-40"`).
    DecFloat,
}

impl BindType {
    pub(crate) fn as_wire_str(self) -> &'static str {
        match self {
            Self::Fixed => "FIXED",
            Self::Real => "REAL",
            Self::Text => "TEXT",
            Self::Boolean => "BOOLEAN",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::TimestampNtz => "TIMESTAMP_NTZ",
            Self::TimestampLtz => "TIMESTAMP_LTZ",
            Self::TimestampTz => "TIMESTAMP_TZ",
            Self::Binary => "BINARY",
            Self::DecFloat => "DECFLOAT",
        }
    }
}

#[derive(Clone, PartialEq)]
pub(crate) enum BindValue {
    Fixed(i128),
    Real32(f32),
    Real64(f64),
    Bool(bool),
    Text(Cow<'static, str>),
    Date(NaiveDate),
    Time(NaiveTime),
    TimestampNtz(NaiveDateTime),
    TimestampLtz(DateTime<Utc>),
    TimestampTz(DateTime<FixedOffset>),
    Binary(Vec<u8>),
    Raw(Cow<'static, str>),
}

impl fmt::Debug for BindValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Bound values may contain secrets or PII, and this Debug output ends up in logs.
        f.write_str("<redacted>")
    }
}

/// Internal typed bind value used by [`IntoBind`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Bind {
    ty: BindType,
    value: Option<BindValue>,
}

impl Bind {
    pub(crate) fn new(ty: BindType, value: BindValue) -> Self {
        Self {
            ty,
            value: Some(value),
        }
    }

    pub(crate) fn null(ty: BindType) -> Self {
        Self { ty, value: None }
    }

    pub(crate) fn ty(&self) -> BindType {
        self.ty
    }

    pub(crate) fn value(&self) -> Option<&BindValue> {
        self.value.as_ref()
    }
}

/// Placeholder name used by [`Statement::bind_named`](crate::Statement::bind_named).
///
/// Pass the bare name (without the leading `:`): use `"id"` for `:id` and `"1"` for `:1`. The name is sent to Snowflake
/// verbatim — leading colons and other characters are not stripped or normalized. Empty names are rejected when the statement
/// is submitted.
///
/// Built from `&'static str` or `String` via [`From`].
///
/// # Examples
///
/// ```
/// use snowflake_connector_rs::bind::BindName;
///
/// let key: BindName = "id".into();
/// assert_eq!(key.as_str(), "id");
///
/// // Numeric names (matching `:1`-style placeholders) are valid.
/// let numeric: BindName = "1".into();
/// assert_eq!(numeric.as_str(), "1");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BindName(Cow<'static, str>);

impl BindName {
    /// Returns the placeholder name as a string slice.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::bind::BindName;
    ///
    /// let key = BindName::from("user_id");
    /// assert_eq!(key.as_str(), "user_id");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&'static str> for BindName {
    fn from(value: &'static str) -> Self {
        Self(value.into())
    }
}

impl From<String> for BindName {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

/// Wrapper that binds a byte buffer as Snowflake `BINARY`.
///
/// A bare `Vec<u8>` could plausibly mean BINARY, TEXT, VARIANT JSON, or staged-upload bytes; the wrapper pins the BINARY
/// interpretation so the intent is explicit at the call site.
///
/// `Option<Binary>::None` produces a typed `BINARY` NULL.
///
/// # Examples
///
/// ```
/// use snowflake_connector_rs::bind::Binary;
///
/// let _ = Binary::new(vec![0x12, 0xAB]);
/// let _ = Binary::new(b"hello".as_slice());
/// ```
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Binary(Vec<u8>);

impl fmt::Debug for Binary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the raw bytes: BINARY binds are where keys/tokens live.
        f.debug_tuple("Binary")
            .field(&format_args!("{} bytes", self.0.len()))
            .finish()
    }
}

impl Binary {
    /// Wraps any byte buffer as a Snowflake `BINARY` bind value.
    ///
    /// Accepts anything convertible to `Vec<u8>` (`Vec<u8>`, `&[u8]`, `[u8; N]`, …).
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::bind::Binary;
    ///
    /// let _ = Binary::new(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    /// ```
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }
}

/// Wrapper for integers bound as Snowflake `NUMBER(38, 0)`.
///
/// Snowflake's widest exact integer is `NUMBER(38, 0)`, accepting absolute values up to [`Integer::MAX_ABS_VALUE`] (`10^38 - 1`).
/// `i128` / `u128` can exceed that range, so they go through [`TryFrom`] and produce
/// [`ErrorKind::BindEncode`](crate::ErrorKind::BindEncode) on overflow.
/// `i8`〜`i64` and `u8`〜`u64` always fit and convert infallibly via [`From`].
///
/// # Examples
///
/// ```
/// use snowflake_connector_rs::bind::Integer;
///
/// let _: Integer = 42_i64.into();
/// let _ = Integer::try_from(12_345_678_901_234_567_890_i128).unwrap();
///
/// assert_eq!(
///     Integer::try_from(i128::MAX).unwrap_err().kind(),
///     snowflake_connector_rs::ErrorKind::BindEncode,
/// );
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Integer(i128);

impl Integer {
    /// Largest absolute value accepted by Snowflake's `NUMBER(38, 0)` (`10^38 - 1`).
    pub const MAX_ABS_VALUE: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
}

impl TryFrom<i128> for Integer {
    type Error = crate::Error;

    /// # Errors
    ///
    /// Returns [`ErrorKind::BindEncode`](crate::ErrorKind::BindEncode) when `|value|` exceeds [`Integer::MAX_ABS_VALUE`].
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::bind::Integer;
    ///
    /// assert!(Integer::try_from(Integer::MAX_ABS_VALUE).is_ok());
    /// assert!(Integer::try_from(Integer::MAX_ABS_VALUE + 1).is_err());
    /// ```
    fn try_from(value: i128) -> crate::Result<Self> {
        if !(-Self::MAX_ABS_VALUE..=Self::MAX_ABS_VALUE).contains(&value) {
            return Err(crate::Error::bind_encode(
                "value exceeds NUMBER(38, 0) range (10^38 - 1)",
            ));
        }
        Ok(Self(value))
    }
}

impl TryFrom<u128> for Integer {
    type Error = crate::Error;

    /// # Errors
    ///
    /// Returns [`ErrorKind::BindEncode`](crate::ErrorKind::BindEncode) when the input exceeds [`Integer::MAX_ABS_VALUE`].
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::bind::Integer;
    ///
    /// assert!(Integer::try_from(u128::from(u64::MAX)).is_ok());
    /// assert!(Integer::try_from(u128::MAX).is_err());
    /// ```
    fn try_from(value: u128) -> crate::Result<Self> {
        let signed = i128::try_from(value).map_err(|_| {
            crate::Error::bind_encode("value exceeds NUMBER(38, 0) range (10^38 - 1)")
        })?;
        Self::try_from(signed)
    }
}

macro_rules! impl_from_int_for_integer {
    ($($t:ty),+ $(,)?) => {
        $(
            impl From<$t> for Integer {
                fn from(value: $t) -> Self {
                    Self(i128::from(value))
                }
            }
        )+
    };
}

impl_from_int_for_integer!(i8, i16, i32, i64, u8, u16, u32, u64);

/// Wrapper for `DateTime<FixedOffset>` bound as Snowflake `TIMESTAMP_TZ`.
///
/// Snowflake's `TIMESTAMP_TZ` carries the offset as a whole-minute count and cannot represent leap seconds. Sub-minute
/// offsets and leap-second values are rejected at construction so that the wall-clock the caller wrote is not silently
/// shifted on the wire.
///
/// # Examples
///
/// ```
/// use chrono::{FixedOffset, NaiveDate};
/// use snowflake_connector_rs::bind::TimestampTz;
///
/// let dt = NaiveDate::from_ymd_opt(2024, 6, 15)
///     .unwrap()
///     .and_hms_opt(12, 30, 45)
///     .unwrap()
///     .and_local_timezone(FixedOffset::east_opt(9 * 3600).unwrap())
///     .unwrap();
/// assert!(TimestampTz::try_from(dt).is_ok());
///
/// // Sub-minute offset (`+01:00:30`).
/// let bad = NaiveDate::from_ymd_opt(2024, 6, 15)
///     .unwrap()
///     .and_hms_opt(12, 30, 45)
///     .unwrap()
///     .and_local_timezone(FixedOffset::east_opt(3_630).unwrap())
///     .unwrap();
/// assert!(TimestampTz::try_from(bad).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimestampTz(DateTime<FixedOffset>);

impl TryFrom<DateTime<FixedOffset>> for TimestampTz {
    type Error = crate::Error;

    /// # Errors
    ///
    /// Returns [`ErrorKind::BindEncode`](crate::ErrorKind::BindEncode) when the offset is not
    /// whole-minute, or when the value is a leap second (`nanosecond() >= 1_000_000_000`).
    fn try_from(value: DateTime<FixedOffset>) -> crate::Result<Self> {
        if value.offset().local_minus_utc() % 60 != 0 {
            return Err(crate::Error::bind_encode(
                "TIMESTAMP_TZ offset must be aligned to whole minutes; sub-minute offsets are not representable on the wire",
            ));
        }
        if value.naive_utc().nanosecond() >= 1_000_000_000 {
            return Err(crate::Error::bind_encode(
                "TIMESTAMP_TZ does not support leap seconds; NaiveDateTime nanosecond field must be < 1_000_000_000",
            ));
        }
        Ok(Self(value))
    }
}

/// Wrapper for `NaiveDateTime` bound as Snowflake `TIMESTAMP_NTZ`.
///
/// Snowflake `TIMESTAMP_NTZ` cannot represent leap seconds. `chrono`'s `NaiveDateTime` can (`nanosecond() >= 1_000_000_000`),
/// so leap-second values are rejected at construction to avoid an off-by-one-second silent shift.
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use snowflake_connector_rs::bind::TimestampNtz;
///
/// let dt = NaiveDate::from_ymd_opt(2024, 6, 15)
///     .unwrap()
///     .and_hms_opt(12, 30, 45)
///     .unwrap();
/// assert!(TimestampNtz::try_from(dt).is_ok());
///
/// let leap = NaiveDate::from_ymd_opt(2016, 12, 31)
///     .unwrap()
///     .and_hms_nano_opt(23, 59, 59, 1_500_000_000)
///     .unwrap();
/// assert!(TimestampNtz::try_from(leap).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimestampNtz(NaiveDateTime);

impl TryFrom<NaiveDateTime> for TimestampNtz {
    type Error = crate::Error;

    /// # Errors
    ///
    /// Returns [`ErrorKind::BindEncode`](crate::ErrorKind::BindEncode) for leap-second values (`nanosecond() >= 1_000_000_000`).
    fn try_from(value: NaiveDateTime) -> crate::Result<Self> {
        if value.nanosecond() >= 1_000_000_000 {
            return Err(crate::Error::bind_encode(
                "TIMESTAMP_NTZ does not support leap seconds; NaiveDateTime nanosecond field must be < 1_000_000_000",
            ));
        }
        Ok(Self(value))
    }
}

/// Wrapper for `DateTime<Utc>` bound as Snowflake `TIMESTAMP_LTZ`.
///
/// Same leap-second rule as [`TimestampNtz`]: leap-second values are rejected at construction.
///
/// # Examples
///
/// ```
/// use chrono::NaiveDate;
/// use snowflake_connector_rs::bind::TimestampLtz;
///
/// let dt = NaiveDate::from_ymd_opt(2024, 6, 15)
///     .unwrap()
///     .and_hms_opt(12, 30, 45)
///     .unwrap()
///     .and_utc();
/// assert!(TimestampLtz::try_from(dt).is_ok());
///
/// let leap = NaiveDate::from_ymd_opt(2016, 12, 31)
///     .unwrap()
///     .and_hms_nano_opt(23, 59, 59, 1_500_000_000)
///     .unwrap()
///     .and_utc();
/// assert!(TimestampLtz::try_from(leap).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimestampLtz(DateTime<Utc>);

impl TryFrom<DateTime<Utc>> for TimestampLtz {
    type Error = crate::Error;

    /// # Errors
    ///
    /// Returns [`ErrorKind::BindEncode`](crate::ErrorKind::BindEncode) for leap-second values (`naive_utc().nanosecond() >= 1_000_000_000`).
    fn try_from(value: DateTime<Utc>) -> crate::Result<Self> {
        if value.naive_utc().nanosecond() >= 1_000_000_000 {
            return Err(crate::Error::bind_encode(
                "TIMESTAMP_LTZ does not support leap seconds; DateTime<Utc> nanosecond field must be < 1_000_000_000",
            ));
        }
        Ok(Self(value))
    }
}

/// Wrapper for `NaiveTime` bound as Snowflake `TIME`.
///
/// Snowflake `TIME` cannot represent leap seconds. `chrono::NaiveTime` can (`nanosecond() >= 1_000_000_000`),
/// so leap-second values are rejected at construction.
///
/// # Examples
///
/// ```
/// use chrono::NaiveTime;
/// use snowflake_connector_rs::bind::Time;
///
/// assert!(Time::try_from(NaiveTime::from_hms_opt(23, 59, 59).unwrap()).is_ok());
///
/// let leap = NaiveTime::from_hms_nano_opt(23, 59, 59, 1_000_000_000).unwrap();
/// assert!(Time::try_from(leap).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Time(NaiveTime);

impl TryFrom<NaiveTime> for Time {
    type Error = crate::Error;

    /// # Errors
    ///
    /// Returns [`ErrorKind::BindEncode`](crate::ErrorKind::BindEncode) for leap-second values (`nanosecond() >= 1_000_000_000`).
    fn try_from(value: NaiveTime) -> crate::Result<Self> {
        if value.nanosecond() >= 1_000_000_000 {
            return Err(crate::Error::bind_encode(
                "TIME does not support leap seconds; NaiveTime nanosecond field must be < 1_000_000_000",
            ));
        }
        Ok(Self(value))
    }
}

/// Marker trait for values accepted by [`Statement::bind`](crate::Statement::bind) and
/// [`Statement::bind_named`](crate::Statement::bind_named).
///
/// Implemented for primary scalars, every wrapper in this module,
/// [`RawBind`], and `Option<T>` where `T: IntoBindNullable`.
///
/// The trait is sealed; outside crates cannot add implementations. To bind a type not covered here, either convert
/// it to one of the supported types or go through [`RawBind`].
pub trait IntoBind: Sized + into_bind_sealed::Sealed {}

/// Marker for [`IntoBind`] types whose `None` produces a typed wire NULL.
///
/// `Option<T>` implements [`IntoBind`] only when `T: IntoBindNullable`. [`RawBind`] does not implement
/// this trait, so `Option<RawBind>` is a compile error; use [`RawBind::null`] for a typed NULL
/// with an explicit wire type.
///
/// # Example
///
/// ```
/// use snowflake_connector_rs::Statement;
///
/// // `Option<&'static str>::None` produces a `TEXT` NULL.
/// let _ = Statement::new("INSERT INTO t (col) VALUES (?)").bind(None::<&'static str>);
/// ```
pub trait IntoBindNullable: IntoBind + into_bind_nullable_sealed::Sealed {}

pub(super) mod into_bind_sealed {
    pub trait Sealed: Sized {
        fn into_bind(self) -> super::Bind;
    }
}

mod into_bind_nullable_sealed {
    pub trait Sealed {
        const DEFAULT_TYPE: super::BindType;
    }
}

pub(crate) fn encode_bind<T>(value: T) -> Bind
where
    T: IntoBind,
{
    <T as into_bind_sealed::Sealed>::into_bind(value)
}

pub(crate) fn default_null_bind_type<T>() -> BindType
where
    T: IntoBindNullable,
{
    <T as into_bind_nullable_sealed::Sealed>::DEFAULT_TYPE
}

/// Escape hatch for binding values when the typed wrappers don't fit.
///
/// You pick the [`BindType`] and supply the value already encoded as a string in the form documented by Snowflake's SQL API.
/// Typical use is binding `DECFLOAT` (no typed wrapper exists for it).
///
/// `Option<RawBind>` is a compile error: the wire type is caller-chosen, so `None` has no default.
/// Use [`RawBind::null`] for a typed NULL.
///
/// # Examples
///
/// ```
/// use snowflake_connector_rs::bind::{RawBind, BindType};
///
/// let payload = RawBind::new(BindType::DecFloat, "1.23e-40");
/// let typed_null = RawBind::null(BindType::DecFloat);
/// # let _ = (payload, typed_null);
/// ```
#[derive(Debug, Clone)]
pub struct RawBind {
    ty: BindType,
    value: Option<String>,
}

impl RawBind {
    /// Builds a `RawBind` with an explicit wire type and a pre-encoded value string.
    ///
    /// `value` is sent to Snowflake verbatim; format it according to Snowflake's documented encoding for `ty`.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::bind::{RawBind, BindType};
    ///
    /// let _ = RawBind::new(BindType::Text, "hello");
    /// ```
    pub fn new(ty: BindType, value: impl Into<String>) -> Self {
        Self {
            ty,
            value: Some(value.into()),
        }
    }

    /// Builds a NULL bind with an explicit wire type.
    ///
    /// # Examples
    ///
    /// ```
    /// use snowflake_connector_rs::bind::{RawBind, BindType};
    ///
    /// let _ = RawBind::null(BindType::TimestampNtz);
    /// ```
    pub fn null(ty: BindType) -> Self {
        Self { ty, value: None }
    }
}

// `RawBind` deliberately does not implement `IntoBindNullable`: its wire type is caller-chosen, so there is no single
// "default type" to use for a `None` value. `Option<RawBind>` is therefore a compile error; callers should use
// `RawBind::null(ty)` to express a typed NULL with an explicit wire type.
impl IntoBind for RawBind {}

impl into_bind_sealed::Sealed for RawBind {
    fn into_bind(self) -> Bind {
        match self.value {
            Some(value) => Bind::new(self.ty, BindValue::Raw(value.into())),
            None => Bind::null(self.ty),
        }
    }
}

impl<T> IntoBind for Option<T> where T: IntoBindNullable {}

impl<T> into_bind_sealed::Sealed for Option<T>
where
    T: IntoBindNullable,
{
    fn into_bind(self) -> Bind {
        match self {
            Some(value) => encode_bind(value),
            None => Bind::null(default_null_bind_type::<T>()),
        }
    }
}

impl<T> IntoBindNullable for Option<T> where T: IntoBindNullable {}

impl<T> into_bind_nullable_sealed::Sealed for Option<T>
where
    T: IntoBindNullable,
{
    const DEFAULT_TYPE: BindType = <T as into_bind_nullable_sealed::Sealed>::DEFAULT_TYPE;
}

macro_rules! impl_fixed_into_bind {
    ($($t:ty),+ $(,)?) => {
        $(
            impl IntoBind for $t {}

            impl into_bind_sealed::Sealed for $t {
                fn into_bind(self) -> Bind {
                    Bind::new(BindType::Fixed, BindValue::Fixed(i128::from(self)))
                }
            }

            impl IntoBindNullable for $t {}

            impl into_bind_nullable_sealed::Sealed for $t {
                const DEFAULT_TYPE: BindType = BindType::Fixed;
            }
        )+
    };
}

impl_fixed_into_bind!(i8, i16, i32, i64, u8, u16, u32, u64);

macro_rules! impl_real_into_bind {
    ($($t:ty => $variant:ident),+ $(,)?) => {
        $(
            impl IntoBind for $t {}

            impl into_bind_sealed::Sealed for $t {
                fn into_bind(self) -> Bind {
                    Bind::new(BindType::Real, BindValue::$variant(self))
                }
            }

            impl IntoBindNullable for $t {}

            impl into_bind_nullable_sealed::Sealed for $t {
                const DEFAULT_TYPE: BindType = BindType::Real;
            }
        )+
    };
}

impl_real_into_bind!(f32 => Real32, f64 => Real64);

impl IntoBind for bool {}

impl into_bind_sealed::Sealed for bool {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Boolean, BindValue::Bool(self))
    }
}

impl IntoBindNullable for bool {}

impl into_bind_nullable_sealed::Sealed for bool {
    const DEFAULT_TYPE: BindType = BindType::Boolean;
}

impl IntoBind for &'static str {}

impl into_bind_sealed::Sealed for &'static str {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Text, BindValue::Text(self.into()))
    }
}

impl IntoBindNullable for &'static str {}

impl into_bind_nullable_sealed::Sealed for &'static str {
    const DEFAULT_TYPE: BindType = BindType::Text;
}

impl IntoBind for String {}

impl into_bind_sealed::Sealed for String {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Text, BindValue::Text(self.into()))
    }
}

impl IntoBindNullable for String {}

impl into_bind_nullable_sealed::Sealed for String {
    const DEFAULT_TYPE: BindType = BindType::Text;
}

impl IntoBind for Cow<'static, str> {}

impl into_bind_sealed::Sealed for Cow<'static, str> {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Text, BindValue::Text(self))
    }
}

impl IntoBindNullable for Cow<'static, str> {}

impl into_bind_nullable_sealed::Sealed for Cow<'static, str> {
    const DEFAULT_TYPE: BindType = BindType::Text;
}

impl IntoBind for NaiveDate {}

impl into_bind_sealed::Sealed for NaiveDate {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Date, BindValue::Date(self))
    }
}

impl IntoBindNullable for NaiveDate {}

impl into_bind_nullable_sealed::Sealed for NaiveDate {
    const DEFAULT_TYPE: BindType = BindType::Date;
}

impl IntoBind for TimestampNtz {}

impl into_bind_sealed::Sealed for TimestampNtz {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::TimestampNtz, BindValue::TimestampNtz(self.0))
    }
}

impl IntoBindNullable for TimestampNtz {}

impl into_bind_nullable_sealed::Sealed for TimestampNtz {
    const DEFAULT_TYPE: BindType = BindType::TimestampNtz;
}

impl IntoBind for TimestampLtz {}

impl into_bind_sealed::Sealed for TimestampLtz {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::TimestampLtz, BindValue::TimestampLtz(self.0))
    }
}

impl IntoBindNullable for TimestampLtz {}

impl into_bind_nullable_sealed::Sealed for TimestampLtz {
    const DEFAULT_TYPE: BindType = BindType::TimestampLtz;
}

impl IntoBind for Binary {}

impl into_bind_sealed::Sealed for Binary {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Binary, BindValue::Binary(self.0))
    }
}

impl IntoBindNullable for Binary {}

impl into_bind_nullable_sealed::Sealed for Binary {
    const DEFAULT_TYPE: BindType = BindType::Binary;
}

impl IntoBind for Integer {}

impl into_bind_sealed::Sealed for Integer {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Fixed, BindValue::Fixed(self.0))
    }
}

impl IntoBindNullable for Integer {}

impl into_bind_nullable_sealed::Sealed for Integer {
    const DEFAULT_TYPE: BindType = BindType::Fixed;
}

impl IntoBind for TimestampTz {}

impl into_bind_sealed::Sealed for TimestampTz {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::TimestampTz, BindValue::TimestampTz(self.0))
    }
}

impl IntoBindNullable for TimestampTz {}

impl into_bind_nullable_sealed::Sealed for TimestampTz {
    const DEFAULT_TYPE: BindType = BindType::TimestampTz;
}

impl IntoBind for Time {}

impl into_bind_sealed::Sealed for Time {
    fn into_bind(self) -> Bind {
        Bind::new(BindType::Time, BindValue::Time(self.0))
    }
}

impl IntoBindNullable for Time {}

impl into_bind_nullable_sealed::Sealed for Time {
    const DEFAULT_TYPE: BindType = BindType::Time;
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<Bind>();
    assert_send_sync::<BindValue>();
    assert_send_sync::<BindName>();
    assert_send_sync::<Binary>();
    assert_send_sync::<Integer>();
    assert_send_sync::<TimestampNtz>();
    assert_send_sync::<TimestampLtz>();
    assert_send_sync::<TimestampTz>();
    assert_send_sync::<Time>();
};

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};

    use super::*;
    use crate::ErrorKind;

    const OUT_OF_RANGE_ABS: i128 = 100_000_000_000_000_000_000_000_000_000_000_000_000_i128;

    fn assert_bind_value(bind: Bind, expected_ty: BindType, expected_value: Option<BindValue>) {
        assert_eq!(bind.ty(), expected_ty);
        assert_eq!(bind.value(), expected_value.as_ref());
    }

    #[test]
    fn binary_debug_elides_raw_bytes() {
        let rendered = format!("{:?}", Binary::new(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(rendered, "Binary(4 bytes)");
    }

    #[test]
    fn bind_value_debug_is_redacted() {
        assert_eq!(
            format!("{:?}", BindValue::Text("secret".into())),
            "<redacted>"
        );
        assert_eq!(
            format!("{:?}", BindValue::Binary(vec![1, 2, 3])),
            "<redacted>"
        );
        assert_eq!(format!("{:?}", BindValue::Fixed(42)), "<redacted>");
    }

    #[test]
    fn option_bind_preserves_underlying_type() {
        assert_bind_value(
            encode_bind(Some("hello")),
            BindType::Text,
            Some(BindValue::Text(Cow::Borrowed("hello"))),
        );
        assert_bind_value(encode_bind(None::<&'static str>), BindType::Text, None);
        assert_bind_value(encode_bind(None::<NaiveDate>), BindType::Date, None);
    }

    #[test]
    fn direct_into_bind_stores_common_types_without_encoding() {
        let cases = [
            (
                encode_bind(42_i64),
                BindType::Fixed,
                Some(BindValue::Fixed(42)),
            ),
            (
                encode_bind(1.5_f64),
                BindType::Real,
                Some(BindValue::Real64(1.5)),
            ),
            (
                encode_bind(NaiveDate::from_ymd_opt(2024, 6, 15).unwrap()),
                BindType::Date,
                Some(BindValue::Date(
                    NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
                )),
            ),
            (
                encode_bind(
                    TimestampNtz::try_from(
                        NaiveDate::from_ymd_opt(2024, 6, 15)
                            .unwrap()
                            .and_hms_opt(12, 30, 45)
                            .unwrap(),
                    )
                    .unwrap(),
                ),
                BindType::TimestampNtz,
                Some(BindValue::TimestampNtz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap(),
                )),
            ),
            (
                encode_bind(
                    TimestampLtz::try_from(
                        NaiveDate::from_ymd_opt(2024, 6, 15)
                            .unwrap()
                            .and_hms_opt(12, 30, 45)
                            .unwrap()
                            .and_utc(),
                    )
                    .unwrap(),
                ),
                BindType::TimestampLtz,
                Some(BindValue::TimestampLtz(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap()
                        .and_utc(),
                )),
            ),
            (
                encode_bind(true),
                BindType::Boolean,
                Some(BindValue::Bool(true)),
            ),
        ];

        for (bind, expected_ty, expected_value) in cases {
            assert_bind_value(bind, expected_ty, expected_value);
        }
    }

    #[test]
    fn string_literal_bind_uses_borrowed_cow() {
        let bind = encode_bind("hello");
        assert_eq!(bind.ty(), BindType::Text);
        match bind.value() {
            Some(BindValue::Text(text)) => {
                assert!(matches!(text, Cow::Borrowed("hello")));
            }
            other => panic!("expected borrowed text bind, got {other:?}"),
        }
    }

    #[test]
    fn bind_name_from_static_str_is_borrowed() {
        let name = BindName::from("id");
        assert!(matches!(name.0, Cow::Borrowed("id")));
    }

    #[test]
    fn wrapper_into_bind_stores_binary_integer_timestamp_tz_and_time() {
        assert_bind_value(
            encode_bind(Binary::new(b"Hello".as_slice())),
            BindType::Binary,
            Some(BindValue::Binary(b"Hello".to_vec())),
        );
        assert_bind_value(
            encode_bind(Integer::try_from(123_i128).unwrap()),
            BindType::Fixed,
            Some(BindValue::Fixed(123)),
        );
        assert_bind_value(
            encode_bind(
                TimestampTz::try_from(
                    NaiveDate::from_ymd_opt(2024, 6, 15)
                        .unwrap()
                        .and_hms_opt(12, 30, 45)
                        .unwrap()
                        .and_utc()
                        .fixed_offset(),
                )
                .unwrap(),
            ),
            BindType::TimestampTz,
            Some(BindValue::TimestampTz(
                NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(12, 30, 45)
                    .unwrap()
                    .and_utc()
                    .fixed_offset(),
            )),
        );
        assert_bind_value(
            encode_bind(Time::try_from(NaiveTime::from_hms_opt(12, 34, 56).unwrap()).unwrap()),
            BindType::Time,
            Some(BindValue::Time(
                NaiveTime::from_hms_opt(12, 34, 56).unwrap(),
            )),
        );
    }

    #[test]
    fn real_bind_preserves_original_float_values() {
        let nan_bind = encode_bind(f64::NAN);
        assert_eq!(nan_bind.ty(), BindType::Real);
        match nan_bind.value() {
            Some(BindValue::Real64(value)) => assert!(value.is_nan()),
            other => panic!("expected f64 NaN bind, got {other:?}"),
        }

        assert_bind_value(
            encode_bind(f64::INFINITY),
            BindType::Real,
            Some(BindValue::Real64(f64::INFINITY)),
        );
        assert_bind_value(
            encode_bind(f64::NEG_INFINITY),
            BindType::Real,
            Some(BindValue::Real64(f64::NEG_INFINITY)),
        );
        assert_bind_value(
            encode_bind(0.0_f64),
            BindType::Real,
            Some(BindValue::Real64(0.0)),
        );
    }

    #[test]
    fn fixed_bind_handles_boundaries() {
        for (value, expected) in [
            (-1_i64, "-1"),
            (0_i64, "0"),
            (i64::MAX, "9223372036854775807"),
            (i64::MIN, "-9223372036854775808"),
        ] {
            assert_bind_value(
                encode_bind(value),
                BindType::Fixed,
                Some(BindValue::Fixed(expected.parse().unwrap())),
            );
        }
        assert_bind_value(
            encode_bind(u64::MAX),
            BindType::Fixed,
            Some(BindValue::Fixed(18_446_744_073_709_551_615_i128)),
        );
    }

    #[test]
    fn integer_try_from_enforces_number_38_range() {
        assert_eq!(
            encode_bind(Integer::try_from(OUT_OF_RANGE_ABS - 1).unwrap())
                .value
                .as_ref()
                .map(|value| match value {
                    BindValue::Fixed(value) => *value,
                    other => panic!("expected fixed bind, got {other:?}"),
                }),
            Some(99_999_999_999_999_999_999_999_999_999_999_999_999_i128)
        );
        assert_eq!(
            encode_bind(Integer::try_from(-(OUT_OF_RANGE_ABS - 1)).unwrap())
                .value
                .as_ref()
                .map(|value| match value {
                    BindValue::Fixed(value) => *value,
                    other => panic!("expected fixed bind, got {other:?}"),
                }),
            Some(-99_999_999_999_999_999_999_999_999_999_999_999_999_i128)
        );

        for value in [OUT_OF_RANGE_ABS, -OUT_OF_RANGE_ABS, i128::MAX, i128::MIN] {
            let err = Integer::try_from(value).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::BindEncode);
        }

        let err = Integer::try_from(u128::MAX).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BindEncode);
    }

    #[test]
    fn integer_from_small_integer_types_is_infallible() {
        fn assert_into_integer<T: Into<Integer>>(value: T) -> Integer {
            value.into()
        }

        let _ = assert_into_integer(1_i8);
        let _ = assert_into_integer(1_i16);
        let _ = assert_into_integer(1_i32);
        let _ = assert_into_integer(1_i64);
        let _ = assert_into_integer(1_u8);
        let _ = assert_into_integer(1_u16);
        let _ = assert_into_integer(1_u32);
        let _ = assert_into_integer(1_u64);
    }

    #[test]
    fn timestamp_tz_rejects_subminute_offsets_and_encodes_whole_minutes() {
        let subminute = NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap()
            .and_local_timezone(FixedOffset::east_opt(3_630).unwrap())
            .unwrap();
        let err = TimestampTz::try_from(subminute).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BindEncode);

        let east = encode_bind(
            TimestampTz::try_from(
                NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(21, 30, 45)
                    .unwrap()
                    .and_local_timezone(FixedOffset::east_opt(9 * 3600).unwrap())
                    .unwrap(),
            )
            .unwrap(),
        );
        assert_bind_value(
            east,
            BindType::TimestampTz,
            Some(BindValue::TimestampTz(
                NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(21, 30, 45)
                    .unwrap()
                    .and_local_timezone(FixedOffset::east_opt(9 * 3600).unwrap())
                    .unwrap(),
            )),
        );

        let west = encode_bind(
            TimestampTz::try_from(
                NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(7, 30, 45)
                    .unwrap()
                    .and_local_timezone(FixedOffset::west_opt(5 * 3600).unwrap())
                    .unwrap(),
            )
            .unwrap(),
        );
        assert_bind_value(
            west,
            BindType::TimestampTz,
            Some(BindValue::TimestampTz(
                NaiveDate::from_ymd_opt(2024, 6, 15)
                    .unwrap()
                    .and_hms_opt(7, 30, 45)
                    .unwrap()
                    .and_local_timezone(FixedOffset::west_opt(5 * 3600).unwrap())
                    .unwrap(),
            )),
        );
    }

    #[test]
    fn timestamp_bind_wrappers_reject_leap_seconds() {
        let leap = || {
            NaiveDate::from_ymd_opt(2016, 12, 31)
                .unwrap()
                .and_hms_nano_opt(23, 59, 59, 1_500_000_000)
                .expect("chrono supports leap-second representation")
        };

        let err = TimestampNtz::try_from(leap()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BindEncode);

        let err = TimestampLtz::try_from(leap().and_utc()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BindEncode);

        let err = TimestampTz::try_from(
            leap()
                .and_local_timezone(FixedOffset::east_opt(9 * 3600).unwrap())
                .unwrap(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BindEncode);
    }

    #[test]
    fn time_rejects_leap_seconds() {
        let leap = NaiveTime::from_hms_nano_opt(23, 59, 59, 1_000_000_000)
            .expect("chrono supports leap-second representation");
        let err = Time::try_from(leap).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::BindEncode);
    }

    #[test]
    fn raw_bind_preserves_explicit_wire_type_and_payload() {
        for (bind, expected_ty, expected_value) in [
            (
                encode_bind(RawBind::new(BindType::Text, "x")),
                BindType::Text,
                "x",
            ),
            (
                encode_bind(RawBind::new(BindType::DecFloat, "1.23e-40")),
                BindType::DecFloat,
                "1.23e-40",
            ),
        ] {
            assert_eq!(bind.ty(), expected_ty);
            match bind.value() {
                Some(BindValue::Raw(value)) => assert_eq!(value.as_ref(), expected_value),
                other => panic!("expected raw bind value, got {other:?}"),
            }
        }
    }

    #[test]
    fn raw_bind_preserves_explicit_wire_type_for_typed_nulls() {
        for (bind, expected_ty) in [
            (encode_bind(RawBind::null(BindType::Text)), BindType::Text),
            (
                encode_bind(RawBind::null(BindType::DecFloat)),
                BindType::DecFloat,
            ),
        ] {
            assert_eq!(bind.ty(), expected_ty);
            assert!(bind.value().is_none());
        }
    }
}
