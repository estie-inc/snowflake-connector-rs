use super::bind::{Bind, BindType, BindValue, IntoBind, into_bind_sealed};

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

// `Bind` is `pub(crate)`, exposed only through the sealed (unnameable) trait.
#[allow(private_interfaces)]
impl into_bind_sealed::Sealed for RawBind {
    fn into_bind(self) -> Bind {
        match self.value {
            Some(value) => Bind::new(self.ty, BindValue::Raw(value.into())),
            None => Bind::null(self.ty),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::statement::bind::encode_bind;

    use super::*;

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
