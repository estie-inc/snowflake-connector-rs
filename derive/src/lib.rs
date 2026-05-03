//! Derive macros for `snowflake-connector-rs`.
//!
//! Enable the `derive` feature on `snowflake-connector-rs` to use
//! `#[derive(FromRow)]` on your row types. The macro resolves named fields
//! against exact raw result labels from the Snowflake schema.
//!
//! ```rust,ignore
//! use snowflake_connector_rs::FromRow;
//!
//! #[derive(Debug, FromRow, PartialEq)]
//! struct UserRow {
//!     id: i64,
//!     #[snowflake(rename = "USER_NAME")]
//!     name: String,
//! }
//! ```

mod attrs;
mod codegen;
mod input;
mod naming;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Derives `snowflake_connector_rs::FromRow` for a struct.
///
/// # Name lookup
///
/// Named-field structs resolve columns by exact raw result label. When no field
/// attribute overrides the lookup, the macro converts each logical field name to
/// `SCREAMING_SNAKE_CASE` at compile time, so `created_at` looks up
/// `CREATED_AT`. Raw identifier prefixes such as `r#type` are stripped before
/// the lookup name is finalized.
///
/// `#[snowflake(rename = "...")]` bypasses container-level renaming and uses the
/// provided raw label verbatim. `#[snowflake(rename_all = "none")]` keeps the
/// logical field name unchanged.
///
/// # Container attributes
///
/// - `rename_all = "SCREAMING_SNAKE_CASE"`: compile-time field-name conversion
///   used by named structs. This is the default.
/// - `rename_all = "none"`: use each logical field name as-is.
/// - `by_position`: decode every field by ordinal instead of by label on named
///   structs. Tuple structs already decode by position automatically.
/// - `crate = "::path"`: override the crate path used in generated code.
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// #[snowflake(rename_all = "none")]
/// struct LowercaseLabels {
///     name: String,
/// }
/// ```
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// #[snowflake(by_position)]
/// struct PositionalRow {
///     id: i64,
///     name: String,
/// }
/// ```
///
/// # Tuple structs
///
/// Tuple structs decode by ordinal automatically; you do not need to add
/// `#[snowflake(by_position)]`. The container attribute remains accepted as a
/// no-op for backward source compatibility but is not required and is omitted
/// from the examples.
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// struct PositionalPair(i64, String);
/// ```
///
/// # Field attributes
///
/// - `rename = "..."`: use the exact raw result label for this field.
/// - `default`: use `Default::default()` when the named lookup fails with
///   `MissingColumn`.
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// struct RenamedField {
///     #[snowflake(rename = "display name")]
///     display_name: String,
/// }
/// ```
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// struct PartialRow {
///     id: i64,
///     #[snowflake(default)]
///     note: Option<String>,
/// }
/// ```
///
/// # Error behavior
///
/// The generated implementation propagates schema and row access errors:
///
/// - `MissingColumn` for required named lookups.
/// - `AmbiguousColumn` when the schema contains duplicate raw labels.
/// - `ColumnCountMismatch` when required positional fields exceed the schema.
/// - `InvalidColumnIndex` and cell decode errors from row access.
///
/// `#[snowflake(default)]` only substitutes a default value for
/// `MissingColumn`. It does not swallow `AmbiguousColumn` or row decode errors.
///
/// # Using the macro
///
/// `snowflake-connector-rs` re-exports this macro when its `derive` feature is
/// enabled. See the base crate docs and the `derive_from_row` example for a
/// full walkthrough of the supported attribute combinations.
#[proc_macro_derive(FromRow, attributes(snowflake))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = input::analyze(input).map(|model| codegen::expand(&model));

    match expanded {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
