//! Derive macros for `snowflake-connector-rs`.
//!
//! Enable the `derive` feature on `snowflake-connector-rs` to use `#[derive(FromRow)]` on your row types.
//! The macro resolves named fields against exact raw result labels from the Snowflake schema.
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
/// Named-field structs resolve columns by exact raw result label. When no field attribute overrides the lookup,
/// the macro converts each logical field name to `SCREAMING_SNAKE_CASE` at compile time, so `created_at` looks up
/// `CREATED_AT`. Raw identifier prefixes such as `r#type` are stripped before the lookup name is finalized.
/// The conversion folds ASCII case only: a non-ASCII character is copied through unchanged rather than uppercased,
/// so it will not match Snowflake's Unicode-aware label casing. Fields whose name contains non-ASCII characters
/// must use `#[snowflake(rename = "...")]` to specify the raw label explicitly.
///
/// `#[snowflake(rename = "...")]` bypasses container-level renaming and uses the provided raw label verbatim.
/// `#[snowflake(rename_all = "none")]` keeps the logical field name unchanged.
///
/// Snowflake stores and resolves unquoted aliases as uppercase. `SELECT 1 AS name` returns the raw label `NAME`,
/// so the default `SCREAMING_SNAKE_CASE` rule keeps matching unannotated structs against typical SELECT queries.
///
/// # Container attributes
///
/// - `rename_all = "SCREAMING_SNAKE_CASE"`: compile-time field-name conversion used by named structs. This is the default.
/// - `rename_all = "none"`: use each logical field name as-is. Pair this with quoted aliases or `SHOW` / `DESCRIBE`
///   results that intentionally keep labels in lowercase or mixed case.
/// - `positional`: decode every field by ordinal instead of by label on named structs.
///   Tuple structs already decode by position automatically.
/// - `crate = "::path"`: override the crate path used in generated code.
///
/// `rename_all` cannot be combined with `positional`; positional decoding does not use field names.
/// `rename_all` also cannot be applied to tuple structs, which are implicitly positional.
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
/// #[snowflake(positional)]
/// struct PositionalRow {
///     id: i64,
///     name: String,
/// }
/// ```
///
/// # Tuple structs
///
/// Tuple structs decode by ordinal automatically; you do not need to add `#[snowflake(positional)]`.
/// The container attribute remains accepted as a no-op for backward source compatibility but is not required
/// and is omitted from the examples. `rename_all` cannot be applied to tuple structs.
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// struct PositionalPair(i64, String);
/// ```
///
/// # Field attributes
///
/// - `rename = "..."`: use the exact raw result label for this field.
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// struct RenamedField {
///     #[snowflake(rename = "display name")]
///     display_name: String,
/// }
/// ```
///
/// # SQL NULL
///
/// Use `Option<T>` when a projected column may carry SQL `NULL`. Missing columns still raise `MissingColumn`;
/// if you need a non-`Option` default, project it explicitly in SQL.
///
/// ```rust,ignore
/// #[derive(snowflake_connector_rs::FromRow)]
/// struct NullableNote {
///     id: i64,
///     note: Option<String>,
/// }
/// ```
///
/// # Error behavior
///
/// The generated implementation validates the schema while building the decode plan, then propagates cell decode
/// errors while reading rows:
///
/// - `MissingColumn` for required named lookups, including `Option<T>` fields.
/// - `AmbiguousColumn` when the schema contains duplicate raw labels.
/// - `ColumnCountMismatch` when required positional fields exceed the schema.
/// - `IncompatibleColumnType` when a column's Snowflake type cannot be decoded as the field's Rust type. This is
///   raised during plan building, so a type mismatch fails the whole `rows` call before any row is read.
/// - cell decode errors while converting individual cells, including SQL `NULL` for a non-`Option` field.
///
/// SQL `NULL` only maps to `None` for `Option<T>`. Other field types propagate their normal `FromCell` decode error.
#[proc_macro_derive(FromRow, attributes(snowflake))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = input::analyze(input).map(|model| codegen::expand(&model));

    match expanded {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
