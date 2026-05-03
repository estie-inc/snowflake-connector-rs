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
/// Snowflake stores and resolves unquoted aliases as uppercase. `SELECT 1 AS name`
/// returns the raw label `NAME`, so the default `SCREAMING_SNAKE_CASE` rule keeps
/// matching unannotated structs against typical SELECT queries.
///
/// `rename_all = "none"` is intended for results whose raw labels intentionally
/// preserve case or use lowercase, such as quoted aliases (`SELECT 1 AS "name"`)
/// or `SHOW` / `DESCRIBE` queries (whose output columns are lowercase and have to
/// be referenced via double-quoted identifiers).
///
/// # Container attributes
///
/// - `rename_all = "SCREAMING_SNAKE_CASE"`: compile-time field-name conversion
///   used by named structs. This is the default.
/// - `rename_all = "none"`: use each logical field name as-is. Pair this with
///   quoted aliases or `SHOW` / `DESCRIBE` results that intentionally keep
///   labels in lowercase or mixed case.
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
/// - `default`: use `Default::default()` when the named column is missing or the
///   cell value is `NULL`. Cell decode errors are still propagated as-is.
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
/// `#[snowflake(default)]` substitutes a default value when either the column is
/// missing (`MissingColumn`) or the cell carries a `NULL` value. It does not
/// swallow `AmbiguousColumn` or row decode errors.
///
/// # Using the macro
///
/// `snowflake-connector-rs` re-exports this macro when its `derive` feature is
/// enabled. The repository ships a runnable walkthrough at
/// `examples/derive_from_row.rs`.
#[proc_macro_derive(FromRow, attributes(snowflake))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = input::analyze(input).map(|model| codegen::expand(&model));

    match expanded {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
