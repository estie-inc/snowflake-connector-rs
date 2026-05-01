//! Derive macros for `snowflake-connector-rs`.

mod attrs;
mod codegen;
mod input;
mod naming;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(FromRow, attributes(snowflake))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let expanded = input::analyze(input).map(|model| codegen::expand(&model));

    match expanded {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
