use syn::Ident;

use crate::attrs::RenameAll;

pub(crate) fn logical_ident_name(ident: &Ident) -> String {
    let ident = ident.to_string();
    ident
        .strip_prefix("r#")
        .unwrap_or(ident.as_str())
        .to_string()
}

pub(crate) fn screaming_snake(s: &str) -> String {
    // For a snake_case input, this is just an uppercase. For mixed-case inputs
    // we insert underscores at lower-to-upper transitions.
    let mut out = String::with_capacity(s.len() + 2);
    let mut prev_lower = false;

    for ch in s.chars() {
        if ch.is_ascii_uppercase() && prev_lower {
            out.push('_');
        }
        out.push(ch.to_ascii_uppercase());
        prev_lower = ch.is_ascii_lowercase() || ch.is_ascii_digit();
    }
    out
}

pub(crate) fn apply_rename_all(s: &str, rename_all: RenameAll) -> String {
    match rename_all {
        RenameAll::ScreamingSnakeCase => screaming_snake(s),
        RenameAll::None => s.to_string(),
    }
}
