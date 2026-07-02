use syn::{DeriveInput, Field, LitStr, Path, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RenameAll {
    ScreamingSnakeCase,
    None,
}

pub(crate) struct ContainerAttrs {
    /// Effective field-name conversion.
    pub(crate) rename_all: RenameAll,
    /// `true` only if the user wrote `#[snowflake(rename_all = "...")]`
    /// explicitly. Used to reject `positional` + explicit `rename_all`.
    pub(crate) rename_all_explicit: bool,
    pub(crate) positional: bool,
    pub(crate) crate_path: Path,
    pub(crate) crate_path_explicit: bool,
}

impl Default for ContainerAttrs {
    fn default() -> Self {
        Self {
            rename_all: RenameAll::ScreamingSnakeCase,
            rename_all_explicit: false,
            positional: false,
            crate_path: syn::parse_str("::snowflake_connector_rs").expect("default path"),
            crate_path_explicit: false,
        }
    }
}

#[derive(Default)]
pub(crate) struct FieldAttrs {
    pub(crate) rename: Option<String>,
}

pub(crate) fn parse_container_attrs(input: &DeriveInput) -> Result<ContainerAttrs> {
    let mut out = ContainerAttrs::default();
    let mut positional_seen = false;

    for attr in &input.attrs {
        if !attr.path().is_ident("snowflake") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                if out.rename_all_explicit {
                    return Err(meta.error("duplicate `rename_all`"));
                }

                let value: LitStr = meta.value()?.parse()?;
                out.rename_all = match value.value().as_str() {
                    "SCREAMING_SNAKE_CASE" => RenameAll::ScreamingSnakeCase,
                    "none" => RenameAll::None,
                    _ => {
                        return Err(meta.error(
                            "only `rename_all = \"SCREAMING_SNAKE_CASE\"` or `\"none\"` is supported",
                        ));
                    }
                };

                out.rename_all_explicit = true;
            } else if meta.path.is_ident("positional") {
                if positional_seen {
                    return Err(meta.error("duplicate `positional`"));
                }

                positional_seen = true;
                out.positional = true;
            } else if meta.path.is_ident("crate") {
                if out.crate_path_explicit {
                    return Err(meta.error("duplicate `crate`"));
                }

                let value: LitStr = meta.value()?.parse()?;
                let s = value.value();
                if s.is_empty() {
                    return Err(meta.error("crate path cannot be empty"));
                }

                let path: Path = syn::parse_str(&s).map_err(|e| meta.error(e.to_string()))?;
                out.crate_path = path;
                out.crate_path_explicit = true;
            } else {
                return Err(meta.error("unknown container attribute"));
            }
            Ok(())
        })?;
    }

    if out.positional && out.rename_all_explicit {
        return Err(syn::Error::new(
            input.ident.span(),
            "`positional` cannot be combined with `rename_all`",
        ));
    }

    Ok(out)
}

pub(crate) fn parse_field_attrs(field: &Field) -> Result<FieldAttrs> {
    let mut out = FieldAttrs::default();

    for attr in &field.attrs {
        if !attr.path().is_ident("snowflake") {
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if out.rename.is_some() {
                    return Err(meta.error("duplicate `rename`"));
                }

                let value: LitStr = meta.value()?.parse()?;
                out.rename = Some(value.value());
            } else if meta.path.is_ident("positional") {
                return Err(meta.error(
                    "field-level `positional` is not supported; use container-level `#[snowflake(positional)]` instead",
                ));
            } else {
                return Err(meta.error("unknown field attribute"));
            }
            Ok(())
        })?;
    }

    Ok(out)
}
