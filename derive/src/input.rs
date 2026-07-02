use quote::format_ident;
use syn::{Data, DeriveInput, Fields, Ident, Path, Result, spanned::Spanned};

use crate::attrs::{ContainerAttrs, parse_container_attrs, parse_field_attrs};
use crate::naming::{apply_rename_all, logical_ident_name};

pub(crate) struct FromRowDerive {
    pub(crate) struct_ident: Ident,
    pub(crate) crate_path: Path,
    pub(crate) shape: StructShape,
    pub(crate) fields: Vec<FieldInfo>,
}

pub(crate) enum StructShape {
    Named,
    Tuple,
}

pub(crate) enum FieldLookup {
    Name(String),
    Position(usize),
}

pub(crate) struct FieldInfo {
    pub(crate) field_ident: Option<Ident>,
    pub(crate) plan_ident: Ident,
    pub(crate) ty: syn::Type,
    pub(crate) lookup: FieldLookup,
}

pub(crate) fn analyze(input: DeriveInput) -> Result<FromRowDerive> {
    validate_generics(&input)?;

    let container = parse_container_attrs(&input)?;
    let data = match &input.data {
        Data::Struct(s) => s,
        Data::Enum(e) => {
            return Err(syn::Error::new(
                e.enum_token.span,
                "FromRow does not support enums",
            ));
        }
        Data::Union(u) => {
            return Err(syn::Error::new(
                u.union_token.span,
                "FromRow does not support unions",
            ));
        }
    };

    let (shape, fields) = match &data.fields {
        Fields::Named(named) => (StructShape::Named, parse_named(named, &container)?),
        Fields::Unnamed(unnamed) => (StructShape::Tuple, parse_unnamed(unnamed, &container)?),
        Fields::Unit => {
            return Err(syn::Error::new(
                input.ident.span(),
                "FromRow does not support unit structs",
            ));
        }
    };

    Ok(FromRowDerive {
        struct_ident: input.ident,
        crate_path: container.crate_path,
        shape,
        fields,
    })
}

fn validate_generics(input: &DeriveInput) -> Result<()> {
    if let Some(lifetime) = input.generics.lifetimes().next() {
        return Err(syn::Error::new(
            lifetime.span(),
            "FromRow does not support lifetime parameters",
        ));
    }

    if input.generics.type_params().next().is_some()
        || input.generics.const_params().next().is_some()
    {
        return Err(syn::Error::new(
            input.generics.span(),
            "FromRow does not support generic parameters",
        ));
    }

    Ok(())
}

fn parse_named(named: &syn::FieldsNamed, container: &ContainerAttrs) -> Result<Vec<FieldInfo>> {
    let mut out = Vec::new();
    for (i, field) in named.named.iter().enumerate() {
        if is_phantom_data(&field.ty) {
            return Err(syn::Error::new(
                field.ty.span(),
                "FromRow does not support PhantomData fields",
            ));
        }

        let field_attrs = parse_field_attrs(field)?;
        let field_rename = field_attrs.rename;
        let ident = field.ident.clone().expect("named");
        let plan_ident = format_ident!("__plan_{}", ident);

        let lookup = if container.positional {
            if field_rename.is_some() {
                return Err(syn::Error::new(
                    field.span(),
                    "container `positional` cannot be combined with field `rename`",
                ));
            }
            FieldLookup::Position(i)
        } else {
            let name = field_rename.unwrap_or_else(|| {
                let raw = logical_ident_name(&ident);
                apply_rename_all(&raw, container.rename_all)
            });
            FieldLookup::Name(name)
        };

        out.push(FieldInfo {
            field_ident: Some(ident),
            plan_ident,
            ty: field.ty.clone(),
            lookup,
        });
    }

    Ok(out)
}

fn parse_unnamed(
    unnamed: &syn::FieldsUnnamed,
    container: &ContainerAttrs,
) -> Result<Vec<FieldInfo>> {
    if container.rename_all_explicit {
        return Err(syn::Error::new(
            unnamed.paren_token.span.span(),
            "`rename_all` cannot be applied to a tuple struct (implicit `positional`)",
        ));
    }

    let mut out = Vec::new();
    for (i, field) in unnamed.unnamed.iter().enumerate() {
        if is_phantom_data(&field.ty) {
            return Err(syn::Error::new(
                field.ty.span(),
                "FromRow does not support PhantomData fields",
            ));
        }

        let field_attrs = parse_field_attrs(field)?;
        if field_attrs.rename.is_some() {
            return Err(syn::Error::new(
                field.span(),
                "`rename` cannot be applied to tuple struct fields",
            ));
        }

        let plan_ident = format_ident!("__plan_{}", i);
        out.push(FieldInfo {
            field_ident: None,
            plan_ident,
            ty: field.ty.clone(),
            lookup: FieldLookup::Position(i),
        });
    }

    Ok(out)
}

fn is_phantom_data(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Path(type_path) => type_path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "PhantomData"),
        _ => false,
    }
}
