use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};

use crate::input::{FieldInfo, FieldLookup, FromRowDerive, StructShape};

pub(crate) fn expand(model: &FromRowDerive) -> TokenStream2 {
    let crate_path = &model.crate_path;
    let struct_ident = &model.struct_ident;
    let plan_ident = format_ident!("__{}Plan", struct_ident);

    let plan_fields = model.fields.iter().map(|field| {
        let ident = &field.plan_ident;
        if field.has_default {
            quote! { #ident: ::core::option::Option<#crate_path::ColumnIndex> }
        } else {
            quote! { #ident: #crate_path::ColumnIndex }
        }
    });

    let build_plan_lines = model
        .fields
        .iter()
        .map(|field| build_plan_line(field, crate_path));
    let plan_init_idents = model
        .fields
        .iter()
        .map(|field| &field.plan_ident)
        .collect::<Vec<_>>();
    let row_body = row_body(model, crate_path);

    quote! {
        const _: () = {
            struct #plan_ident {
                #(#plan_fields,)*
            }

            impl #crate_path::FromRow for #struct_ident {
                type Plan = #plan_ident;

                fn build_plan(
                    ctx: #crate_path::RowPlanContext<'_>,
                ) -> #crate_path::Result<Self::Plan> {
                    let schema = ctx.schema();
                    #(#build_plan_lines)*
                    ::core::result::Result::Ok(#plan_ident { #(#plan_init_idents),* })
                }

                fn from_row_with_plan(
                    row: #crate_path::RowRef<'_>,
                    plan: &Self::Plan,
                ) -> #crate_path::Result<Self> {
                    #row_body
                }
            }
        };
    }
}

fn build_plan_line(field: &FieldInfo, crate_path: &syn::Path) -> TokenStream2 {
    let ident = &field.plan_ident;
    match &field.lookup {
        FieldLookup::Name(name) => {
            if field.has_default {
                quote! {
                    let #ident: ::core::option::Option<#crate_path::ColumnIndex> =
                        match schema.column(#name) {
                            ::core::result::Result::Ok(idx) => ::core::option::Option::Some(idx),
                            ::core::result::Result::Err(
                                #crate_path::SchemaError::MissingColumn { .. },
                            ) => ::core::option::Option::None,
                            ::core::result::Result::Err(e) => {
                                return ::core::result::Result::Err(e.into());
                            }
                        };
                }
            } else {
                quote! { let #ident = schema.column(#name)?; }
            }
        }
        FieldLookup::Position(pos) => {
            let pos_lit = syn::Index::from(*pos);
            if field.has_default {
                quote! {
                    let #ident: ::core::option::Option<#crate_path::ColumnIndex> = if schema.len() > #pos_lit {
                        ::core::option::Option::Some(schema.columns()[#pos_lit].index())
                    } else { ::core::option::Option::None };
                }
            } else {
                quote! {
                    if schema.len() <= #pos_lit {
                        return ::core::result::Result::Err(#crate_path::Error::Schema(
                            #crate_path::SchemaError::ColumnCountMismatch {
                                expected: #pos_lit + 1,
                                actual: schema.len(),
                            }
                        ));
                    }
                    let #ident = schema.columns()[#pos_lit].index();
                }
            }
        }
    }
}

fn row_body(model: &FromRowDerive, crate_path: &syn::Path) -> TokenStream2 {
    match model.shape {
        StructShape::Named => {
            let assigns = model.fields.iter().map(|field| {
                let field_ident = field.field_ident.as_ref().expect("named");
                let value = decode_field_value(field, crate_path);
                quote! { #field_ident: #value }
            });
            quote! { ::core::result::Result::Ok(Self { #(#assigns),* }) }
        }
        StructShape::Tuple => {
            let assigns = model
                .fields
                .iter()
                .map(|field| decode_field_value(field, crate_path));
            quote! { ::core::result::Result::Ok(Self ( #(#assigns),* )) }
        }
    }
}

fn decode_field_value(field: &FieldInfo, crate_path: &syn::Path) -> TokenStream2 {
    let plan_ident = &field.plan_ident;
    let ty = &field.ty;
    if field.has_default {
        quote! {
            match plan.#plan_ident {
                ::core::option::Option::Some(idx) => {
                    let cell = row.cell(idx)?;
                    if cell.is_null() {
                        <#ty as ::core::default::Default>::default()
                    } else {
                        <#ty as #crate_path::FromCell>::from_cell(cell)?
                    }
                }
                ::core::option::Option::None => <#ty as ::core::default::Default>::default(),
            }
        }
    } else {
        quote! { row.get::<#ty>(plan.#plan_ident)? }
    }
}
