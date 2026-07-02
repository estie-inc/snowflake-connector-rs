use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};

use crate::input::{FieldInfo, FieldLookup, FromRowDerive, StructShape};

pub(crate) fn expand(model: &FromRowDerive) -> TokenStream2 {
    let crate_path = &model.crate_path;
    let struct_ident = &model.struct_ident;
    let plan_ident = format_ident!("__{}Plan", struct_ident);

    let plan_fields = model.fields.iter().map(|field| {
        let ident = &field.plan_ident;
        quote! { #ident: #crate_path::ColumnIndex }
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
    let row_body = row_body(model);

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
        FieldLookup::Name(name) => quote! { let #ident = schema.column_index(#name)?; },
        FieldLookup::Position(pos) => {
            let pos_lit = syn::Index::from(*pos);
            quote! {
                if schema.len() <= #pos_lit {
                    return ::core::result::Result::Err(
                        #crate_path::SchemaError::ColumnCountMismatch(
                            #crate_path::ColumnCountMismatchError::new(#pos_lit + 1, schema.len())
                        )
                        .into()
                    );
                }
                let #ident = schema.columns()[#pos_lit].index();
            }
        }
    }
}

fn row_body(model: &FromRowDerive) -> TokenStream2 {
    match model.shape {
        StructShape::Named => {
            let assigns = model.fields.iter().map(|field| {
                let field_ident = field.field_ident.as_ref().expect("named");
                let value = decode_field_value(field);
                quote! { #field_ident: #value }
            });
            quote! { ::core::result::Result::Ok(Self { #(#assigns),* }) }
        }
        StructShape::Tuple => {
            let assigns = model.fields.iter().map(decode_field_value);
            quote! { ::core::result::Result::Ok(Self ( #(#assigns),* )) }
        }
    }
}

fn decode_field_value(field: &FieldInfo) -> TokenStream2 {
    let plan_ident = &field.plan_ident;
    let ty = &field.ty;
    quote! { row.get::<#ty>(plan.#plan_ident)? }
}
