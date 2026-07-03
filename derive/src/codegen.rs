use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::input::{FieldInfo, FieldLookup, FromRowDerive, StructShape};

pub(crate) fn expand(model: &FromRowDerive) -> TokenStream2 {
    let crate_path = &model.crate_path;
    let struct_ident = &model.struct_ident;
    let plan_len = syn::LitInt::new(&model.fields.len().to_string(), struct_ident.span());

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
            impl #crate_path::FromRow for #struct_ident {
                type Plan = [#crate_path::ColumnIndex; #plan_len];

                fn build_plan(
                    ctx: #crate_path::RowPlanContext<'_>,
                ) -> #crate_path::Result<Self::Plan> {
                    let schema = ctx.schema();
                    #(#build_plan_lines)*
                    ::core::result::Result::Ok([#(#plan_init_idents),*])
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
            let assigns = model.fields.iter().enumerate().map(|(index, field)| {
                let field_ident = field.field_ident.as_ref().expect("named");
                let value = decode_field_value(field, index);
                quote! { #field_ident: #value }
            });
            quote! { ::core::result::Result::Ok(Self { #(#assigns),* }) }
        }
        StructShape::Tuple => {
            let assigns = model
                .fields
                .iter()
                .enumerate()
                .map(|(index, field)| decode_field_value(field, index));
            quote! { ::core::result::Result::Ok(Self ( #(#assigns),* )) }
        }
    }
}

fn decode_field_value(field: &FieldInfo, index: usize) -> TokenStream2 {
    let index = syn::Index::from(index);
    let ty = &field.ty;
    quote! { row.get::<#ty>(plan[#index])? }
}
