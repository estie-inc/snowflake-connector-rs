use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

use crate::input::{FieldInfo, FieldLookup, FromRowDerive, StructShape};

pub(crate) fn expand(model: &FromRowDerive) -> TokenStream2 {
    let crate_path = &model.crate_path;
    let struct_ident = &model.struct_ident;

    let plan_field_tys = model.fields.iter().map(|field| {
        let ty = &field.ty;
        quote! { #crate_path::CellPlan<#ty> }
    });
    let plan_field_inits = model
        .fields
        .iter()
        .map(|field| plan_field_init(field, crate_path));
    let row_body = row_body(model);

    quote! {
        const _: () = {
            impl #crate_path::FromRow for #struct_ident {
                type Plan = ( #(#plan_field_tys,)* );

                fn build_plan(
                    ctx: #crate_path::RowPlanContext<'_>,
                ) -> #crate_path::Result<Self::Plan> {
                    let schema = ctx.schema();
                    ::core::result::Result::Ok(( #(#plan_field_inits,)* ))
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

fn plan_field_init(field: &FieldInfo, crate_path: &syn::Path) -> TokenStream2 {
    let ty = &field.ty;
    match &field.lookup {
        FieldLookup::Name(name) => quote! {
            #crate_path::CellPlan::<#ty>::by_name(schema, #name)?
        },
        FieldLookup::Position(pos) => {
            let pos = *pos;
            quote! {
                #crate_path::CellPlan::<#ty>::by_position(schema, #pos)?
            }
        }
    }
}

fn row_body(model: &FromRowDerive) -> TokenStream2 {
    match model.shape {
        StructShape::Named => {
            let assigns = model.fields.iter().enumerate().map(|(index, field)| {
                let field_ident = field.field_ident.as_ref().expect("named");
                let value = decode_field_value(index);
                quote! { #field_ident: #value }
            });
            quote! { ::core::result::Result::Ok(Self { #(#assigns),* }) }
        }
        StructShape::Tuple => {
            let assigns = model
                .fields
                .iter()
                .enumerate()
                .map(|(index, _)| decode_field_value(index));
            quote! { ::core::result::Result::Ok(Self ( #(#assigns),* )) }
        }
    }
}

fn decode_field_value(index: usize) -> TokenStream2 {
    let index = syn::Index::from(index);
    quote! { row.get_planned(&plan.#index)? }
}
