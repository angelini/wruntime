use proc_macro::TokenStream;

use quote::quote;
use syn::ext::IdentExt;
use syn::{parse_macro_input, Data, DeriveInput, Fields, LitStr, Meta};

#[proc_macro_derive(FromRow, attributes(wr_db))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_from_row(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_from_row(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if let Some(attribute) = input
        .attrs
        .iter()
        .find(|attribute| attribute.path().is_ident("wr_db"))
    {
        return Err(syn::Error::new_spanned(
            attribute,
            "#[wr_db(...)] is only supported on fields",
        ));
    }

    if !input.generics.params.is_empty() || input.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "`FromRow` does not support generic structs",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(_) | Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    &input.ident,
                    "`FromRow` can only be derived for structs with named fields",
                ));
            }
        },
        Data::Enum(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "`FromRow` cannot be derived for enums",
            ));
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                &input.ident,
                "`FromRow` cannot be derived for unions",
            ));
        }
    };

    let struct_name = &input.ident;
    let mut initializers = Vec::with_capacity(fields.len());
    for field in fields {
        let field_name = field.ident.as_ref().expect("named fields have identifiers");
        let field_type = &field.ty;
        let attributes = parse_field_attributes(field)?;
        let field_context = field_name.unraw().to_string();

        let decode = if attributes.flatten {
            quote! {
                <#field_type as ::wr_sdk::db::__private::FromRowDecoder>::from_row_decoder(row)
            }
        } else {
            let column_name = attributes
                .rename
                .unwrap_or_else(|| LitStr::new(&field_name.unraw().to_string(), field_name.span()));
            quote! { row.take::<#field_type>(#column_name) }
        };

        initializers.push(quote! {
            #field_name: #decode.map_err(|error| ::wr_sdk::db::DbError::Field {
                field: #field_context,
                source: ::std::boxed::Box::new(error),
            })?
        });
    }

    Ok(quote! {
        impl ::wr_sdk::db::FromRow for #struct_name {
            fn from_row(row: ::wr_sdk::db::Row) -> ::std::result::Result<Self, ::wr_sdk::db::DbError> {
                let mut row = ::wr_sdk::db::__private::RowDecoder::new(row);
                <Self as ::wr_sdk::db::__private::FromRowDecoder>::from_row_decoder(&mut row)
            }
        }

        impl ::wr_sdk::db::__private::FromRowDecoder for #struct_name {
            fn from_row_decoder(
                row: &mut ::wr_sdk::db::__private::RowDecoder,
            ) -> ::std::result::Result<Self, ::wr_sdk::db::DbError> {
                ::std::result::Result::Ok(Self {
                    #(#initializers,)*
                })
            }
        }
    })
}

#[derive(Default)]
struct FieldAttributes {
    rename: Option<LitStr>,
    flatten: bool,
}

fn parse_field_attributes(field: &syn::Field) -> syn::Result<FieldAttributes> {
    let mut parsed = FieldAttributes::default();

    for attribute in &field.attrs {
        if !attribute.path().is_ident("wr_db") {
            continue;
        }

        match &attribute.meta {
            Meta::List(list) if list.tokens.is_empty() => {
                return Err(syn::Error::new_spanned(
                    attribute,
                    "expected `rename = \"...\"` or `flatten`",
                ));
            }
            Meta::List(_) => {}
            Meta::Path(_) | Meta::NameValue(_) => {
                return Err(syn::Error::new_spanned(
                    attribute,
                    "expected `rename = \"...\"` or `flatten`",
                ));
            }
        }

        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if parsed.rename.is_some() {
                    return Err(meta.error("duplicate `rename` attribute"));
                }
                let value = meta
                    .value()
                    .map_err(|_| meta.error("`rename` requires a string literal value"))?;
                let rename = value
                    .parse::<LitStr>()
                    .map_err(|_| meta.error("`rename` requires a string literal value"))?;
                parsed.rename = Some(rename);
                Ok(())
            } else if meta.path.is_ident("flatten") {
                if parsed.flatten {
                    return Err(meta.error("duplicate `flatten` attribute"));
                }
                if meta.input.peek(syn::Token![=]) || meta.input.peek(syn::token::Paren) {
                    return Err(meta.error("`flatten` does not take a value"));
                }
                parsed.flatten = true;
                Ok(())
            } else {
                Err(meta.error("unsupported #[wr_db] attribute; expected `rename` or `flatten`"))
            }
        })?;
    }

    if parsed.flatten {
        if let Some(rename) = &parsed.rename {
            return Err(syn::Error::new(
                rename.span(),
                "`rename` and `flatten` cannot be combined",
            ));
        }
    }

    Ok(parsed)
}
