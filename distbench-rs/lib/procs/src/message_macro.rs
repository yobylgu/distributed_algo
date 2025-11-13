//! Implementation of the `#[distbench::message]` attribute macro.
//!
//! This macro automatically derives common traits for message types.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Implements the `#[distbench::message]` macro.
///
/// Automatically adds:
/// - `#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]`
/// - `impl distbench::signing::Hashable`
/// - `impl distbench::signing::Verifiable`
///
/// The generated trait implementations assume that *all* fields of the
/// struct also implement `Hashable` and `Verifiable`.
pub(crate) fn message_impl(item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as DeriveInput);
    let struct_name = &input.ident;

    // Add the original derives
    let new_attr_tokens =
        syn::parse_quote!(#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]);
    input.attrs.insert(0, new_attr_tokens);

    // Get field names
    let fields = match &input.data {
        syn::Data::Struct(syn::DataStruct {
            fields: syn::Fields::Named(fields),
            ..
        }) => &fields.named,
        _ => {
            return syn::Error::new_spanned(
                input,
                "message macro only supports structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    // Get just the idents
    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();

    // Generate Hashable impl
    let hashable_impl = quote! {
        impl ::distbench::signing::Digest for #struct_name {
            fn digest(&self) -> [u8; 32] {
                let mut hasher = ::blake3::Hasher::new();
                #(
                    hasher.update(&self.#field_names.digest());
                )*
                hasher.finalize().into()
            }
        }
    };

    // Generate Verifiable impl
    let verifiable_impl = quote! {
        impl ::distbench::signing::Verifiable<#struct_name> for #struct_name {
            fn verify(self, keystore: &::distbench::community::KeyStore) -> Result<Self, ::distbench::PeerError> {
                ::log::trace!("Verifying message of type {}", stringify!(#struct_name));
                Ok(Self {
                    #(
                        #field_names: self.#field_names.verify(keystore)?,
                    )*
                })
            }
        }
    };

    let as_ref_impl = quote! {
        impl AsRef<#struct_name> for #struct_name {
            fn as_ref(&self) -> &#struct_name {
                self
            }
        }
    };

    let output = quote! {
        #input
        #hashable_impl
        #verifiable_impl
        #as_ref_impl
    };

    output.into()
}
