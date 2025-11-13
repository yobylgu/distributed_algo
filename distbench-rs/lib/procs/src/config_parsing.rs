//! Configuration field parsing utilities.
//!
//! This module provides functionality for parsing `#[distbench::config]` attributes
//! on algorithm struct fields.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{Data, DeriveInput, Field, Fields, Type};

/// Information about a configuration field.
pub(crate) struct ConfigField {
    pub field_name: syn::Ident,
    pub field_type: Type,
    pub default_value: Option<syn::Expr>,
}

/// Parses a field to check if it has a `#[distbench::config]` attribute.
///
/// Supports two forms:
/// - `#[distbench::config]` - Required field
/// - `#[distbench::config(default = value)]` - Optional field with default
///
/// # Arguments
///
/// * `field` - The field to parse
///
/// # Returns
///
/// * `Ok(Some(ConfigField))` - Field has config attribute
/// * `Ok(None)` - Field does not have config attribute
/// * `Err(syn::Error)` - Invalid config attribute format
pub(crate) fn parse_config_field(field: &Field) -> Result<Option<ConfigField>, syn::Error> {
    let Some(field_name) = &field.ident else {
        return Ok(None);
    };

    for attr in &field.attrs {
        let path_str = attr.into_token_stream().to_string().replace(" ", "");
        let is_config = path_str.contains("distbench::config");

        if is_config {
            let default_value = match &attr.meta {
                // Case 1: #[distbench::config]
                syn::Meta::Path(_) => None,

                // Case 2: #[distbench::config(...)]
                syn::Meta::List(meta_list) => {
                    if meta_list.tokens.is_empty() {
                        // Case 2a: #[distbench::config()]
                        None
                    } else {
                        // Attempt to parse the tokens as `Meta`
                        match syn::parse2::<syn::Meta>(meta_list.tokens.clone()) {
                            // Case 2b: #[distbench::config(default = ...)]
                            Ok(syn::Meta::NameValue(nv)) => {
                                if nv.path.is_ident("default") {
                                    Some(nv.value)
                                } else {
                                    // Case 2c: #[distbench::config(other = ...)] - Invalid
                                    return Err(syn::Error::new_spanned(
                                        nv.path,
                                        "Invalid config attribute. Expected `default = ...`",
                                    ));
                                }
                            }
                            // Case 2d: #[distbench::config(foo)] or other invalid Meta
                            Ok(other_meta) => {
                                return Err(syn::Error::new_spanned(
                                    other_meta,
                                    "Invalid config attribute format. Expected `#[distbench::config(default = ...)]` or `#[distbench::config]`"
                                ));
                            }
                            // Case 2e: #[distbench::config(1000)] or other invalid syntax
                            Err(_) => {
                                return Err(syn::Error::new_spanned(
                                    meta_list.tokens.clone(),
                                    "Invalid config attribute format. Expected `default = ...`",
                                ));
                            }
                        }
                    }
                }

                // Case 3: #[distbench::config = ...] - Invalid
                syn::Meta::NameValue(nv) => {
                    return Err(syn::Error::new_spanned(
                        nv,
                        "Invalid config attribute format. Use `#[distbench::config]` or `#[distbench::config(default = ...)]`"
                    ));
                }
            };

            return Ok(Some(ConfigField {
                field_name: field_name.clone(),
                field_type: field.ty.clone(),
                default_value,
            }));
        }
    }
    Ok(None)
}

/// Extracts config and non-config fields from a struct.
///
/// # Arguments
///
/// * `input` - The struct definition
///
/// # Returns
///
/// A tuple of (config_fields, default_fields)
///
/// # Errors
///
/// Returns an error if any config attribute is malformed.
pub(crate) fn extract_fields(
    input: &DeriveInput,
) -> Result<(Vec<ConfigField>, Vec<Field>), syn::Error> {
    let mut config_fields = Vec::new();
    let mut default_fields = Vec::new();

    if let Data::Struct(data_struct) = &input.data {
        if let Fields::Named(fields) = &data_struct.fields {
            for field in &fields.named {
                match parse_config_field(field)? {
                    Some(config_field) => {
                        config_fields.push(config_field);
                    }
                    None => {
                        default_fields.push(field.clone());
                    }
                }
            }
        }
    }

    Ok((config_fields, default_fields))
}

/// Generates the configuration struct for an algorithm.
///
/// Creates a struct like `AlgorithmNameConfig` with optional fields
/// for each config parameter.
///
/// # Arguments
///
/// * `alg_name` - The name of the algorithm
/// * `config_fields` - The configuration fields to include
pub(crate) fn generate_config_struct(
    alg_name: &syn::Ident,
    config_fields: &[ConfigField],
) -> TokenStream2 {
    let config_name = syn::Ident::new(
        &format!("{}Config", alg_name),
        proc_macro2::Span::call_site(),
    );

    let field_defs: Vec<_> = config_fields
        .iter()
        .map(|cf| {
            let name = &cf.field_name;
            let ty = &cf.field_type;
            quote! { pub #name: Option<#ty> }
        })
        .collect();

    quote! {
        #[derive(::serde::Serialize, ::serde::Deserialize)]
        pub struct #config_name {
            #(#field_defs),*
        }
    }
}
