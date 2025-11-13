//! Implementation of the `#[distbench::handlers]` attribute macro.
//!
//! This macro generates the AlgorithmHandler trait implementation and peer
//! method implementations from handler methods.

use crate::handler_parsing::{extract_all_handlers, generate_algorithm_handler_impl};
use crate::peer_generation::{
    generate_peer_ergonomic_fns, generate_peer_methods, generate_peer_trait_fns,
};
use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemImpl, Type};

/// Implements the `#[distbench::handlers]` macro.
///
/// This macro:
/// - Extracts all handler methods from the impl block
/// - Generates the `AlgorithmHandler` trait implementation
/// - Generates corresponding methods on the Peer type
pub(crate) fn algorithm_handlers_impl(item: TokenStream) -> TokenStream {
    let input = match syn::parse::<ItemImpl>(item) {
        Ok(input) => input,
        Err(e) => {
            return TokenStream::from(e.to_compile_error());
        }
    };

    let self_ty = &input.self_ty;

    let base_ident = match &**self_ty {
        Type::Path(syn::TypePath { qself: None, path }) => {
            if let Some(segment) = path.segments.last() {
                segment.ident.clone()
            } else {
                return TokenStream::from(quote! {
                    compile_error!("Could not extract a name from the implemented type.");
                });
            }
        }
        _ => {
            return TokenStream::from(quote! {
                compile_error!("The item is implemented for a type that does not have a simple name (e.g., references, arrays).");
            });
        }
    };

    let peer_name = syn::Ident::new(
        &format!("{}Peer", base_ident),
        proc_macro2::Span::call_site(),
    );

    let peer_name_inner = syn::Ident::new(
        &format!("{}Inner", peer_name),
        proc_macro2::Span::call_site(),
    );

    let peer_name_service = syn::Ident::new(
        &format!("{}Service", peer_name),
        proc_macro2::Span::call_site(),
    );

    // Extract all methods as handlers
    let handlers = match extract_all_handlers(&input) {
        Ok(handlers) => handlers,
        Err(e) => {
            return e.to_compile_error().into();
        }
    };

    let algorithm_handler_impl = generate_algorithm_handler_impl(&base_ident, &handlers);
    let peer_methods = generate_peer_methods(&handlers);
    let peer_methods_ergonomic = generate_peer_ergonomic_fns(&handlers);
    let peer_trait_fns = generate_peer_trait_fns(&handlers);

    let peer_trait_impl = quote! {
        #[async_trait::async_trait]
        trait #peer_name_service: Send + Sync {
            #(#peer_trait_fns)*
        }
    };

    // Generate only the impl block for Peer methods, not the struct itself
    let peer_methods_impl = quote! {
        #[async_trait::async_trait]
        impl<F, T, CM> #peer_name_service for #peer_name_inner<F, T, CM>
            where
            F: ::distbench::Format,
            T: ::distbench::transport::Transport,
            CM: ::distbench::transport::ConnectionManager<T>
        {
            #(#peer_methods)*
        }
    };

    let peer_methods_ergonomic_impl = quote! {
        impl #peer_name
        {
            #(#peer_methods_ergonomic)*
        }
    };

    let expanded = quote! {
        #[allow(dead_code)]
        #input

        #algorithm_handler_impl

        #peer_methods_ergonomic_impl

        #peer_trait_impl

        #peer_methods_impl
    };

    TokenStream::from(expanded)
}
