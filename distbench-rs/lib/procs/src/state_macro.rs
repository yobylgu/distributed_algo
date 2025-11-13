//! Implementation of the `#[distbench::state]` attribute macro.
//!
//! This macro transforms an algorithm state struct into a complete algorithm
//! implementation with configuration support and peer management.

use crate::config_parsing::{extract_fields, generate_config_struct};
use crate::peer_generation::generate_peer_structs;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, ToTokens};
use syn::{Data, DeriveInput, Field, Fields};

/// Implements the `#[distbench::state]` macro.
///
/// This macro:
/// - Extracts `#[distbench::config]` fields
/// - Generates a corresponding Config struct
/// - Adds `peers` and `stopped` fields
/// - Implements the `AlgorithmFactory` trait
/// - Generates a Peer struct
/// - Implements the `SelfTerminating` trait
pub(crate) fn algorithm_state_impl(item: TokenStream) -> TokenStream {
    let mut input = match syn::parse::<DeriveInput>(item.clone()) {
        Ok(input) => input,
        Err(_) => {
            // Return original item if parsing fails (better for IDE)
            return item;
        }
    };
    let alg_name = &input.ident;

    let alg_name_str = alg_name.to_string();
    let peer_name = syn::Ident::new(
        &format!("{}Peer", alg_name_str),
        proc_macro2::Span::call_site(),
    );

    let config_name = syn::Ident::new(
        &format!("{}Config", alg_name_str),
        proc_macro2::Span::call_site(),
    );

    // Extract config fields, handling potential errors
    let (config_fields, default_fields) = match extract_fields(&input) {
        Ok((config, default)) => (config, default),
        Err(e) => {
            // Convert the syn::Error into a compile_error! token stream
            return e.into_compile_error().into();
        }
    };

    // Generate config struct if there are config fields
    let config_struct = generate_config_struct(alg_name, &config_fields);

    if let Data::Struct(ref mut data_struct) = input.data {
        if let Fields::Named(ref mut fields) = data_struct.fields {
            // Remove config attributes from all fields
            for field in fields.named.iter_mut() {
                field.attrs.retain(|attr| {
                    let path_str = attr.into_token_stream().to_string().replace(" ", "");
                    !path_str.contains("distbench::config")
                });
            }

            let id_field: Field = syn::parse_quote! {
                __id: ::distbench::community::PeerId
            };
            let key_field: Field = syn::parse_quote! {
                __key: ::distbench::crypto::PrivateKey
            };
            let connections_field: Field = syn::parse_quote! {
                __connections: ::std::collections::HashMap<::distbench::community::PeerId, #peer_name>
            };
            let stopped_field: Field = syn::parse_quote! {
                #[allow(dead_code)]
                __stopped_tx: ::std::sync::Arc<::tokio::sync::watch::Sender<bool>>
            };
            let status_rx_field: Field = syn::parse_quote! {
                #[allow(dead_code)]
                __stopped_rx: ::tokio::sync::watch::Receiver<bool>
            };
            let network_size_field: Field = syn::parse_quote! {
                __network_size: u32
            };
            fields.named.extend(vec![
                id_field,
                key_field,
                connections_field,
                stopped_field,
                status_rx_field,
                network_size_field,
            ]);
        }
    }

    let field_inits = generate_field_initializers(&config_fields, &default_fields);
    let helper_fns = generate_helper_fns(alg_name, &peer_name);
    let factory_impl = generate_factory_impl(&config_name, alg_name, &peer_name, &field_inits);
    let peer_struct = generate_peer_structs(&peer_name);
    let self_terminating_impl = generate_self_terminating_impl(alg_name);
    let named_impl = generate_named_impl(alg_name);

    let expanded = quote! {
        #input

        #helper_fns

        #config_struct

        #factory_impl

        #named_impl

        #peer_struct

        #self_terminating_impl
    };

    TokenStream::from(expanded)
}

/// Generates helper functions for the algorithm.
fn generate_helper_fns(alg_name: &syn::Ident, peer_name: &syn::Ident) -> TokenStream2 {
    quote! {
        impl #alg_name {
            fn N(&self) -> u32 {
                self.__network_size + 1
            }

            fn id(&self) -> &distbench::community::PeerId {
                &self.__id
            }

            fn peers(&self) -> impl Iterator<Item = (&distbench::community::PeerId, &#peer_name)> {
                self.__connections.iter().map(|(peer_id, peer)| (peer_id, peer))
            }

            fn peer(&self, id: &distbench::community::PeerId) -> Option<#peer_name> {
                self.__connections.get(id).cloned()
            }

            fn sign<M>(&self, msg: M) -> ::distbench::signing::Signed<M>
                where M: ::distbench::signing::Digest + ::serde::Serialize + for<'de> ::serde::de::Deserialize<'de>
            {
                let signature = self.__key.sign(&msg.digest());
                let id = self.__id.clone();

                ::distbench::signing::Signed::new(msg, signature, id)
            }
        }
    }
}

/// Generates field initializers for the AlgorithmFactory impl.
fn generate_field_initializers(
    config_fields: &[crate::config_parsing::ConfigField],
    default_fields: &[Field],
) -> Vec<TokenStream2> {
    config_fields
        .iter()
        .map(|cf| {
            let name = &cf.field_name;
            let default = &cf.default_value;
            if let Some(default) = default {
                quote! { #name: self.#name.unwrap_or(#default) }
            } else {
                let name_str = name.to_string();
                quote! { #name: self.#name.ok_or(::distbench::ConfigError::RequiredField { field: #name_str.to_string() })? }
            }
        })
        .chain(default_fields.iter().map(|f| {
            let name = &f.ident;
            quote! { #name: Default::default() }
        }))
        .collect()
}

/// Generates the Named trait implementation.
fn generate_named_impl(alg_name: &syn::Ident) -> TokenStream2 {
    let alg_name_str = alg_name.to_string();
    quote! {
        #[async_trait::async_trait]
        impl ::distbench::algorithm::Named for #alg_name {
            fn name(&self) -> &str {
                #alg_name_str
            }
        }
    }
}

/// Generates the AlgorithmFactory trait implementation.
fn generate_factory_impl(
    config_name: &syn::Ident,
    alg_name: &syn::Ident,
    peer_name: &syn::Ident,
    field_inits: &[TokenStream2],
) -> TokenStream2 {
    let alg_name_str = alg_name.to_string();
    let peer_name_trait = syn::Ident::new(
        &format!("{}Service", peer_name),
        proc_macro2::Span::call_site(),
    );

    let peer_name_inner = syn::Ident::new(
        &format!("{}Inner", peer_name),
        proc_macro2::Span::call_site(),
    );

    quote! {
        impl<F, T, CM> ::distbench::AlgorithmFactory<F, T, CM> for #config_name
        where
            T: ::distbench::transport::Transport + 'static,
            CM: ::distbench::transport::ConnectionManager<T> + 'static,
            F: ::distbench::Format + 'static,
        {
            type Algorithm = #alg_name;

            fn build(
                self,
                format: ::std::sync::Arc<F>,
                key: ::distbench::crypto::PrivateKey,
                id: ::distbench::community::PeerId,
                community: ::std::sync::Arc<::distbench::community::Community<T, CM>>,
            ) -> Result<::std::sync::Arc<Self::Algorithm>, ::distbench::ConfigError> {
                ::log::trace!("{}::build() - Building algorithm instance for node {:?}", #alg_name_str, id);
                let conn_managers = community.clone().neighbours();
                ::log::trace!("{}::build() - Creating peer proxies for {} neighbours", #alg_name_str, conn_managers.len());
                let connections: ::std::collections::HashMap<_, _> = conn_managers
                    .into_iter()
                    .map(|(peer_id, conn_manager)| {
                        ::log::trace!("{}::build() - Creating peer proxy for {:?}", #alg_name_str, peer_id);
                        (peer_id, #peer_name::new(std::sync::Arc::new(
                            Box::new(#peer_name_inner::new(
                                conn_manager,
                                format.clone(),
                                community.clone()))
                            as Box<dyn #peer_name_trait>)))
                    })
                    .collect();

                // Initial state is 'not stopped' (false)
                let (stopped_tx, stopped_rx) = ::tokio::sync::watch::channel(false);

                ::log::trace!("{}::build() - Algorithm instance built successfully", #alg_name_str);
                Ok(::std::sync::Arc::new(Self::Algorithm {
                    #(#field_inits,)*
                    __connections: connections,
                    __network_size: community.size() as u32,
                    __stopped_tx: ::std::sync::Arc::new(stopped_tx),
                    __stopped_rx: stopped_rx,
                    __key: key,
                    __id: id,
                }))
            }
        }
    }
}

/// Generates the SelfTerminating trait implementation.
fn generate_self_terminating_impl(alg_name: &syn::Ident) -> TokenStream2 {
    let alg_name_str = alg_name.to_string();
    quote! {
        #[async_trait::async_trait]
        impl ::distbench::SelfTerminating for #alg_name {
            async fn terminate(&self) {
                ::log::trace!("{}.terminate() - Sending termination signal", #alg_name_str);
                // Send 'true' to signal stopped.
                // Ignore result: Err means all receivers dropped.
                let _ = self.__stopped_tx.send(true);
                ::log::trace!("{}.terminate() - Termination signal sent", #alg_name_str);
            }

            async fn terminated(&self) -> bool {
                let mut rx = self.__stopped_rx.clone();
                // wait_for checks current value, and if not true,
                // awaits until the predicate (*stopped) is true.
                // Err means sender was dropped, which is fine.
                let _ = rx.wait_for(|stopped| *stopped).await;
                true
            }
        }
    }
}
