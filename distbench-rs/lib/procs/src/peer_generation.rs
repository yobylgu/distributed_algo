//! Peer proxy generation utilities.
//!
//! This module generates peer proxy types that provide type-safe methods
//! for sending messages to other nodes.

use crate::handler_parsing::HandlerInfo;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

pub(crate) fn generate_peer_trait_fns(handlers: &[HandlerInfo]) -> Vec<TokenStream2> {
    handlers
        .iter()
        .map(|handler| {
            let method_name = &handler.method_name;
            let msg_type = &handler.msg_type;
            let reply_type = &handler.reply_type;
            if handler.reply_type.is_some() {
                quote! {
                    async fn #method_name(&self, msg: &#msg_type) -> Result<#reply_type, ::distbench::PeerError>;
                }
            } else {
                quote! {
                    async fn #method_name(&self, msg: &#msg_type) -> Result<(), ::distbench::PeerError>;
                }
            }
        })
        .collect()
}

pub(crate) fn generate_peer_ergonomic_fns(handlers: &[HandlerInfo]) -> Vec<TokenStream2> {
    handlers
        .iter()
        .map(|handler| {
            let method_name = &handler.method_name;
            let msg_type = &handler.msg_type;
            let reply_type = &handler.reply_type;
            if handler.reply_type.is_some() {
                quote! {
                    async fn #method_name(&self, msg: impl AsRef<#msg_type>) -> Result<#reply_type, ::distbench::PeerError> {
                        self.0.#method_name(msg.as_ref()).await
                    }
                }
            } else {
                quote! {
                    async fn #method_name(&self, msg: impl AsRef<#msg_type>) -> Result<(), ::distbench::PeerError> {
                        self.0.#method_name(msg.as_ref()).await
                    }
                }
            }
        })
        .collect()
}

/// Generates peer methods for communicating with other nodes.
///
/// For each handler method, generates a corresponding method on the peer type
/// that serializes the message and sends it via the connection manager.
///
/// # Arguments
///
/// * `handlers` - The handler methods to generate peer methods for
pub(crate) fn generate_peer_methods(handlers: &[HandlerInfo]) -> Vec<TokenStream2> {
    handlers
        .iter()
        .map(|handler| {
            let method_name = &handler.method_name;
            let msg_type = &handler.msg_type;
            let msg_type_str = quote!(#msg_type).to_string().replace(" ", "");

            match &handler.reply_type {
                None => {
                    // Cast method (no reply expected)
                    quote! {
                        async fn #method_name(&self, msg: &#msg_type) -> Result<(), ::distbench::PeerError> {
                            use ::distbench::transport::ConnectionManager;
                            use ::distbench::Format;

                            ::log::trace!("Peer::{} - Serializing message of type {}", stringify!(#method_name), #msg_type_str);
                            let msg_bytes = self.format.serialize(msg)
                                .map_err(|e| ::distbench::PeerError::SerializationFailed {
                                    message: format!("Failed to serialize message of type '{}': {}", #msg_type_str, e)
                                })?;

                            let envelope = ::distbench::NodeMessage::Algorithm(
                                #msg_type_str.to_string(),
                                msg_bytes
                            );

                            ::log::trace!("Peer::{} - Serializing envelope with rkyv", stringify!(#method_name));
                            // Use rkyv to serialize the NodeMessage envelope
                            let envelope_bytes = ::rkyv::to_bytes::<_, 256>(&envelope)
                                .map_err(|e| ::distbench::PeerError::SerializationFailed {
                                    message: format!("Failed to serialize envelope with rkyv: {}", e)
                                })?;

                            ::log::trace!("Peer::{} - Casting {} bytes", stringify!(#method_name), envelope_bytes.len());
                            self.connection_manager.cast(envelope_bytes.to_vec()).await
                                .map_err(|e| ::distbench::PeerError::TransportError {
                                    message: format!("Failed to cast message: {}", e)
                                })?;

                            ::log::trace!("Peer::{} - Cast complete", stringify!(#method_name));
                            Ok(())
                        }
                    }
                }
                Some(reply_type) => {
                    // Send method (expects reply)
                    let reply_type_str = quote!(#reply_type).to_string().replace(" ", "");
                    quote! {
                        async fn #method_name(&self, msg: &#msg_type) -> Result<#reply_type, ::distbench::PeerError> {
                            use ::distbench::transport::ConnectionManager;
                            use ::distbench::signing::Verifiable;
                            use ::distbench::Format;

                            ::log::trace!("Peer::{} - Serializing message of type {}", stringify!(#method_name), #msg_type_str);
                            let msg_bytes = self.format.serialize(msg)
                                .map_err(|e| ::distbench::PeerError::SerializationFailed {
                                    message: format!("Failed to serialize message of type '{}': {}", #msg_type_str, e)
                                })?;

                            let envelope = ::distbench::NodeMessage::Algorithm(
                                #msg_type_str.to_string(),
                                msg_bytes
                            );

                            ::log::trace!("Peer::{} - Serializing envelope with rkyv", stringify!(#method_name));
                            // Use rkyv to serialize the NodeMessage envelope
                            let envelope_bytes = ::rkyv::to_bytes::<_, 256>(&envelope)
                                .map_err(|e| ::distbench::PeerError::SerializationFailed {
                                    message: format!("Failed to serialize envelope with rkyv: {}", e)
                                })?;

                            ::log::trace!("Peer::{} - Sending {} bytes and waiting for reply", stringify!(#method_name), envelope_bytes.len());
                            let reply_bytes = self.connection_manager.send(envelope_bytes.to_vec()).await
                                .map_err(|e| ::distbench::PeerError::TransportError {
                                    message: format!("Failed to send message: {}", e)
                                })?;

                            ::log::trace!("Peer::{} - Received reply: {} bytes, deserializing", stringify!(#method_name), reply_bytes.len());
                            let reply: #reply_type = self.format.deserialize(&reply_bytes)
                                .map_err(|e| ::distbench::PeerError::DeserializationFailed {
                                    message: format!("Failed to deserialize reply of type '{}': {}", #reply_type_str, e)
                                })?;

                            let reply = reply.verify(&self.community.keystore())?;

                            ::log::trace!("Peer::{} - Send/reply complete", stringify!(#method_name));
                            Ok(reply)
                        }
                    }
                }
            }
        })
        .collect()
}

/// Generates the peer struct definition.
///
/// Creates a struct like `AlgorithmNamePeer<T, CM>` that wraps a connection manager and format.
///
/// # Arguments
///
/// * `peer_name` - The name for the peer type
pub(crate) fn generate_peer_structs(peer_name: &syn::Ident) -> TokenStream2 {
    let inner_peer_name = syn::Ident::new(
        &format!("{}Inner", peer_name.to_string()),
        proc_macro2::Span::call_site(),
    );

    let peer_name_service = syn::Ident::new(
        &format!("{}Service", peer_name.to_string()),
        proc_macro2::Span::call_site(),
    );

    quote! {
        #[derive(Clone)]
        struct #inner_peer_name<F: ::distbench::Format, T: ::distbench::transport::Transport + 'static, CM: ::distbench::transport::ConnectionManager<T> + 'static> {
            connection_manager: ::std::sync::Arc<CM>,
            format: ::std::sync::Arc<F>,
            community: ::std::sync::Arc<::distbench::community::Community<T, CM>>,
            _phantom: ::std::marker::PhantomData<T>,
        }

        impl<F: ::distbench::Format, T: ::distbench::transport::Transport + 'static, CM: ::distbench::transport::ConnectionManager<T> + 'static> #inner_peer_name<F, T, CM> {
            pub fn new(connection_manager: ::std::sync::Arc<CM>, format: ::std::sync::Arc<F>, community: ::std::sync::Arc<::distbench::community::Community<T, CM>>) -> Self {
                Self {
                    connection_manager,
                    format,
                    community,
                    _phantom: ::std::marker::PhantomData,
                }
            }
        }

        #[derive(Clone)]
        struct #peer_name(std::sync::Arc<Box<dyn #peer_name_service>>);

        impl #peer_name {
            pub fn new(inner: ::std::sync::Arc<Box<dyn #peer_name_service>>) -> Self {
                Self(inner)
            }
        }
    }
}
