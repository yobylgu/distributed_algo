//! Algorithm traits and interfaces.
//!
//! This module defines the core traits that distributed algorithms must implement
//! to work with the framework.

use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use crate::community::{Community, KeyStore, PeerId};
use crate::crypto::PrivateKey;
use crate::encoding::Format;
use crate::error::ConfigError;
use crate::transport::{ConnectionManager, Transport};

/// Trait for handling incoming messages in a distributed algorithm.
///
/// Implementors of this trait define how their algorithm processes messages
/// received from other nodes.
///
/// # Type Parameters
///
/// * `F` - The serialization format to use for message encoding/decoding
#[async_trait]
pub trait AlgorithmHandler<F: Format> {
    /// Handles an incoming message from a peer.
    ///
    /// # Arguments
    ///
    /// * `src` - The ID of the peer that sent the message
    /// * `msg_type_id` - The type identifier for the message (used for deserialization)
    /// * `msg_bytes` - The serialized message payload
    /// * `format` - The serialization format to use for encoding/decoding
    ///
    /// # Returns
    ///
    /// * `Ok(Some(response))` - A response message to send back to the sender
    /// * `Ok(None)` - No response needed (for cast messages)
    /// * `Err(e)` - An error occurred while processing the message
    async fn handle(
        &self,
        src: PeerId,
        msg_type_id: String,
        msg_bytes: Vec<u8>,
        keystore: KeyStore,
        format: &F,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Trait for algorithms that can self-terminate.
///
/// Algorithms implement this trait to signal when they have completed their
/// execution and should shut down.
#[async_trait]
pub trait SelfTerminating {
    /// Initiates termination of the algorithm.
    async fn terminate(&self);

    /// Checks if the algorithm has terminated.
    ///
    /// # Returns
    ///
    /// `true` if the algorithm has terminated, otherwise waits for termination.
    async fn terminated(&self) -> bool;
}

/// Trait for algorithms that have a name.
///
/// Algorithms implement this trait to provide a name for themselves.
pub trait Named {
    /// Returns the name of the algorithm.
    fn name(&self) -> &str;
}

/// The main trait that distributed algorithms must implement.
///
/// This trait combines self-termination capabilities and provides lifecycle
/// hooks for algorithm initialization and cleanup.
///
/// # Examples
///
/// ```ignore
/// use distbench::algorithm::{Algorithm, SelfTerminating};
///
/// struct MyAlgorithm {
///     // ... fields
/// }
///
/// #[async_trait]
/// impl Algorithm for MyAlgorithm {
///     async fn on_start(&self) {
///         // Initialize algorithm state
///     }
///
///     async fn on_exit(&self) {
///         // Clean up resources
///     }
/// }
/// ```
#[async_trait]
pub trait Algorithm: Send + Sync + SelfTerminating + Named {
    /// Called when the algorithm starts running.
    ///
    /// This is invoked after all nodes in the community have synchronized
    /// and are ready to begin execution.
    async fn on_start(&self) {}

    /// Called when the algorithm is exiting.
    ///
    /// This is invoked after the algorithm has terminated and the node
    /// is shutting down. Use this for cleanup operations.
    async fn on_exit(&self) {}

    /// Called in the end of the algorithm to get a final report.
    ///
    /// This is invoked after the algorithm has terminated and the node
    /// has shut down. Use this for reporting any final metrics or information.
    ///
    /// # Returns
    ///
    /// An optional `HashMap` of key-value pairs representing the report.
    async fn report(&self) -> Option<HashMap<impl Display, impl Display>> {
        None::<HashMap<&str, &str>>
    }
}

/// Factory trait for creating algorithm instances.
///
/// Implementors of this trait know how to construct algorithm instances
/// from configuration and a community context.
///
/// # Type Parameters
///
/// * `T` - The transport layer implementation
pub trait AlgorithmFactory<F: Format, T: Transport, CM: ConnectionManager<T>> {
    /// The type of algorithm this factory produces.
    type Algorithm: Algorithm;

    /// Builds an algorithm instance from this configuration.
    ///
    /// # Arguments
    ///
    /// * `format` - The serialization format to use for encoding/decoding
    /// * `community` - The community context in which the algorithm will run
    ///
    /// # Returns
    ///
    /// An `Arc` wrapping the algorithm instance.
    ///
    /// # Errors
    ///
    /// Returns a `ConfigError` if the algorithm cannot be constructed
    /// (e.g., missing required configuration fields).
    fn build(
        self,
        format: Arc<F>,
        key: PrivateKey,
        id: PeerId,
        community: Arc<Community<T, CM>>,
    ) -> Result<Arc<Self::Algorithm>, ConfigError>;
}
