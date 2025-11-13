//! Framework for building distributed algorithms.
//!
//! This library provides a framework for implementing and running distributed algorithms.
//! It handles the infrastructure concerns (networking, message passing, lifecycle management)
//! so you can focus on algorithm logic.
//!
//! # Main Components
//!
//! - **[`Node`]** - Represents a participant in the distributed system
//! - **[`Community`]** - Represents the set of peers that communicate with each other
//! - **[`Algorithm`]** - The trait that distributed algorithms implement
//! - **[`transport::Transport`]** - Abstraction over the network layer (TCP, channels, etc.)
//!
//! # Example
//!
//! ```ignore
//! use distbench::{Node, algorithm::Algorithm, community::Community};
//!
//! // Define your algorithm by implementing the Algorithm trait
//! struct EchoAlgorithm { /* ... */ }
//!
//! // Create a community of peers
//! let community = Community::new(/* ... */);
//!
//! // Create and start a node
//! let node = Node::new(peer_id, Arc::new(community), Arc::new(algorithm), Arc::new(JsonFormat))?;
//! node.start(stop_signal).await?;
//! ```
//!
pub mod algorithm;
pub mod community;
pub mod crypto;
pub mod encoding;
pub mod error;
pub mod managers;
pub mod messages;
pub mod node;
pub mod signing;
pub mod status;
pub mod transport;

// Re-export commonly used types at the crate root
pub use algorithm::{Algorithm, AlgorithmFactory, AlgorithmHandler, SelfTerminating};
pub use community::{Community, PeerId};
pub use encoding::{BincodeFormat, Format, FormatError, JsonFormat};
pub use error::{ConfigError, PeerError};
pub use messages::NodeMessage;
pub use node::Node;
pub use procs::handlers;
pub use procs::message;
pub use procs::state;
pub use status::NodeStatus;

// Task-local storage for the current node ID context.
//
// This is used to associate logging and operations with a specific node
// in a multi-node environment.
tokio::task_local! {
    pub static NODE_ID_CTX: String;
}

/// Returns the current node ID from the task-local context, if available.
///
/// # Returns
///
/// `Some(node_id)` if called within a node context, `None` otherwise.
pub fn get_node_context() -> Option<String> {
    NODE_ID_CTX.try_with(|id| id.clone()).ok()
}
