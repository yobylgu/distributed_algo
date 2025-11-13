//! Message types for node-to-node communication.
//!
//! This module defines the message types used for communication between nodes
//! in the distributed system.

use rkyv::{Archive, Deserialize, Serialize};

/// Messages exchanged between nodes for coordination and algorithm execution.
///
/// `NodeMessage` is an envelope type that wraps different kinds of messages
/// that nodes send to each other during execution.
#[derive(Serialize, Deserialize, Archive)]
#[archive(check_bytes)]
pub enum NodeMessage {
    /// Indicates that a node has started and is ready to participate.
    Started,

    /// Indicates that a node has finished execution.
    Finished,

    /// Requests the public key from a peer.
    AnnouncePubKey([u8; 32]),

    /// An algorithm-specific message.
    ///
    /// # Fields
    ///
    /// * `0` - The message type identifier (used for deserialization)
    /// * `1` - The serialized message payload
    Algorithm(String, Vec<u8>),
}
