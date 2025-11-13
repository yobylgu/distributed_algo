//! Error types for the framework.
//!
//! This module defines the error types used throughout the framework for
//! peer communication and configuration.

use thiserror::Error;

/// Errors that can occur during peer communication and algorithm operations.
///
/// This error type covers serialization/deserialization failures, transport
/// errors, and protocol violations.
#[derive(Debug, Error)]
pub enum PeerError {
    /// Failed to serialize a message.
    ///
    /// This typically occurs when attempting to convert a message structure
    /// to bytes for transmission over the network.
    #[error("Failed to serialize message: {message}")]
    SerializationFailed { message: String },

    /// Failed to deserialize a message.
    ///
    /// This typically occurs when attempting to parse received bytes into
    /// a message structure, often due to protocol mismatches or corrupted data.
    #[error("Failed to deserialize message: {message}")]
    DeserializationFailed { message: String },

    /// A transport-level error occurred.
    ///
    /// This covers network-level errors such as connection failures,
    /// send/receive errors, or timeouts.
    #[error("Transport error: {message}")]
    TransportError { message: String },

    /// Received a message with an unknown type identifier.
    ///
    /// This occurs when the message type ID doesn't match any registered
    /// message handler.
    #[error("Unknown message type: {message}")]
    UnknownMessageType { message: String },

    /// The peer is unknown.
    #[error("Unknown peer: {peer_id}")]
    UnknownPeer { peer_id: String },

    /// The signature is invalid.
    #[error("Invalid signature: {peer_id}")]
    InvalidSignature { peer_id: String },
}

/// Errors that can occur during algorithm configuration.
///
/// This error type is used when setting up an algorithm instance from
/// configuration data.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A required configuration field was not provided.
    ///
    /// This error indicates that a mandatory configuration parameter
    /// is missing.
    #[error("Config field '{field}' is required")]
    RequiredField { field: String },
}
