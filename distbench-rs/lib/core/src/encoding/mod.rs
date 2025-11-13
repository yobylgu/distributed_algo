//! Encoding abstraction for message serialization.
//!
//! This module provides a generic abstraction over different serialization formats,
//! allowing the framework to be agnostic over the choice of serializer (JSON, bincode, etc.).
//!
//! # Design
//!
//! The [`Format`] trait provides a common interface for serialization and deserialization.
//! Concrete implementations are provided for:
//! - [`JsonFormat`]: Human-readable JSON serialization (default)
//! - [`BincodeFormat`]: Compact binary serialization
//!
//! # Usage
//!
//! By default, the framework uses JSON:
//! ```ignore
//! let node = Node::new(config, Arc::new(JsonFormat)); // Uses JsonFormat by default
//! ```
//!
//! To use a different format:
//! ```ignore
//! let node = Node::with_format(config, BincodeFormat);
//! ```

pub mod bincode;
pub mod error;
pub mod json;

use serde::{Deserialize, Serialize};

pub use bincode::BincodeFormat;
pub use error::FormatError;
pub use json::JsonFormat;

/// Trait for serialization formats.
///
/// This trait abstracts over different serialization formats, allowing the framework
/// to be generic over the choice of serializer. Implementations must be thread-safe
/// and support any type that implements [`Serialize`] and [`Deserialize`].
///
/// Note: This trait is not object-safe due to the generic methods. For dynamic dispatch,
/// wrap the format in an `Arc` and clone it as needed, or use a concrete type.
pub trait Format: Send + Sync + Clone + 'static {
    /// Serialize a value to bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, FormatError>;

    /// Deserialize bytes to a value.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    fn deserialize<'de, T: Deserialize<'de>>(&self, bytes: &'de [u8]) -> Result<T, FormatError>;

    /// Returns the name of this format (for debugging/logging).
    fn name(&self) -> &'static str;
}
