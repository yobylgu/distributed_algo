//! Error types for encoding operations.

use thiserror::Error;

/// Errors that can occur during serialization/deserialization.
#[derive(Debug, Error)]
pub enum FormatError {
    /// JSON serialization failed.
    #[error("JSON serialization failed: {0}")]
    JsonSerialization(#[source] serde_json::Error),

    /// JSON deserialization failed.
    #[error("JSON deserialization failed: {0}")]
    JsonDeserialization(#[source] serde_json::Error),

    /// Bincode serialization failed.
    #[error("Bincode serialization failed: {0}")]
    BincodeSerialization(#[source] bincode::Error),

    /// Bincode deserialization failed.
    #[error("Bincode deserialization failed: {0}")]
    BincodeDeserialization(#[source] bincode::Error),
}

impl From<serde_json::Error> for FormatError {
    fn from(err: serde_json::Error) -> Self {
        // We can't determine if it's serialization or deserialization,
        // so we'll use a generic conversion. In practice, the specific
        // methods will use the appropriate variant.
        FormatError::JsonSerialization(err)
    }
}

impl From<bincode::Error> for FormatError {
    fn from(err: bincode::Error) -> Self {
        FormatError::BincodeSerialization(err)
    }
}
