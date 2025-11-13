//! JSON serialization format implementation.

use super::{Format, FormatError};
use serde::{Deserialize, Serialize};

/// JSON serialization format.
///
/// This is the default format used by the framework. It produces human-readable
/// JSON output, which is useful for debugging and logging.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonFormat;

impl Format for JsonFormat {
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, FormatError> {
        let result = serde_json::to_vec(value).map_err(FormatError::JsonSerialization)?;
        Ok(result)
    }

    fn deserialize<'de, T: Deserialize<'de>>(&self, bytes: &'de [u8]) -> Result<T, FormatError> {
        let result = serde_json::from_slice(bytes).map_err(FormatError::JsonDeserialization)?;
        Ok(result)
    }

    fn name(&self) -> &'static str {
        "json"
    }
}
