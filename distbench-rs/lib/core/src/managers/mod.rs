//! Connection manager implementations.
//!
//! This module provides connection manager implementations that handle
//! connection lifecycle and message sending.

pub mod thin;

pub use thin::ThinConnectionManager;
