//! Transport layer abstraction.
//!
//! This module provides the core abstractions for network communication in the
//! distributed system. It defines traits for:
//! - **Transport** - The overall network layer (TCP, in-memory channels, etc.)
//! - **Connection** - A single connection to a peer
//! - **ConnectionManager** - Manages connections and message sending
//! - **Server** - Handles incoming messages
//!
//! The transport layer is designed to be pluggable, allowing different
//! implementations for different use cases (production TCP, testing channels, etc.).

use std::{fmt::Display, hash::Hash, sync::Arc};

pub mod channel;
pub mod error;
pub mod tcp;

pub use crate::managers::ThinConnectionManager;
use async_trait::async_trait;
pub use error::{Result, TransportError};
use tokio::sync::Notify;

use crate::algorithm::SelfTerminating;

/// Trait for network addresses.
///
/// An address must be hashable, comparable, cloneable, displayable,
/// and safe to send across threads.
pub trait Address: Hash + Eq + Clone + Display + Send + Sync {}

/// A connection to a remote peer.
///
/// `Connection` represents an active connection to another node in the system.
/// It provides methods for both request-response communication (`send`) and
/// one-way communication (`cast`).
///
/// # Type Parameters
///
/// * `T` - The transport layer that created this connection
#[async_trait]
pub trait Connection<T: Transport>: Send + Sync + Clone {
    /// Sends a message and waits for a response.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message bytes to send
    ///
    /// # Returns
    ///
    /// The response message bytes.
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the message cannot be sent or
    /// the response cannot be received.
    async fn send(&self, msg: Vec<u8>) -> Result<Vec<u8>>;

    /// Sends a message without waiting for a response.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message bytes to send
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the message cannot be sent.
    async fn cast(&self, msg: Vec<u8>) -> Result<()>;

    /// Closes this connection.
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the connection cannot be closed cleanly.
    async fn close(&self) -> Result<()>;
}

/// Manages connections to a peer and provides message sending capabilities.
///
/// A `ConnectionManager` abstracts over the details of establishing and
/// maintaining connections, providing a simple interface for sending messages.
///
/// # Type Parameters
///
/// * `T` - The transport layer implementation
#[async_trait]
pub trait ConnectionManager<T: Transport>: Send + Sync {
    /// Creates a new connection manager for the given address.
    ///
    /// # Arguments
    ///
    /// * `transport` - The transport layer to use for connecting
    /// * `address` - The address of the peer to connect to
    fn new(transport: Arc<T>, address: T::Address) -> Self;

    /// Sends a message and waits for a response.
    ///
    /// The connection manager will establish a connection if needed.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message bytes to send
    ///
    /// # Returns
    ///
    /// The response message bytes.
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the message cannot be sent or
    /// the response cannot be received.
    async fn send(&self, msg: Vec<u8>) -> Result<Vec<u8>>;

    /// Sends a message without waiting for a response.
    ///
    /// The connection manager will establish a connection if needed.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message bytes to send
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the message cannot be sent.
    async fn cast(&self, msg: Vec<u8>) -> Result<()>;
}

/// The transport layer abstraction.
///
/// A `Transport` implementation provides the ability to connect to peers
/// and serve incoming connections. Different implementations can be used
/// for different scenarios (e.g., TCP for production, channels for testing).
///
/// # Type Parameters
///
/// * `Address` - The type of network address used by this transport
/// * `Connection` - The type of connection created by this transport
#[async_trait]
pub trait Transport: Clone + Send + Sync {
    /// The address type for this transport.
    type Address: Address;

    /// The connection type for this transport.
    type Connection: Connection<Self>;

    /// Establishes a connection to the specified address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address to connect to
    ///
    /// # Returns
    ///
    /// A connection to the peer at the specified address.
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the connection cannot be established.
    async fn connect(&self, addr: Self::Address) -> Result<Self::Connection>;

    /// Starts serving incoming connections.
    ///
    /// This method runs until the stop signal is triggered or the server
    /// terminates itself.
    ///
    /// # Arguments
    ///
    /// * `server` - The server implementation that will handle incoming messages
    /// * `stop_signal` - A signal that can be used to stop serving
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if serving fails.
    async fn serve(
        &self,
        server: impl Server<Self> + 'static,
        stop_signal: Arc<Notify>,
    ) -> Result<()>;
}

/// A server that handles incoming messages.
///
/// Implementors of this trait define how to process messages received from peers.
///
/// # Type Parameters
///
/// * `T` - The transport layer implementation
#[async_trait]
pub trait Server<T: Transport>: Clone + Send + Sync + SelfTerminating {
    /// Handles an incoming message.
    ///
    /// # Arguments
    ///
    /// * `addr` - The address of the peer that sent the message
    /// * `msg` - The message bytes
    ///
    /// # Returns
    ///
    /// * `Ok(Some(response))` - A response to send back to the sender
    /// * `Ok(None)` - No response needed
    /// * `Err(e)` - An error occurred while handling the message
    async fn handle(&self, addr: &T::Address, msg: Vec<u8>) -> Result<Option<Vec<u8>>>;
}
