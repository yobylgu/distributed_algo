//! Thin connection manager implementation.
//!
//! A lightweight connection manager that lazily establishes connections.

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::transport::{Connection, ConnectionManager, Result, Transport};

/// A lightweight connection manager that lazily establishes connections.
///
/// `ThinConnectionManager` caches a connection to a peer and reuses it
/// for multiple message sends. The connection is established on the first
/// message send and then reused for subsequent sends.
///
/// This is useful for reducing connection overhead when communicating
/// with the same peer multiple times.
///
/// # Type Parameters
///
/// * `T` - The transport layer implementation
pub struct ThinConnectionManager<T: Transport> {
    transport: Arc<T>,
    address: T::Address,
    connection: Arc<RwLock<Option<T::Connection>>>,
}

impl<T: Transport> ThinConnectionManager<T> {
    /// Ensures a connection is established, creating one if needed.
    ///
    /// This method uses double-checked locking to minimize contention
    /// when the connection is already established.
    ///
    /// # Returns
    ///
    /// A connection to the peer.
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the connection cannot be established.
    async fn ensure_connected(&self) -> Result<T::Connection> {
        {
            let conn_guard = self.connection.read().await;
            if let Some(conn) = conn_guard.as_ref() {
                return Ok(conn.clone());
            }
        }

        let mut conn_guard = self.connection.write().await;

        if let Some(conn) = conn_guard.as_ref() {
            return Ok(conn.clone());
        }

        let conn = self.transport.connect(self.address.clone()).await?;
        *conn_guard = Some(conn.clone());

        Ok(conn)
    }
}

impl<T: Transport> Clone for ThinConnectionManager<T> {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            address: self.address.clone(),
            connection: self.connection.clone(),
        }
    }
}

#[async_trait]
impl<T: Transport> ConnectionManager<T> for ThinConnectionManager<T> {
    fn new(transport: Arc<T>, address: T::Address) -> Self {
        Self {
            transport,
            address,
            connection: Arc::new(RwLock::new(None)),
        }
    }

    async fn send(&self, msg: Vec<u8>) -> Result<Vec<u8>> {
        let connection = self.ensure_connected().await?;
        connection.send(msg).await
    }

    async fn cast(&self, msg: Vec<u8>) -> Result<()> {
        let connection = self.ensure_connected().await?;
        connection.cast(msg).await
    }
}
