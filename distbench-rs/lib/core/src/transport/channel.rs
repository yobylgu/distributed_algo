//! In-memory channel-based transport implementation.
//!
//! This module provides a transport implementation that uses in-memory channels
//! instead of network sockets. This is primarily useful for testing and
//! simulating distributed systems without network overhead.

use async_trait::async_trait;
use std::{collections::HashMap, fmt::Display, sync::Arc};
use tokio::sync::{mpsc, oneshot, Notify, RwLock};

use crate::community::PeerId;
pub use crate::transport::{Address, Connection, Result, Server, Transport, TransportError};

impl Address for PeerId {}

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

type InternalMessage = (Option<oneshot::Sender<Result<Vec<u8>>>>, Vec<u8>);
type ConnectionRequest = (PeerId, oneshot::Sender<mpsc::Sender<InternalMessage>>);

/// Builder for creating channel-based transport instances.
///
/// Multiple transport instances can share the same registry, allowing
/// them to communicate with each other through in-memory channels.
pub struct ChannelTransportBuilder {
    registry: Arc<RwLock<HashMap<PeerId, mpsc::Sender<ConnectionRequest>>>>,
}

impl ChannelTransportBuilder {
    /// Creates a new channel transport builder with an empty registry.
    pub fn new() -> Self {
        Self {
            registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Builds a new channel transport for the given peer ID.
    ///
    /// # Arguments
    ///
    /// * `local_addr` - The peer ID that this transport instance represents
    ///
    /// # Returns
    ///
    /// A new channel transport instance that shares the registry with
    /// other transports created from this builder.
    pub fn build(&self, local_addr: PeerId) -> Arc<ChannelTransport> {
        Arc::new(ChannelTransport::new(local_addr, self.registry.clone()))
    }
}

/// A connection using in-memory channels.
///
/// This connection type allows sending messages to a peer using
/// tokio channels instead of network sockets.
#[derive(Clone)]
pub struct ChannelConnection {
    tx: mpsc::Sender<InternalMessage>,
}

#[async_trait]
impl Connection<ChannelTransport> for ChannelConnection {
    async fn send(&self, msg: Vec<u8>) -> Result<Vec<u8>> {
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send((Some(response_tx), msg))
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        response_rx
            .await
            .map_err(|_| TransportError::ConnectionClosed)?
    }

    async fn cast(&self, msg: Vec<u8>) -> Result<()> {
        self.tx
            .send((None, msg))
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        Ok(())
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

/// Channel-based transport implementation.
///
/// This transport uses in-memory channels for communication between nodes,
/// making it ideal for testing distributed algorithms without network overhead.
/// All nodes using the same registry can communicate with each other.
#[derive(Clone)]
pub struct ChannelTransport {
    registry: Arc<RwLock<HashMap<PeerId, mpsc::Sender<ConnectionRequest>>>>,
    local_addr: PeerId,
}

impl ChannelTransport {
    /// Creates a new channel transport.
    ///
    /// # Arguments
    ///
    /// * `local_addr` - The peer ID for this transport instance
    /// * `registry` - Shared registry of all peers in the system
    pub fn new(
        local_addr: PeerId,
        registry: Arc<RwLock<HashMap<PeerId, mpsc::Sender<ConnectionRequest>>>>,
    ) -> Self {
        Self {
            registry,
            local_addr,
        }
    }
}

#[async_trait]
impl Transport for ChannelTransport {
    type Address = PeerId;
    type Connection = ChannelConnection;

    async fn connect(&self, addr: Self::Address) -> Result<Self::Connection> {
        let registry = self.registry.read().await;
        let tx = registry
            .get(&addr)
            .ok_or(TransportError::UnknownPeer {
                addr: addr.to_string(),
            })?
            .clone();

        let (channel_tx, channel_rx) = oneshot::channel();
        let message = (self.local_addr.clone(), channel_tx);

        tx.send(message)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        let tx = channel_rx
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        Ok(ChannelConnection { tx })
    }

    async fn serve(
        &self,
        server: impl Server<Self> + 'static,
        stop_signal: Arc<Notify>,
    ) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(100);

        {
            let mut registry = self.registry.write().await;
            registry.insert(self.local_addr.clone(), tx);
        }

        loop {
            tokio::select! {
                _ = stop_signal.notified() => {
                    break;
                },

                terminated = server.terminated() => {
                    if terminated {
                        break;
                    }
                },
                Some((from, request_tx)) = rx.recv() => {
                    let (tx, mut rx) = mpsc::channel(100);
                    let server = server.clone();
                    request_tx
                        .send(tx)
                        .map_err(|_| TransportError::ConnectionClosed)?;

                    let signal_clone = stop_signal.clone();
                    let node_context = crate::get_node_context();

                    let handler_future = async move {
                        loop {
                            tokio::select! {
                                _ = signal_clone.notified() => {
                                    server.terminate().await;
                                    break;
                                },
                                terminated = server.terminated() => {
                                    if terminated {
                                        break;
                                    }
                                },
                                Some((response_tx, payload)) = rx.recv() => {
                                    let result = server.handle(&from, payload).await;

                                    if let Some(response_tx) = response_tx {
                                        let response = match result {
                                            Ok(Some(data)) => Ok(data),
                                            Ok(None) => Ok(Vec::new()),
                                            Err(e) => Err(e),
                                        };

                                        let _ = response_tx.send(response);
                                    }
                                }
                            }
                        }
                    };

                    if let Some(ctx) = node_context {
                        tokio::spawn(crate::NODE_ID_CTX.scope(ctx, handler_future));
                    } else {
                        tokio::spawn(handler_future);
                    }
                },
            }
        }

        let mut registry = self.registry.write().await;
        registry.remove(&self.local_addr);

        Ok(())
    }
}
