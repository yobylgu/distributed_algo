//! TCP socket-based transport implementation.
//!
//! Uses TCP sockets for 'local' and 'network' modes.
//! Implements a multiplexed connection for concurrent `send` and `cast`
//! operations over a single `TcpStream`.
//!
//! Uses an application-level handshake:
//! 1. Client sends its `u16` numeric ID on connect.
//! 2. Server reads this `u16` ID to identify the peer, ignoring SocketAddr.
//! 3. All framework communication uses this `u16` ID as the `TcpAddress`.

use async_trait::async_trait;
use dashmap::DashMap;
use log::{error, trace, warn};
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinSet;

use super::{Address, Connection, Result, Server, Transport, TransportError};

const FRAME_TYPE_CAST: u8 = 0;
const FRAME_TYPE_SEND: u8 = 1;
const FRAME_TYPE_RESPONSE_OK: u8 = 2;
const FRAME_TYPE_RESPONSE_ERR: u8 = 3;

/// A numeric node identifier (node index) used as the logical address.
#[derive(Hash, Eq, PartialEq, Clone, Debug, Copy)]
pub struct TcpAddress(pub u16);

impl Display for TcpAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Address for TcpAddress {}

/// Transport implementation using TCP sockets.
#[derive(Clone)]
pub struct TcpTransport {
    /// This node's own logical ID.
    pub local_id: TcpAddress,
    /// Map of all logical IDs to their physical socket addresses.
    pub peer_sockets: Arc<HashMap<TcpAddress, SocketAddr>>,
}

impl TcpTransport {
    /// Creates a new TCP transport.
    pub fn new(local_id: TcpAddress, peer_sockets: Arc<HashMap<TcpAddress, SocketAddr>>) -> Self {
        Self {
            local_id,
            peer_sockets,
        }
    }
}

#[async_trait]
impl Transport for TcpTransport {
    type Address = TcpAddress;
    type Connection = TcpConnection;

    /// Establishes a new TCP connection to the target logical address.
    async fn connect(&self, addr: Self::Address) -> Result<Self::Connection> {
        let socket_addr =
            self.peer_sockets
                .get(&addr)
                .ok_or_else(|| TransportError::ConnectionFailed {
                    address: addr.to_string(),
                    message: "No physical SocketAddr found for numeric ID".to_string(),
                })?;

        trace!(
            "Connecting to logical {} (physical {})...",
            addr,
            socket_addr
        );
        let socket = TcpStream::connect(socket_addr).await.map_err(|e| {
            TransportError::ConnectionFailed {
                address: socket_addr.to_string(),
                message: e.to_string(),
            }
        })?;
        socket.set_nodelay(true).map_err(map_io_err)?;

        // --- Perform Handshake ---
        let (reader, mut writer) = socket.into_split();
        let mut buf_writer = BufWriter::new(&mut writer);
        // Send our own numeric ID so the server knows who we are
        buf_writer
            .write_u16(self.local_id.0)
            .await
            .map_err(map_io_err)?;
        buf_writer.flush().await.map_err(map_io_err)?;
        trace!(
            "Connected to {} and sent handshake (id {})",
            addr,
            self.local_id
        );
        // --- End Handshake ---

        Ok(TcpConnection::new(reader, writer, self.local_id))
    }

    /// Binds to the local address and listens for incoming connections.
    async fn serve(
        &self,
        server: impl Server<Self> + 'static,
        stop_signal: Arc<Notify>,
    ) -> Result<()> {
        let local_socket_addr =
            self.peer_sockets
                .get(&self.local_id)
                .ok_or_else(|| TransportError::ListenFailed {
                    address: self.local_id.to_string(),
                    message: "No physical SocketAddr found for local numeric ID".to_string(),
                })?;

        let listener = TcpListener::bind(local_socket_addr).await.map_err(|e| {
            TransportError::ListenFailed {
                address: local_socket_addr.to_string(),
                message: e.to_string(),
            }
        })?;

        trace!(
            "Listening for connections on logical {} (physical {})",
            self.local_id,
            local_socket_addr
        );

        let mut connection_handlers = JoinSet::new();

        loop {
            tokio::select! {
                _ = stop_signal.notified() => {
                    trace!("Stop signal received, shutting down listener on {}", self.local_id);
                    break;
                },
                terminated = server.terminated() => {
                    if terminated {
                        trace!("Server terminated, shutting down listener on {}", self.local_id);
                        break;
                    }
                }
                Ok((socket, remote_socket_addr)) = listener.accept() => {
                    trace!("Accepted connection from physical {}", remote_socket_addr);
                    socket.set_nodelay(true).map_err(map_io_err)?;

                    let server_clone = server.clone();
                    let stop_signal_clone = stop_signal.clone();
                    let node_context = crate::get_node_context();

                    connection_handlers.spawn(async move {
                        let connection_task = async move {
                            if let Err(e) = handle_incoming_connection(
                                server_clone,
                                socket,
                                remote_socket_addr,
                                stop_signal_clone,
                            ).await {
                                match e {
                                    TransportError::Io { message } if message.contains("Connection reset by peer") || message.contains("unexpected EOF") || message.contains("Connection refused") => {
                                        trace!("Connection from {} closed gracefully", remote_socket_addr);
                                    }
                                    _ => warn!("Connection from {} ended with error: {}", remote_socket_addr, e),
                                }
                            }
                        };

                        if let Some(ctx) = node_context {
                            crate::NODE_ID_CTX.scope(ctx, connection_task).await;
                        } else {
                            connection_task.await;
                        }
                    });
                },
            }
        }

        trace!(
            "Serve loop for {} stopped. Waiting for {} active connections to finish...",
            self.local_id,
            connection_handlers.len()
        );
        connection_handlers.shutdown().await;
        trace!(
            "All connections for {} closed. TCP server exiting.",
            self.local_id
        );
        Ok(())
    }
}

/// Handles a single, accepted, incoming TCP connection.
async fn handle_incoming_connection<S: Server<TcpTransport>>(
    server: S,
    socket: TcpStream,
    remote_socket_addr: SocketAddr, // Used for logging
    stop_signal: Arc<Notify>,
) -> Result<()> {
    let (reader, writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // --- Perform Handshake ---
    let remote_id_val = reader.read_u16().await.map_err(|e| {
        error!(
            "Failed to read handshake from {}: {}",
            remote_socket_addr, e
        );
        map_io_err(e)
    })?;
    let src_addr = TcpAddress(remote_id_val);
    trace!(
        "Received handshake from physical {}, logical ID is {}",
        remote_socket_addr,
        src_addr
    );
    // --- End Handshake ---

    loop {
        tokio::select! {
            _ = stop_signal.notified() => {
                trace!("Handler for {} ({}) received stop signal, exiting.", src_addr, remote_socket_addr);
                break;
            },
            terminated = server.terminated() => {
                if terminated {
                    trace!("Handler for {} ({}) detected server termination, exiting.", src_addr, remote_socket_addr);
                    break;
                }
            },
            frame_result = read_frame(&mut reader) => {
                let (msg_type, request_id, payload) = match frame_result {
                    Ok(frame) => frame,
                    Err(e) => {
                        trace!("Read frame error for {}: {}. Closing handler.", src_addr, e);
                        return Err(e); // Connection closed or error
                    }
                };

                match msg_type {
                    FRAME_TYPE_CAST => {
                        trace!("Handling Cast ({} bytes) from logical {}", payload.len(), src_addr);
                        let _ = server.handle(&src_addr, payload).await.map_err(|e| {
                            error!(
                                "Error handling Cast from {}: {}. Message dropped.",
                                src_addr, e
                            )
                        });
                    }
                    FRAME_TYPE_SEND => {
                        trace!("Handling Send (id {}) from logical {}", request_id, src_addr);
                        match server.handle(&src_addr, payload).await {
                            Ok(Some(data)) => {
                                trace!(
                                    "Replying to Send (id {}) from {} with {} bytes",
                                    request_id,
                                    src_addr,
                                    data.len()
                                );
                                write_frame(&mut writer, FRAME_TYPE_RESPONSE_OK, request_id, data).await?;
                            }
                            Ok(None) => {
                                trace!(
                                    "Replying to Send (id {}) from {} with empty response",
                                    request_id,
                                    src_addr
                                );
                                write_frame(&mut writer, FRAME_TYPE_RESPONSE_OK, request_id, Vec::new())
                                    .await?;
                            }
                            Err(e) => {
                                trace!(
                                    "Replying to Send (id {}) from {} with error: {}",
                                    request_id,
                                    src_addr,
                                    e
                                );
                                let error_payload = e.to_string().into_bytes();
                                write_frame(
                                    &mut writer,
                                    FRAME_TYPE_RESPONSE_ERR,
                                    request_id,
                                    error_payload,
                                )
                                .await?;
                            }
                        }
                    }
                    _ => {
                        warn!(
                            "Received unexpected frame type {} from logical {}",
                            msg_type, src_addr
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

/// A multiplexed, client-side TCP connection.
#[derive(Clone)]
pub struct TcpConnection {
    tx: mpsc::Sender<ConnectionMessage>,
    next_request_id: Arc<AtomicU64>,
}

/// Internal message type for the connection worker task.
#[derive(Debug)]
enum ConnectionMessage {
    Send(u64, Vec<u8>, oneshot::Sender<Result<Vec<u8>>>),
    Cast(Vec<u8>),
    Close,
}

impl TcpConnection {
    fn new(reader: OwnedReadHalf, writer: OwnedWriteHalf, local_id: TcpAddress) -> Self {
        let (tx, rx) = mpsc::channel(100);
        let pending_requests = Arc::new(DashMap::new());
        let next_request_id = Arc::new(AtomicU64::new(1));

        tokio::spawn(async move {
            let reader = BufReader::new(reader);
            let writer = BufWriter::new(writer);

            if let Err(e) = Self::run(reader, writer, rx, pending_requests, local_id).await {
                match &e {
                    TransportError::Io { message }
                        if message.contains("unexpected EOF")
                            || message.contains("Connection reset by peer") =>
                    {
                        trace!(
                            "Connection task (from {}) exited gracefully: {}",
                            local_id,
                            e
                        );
                    }
                    _ => {
                        warn!(
                            "Connection task (from {}) exited with error: {}",
                            local_id, e
                        );
                    }
                }
            } else {
                trace!("Connection task (from {}) exited gracefully", local_id);
            }
        });

        Self {
            tx,
            next_request_id,
        }
    }

    async fn run(
        mut reader: BufReader<OwnedReadHalf>,
        mut writer: BufWriter<OwnedWriteHalf>,
        mut rx: mpsc::Receiver<ConnectionMessage>,
        pending_requests: Arc<DashMap<u64, oneshot::Sender<Result<Vec<u8>>>>>,
        local_id: TcpAddress,
    ) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    let close = Self::handle_outgoing_message(msg, &mut writer, &pending_requests).await?;
                    if close {
                        trace!("Close request received, shutting down connection task from {}", local_id);
                        break;
                    }
                },
                frame = read_frame(&mut reader) => {
                    Self::handle_incoming_frame(frame?, &pending_requests, local_id)?;
                }
            }
        }
        Ok(())
    }

    async fn handle_outgoing_message<W: AsyncWriteExt + Unpin>(
        msg: ConnectionMessage,
        writer: &mut W,
        pending_requests: &Arc<DashMap<u64, oneshot::Sender<Result<Vec<u8>>>>>,
    ) -> Result<bool> {
        match msg {
            ConnectionMessage::Send(id, payload, response_tx) => {
                trace!("Sending Send (id {}) ({} bytes)", id, payload.len());
                pending_requests.insert(id, response_tx);
                write_frame(writer, FRAME_TYPE_SEND, id, payload).await?;
            }
            ConnectionMessage::Cast(payload) => {
                trace!("Sending Cast ({} bytes)", payload.len());
                write_frame(writer, FRAME_TYPE_CAST, 0, payload).await?;
            }
            ConnectionMessage::Close => {
                writer.shutdown().await.map_err(map_io_err)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn handle_incoming_frame(
        frame: (u8, u64, Vec<u8>),
        pending_requests: &Arc<DashMap<u64, oneshot::Sender<Result<Vec<u8>>>>>,
        local_id: TcpAddress,
    ) -> Result<()> {
        let (msg_type, request_id, payload) = frame;
        match msg_type {
            FRAME_TYPE_RESPONSE_OK => {
                trace!(
                    "{} received ResponseOK (id {}) ({} bytes)",
                    local_id,
                    request_id,
                    payload.len()
                );
                if let Some((_, tx)) = pending_requests.remove(&request_id) {
                    let _ = tx.send(Ok(payload));
                }
            }
            FRAME_TYPE_RESPONSE_ERR => {
                trace!(
                    "{} received ResponseErr (id {}) ({} bytes)",
                    local_id,
                    request_id,
                    payload.len()
                );
                if let Some((_, tx)) = pending_requests.remove(&request_id) {
                    let err_msg = String::from_utf8_lossy(&payload).to_string();
                    let _ = tx.send(Err(TransportError::AlgorithmError { message: err_msg }));
                }
            }
            _ => {
                warn!(
                    "Client connection {} received unexpected frame type {}",
                    local_id, msg_type
                );
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Connection<TcpTransport> for TcpConnection {
    async fn send(&self, msg: Vec<u8>) -> Result<Vec<u8>> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
        let (response_tx, response_rx) = oneshot::channel();

        self.tx
            .send(ConnectionMessage::Send(request_id, msg, response_tx))
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;

        response_rx
            .await
            .map_err(|_| TransportError::ConnectionClosed)?
    }

    async fn cast(&self, msg: Vec<u8>) -> Result<()> {
        self.tx
            .send(ConnectionMessage::Cast(msg))
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        self.tx
            .send(ConnectionMessage::Close)
            .await
            .map_err(|_| TransportError::ConnectionClosed)?;
        Ok(())
    }
}

// --- Frame Read/Write Helpers ---

/// Frame: [type (u8)][id (u64)][len (u32)][payload]
async fn write_frame<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    msg_type: u8,
    request_id: u64,
    payload: Vec<u8>,
) -> Result<()> {
    writer.write_u8(msg_type).await.map_err(map_io_err)?;
    writer.write_u64(request_id).await.map_err(map_io_err)?;
    writer
        .write_u32(payload.len() as u32)
        .await
        .map_err(map_io_err)?;
    writer.write_all(&payload).await.map_err(map_io_err)?;
    writer.flush().await.map_err(map_io_err)?;
    Ok(())
}

/// Frame: [type (u8)][id (u64)][len (u32)][payload]
async fn read_frame<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<(u8, u64, Vec<u8>)> {
    let msg_type = reader.read_u8().await.map_err(map_io_err)?;
    let request_id = reader.read_u64().await.map_err(map_io_err)?;
    let len = reader.read_u32().await.map_err(map_io_err)?;

    let mut payload = vec![0; len as usize];
    reader.read_exact(&mut payload).await.map_err(map_io_err)?;

    Ok((msg_type, request_id, payload))
}

fn map_io_err(e: std::io::Error) -> TransportError {
    TransportError::Io {
        message: e.to_string(),
    }
}
