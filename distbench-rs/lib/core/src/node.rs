//! Node implementation for distributed algorithms.
//!
//! This module provides the core [`Node`] structure that represents a participant
//! in a distributed system, along with supporting types for peer communication.

use async_trait::async_trait;
use log::{error, trace};
use serde::Serialize;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, Notify};
use typetag::serde;

use crate::algorithm::{Algorithm, AlgorithmHandler};
use crate::community::{Community, PeerId};
use crate::crypto::{PrivateKey, PublicKey};
use crate::encoding::{Format, JsonFormat};
use crate::messages::NodeMessage;
use crate::status::NodeStatus;
use crate::transport::{self, ConnectionManager, Server, Transport, TransportError};
use crate::SelfTerminating;
use crate::NODE_ID_CTX;

/// Internal peer representation for node-to-node communication.
///
/// Wraps a connection manager and provides methods to send node lifecycle messages.
struct NodePeer<T, CM>
where
    T: Transport,
    CM: ConnectionManager<T>,
{
    conn_manager: Arc<CM>,
    _phantom: PhantomData<T>,
}

impl<T, CM> NodePeer<T, CM>
where
    T: Transport,
    CM: ConnectionManager<T>,
{
    /// Creates a new `NodePeer` with the given connection manager.
    pub fn new(conn_manager: Arc<CM>) -> Self {
        Self {
            conn_manager,
            _phantom: PhantomData,
        }
    }

    /// Notifies the peer that this node has started.
    pub async fn started(&self) -> Result<(), TransportError> {
        trace!("NodePeer::started - Sending Started message");
        let message = NodeMessage::Started;
        let bytes =
            rkyv::to_bytes::<_, 256>(&message).map_err(|e| TransportError::AlgorithmError {
                message: format!("Failed to serialize NodeMessage::Started: {}", e),
            })?;
        trace!(
            "NodePeer::started - Serialized message, {} bytes",
            bytes.len()
        );
        self.conn_manager.cast(bytes.to_vec()).await
    }

    /// Notifies the peer that this node has finished.
    pub async fn finished(&self) -> Result<(), TransportError> {
        trace!("NodePeer::finished - Sending Finished message");
        let message = NodeMessage::Finished;
        let bytes =
            rkyv::to_bytes::<_, 256>(&message).map_err(|e| TransportError::AlgorithmError {
                message: format!("Failed to serialize NodeMessage::Finished: {}", e),
            })?;
        trace!(
            "NodePeer::finished - Serialized message, {} bytes",
            bytes.len()
        );
        self.conn_manager.cast(bytes.to_vec()).await
    }

    /// Announce our public key to the peer.
    pub async fn announce_pubkey(&self, key: &PublicKey) -> Result<(), TransportError> {
        trace!("NodePeer::announce_pubkey - Announcing public key to peer");
        let message = NodeMessage::AnnouncePubKey(key.to_bytes());
        let bytes =
            rkyv::to_bytes::<_, 256>(&message).map_err(|e| TransportError::AlgorithmError {
                message: format!("Failed to serialize NodeMessage::GetPubKey: {}", e),
            })?;
        trace!(
            "NodePeer::announce_pubkey - Serialized message, {} bytes",
            bytes.len()
        );
        self.conn_manager.cast(bytes.to_vec()).await
    }
}

#[derive(Serialize)]
struct FullReport {
    elapsed_time: u64,
    messages_received: u64,
    bytes_received: u64,
    algorithm: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<HashMap<String, String>>,
}

struct AlgoMetrics {
    start_stamp: AtomicU64,
    messages_received: AtomicU64,
    bytes_received: AtomicU64,
}

impl AlgoMetrics {
    pub fn new() -> Self {
        Self {
            start_stamp: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
        }
    }

    /// Records the start time of the algorithm.
    pub fn start(&self) {
        // Get the current Unix timestamp in seconds
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time is before the UNIX epoch")
            .as_secs();
        self.start_stamp.store(start_time, Ordering::Relaxed);
    }

    /// Records a single received message.
    ///
    /// Increments the `messages_received` count by 1 and
    /// `bytes_received` by the length of the data slice.
    pub fn received(&self, data: &[u8]) {
        // Increment message count by 1
        self.messages_received.fetch_add(1, Ordering::Relaxed);

        // Increment byte count by the length of the slice.
        // We cast data.len() (which is usize) to u64.
        self.bytes_received
            .fetch_add(data.len() as u64, Ordering::Relaxed);
    }

    /// Generates a JSON report of the metrics and algorithm-specific details.
    ///
    /// It calls the algorithm's `report()` method and combines its output
    /// with the internal metrics.
    ///
    /// # Arguments
    /// * `algorithm` - An object implementing the `Algorithm` trait.
    ///
    /// # Returns
    /// A `String` containing the JSON report (pretty-printed).
    ///
    /// # Panics
    /// Panics if JSON serialization fails.
    pub async fn report_json<A>(&self, algorithm: Arc<A>) -> String
    where
        A: Algorithm + ?Sized,
    {
        trace!("AlgoMetrics::report_json - Generating metrics report");
        let details_map = algorithm.report().await.map(|report_map| {
            report_map
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<String, String>>()
        });
        let start_time =
            SystemTime::UNIX_EPOCH + Duration::from_secs(self.start_stamp.load(Ordering::Relaxed));

        let elapsed_time = SystemTime::now()
            .duration_since(start_time)
            .unwrap_or_default()
            .as_secs();

        let report = FullReport {
            elapsed_time,
            messages_received: self.messages_received.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            algorithm: algorithm.name().to_string(),
            details: details_map,
        };

        trace!(
            "AlgoMetrics::report_json - Report: {} messages, {} bytes, {} seconds elapsed",
            report.messages_received,
            report.bytes_received,
            report.elapsed_time
        );

        serde_json::to_string(&report).expect("Failed to serialize metrics report")
    }
}

impl Default for AlgoMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A node in the distributed system.
///
/// A `Node` represents a single participant in a distributed algorithm execution.
/// It manages the node's lifecycle (starting, running, stopping, termination),
/// coordinates with peers in the community, and executes the assigned algorithm.
///
/// # Type Parameters
///
/// * `T` - The transport layer implementation (e.g., TCP, channels)
/// * `CM` - The connection manager implementation
/// * `A` - The algorithm implementation to execute on this node
/// * `F` - The serialization format for algorithm messages (defaults to JSON)
///
/// # Examples
///
/// ```ignore
/// use distbench::{Node, community::Community, JsonFormat};
/// use std::sync::Arc;
///
/// // Using default JSON format
/// let node = Node::new(
///     peer_id,
///     Arc::new(community),
///     Arc::new(algorithm),
///     Arc::new(JsonFormat),
/// )?;
/// ```
pub struct Node<T, CM, A, F = JsonFormat>
where
    T: Transport,
    CM: ConnectionManager<T>,
    A: Algorithm,
    F: Format,
{
    id: PeerId,
    key: PrivateKey,
    status_tx: Arc<watch::Sender<NodeStatus>>,
    status_rx: watch::Receiver<NodeStatus>,
    community: Arc<Community<T, CM>>,
    algo: Arc<A>,
    format: Arc<F>,
    metrics: Arc<AlgoMetrics>,
}

impl<T: Transport, CM: ConnectionManager<T>, A: Algorithm, F: Format> Clone for Node<T, CM, A, F> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            key: self.key.clone(),
            status_tx: Arc::clone(&self.status_tx),
            status_rx: self.status_rx.clone(),
            community: Arc::clone(&self.community),
            algo: Arc::clone(&self.algo),
            format: self.format.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

impl<T, CM, A, F> Node<T, CM, A, F>
where
    T: Transport + 'static,
    CM: ConnectionManager<T> + 'static,
    A: Algorithm + AlgorithmHandler<F> + 'static,
    F: Format,
{
    /// Creates a new node with the specified ID, community, algorithm, and format.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier for this node
    /// * `community` - The community this node belongs to
    /// * `algo` - The algorithm instance to execute
    /// * `format` - The serialization format to use for algorithm messages
    ///
    /// # Errors
    ///
    /// Returns a `ConfigError` if the node cannot be initialized.
    pub fn new(
        id: PeerId,
        key: PrivateKey,
        community: Arc<Community<T, CM>>,
        algo: Arc<A>,
        format: Arc<F>,
    ) -> Result<Self, crate::error::ConfigError> {
        let (status_tx, status_rx) = watch::channel(NodeStatus::NotStarted);
        community.set_pubkey(id.clone(), key.pubkey());

        Ok(Self {
            id,
            key,
            community,
            algo,
            format,
            status_tx: Arc::new(status_tx),
            status_rx,
            metrics: Arc::new(AlgoMetrics::new()),
        })
    }

    /// Returns the ID of this node.
    pub fn id(&self) -> &PeerId {
        &self.id
    }

    /// Monitors the node's lifecycle and coordinates state transitions.
    ///
    /// This method runs in a separate task and handles:
    /// - Synchronizing startup with other nodes
    /// - Calling algorithm lifecycle hooks (`on_start`, `on_exit`)
    /// - Coordinating shutdown when the algorithm terminates
    async fn monitor(&self) {
        trace!(
            "Node::monitor - Starting monitor task for node {}",
            self.id.to_string()
        );
        let mut status_rx = self.status_rx.clone();
        let mut algo_terminated = false;
        let mut on_start_called = false;

        loop {
            let status = status_rx.borrow_and_update().clone();
            trace!("Node::monitor - Current status: {:?}", status);
            match status {
                NodeStatus::KeySharing => {
                    trace!("Node::monitor - KeySharing state, announcing public key to peers");
                    let pubkey = self.key.pubkey();
                    let mut futures = Vec::new();

                    for conn in self.community.all_peers() {
                        let peer = NodePeer::new(conn);
                        futures.push(async move { peer.announce_pubkey(&pubkey).await });
                    }
                    futures::future::join_all(futures).await;

                    if self.community.keystore_full() {
                        trace!("Node::monitor - Keystore full, transitioning to Starting");
                        // Give the other nodes time to announce their public keys as well
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        let _ = self.status_tx.send(NodeStatus::Starting);
                    } else {
                        trace!("Node::monitor - Waiting for all public keys");
                    }
                }
                NodeStatus::Starting => {
                    trace!("Node::monitor - Starting state, notifying peers");
                    let mut futures = Vec::new();

                    for conn in self.community.all_peers() {
                        let peer = NodePeer::new(conn);
                        futures.push(async move { peer.started().await });
                    }
                    futures::future::join_all(futures).await;

                    let statuses = self.community.statuses();
                    if statuses
                        .values()
                        .all(|status| *status == NodeStatus::Running)
                    {
                        trace!("Node::monitor - All peers running, transitioning to Running");
                        let _ = self.status_tx.send(NodeStatus::Running);
                        self.metrics.start();
                    } else {
                        trace!("Node::monitor - Waiting for all peers to be running");
                    }
                }
                NodeStatus::Running => {
                    if !on_start_called {
                        trace!("Node::monitor - Running state, calling algorithm on_start");
                        self.algo.on_start().await;
                        on_start_called = true;
                        trace!("Node::monitor - Algorithm on_start completed");
                    }
                }
                NodeStatus::Stopping => {
                    trace!("Node::monitor - Stopping state, notifying peers of finish");
                    let mut futures = Vec::new();
                    for conn in self.community.all_peers() {
                        let peer = NodePeer::new(conn);
                        futures.push(async move { peer.finished().await });
                    }

                    futures::future::join_all(futures).await;

                    let statuses = self.community.statuses();
                    if statuses
                        .values()
                        .all(|status| *status == NodeStatus::Terminated)
                    {
                        trace!("Node::monitor - All peers terminated, transitioning to Terminated");
                        let _ = self.status_tx.send(NodeStatus::Terminated);
                    } else {
                        trace!("Node::monitor - Waiting for all peers to terminate");
                    }
                }
                NodeStatus::Terminated => {
                    trace!("Node::monitor - Terminated state, calling algorithm on_exit");
                    self.algo.on_exit().await;
                    trace!("Node::monitor - Monitor task exiting");
                    break;
                }
                NodeStatus::NotStarted => {
                    trace!("Node::monitor - NotStarted state");
                }
            }

            if algo_terminated {
                if status_rx.changed().await.is_err() {
                    error!("Monitor task stopped");
                    break;
                }
            } else {
                tokio::select! {
                    res = status_rx.changed() => {
                        if res.is_err() {
                            error!("Monitor task stopped");
                            break;
                        }
                    }
                    res = self.algo.terminated() => {
                        if res {
                            trace!("Node::monitor - Algorithm terminated, initiating node shutdown");
                            self.terminate().await;
                            algo_terminated = true;
                        }
                    }
                }
            }
        }
    }

    /// Starts the node and begins serving requests.
    ///
    /// This spawns background tasks for monitoring and serving, then transitions
    /// the node to the `Starting` state.
    ///
    /// # Arguments
    ///
    /// * `stop_signal` - A signal that can be used to stop the node externally
    /// * `report_folder` - A folder to write the report to if provided
    ///
    /// # Returns
    ///
    /// A `JoinHandle` for the serving task, which completes when the node stops.
    ///
    /// # Errors
    ///
    /// Returns a `TransportError` if the node fails to start serving.
    pub async fn start(
        &self,
        stop_signal: Arc<Notify>,
    ) -> transport::Result<tokio::task::JoinHandle<transport::Result<String>>> {
        trace!("Node::start - Starting node {}", self.id.to_string());
        let transport = self.community.transport();
        let serve_self = self.clone();
        let monitor_self = self.clone();
        let node_id_str = self.id.to_string();
        let node_id_str_2 = self.id.to_string();

        trace!("Node::start - Spawning monitor task");
        let _ = tokio::spawn(NODE_ID_CTX.scope(node_id_str.clone(), async move {
            monitor_self.monitor().await;
        }));

        let serve_handle = tokio::spawn(NODE_ID_CTX.scope(node_id_str_2.clone(), async move {
            trace!("Node::start - Spawning serve task");
            let algo = serve_self.algo.clone();
            let metrics = serve_self.metrics.clone();
            let result = transport.serve(serve_self, stop_signal).await;
            match result {
                Ok(_) => {
                    trace!("Node::start - Server stopped, generating report");
                    Ok(metrics.report_json(algo).await)
                }
                Err(e) => Err(e),
            }
        }));

        tokio::time::sleep(Duration::from_millis(200)).await;

        let _ = self.status_tx.send(NodeStatus::KeySharing);

        Ok(serve_handle)
    }
}

#[async_trait]
impl<T, CM, A, F> Server<T> for Node<T, CM, A, F>
where
    T: Transport + 'static,
    CM: ConnectionManager<T> + 'static,
    A: Algorithm + AlgorithmHandler<F> + 'static,
    F: Format,
{
    async fn handle(&self, src: &T::Address, msg: Vec<u8>) -> transport::Result<Option<Vec<u8>>> {
        let status = self.status_tx.borrow().clone();
        let peer_id = self
            .community
            .id_of(&src)
            .ok_or(TransportError::UnknownPeer {
                addr: src.to_string(),
            })?;

        // Use rkyv to deserialize the NodeMessage
        let archived = rkyv::check_archived_root::<NodeMessage>(&msg).map_err(|e| {
            TransportError::AlgorithmError {
                message: format!("Failed to deserialize NodeMessage with rkyv: {}", e),
            }
        })?;
        let msg: NodeMessage = rkyv::Deserialize::deserialize(archived, &mut rkyv::Infallible)
            .map_err(|e| TransportError::AlgorithmError {
                message: format!("Failed to deserialize NodeMessage with rkyv: {}", e),
            })?;

        trace!(
            "Node::handle - Received {:?} from {}",
            match &msg {
                NodeMessage::Started => "Started".to_string(),
                NodeMessage::Finished => "Finished".to_string(),
                NodeMessage::AnnouncePubKey(_) => "AnnouncePubKey".to_string(),
                NodeMessage::Algorithm(msg_type, _) => format!("Algorithm({})", msg_type),
            },
            peer_id.to_string()
        );

        let result = match msg {
            NodeMessage::Algorithm(msg_type, msg) => {
                if status < NodeStatus::Starting {
                    trace!(
                        "Node::handle - Rejecting message, node not ready (status: {:?})",
                        status
                    );
                    return Err(TransportError::NotReady {
                        message: "Node is still not in the Starting state".to_string(),
                    });
                }
                self.metrics.received(&msg);

                self.algo
                    .handle(
                        peer_id,
                        msg_type,
                        msg,
                        self.community.keystore(),
                        &self.format,
                    )
                    .await
                    .map_err(|e| TransportError::AlgorithmError {
                        message: e.to_string(),
                    })?
            }
            NodeMessage::Started => {
                trace!("Node::handle - Peer {} started", peer_id.to_string());
                self.community.set_status(peer_id, NodeStatus::Running);
                if *self.status_tx.borrow() == NodeStatus::Starting {
                    let statuses = self.community.statuses();
                    if statuses
                        .values()
                        .all(|status| *status == NodeStatus::Running)
                    {
                        self.status_tx.send(NodeStatus::Running)?;
                        self.metrics.start();
                    }
                }
                None
            }
            NodeMessage::Finished => {
                trace!("Node::handle - Peer {} finished", peer_id.to_string());
                self.community.set_status(peer_id, NodeStatus::Terminated);

                if *self.status_tx.borrow() == NodeStatus::Stopping {
                    let statuses = self.community.statuses();
                    if statuses
                        .values()
                        .all(|status| *status == NodeStatus::Terminated)
                    {
                        trace!("Node::handle - All peers terminated, transitioning to Terminated");
                        self.status_tx.send(NodeStatus::Terminated)?;
                    }
                }
                None
            }
            NodeMessage::AnnouncePubKey(key) => {
                trace!(
                    "Node::handle - Received public key from peer {}",
                    peer_id.to_string()
                );
                self.community
                    .set_pubkey(peer_id, PublicKey::from_bytes(&key));

                if self.community.keystore_full()
                    && *self.status_tx.borrow() == NodeStatus::KeySharing
                {
                    trace!("Node::handle - Keystore full, transitioning to Starting");
                    // Give the other nodes time to announce their public keys as well
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    self.status_tx.send(NodeStatus::Starting)?;
                }
                None
            }
        };

        trace!(
            "Node::handle - Handler complete, returning {} bytes",
            result.as_ref().map(|r| r.len()).unwrap_or(0)
        );
        Ok(result)
    }
}

#[async_trait]
impl<T, CM, A, F> SelfTerminating for Node<T, CM, A, F>
where
    T: Transport + 'static,
    CM: ConnectionManager<T> + 'static,
    A: Algorithm + AlgorithmHandler<F> + 'static,
    F: Format,
{
    async fn terminate(&self) {
        trace!("Node::terminate - Initiating termination");
        let _ = self.status_tx.send(NodeStatus::Stopping);
    }

    async fn terminated(&self) -> bool {
        let mut status_rx = self.status_rx.clone();

        let _ = status_rx
            .wait_for(|status| *status == NodeStatus::Terminated)
            .await;
        true
    }
}
