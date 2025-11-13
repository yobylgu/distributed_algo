//! Community management for distributed systems.
//!
//! This module defines the [`Community`] type which represents a collection of
//! peers that communicate with each other in a distributed system.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dashmap::DashMap;
use log::trace;
use serde::{Deserialize, Serialize};

use crate::{crypto::PublicKey, status::NodeStatus, transport};

/// Unique identifier for a peer in the distributed system.
///
/// `PeerId` is used to identify nodes throughout the system. It wraps a string
/// identifier and implements the necessary traits for use as a key in collections
/// and as a transport address.
#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct PeerId(String);

impl PeerId {
    /// Creates a new peer ID from a string.
    ///
    /// # Arguments
    ///
    /// * `id` - The string identifier for this peer
    pub fn new(id: String) -> Self {
        Self(id)
    }

    /// Converts this peer ID to a string.
    ///
    /// # Returns
    ///
    /// A clone of the underlying string identifier.
    pub fn to_string(&self) -> String {
        self.0.clone()
    }
}

/// A community of peers that communicate with each other.
///
/// The `Community` manages:
/// - The set of neighbor peers for a given node
/// - Connection managers for communicating with each peer
/// - Address-to-PeerId mappings
/// - Status tracking for all peers
/// - Public keys for peers (if applicable)
///
/// # Type Parameters
///
/// * `T` - The transport layer implementation
pub struct Community<T: transport::Transport, CM: transport::ConnectionManager<T>> {
    neighbours: HashSet<PeerId>,
    connections: HashMap<PeerId, Arc<CM>>,
    peers: HashMap<T::Address, PeerId>,
    transport: Arc<T>,
    peer_status: DashMap<PeerId, NodeStatus>,
    keys: Arc<DashMap<PeerId, PublicKey>>,
}

impl<T: transport::Transport + 'static, CM: transport::ConnectionManager<T>> Community<T, CM> {
    /// Creates a new community.
    ///
    /// # Arguments
    ///
    /// * `neighbours` - The set of peer IDs that are considered neighbors
    /// * `peer_addresses` - A mapping from peer IDs to their network addresses
    /// * `transport` - The transport layer to use for communication
    ///
    /// # Returns
    ///
    /// A new `Community` instance configured with the provided peers and transport.
    pub fn new(
        neighbours: HashSet<PeerId>,
        peer_addresses: HashMap<PeerId, T::Address>,
        transport: Arc<T>,
    ) -> Self {
        trace!(
            "Community::new - Creating community with {} neighbours, {} total peers",
            neighbours.len(),
            peer_addresses.len()
        );

        let connections: HashMap<PeerId, Arc<CM>> = peer_addresses
            .iter()
            .map(|(id, addr)| {
                trace!(
                    "Community::new - Creating connection manager for peer {} at {}",
                    id.to_string(),
                    addr
                );
                let conn_manager: Arc<CM> = Arc::new(CM::new(transport.clone(), addr.clone()));
                (id.clone(), conn_manager)
            })
            .collect();

        let peers = peer_addresses
            .iter()
            .map(|(id, addr)| (addr.clone(), id.clone()))
            .collect();

        trace!("Community::new - Community initialized");
        Self {
            neighbours,
            connections,
            peers,
            transport,
            keys: Default::default(),
            peer_status: Default::default(),
        }
    }

    /// Sets the status of a peer.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the peer whose status to update
    /// * `status` - The new status for the peer
    pub fn set_status(&self, id: PeerId, status: NodeStatus) {
        trace!(
            "Community::set_status - Setting peer {} status to {:?}",
            id.to_string(),
            status
        );
        self.peer_status.insert(id, status);
    }

    /// Returns the current status of all known peers.
    ///
    /// # Returns
    ///
    /// A map from peer IDs to their current status. Peers with no recorded
    /// status will have the default status.
    pub fn statuses(&self) -> HashMap<PeerId, NodeStatus> {
        self.peer_status
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Returns an iterator over connection managers for all peers.
    ///
    /// # Returns
    ///
    /// An iterator yielding connection managers for each peer in the community.
    pub fn all_peers(&self) -> impl Iterator<Item = Arc<CM>> + '_ {
        self.connections.values().cloned()
    }

    /// Looks up the peer ID for a given address.
    ///
    /// # Arguments
    ///
    /// * `addr` - The network address to look up
    ///
    /// # Returns
    ///
    /// `Some(peer_id)` if the address is known, `None` otherwise.
    pub fn id_of(&self, addr: &T::Address) -> Option<PeerId> {
        self.peers.get(addr).cloned()
    }

    /// Returns the connection manager for a specific peer.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the peer to get the connection for
    ///
    /// # Returns
    ///
    /// `Some(connection_manager)` if the peer is known, `None` otherwise.
    pub fn connection(&self, id: &PeerId) -> Option<Arc<CM>> {
        self.connections.get(id).cloned()
    }

    /// Check if we have all the public keys for all peers.
    ///
    /// # Returns
    ///
    /// `true` if we have all the public keys for all peers, `false` otherwise.
    pub fn keystore_full(&self) -> bool {
        self.peers.values().all(|id| self.keys.contains_key(id))
    }

    /// Checks if a peer is a neighbour.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the peer to check
    ///
    /// # Returns
    ///
    /// `true` if the peer is a neighbour, `false` otherwise.
    pub fn is_neighbour(&self, id: &PeerId) -> bool {
        self.neighbours.contains(id)
    }

    /// Returns connection managers for all neighbor peers.
    ///
    /// # Returns
    ///
    /// A map from neighbor peer IDs to their connection managers.
    pub fn neighbours(&self) -> HashMap<PeerId, Arc<CM>> {
        self.neighbours
            .iter()
            .filter_map(|id| self.connections.get(id).map(|cm| (id.clone(), cm.clone())))
            .collect()
    }

    /// Returns the transport layer used by this community.
    ///
    /// # Returns
    ///
    /// An `Arc` pointing to the transport instance.
    pub fn transport(&self) -> Arc<T> {
        self.transport.clone()
    }

    /// Sets the public key for a peer.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the peer whose public key to set
    /// * `key` - The public key to set for the peer
    pub fn set_pubkey(&self, id: PeerId, key: PublicKey) {
        trace!(
            "Community::set_pubkey - Setting public key for peer {}",
            id.to_string()
        );
        self.keys.insert(id, key);
        trace!(
            "Community::set_pubkey - Keystore now has {}/{} keys",
            self.keys.len(),
            self.peers.len()
        );
    }

    pub fn keystore(&self) -> KeyStore {
        KeyStore {
            keys: self.keys.clone(),
        }
    }

    /// Returns the number of nodes in the community.
    pub fn size(&self) -> usize {
        self.peers.len()
    }
}

#[derive(Debug)]
pub struct KeyStore {
    keys: Arc<DashMap<PeerId, PublicKey>>,
}

impl KeyStore {
    pub fn get(
        &self,
        id: &PeerId,
    ) -> std::option::Option<dashmap::mapref::one::Ref<'_, PeerId, PublicKey>> {
        self.keys.get(id)
    }
}
