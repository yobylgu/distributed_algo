//! Transport setup utilities.
//!
//! This module provides functions for setting up different transport types
//! (offline channels, TCP, etc.) based on configuration.

use distbench::community::{Community, PeerId};
use distbench::transport::channel::{ChannelTransport, ChannelTransportBuilder};
use distbench::transport::ConnectionManager;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::config::ConfigFile;

/// Sets up an offline (channel-based) transport for a node.
///
/// Creates a community with channel-based connections to all peers defined
/// in the configuration. Channel transport uses in-memory channels instead
/// of network sockets, making it ideal for testing and simulation.
///
/// # Arguments
///
/// * `config` - The complete configuration file
/// * `builder` - The channel transport builder (shared across all nodes)
/// * `node_id` - The ID of this node
/// * `neighbours` - List of neighbor IDs for this node
///
/// # Returns
///
/// A community configured with channel transport to all peers.
///
/// # Examples
///
/// ```ignore
/// use distbench::transport::channel::ChannelTransportBuilder;
/// use distbench::community::PeerId;
/// use distbench::transport_setup::setup_offline_transport;
///
/// let builder = ChannelTransportBuilder::new();
/// let node_id = PeerId::new("node1".to_string());
/// let community = setup_offline_transport(
///     &config,
///     &builder,
///     &node_id,
///     &vec!["node2".to_string(), "node3".to_string()],
/// );
/// ```
pub fn setup_offline_transport<CM: ConnectionManager<ChannelTransport>>(
    config: &ConfigFile,
    builder: &ChannelTransportBuilder,
    node_id: &PeerId,
    neighbours: &[String],
) -> Arc<Community<ChannelTransport, CM>> {
    let transport = builder.build(node_id.clone());

    // Create addresses for all peers (excluding self)
    let all_node_addrs: HashMap<PeerId, PeerId> = config
        .keys()
        .filter_map(|id_str| {
            let id = PeerId::new(id_str.clone());
            if id != *node_id {
                Some((id.clone(), id))
            } else {
                None
            }
        })
        .collect();

    // Convert neighbor strings to PeerIds (excluding self)
    let neighbour_ids: HashSet<PeerId> = neighbours
        .iter()
        .filter_map(|id| {
            let id = PeerId::new(id.clone());
            if id != *node_id {
                Some(id)
            } else {
                None
            }
        })
        .collect();

    let community = Community::new(neighbour_ids, all_node_addrs, transport);

    Arc::new(community)
}
