//! Node status tracking.
//!
//! This module defines the different states a node can be in during its lifecycle.

/// The current status of a node in the distributed system.
///
/// Nodes progress through these states during their lifecycle:
/// `NotStarted` → `Starting` → `Running` → `Stopping` → `Terminated`
#[derive(Clone, Default, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum NodeStatus {
    /// The node has not yet started.
    #[default]
    NotStarted = 0,

    /// Key sharing is in progress.
    KeySharing = 1,

    /// The node is in the process of starting up and synchronizing with peers.
    Starting = 2,

    /// The node is running and actively executing the algorithm.
    Running = 3,

    /// The node is in the process of shutting down.
    Stopping = 4,

    /// The node has completely terminated.
    Terminated = 5,
}
