"""Community layer for managing peer relationships.

This module provides the Community class which manages:
- Peer relationships and neighbor sets
- Address-to-peer-ID mapping
- Peer status tracking (lifecycle states)
- Connection managers for each peer
- Cryptographic public key storage
"""

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

from distbench.connection import ConnectionManager
from distbench.transport.base import Address, Transport

logger = logging.getLogger(__name__)


class NodeStatus(Enum):
    """Node lifecycle states.

    A node progresses through these states during its lifetime:
    - NOT_STARTED: Initial state before any operations
    - KEY_SHARING: Exchanging cryptographic public keys with peers
    - STARTING: Synchronizing startup with other nodes
    - RUNNING: Algorithm is actively executing
    - STOPPING: Coordinating graceful shutdown
    - TERMINATED: Node has completely stopped
    """

    NOT_STARTED = "not_started"
    KEY_SHARING = "key_sharing"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    TERMINATED = "terminated"


@dataclass(frozen=True)
class PeerId:
    """Unique identifier for a peer in the distributed system.

    Peer IDs are immutable and hashable for use in sets and dictionaries.
    """

    id: str

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return self.id


T = TypeVar("T", bound=Transport)
A = TypeVar("A", bound=Address)


class Community(Generic[T, A]):
    """Manages peer relationships and communication in a distributed system.

    The Community class is responsible for:
    - Tracking which peers are neighbors (direct communication)
    - Managing connection managers for each peer
    - Mapping network addresses to peer identities
    - Tracking lifecycle status of all peers
    - Storing cryptographic public keys for peers
    """

    def __init__(
        self,
        neighbours: set[PeerId],
        peer_addresses: dict[PeerId, A],
        transport: T,
    ) -> None:
        """Initialize a community with peer information.

        Args:
            neighbours: Set of peer IDs that are direct neighbors
            peer_addresses: Mapping from peer IDs to their network addresses
            transport: Transport instance for creating connections
        """
        self.neighbours = neighbours
        self.transport = transport

        # Create connection managers for all peers
        self.connections: dict[PeerId, ConnectionManager[T, A]] = {
            peer_id: ConnectionManager(transport, addr) for peer_id, addr in peer_addresses.items()
        }

        # Reverse mapping: address -> peer ID (for identifying incoming messages)
        self.peers: dict[A, PeerId] = {addr: peer_id for peer_id, addr in peer_addresses.items()}

        # Status tracking with thread-safe access
        self._peer_status: dict[PeerId, NodeStatus] = {}
        self._status_lock = asyncio.Lock()

        # Cryptographic key storage
        self._keys: dict[PeerId, bytes] = {}
        self._keystore_lock = asyncio.Lock()

        logger.trace(
            f"Community initialized with {len(self.connections)} peers, "
            f"{len(self.neighbours)} neighbors"
        )

    async def set_status(self, peer_id: PeerId, status: NodeStatus) -> None:
        """Update the status of a peer.

        Args:
            peer_id: ID of the peer to update
            status: New status for the peer
        """
        async with self._status_lock:
            old_status = self._peer_status.get(peer_id, NodeStatus.NOT_STARTED)
            self._peer_status[peer_id] = status
            if old_status != status:
                logger.trace(f"Peer {peer_id} status: {old_status.value} -> {status.value}")

    async def get_status(self, peer_id: PeerId) -> NodeStatus:
        """Get the current status of a peer.

        Args:
            peer_id: ID of the peer to query

        Returns:
            Current status of the peer (NOT_STARTED if unknown)
        """
        async with self._status_lock:
            return self._peer_status.get(peer_id, NodeStatus.NOT_STARTED)

    async def statuses(self) -> dict[PeerId, NodeStatus]:
        """Get the current status of all known peers.

        Returns:
            Dictionary mapping peer IDs to their current status
        """
        async with self._status_lock:
            return {
                peer_id: self._peer_status.get(peer_id, NodeStatus.NOT_STARTED)
                for peer_id in self.peers.values()
            }

    def id_of(self, addr: A) -> PeerId | None:
        """Look up a peer ID by its network address.

        This is used to identify the sender of incoming messages.

        Args:
            addr: Network address to look up

        Returns:
            Peer ID associated with the address, or None if not found
        """
        return self.peers.get(addr)

    def connection(self, peer_id: PeerId) -> ConnectionManager[T, A] | None:
        """Get the connection manager for a specific peer.

        Args:
            peer_id: ID of the peer

        Returns:
            Connection manager for the peer, or None if peer is unknown
        """
        return self.connections.get(peer_id)

    def get_neighbours(self) -> dict[PeerId, ConnectionManager[T, A]]:
        """Get connection managers for all neighbor peers.

        Returns:
            Dictionary mapping neighbor peer IDs to their connection managers
        """
        return {
            peer_id: self.connections[peer_id]
            for peer_id in self.neighbours
            if peer_id in self.connections
        }

    async def add_key(self, peer_id: PeerId, public_key: bytes) -> None:
        """Store a peer's cryptographic public key.

        Args:
            peer_id: ID of the peer
            public_key: Ed25519 public key bytes (32 bytes)
        """
        async with self._keystore_lock:
            self._keys[peer_id] = public_key
            logger.trace(f"Stored public key for peer {peer_id}")

    def get_key(self, peer_id: PeerId) -> bytes | None:
        """Retrieve a peer's cryptographic public key.

        Args:
            peer_id: ID of the peer

        Returns:
            Public key bytes, or None if not found
        """
        return self._keys.get(peer_id)

    async def has_all_keys(self) -> bool:
        """Check if we have public keys for all known peers.

        Returns:
            True if all peers have provided their public keys
        """
        async with self._keystore_lock:
            return all(peer_id in self._keys for peer_id in self.peers.values())

    async def close_all(self) -> None:
        """Close all connection managers.

        This should be called during shutdown to release resources.
        """
        logger.trace("Closing all connections in community")
        for conn_manager in self.connections.values():
            try:
                await conn_manager.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
