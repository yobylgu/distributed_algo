"""Dolev's algorithm for Byzantine fault-tolerant reliable broadcast.

Implements reliable communication in (2f+1)-connected networks despite up to f
Byzantine processes. Messages carry the path they've traversed, and nodes deliver
messages only after receiving them through f+1 node-disjoint paths.

Reference: Dolev, D. (1982). "The Byzantine Generals Strike Again"

Algorithm 1 from the homework:
1. upon event <Dolev, Init> do
2.   delivered = False
3.   paths = ∅
4. upon event <Dolev, Broadcast | m> do
5.   forall p_j ∈ neighbors(p_i) do
6.     trigger <al, Send | p_j, [m, []]>
7.   delivered = True
8.   trigger <Dolev, Deliver | m>
9. upon event <al, Deliver | p_j, [m, path]> do
10.   paths.insert(path + [p_j])
11.   forall p_k ∈ neighbors(p_i) \ (path ∪ {p_j}) do
12.     trigger <al, Send | p_k, [m, path + [p_j]]>
13. upon event (p_i is connected to source through f+1 node-disjoint paths) and (delivered = False) do
14.   trigger <Dolev, Deliver | m>
15.   delivered = True
"""

import asyncio
import logging
import random
from typing import Any

from distbench.algorithm import Algorithm
from distbench.community import PeerId
from distbench.decorators import config_field, distbench, handler, message

logger = logging.getLogger(__name__)


@message
class DolevMessage:
    """Message with traversal path for Dolev's protocol.

    Attributes:
        content: The actual message payload
        path: List of node IDs that this message has traversed
    """
    content: str
    path: list[str]


def find_node_disjoint_paths(
    paths: list[list[str]],
    source: str,
    target: str,
    min_paths: int
) -> list[list[str]]:
    """Find node-disjoint paths from source to target.

    Uses a greedy approach that is robust against Byzantine manipulation:
    - Paths must start with source and end with target
    - Two paths are node-disjoint if they share no intermediate nodes
    - Returns up to min_paths disjoint paths

    Args:
        paths: List of all collected paths
        source: Source node ID
        target: Target node ID (receiver)
        min_paths: Minimum number of disjoint paths needed

    Returns:
        List of node-disjoint paths (may be fewer than min_paths if not available)
    """
    # Filter valid paths: must contain source and end with target
    valid_paths = []
    for path in paths:
        if len(path) >= 2 and source in path and path[-1] == target:
            # Extract the path segment from source to target
            try:
                src_idx = path.index(source)
                # Create path from source to target
                segment = path[src_idx:]
                valid_paths.append(segment)
            except ValueError:
                continue

    if not valid_paths:
        logger.trace(f"No valid paths from {source} to {target}")
        return []

    # Greedy algorithm to find node-disjoint paths
    disjoint_paths: list[list[str]] = []
    used_nodes: set[str] = {source, target}  # Source and target can be shared

    # Sort by path length (prefer shorter paths)
    valid_paths.sort(key=len)

    for path in valid_paths:
        # Check if this path shares intermediate nodes with already selected paths
        intermediate_nodes = set(path[1:-1])  # Exclude source and target

        if not intermediate_nodes.intersection(used_nodes - {source, target}):
            # This path is disjoint with all previously selected paths
            disjoint_paths.append(path)
            used_nodes.update(intermediate_nodes)

            if len(disjoint_paths) >= min_paths:
                break

    logger.trace(f"Found {len(disjoint_paths)} node-disjoint paths from {source} to {target}")
    return disjoint_paths


@distbench
class Dolev(Algorithm):
    """Dolev's algorithm for Byzantine fault-tolerant reliable broadcast.

    Provides reliable communication despite f Byzantine processes if the network
    is at least (2f+1)-connected. Uses authenticated channels and path tracking
    to ensure messages are received through multiple node-disjoint paths.
    """

    # Configuration parameters
    f: int = config_field(required=True)  # Max number of Byzantine processes
    is_source: bool = config_field(default=False)  # Is this the broadcast source?
    broadcast_content: str = config_field(default="Hello, Byzantine world!")
    is_byzantine: bool = config_field(default=False)  # Is this a Byzantine (faulty) node?
    byzantine_behavior: str = config_field(default="drop")  # Type: drop, corrupt, delay

    def __init__(self, config: dict[str, object], peers: dict[PeerId, Any]) -> None:
        """Initialize Dolev's algorithm state."""
        super().__init__()
        self.delivered = False
        self.paths: list[list[str]] = []
        self.message_content: str | None = None
        # Track which messages we've already forwarded to prevent loops
        self.forwarded_paths: set[tuple[str, ...]] = set()

    async def on_start(self) -> None:
        """Initialize the algorithm.

        If this is the source node, broadcasts the message to all neighbors
        with an empty path and immediately delivers it locally.
        """
        logger.info(f"Dolev's algorithm starting (f={self.f}, N={self.N()})")
        logger.info(f"My neighbors: {list(self.peers.keys())}")

        if self.is_byzantine:
            logger.warning(f"[BYZANTINE NODE] Behavior: {self.byzantine_behavior}")

        if self.is_source:
            logger.info(f"I am the source, broadcasting: '{self.broadcast_content}'")
            self.message_content = self.broadcast_content

            # Broadcast to all neighbors with empty path
            msg = DolevMessage(content=self.broadcast_content, path=[])

            for peer_id, peer in self.peers.items():
                try:
                    logger.info(f"Sending initial message to {peer_id}")
                    await peer.receive(msg)
                except Exception as e:
                    logger.error(f"Error sending to {peer_id}: {e}")

            # Source delivers immediately
            self.delivered = True
            logger.info("✓ Source delivered message immediately")

            # Source can terminate after broadcasting
            await asyncio.sleep(2.0)  # Give others time to process
            await self.terminate()

    async def on_exit(self) -> None:
        """Cleanup on algorithm exit."""
        logger.info("Dolev's algorithm exiting")

    async def report(self) -> dict[str, str]:
        """Report the algorithm results.

        Returns:
            Dictionary with delivery status, number of paths, and message content
        """
        return {
            "delivered": str(self.delivered),
            "paths_collected": str(len(self.paths)),
            "is_source": str(self.is_source),
            "is_byzantine": str(self.is_byzantine),
            "message": self.message_content or "not delivered",
        }

    @handler
    async def receive(self, src: PeerId, msg: DolevMessage) -> None:
        """Handle incoming message from a neighbor.

        Steps:
        1. Store path + [sender] in collected paths
        2. Forward to all neighbors except those in (path ∪ {sender})
        3. Check if we have f+1 node-disjoint paths and deliver if so

        Args:
            src: The neighbor that sent this message
            msg: The Dolev message with content and path
        """
        # Byzantine behavior: probabilistically drop, corrupt, or delay messages
        if self.is_byzantine:
            if self.byzantine_behavior == "drop":
                if random.random() < 0.5:  # Drop 50% of messages
                    logger.warning(f"[BYZANTINE] Dropping message from {src}")
                    return
            elif self.byzantine_behavior == "corrupt":
                if random.random() < 0.3:  # Corrupt 30% of paths
                    logger.warning(f"[BYZANTINE] Corrupting path from {src}")
                    # Add fake nodes to the path
                    msg.path.append("FAKE_NODE")
            elif self.byzantine_behavior == "delay":
                delay = random.uniform(0.5, 2.0)
                logger.warning(f"[BYZANTINE] Delaying message from {src} by {delay:.2f}s")
                await asyncio.sleep(delay)

        # Create new path with sender appended: path + [sender]
        new_path = msg.path + [str(src)]

        logger.info(f"Received message from {src}, path length: {len(new_path)}")
        logger.debug(f"Path: {' -> '.join(new_path)} -> {self.id()}")

        # Check for duplicate/already forwarded path (deduplication for efficiency)
        path_tuple = tuple(new_path)
        if path_tuple in self.forwarded_paths:
            logger.trace("Already processed this path, ignoring")
            return
        self.forwarded_paths.add(path_tuple)

        # Store the path up to the sender: path + [sender]
        self.paths.append(new_path)

        # Store message content if we don't have it yet
        if self.message_content is None:
            self.message_content = msg.content

        # Forward to neighbors not in (path ∪ {sender})
        nodes_in_path = set(new_path)

        for peer_id, peer in self.peers.items():
            if str(peer_id) not in nodes_in_path:
                try:
                    # Forward with the extended path
                    forward_msg = DolevMessage(content=msg.content, path=new_path)
                    logger.debug(f"Forwarding to {peer_id}")
                    await peer.receive(forward_msg)
                except Exception as e:
                    logger.error(f"Error forwarding to {peer_id}: {e}")

        # Check delivery condition
        if not self.delivered:
            await self._check_delivery()

    async def _check_delivery(self) -> None:
        """Check if we have enough node-disjoint paths to deliver the message.

        Attempts to find f+1 node-disjoint paths from the source to this node.
        If successful, delivers the message exactly once.
        """
        if self.delivered or self.message_content is None:
            return

        # We need to identify the source from the paths
        # The source is the first node in the paths
        if not self.paths:
            return

        # Find the source (should be consistent across all paths)
        potential_sources = set()
        for path in self.paths:
            if len(path) >= 1:
                potential_sources.add(path[0])

        if len(potential_sources) != 1:
            logger.warning(f"Multiple potential sources detected: {potential_sources}")
            # In a real implementation, we'd need more sophisticated source identification
            # For now, take the most common one
            if potential_sources:
                source = max(potential_sources, key=lambda s: sum(1 for p in self.paths if p[0] == s))
            else:
                return
        else:
            source = potential_sources.pop()

        # Paths are stored as path + [sender], so we need to append ourselves
        # to get complete paths for disjointness checking
        full_paths = [p + [str(self.id())] for p in self.paths]

        # Find node-disjoint paths
        required_paths = self.f + 1
        disjoint_paths = find_node_disjoint_paths(
            full_paths,
            source,
            str(self.id()),
            required_paths
        )

        logger.debug(f"Found {len(disjoint_paths)}/{required_paths} disjoint paths")

        if len(disjoint_paths) >= required_paths:
            self.delivered = True
            logger.info(f"✓ DELIVERED message: '{self.message_content}'")
            logger.info(f"  via {len(disjoint_paths)} node-disjoint paths from {source}")

            for i, path in enumerate(disjoint_paths):
                logger.debug(f"  Path {i+1}: {' -> '.join(path)}")

            # Wait a bit before terminating to let others finish
            await asyncio.sleep(1.0)
            await self.terminate()
