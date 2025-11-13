"""Chang-Roberts Leader Election algorithm implementation."""

import asyncio
import logging
import random
from typing import Any, Tuple

from distbench.algorithm import Algorithm, Peer
from distbench.community import PeerId
from distbench.decorators import config_field, distbench, handler, message

logger = logging.getLogger(__name__)


@message
class ElectionMessage:
    elector: int


@message
class TerminationMessage:
    leader: int


@distbench
class ChangRoberts(Algorithm):
    """Implements Chang-Roberts leader election for a ring.

    This implementation constructs a logical ring by sorting all node IDs
    alphabetically. It requires a unique integer `node_id` in the config
    for comparisons.
    """

    node_id: int = config_field(required=True)

    def __init__(self, config: dict[str, object], peers: dict[PeerId, Any]) -> None:
        """Initialize Chang-Roberts state."""
        super().__init__()
        self.leader: int | None = None
        self._next_peer_tuple: Tuple[PeerId, Peer] | None = None

    def _get_next_peer_tuple(self) -> Tuple[PeerId, Peer]:
        """Finds the next peer in the logical (sorted-ID) ring."""
        if self._next_peer_tuple:
            return self._next_peer_tuple

        # Get all node IDs (peers + self) and sort them
        all_ids = [str(pid) for pid in self.peers.keys()] + [str(self.id())]
        all_ids.sort()

        # Find our index
        my_index = all_ids.index(str(self.id()))
        # Find next index, wrapping around
        next_index = (my_index + 1) % len(all_ids)

        # Get the PeerId and Peer object for the next node
        next_peer_id_str = all_ids[next_index]
        next_peer_id = PeerId(next_peer_id_str)
        next_peer = self.peers[next_peer_id]

        self._next_peer_tuple = (next_peer_id, next_peer)
        logger.debug(f"My next peer in the ring is {next_peer_id_str}")
        return self._next_peer_tuple

    async def on_start(self) -> None:
        # Determine our successor in the logical ring
        peer_id, peer = self._get_next_peer_tuple()

        # Wait a random time to avoid all nodes starting at once
        delay = random.uniform(1.0, 3.0)
        await asyncio.sleep(delay)

        if self.leader is not None:
            return

        logger.info(f"Starting election, sending my ID {self.node_id} to {peer_id}")
        try:
            await peer.election(ElectionMessage(elector=self.node_id))
        except Exception as e:
            logger.error(f"Error sending initial election message: {e}")

    async def report(self) -> dict[str, str]:
        return {
            "leader": str(self.leader) if self.leader is not None else "None",
            "node_id": str(self.node_id),
        }

    @handler
    async def election(self, src: PeerId, msg: ElectionMessage) -> None:
        (peer_id, next_peer) = self._get_next_peer_tuple()
        logger.info(f"Got election message from {src} with elector ID: {msg.elector}")

        if msg.elector == self.node_id:
            # We are the leader!
            self.leader = self.node_id
            logger.info(f"*** I AM ELECTED LEADER: {self.node_id} ***")
            logger.info("Sending termination message to ring...")
            try:
                await next_peer.termination(TerminationMessage(leader=self.node_id))
            except Exception as e:
                logger.error(f"Error sending termination message: {e}")
            await self.terminate()

        elif msg.elector > self.node_id:
            # Forward the message
            logger.info(f"Forwarding elector {msg.elector} to {peer_id}")
            try:
                await next_peer.election(msg)
            except Exception as e:
                logger.error(f"Error forwarding election message: {e}")
        else:
            # msg.elector < self.node_id
            # We replace the elector with our ID and forward
            logger.info(
                f"Replacing elector {msg.elector} with my ID {self.node_id} "
                f"and sending to {peer_id}"
            )
            try:
                await next_peer.election(ElectionMessage(elector=self.node_id))
            except Exception as e:
                logger.error(f"Error sending election message: {e}")

    @handler
    async def termination(self, src: PeerId, msg: TerminationMessage) -> None:
        if self.leader is not None:
            # We already know the leader
            return

        (_, next_peer) = self._get_next_peer_tuple()
        self.leader = msg.leader
        logger.info(f"Received termination. Leader is: {self.leader}")

        if self.leader != self.node_id:
            # Forward the termination message
            try:
                await next_peer.termination(msg)
            except Exception as e:
                logger.error(f"Error forwarding termination message: {e}")

        await self.terminate()
