"""MessageChain algorithm implementation.

Demonstrates forwarding and signing messages along a path.
"""

import asyncio
import logging
from typing import Any, List

from distbench.algorithm import Algorithm
from distbench.community import PeerId
from distbench.decorators import config_field, distbench, handler, message
from distbench.signing import Signed

logger = logging.getLogger(__name__)


@message
class ChainMessage:
    """A message that contains a payload and can be signed."""

    hop_count: int
    original_value: str
    node_name: str  # The node that created this specific hop

    def __str__(self) -> str:
        return f"Hop #{self.hop_count} by {self.node_name} " f"(value: '{self.original_value}')"


@message
class ChainBundle:
    """A container message that holds a vector of signed messages."""

    chain: List[Signed[ChainMessage]]


@distbench
class MessageChain(Algorithm):
    """
    An algorithm where nodes forward a chain of signed messages,
    adding their own signed message at each hop.
    """

    is_initiator: bool = config_field(default=False)
    initial_value: str = config_field(default="Chain start!")
    max_hops: int = config_field(default=5)

    def __init__(self, config: dict[str, object], peers: dict[PeerId, Any]) -> None:
        """Initialize MessageChain state."""
        super().__init__()
        # Store the paths of nodes (as PeerIds)
        self.received_chains: list[list[PeerId]] = []
        # Track processed chains (by node name path) for deduplication
        # A tuple is hashable and can be used in a set.
        self.processed_chains: set[tuple[str, ...]] = set()

    async def on_start(self) -> None:
        logger.info(f"MessageChain starting (N={self.N()} nodes)")

        if self.is_initiator:
            logger.info(f"I am the initiator, starting chain with: '{self.initial_value}'")

            # Create initial signed message
            msg = self.sign(
                ChainMessage(
                    hop_count=0,
                    original_value=self.initial_value,
                    node_name=str(self.id()),
                )
            )

            # The Signed wrapper's __str__ will be used here
            logger.info(f"Created initial message: {msg}")

            # Send to all peers
            bundle = ChainBundle(chain=[msg])
            for peer_id, peer in self.peers.items():
                try:
                    await peer.receive_chain(bundle)
                except Exception as e:
                    logger.warning(f"Error sending init chain to {peer_id}: {e}")

            # Initiator terminates after starting the chain
            logger.info("Initiator finished sending, terminating")
            await self.terminate()

    async def on_exit(self) -> None:
        logger.info("MessageChain exiting")

    async def report(self) -> dict[str, str]:
        return {
            "chains_received": str(len(self.received_chains)),
            "is_initiator": str(self.is_initiator),
        }

    @handler
    async def receive_chain(self, src: PeerId, bundle: ChainBundle) -> None:
        if not bundle.chain:
            logger.warning(f"Received empty chain from {src}")
            return

        logger.info(f"Received chain from {src} with {len(bundle.chain)} messages")

        # Deduplication: Check if we've already processed this exact chain path
        # We use msg.node_name (the inner value) for the ID
        chain_id = tuple(msg.node_name for msg in bundle.chain)
        if chain_id in self.processed_chains:
            logger.trace("Already processed this chain, ignoring")
            return
        self.processed_chains.add(chain_id)

        # Log the received chain
        logger.debug("Chain messages:")
        for i, signed_msg in enumerate(bundle.chain):
            # signed_msg.__str__ will call ChainMessage.__str__ and add signer
            logger.debug(f"  [{i}] {signed_msg}")

        # Get info from messages
        first_msg = bundle.chain[0]
        last_msg = bundle.chain[-1]

        # Store node names from the chain
        node_path = [PeerId(msg.node_name) for msg in bundle.chain]
        self.received_chains.append(node_path)

        # Create our signed message to extend the chain
        our_msg = self.sign(
            ChainMessage(
                hop_count=last_msg.hop_count + 1,
                original_value=first_msg.original_value,
                node_name=str(self.id()),
            )
        )

        # Create new chain with all previous messages plus ours
        new_chain = bundle.chain + [our_msg]
        logger.info(f"Forwarding extended chain ({len(new_chain)} total messages)")

        # Forward to all peers (except the one we got it from)
        for peer_id, peer in self.peers.items():
            if peer_id == src:
                continue
            try:
                await peer.receive_chain(ChainBundle(chain=new_chain))
            except Exception as e:
                logger.error(f"Error forwarding to {peer_id}: {e}")

        # Check for termination
        if our_msg.hop_count >= self.max_hops:
            logger.info(f"Forwarded chain to max hops ({self.max_hops}), terminating")
            await self.terminate()
