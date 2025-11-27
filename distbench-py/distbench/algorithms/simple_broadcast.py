"""Simple Broadcast algorithm using layering.

This demonstrates the algorithm layering feature:
- SimpleBroadcast: A lower-layer algorithm that handles broadcasting
- SimpleBroadcastUpper: An upper-layer algorithm that uses the lower layer
"""

import asyncio
import logging

from distbench.algorithm import Algorithm
from distbench.community import PeerId
from distbench.decorators import (
    child_algorithm,
    config_field,
    distbench,
    handler,
    interface,
    message,
)

logger = logging.getLogger(__name__)


@message
class SimpleBroadcastMessage:
    """Message used internally by SimpleBroadcast."""

    payload: bytes


@distbench
class SimpleBroadcast(Algorithm):
    """Lower layer algorithm that handles broadcasting."""

    # Configuration
    max_retries: int = config_field(default=0)

    # State
    messages_broadcasted: int = 0

    @interface
    async def broadcast(self, msg: bytes) -> None:
        """Broadcast a message to all peers and deliver to parent."""
        logger.info(f"SimpleBroadcast: Broadcasting message to {len(self.peers)} peers")

        # Wrap the AlgorithmMessage in our own message type for peer-to-peer communication
        broadcast_msg = SimpleBroadcastMessage(payload=msg)

        tasks = []
        for peer_id, peer in self.peers.items():
            tasks.append(self._send_to_peer(peer_id, peer, broadcast_msg))

        # We don't wait for results, but we schedule them
        # In a real implementation we might want to track retries based on max_retries
        await asyncio.gather(*tasks, return_exceptions=True)

        self.messages_broadcasted += 1

        # Deliver to parent layer
        try:
            await self.deliver_message(self.id(), msg)
        except Exception as e:
            logger.info(f"SimpleBroadcast: Failed to deliver to parent: {e}")

    async def _send_to_peer(self, peer_id: PeerId, peer, msg: SimpleBroadcastMessage) -> None:
        try:
            await peer.simple_broadcast_message(msg)
            logger.info(f"SimpleBroadcast: Sent to peer {peer_id}")
        except Exception as e:
            logger.info(f"SimpleBroadcast: Error sending to peer {peer_id}: {e}")

    @handler
    async def simple_broadcast_message(self, src: PeerId, msg: SimpleBroadcastMessage) -> None:
        logger.info(f"SimpleBroadcast: Received message from {src} with {len(msg.payload)} bytes")

        # Extract the inner AlgorithmMessage and deliver to parent layer
        try:
            await self.deliver_message(src, msg.payload)
        except Exception as e:
            logger.info(f"SimpleBroadcast: Failed to deliver to parent: {e}")

    async def on_start(self) -> None:
        logger.info(f"SimpleBroadcast starting with max_retries: {self.max_retries}")

    async def on_exit(self) -> None:
        logger.info("SimpleBroadcast exiting")

    async def report(self) -> dict[str, str] | None:
        return {
            "messages_broadcasted": str(self.messages_broadcasted),
            "max_retries": str(self.max_retries),
        }


@message
class BroadcastSend:
    content: bytes
    sequence: int


@message
class BroadcastEcho:
    content: bytes
    original_sender: str


@distbench
class SimpleBroadcastUpper(Algorithm):
    """Upper layer algorithm that uses SimpleBroadcast."""

    # Configuration
    start_node: bool = config_field(default=False)
    messages: list[str] = config_field(default_factory=list)

    # State
    send_received: list[BroadcastSend] = config_field(default_factory=list)
    echo_received: list[BroadcastEcho] = config_field(default_factory=list)

    # Child algorithm
    broadcast: SimpleBroadcast = child_algorithm(SimpleBroadcast)

    async def on_start(self) -> None:
        logger.info(
            f"SimpleBroadcastUpper starting | start_node: {self.start_node}, messages: {self.messages}"
        )

        if self.start_node:
            logger.info("SimpleBroadcastUpper: Initiating broadcast through lower layer")

            # Broadcast BroadcastSend messages
            for i, message_str in enumerate(self.messages):
                send_msg = BroadcastSend(content=message_str.encode("utf-8"), sequence=i)
                try:
                    await self.broadcast.broadcast(send_msg)
                except Exception as e:
                    logger.info(f"SimpleBroadcastUpper: Error broadcasting send: {e}")

            logger.info("SimpleBroadcastUpper: Initiating broadcast through lower layer, echo")

            # Also broadcast a BroadcastEcho message
            echo_msg = BroadcastEcho(
                content=b"Echo from upper layer",
                original_sender=str(self.id()),
            )

            try:
                await self.broadcast.broadcast(echo_msg)
            except Exception as e:
                logger.info(f"SimpleBroadcastUpper: Error broadcasting echo: {e}")

    async def on_exit(self) -> None:
        logger.info("SimpleBroadcastUpper exiting")

    async def report(self) -> dict[str, str] | None:
        return {
            "send_received": str(len(self.send_received)),
            "echo_received": str(len(self.echo_received)),
            "start_node": str(self.start_node),
        }

    @handler(from_child="broadcast")
    async def broadcast_send(self, src: PeerId, msg: BroadcastSend) -> None:
        logger.info(
            f"SimpleBroadcastUpper: Received BroadcastSend from {src} | sequence: {msg.sequence}, {len(msg.content)} bytes"
        )
        self.send_received.append(msg)

    @handler(from_child="broadcast")
    async def broadcast_echo(self, src: PeerId, msg: BroadcastEcho) -> None:
        logger.info(
            f"SimpleBroadcastUpper: Received BroadcastEcho from {src} | original_sender: {msg.original_sender}, {len(msg.content)} bytes"
        )
        self.echo_received.append(msg)
