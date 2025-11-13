"""Echo algorithm implementation

A simple distributed algorithm where a start node sends messages to all
other nodes, which echo them back.
"""

import asyncio
import logging
from typing import Any

from distbench.algorithm import Algorithm
from distbench.community import PeerId
from distbench.decorators import config_field, distbench, handler, message

logger = logging.getLogger(__name__)


@message
class Message:
    """A message to be echoed by peers."""

    sender: str
    message: str


@distbench
class Echo(Algorithm):
    """Echo algorithm implementation.

    If start_node is True, this node initiates by sending messages to all peers.
    All nodes echo received messages back to the sender.
    """

    start_node: bool = config_field(default=False)

    def __init__(self, config: dict[str, object], peers: dict[PeerId, Any]) -> None:
        """Initialize Echo algorithm."""
        super().__init__()
        self.messages_received = 0

    async def on_start(self) -> None:
        """Called when algorithm starts.

        If this is the start node, send messages to all peers.
        """
        logger.info("Echo algorithm starting")

        if self.start_node:
            logger.info("This is the start node, sending messages to all peers")
            msg = Message(sender="Test", message="Hello, world!")

            for peer_id, peer in self.peers.items():
                try:
                    logger.info(f"Sending message to {peer_id}")
                    # Call the message handler on the peer
                    response = await peer.message(msg)
                    if response:
                        logger.info(f"Message echoed back: {response}")
                    else:
                        logger.error(f"Message not echoed by {peer_id}")

                except Exception as e:
                    logger.error(f"Error echoing message to {peer_id}: {e}")

        # Wait a bit for all messages to complete
        await asyncio.sleep(1.0)

        # Terminate the algorithm
        await self.terminate()

    async def on_exit(self) -> None:
        """Called when algorithm exits."""
        logger.info("Echo algorithm exiting")

    async def report(self) -> dict[str, str]:
        """Generate algorithm report.

        Returns:
            Dictionary with metrics
        """
        return {
            "messages_received": str(self.messages_received),
        }

    @handler
    async def message(self, src: PeerId, msg: Message) -> str | None:
        """Handle an incoming echo message.

        Args:
            src: Sender's peer ID
            msg: Message to echo

        Returns:
            String echoing the message content
        """
        logger.info(f"Received message from {src}: {msg.message}")
        self.messages_received += 1
        return msg.message
