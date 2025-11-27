"""Local/direct transport for offline mode.

This module provides a simplified transport where nodes communicate
directly by calling each other's handlers within the same process.
"""

import asyncio
import logging
from dataclasses import dataclass

from distbench.context import current_node_id
from distbench.transport.base import Connection, Server, Transport

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LocalAddress:
    """Address for local/direct communication."""

    node_id: str

    def __hash__(self) -> int:
        return hash(self.node_id)

    def __str__(self) -> str:
        return self.node_id


class LocalConnection(Connection):
    """Direct connection that calls the server handler directly."""

    def __init__(
        self, server: Server, target_address: LocalAddress, source_address: LocalAddress
    ) -> None:
        """Initialize a local connection.

        Args:
            server: The target server to call directly
            target_address: Address of the target server
            source_address: Address of the sender (this node)
        """
        self.server = server
        self.target_address = target_address
        self.source_address = source_address

    async def send(self, msg: bytes) -> bytes:
        """Send message by calling server handler directly.

        Args:
            msg: Message bytes to send

        Returns:
            Response bytes from the server
        """
        # Pass the source address so the server knows who sent the message
        token = current_node_id.set(str(self.target_address.node_id))
        response = await self.server.handle(self.source_address, msg)
        current_node_id.reset(token)

        return response if response is not None else b""

    async def cast(self, msg: bytes) -> None:
        """Send message without waiting for response.

        Args:
            msg: Message bytes to send
        """

        # For cast, just call the handler and don't wait
        async def spawn_handle() -> None:
            token = current_node_id.set(str(self.target_address.node_id))
            try:
                await self.server.handle(self.source_address, msg)
            finally:
                current_node_id.reset(token)

        asyncio.create_task(spawn_handle())

    async def close(self) -> None:
        """Close the connection (no-op for local connections)."""
        pass


class LocalTransport(Transport):
    """Local transport where nodes call each other directly.

    This is the simplest possible transport for offline mode,
    where all nodes run in the same process and can call each
    other's handlers directly.
    """

    def __init__(self) -> None:
        """Initialize the local transport."""
        # Registry of servers by node ID
        self.servers: dict[str, Server] = {}
        self.lock = asyncio.Lock()
        # Track which node is currently using this transport for connect()
        self.current_node_id: str | None = None

    def register_server(self, node_id: str, server: Server) -> None:
        """Register a server for a specific node ID.

        Args:
            node_id: The node's identifier
            server: The server instance
        """
        self.servers[node_id] = server
        logger.trace(f"Registered server for node {node_id}")

    def set_current_node(self, node_id: str) -> None:
        """Set the current node for connection tracking.

        Args:
            node_id: ID of the node making connections
        """
        self.current_node_id = node_id

    async def connect(self, addr: LocalAddress) -> LocalConnection:
        """Create a direct connection to another node.

        Args:
            addr: Target node's local address

        Returns:
            A local connection to the target node

        Raises:
            ConnectionError: If the target node doesn't exist
        """
        if addr.node_id not in self.servers:
            raise ConnectionError(f"No server registered for node {addr.node_id}")

        if self.current_node_id is None:
            raise ConnectionError("Current node ID not set for LocalTransport")

        server = self.servers[addr.node_id]
        source_addr = LocalAddress(self.current_node_id)
        return LocalConnection(server, addr, source_addr)

    async def serve(self, server: Server, stop_event: asyncio.Event) -> None:
        """Start serving (just wait for stop signal in local mode).

        Args:
            server: Server instance (not used in local mode)
            stop_event: Event that signals when to stop serving
        """
        # In local mode, we don't actually need to do anything except wait
        # because all communication is direct method calls
        await stop_event.wait()
        logger.trace("Local transport stopped serving")
