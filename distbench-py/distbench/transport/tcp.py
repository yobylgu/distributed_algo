"""TCP transport for network mode.

This module provides a TCP-based transport implementation for real
network communication between distributed nodes.

It uses logical numeric IDs (TcpAddress) for peer identification,
which are mapped to physical socket addresses (SocketAddress).
"""

import asyncio
import contextlib
import logging
import struct
from dataclasses import dataclass
from typing import cast

from distbench.transport.base import Address, Connection, Server, Transport

logger = logging.getLogger(__name__)

# Special marker for cast messages (no response expected)
CAST_MARKER = 0xFFFFFFFF


@dataclass(frozen=True)
class SocketAddress:
    """Represents a physical host:port tuple."""

    host: str
    port: int

    def __hash__(self) -> int:
        return hash((self.host, self.port))

    def __str__(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class TcpAddress:
    """Logical TCP address (a stable numeric ID)."""

    id: int  # This is a u16 in the Rust implementation

    def __hash__(self) -> int:
        return hash(self.id)

    def __str__(self) -> str:
        return f"TcpAddr({self.id})"


class TcpConnection(Connection):
    """TCP socket connection for network communication.

    Uses length-prefixed message framing:
    - For send(): [4-byte length][message bytes] -> wait for [4-byte length][response bytes]
    - For cast(): [CAST_MARKER][4-byte length][message bytes] -> no response
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Initialize a TCP connection.

        Args:
            reader: Async stream reader for receiving data
            writer: Async stream writer for sending data
        """
        self.reader = reader
        self.writer = writer
        self.lock = asyncio.Lock()
        self.closed = False

    async def send(self, msg: bytes) -> bytes:
        """Send message and wait for response.

        Args:
            msg: Message bytes to send

        Returns:
            Response bytes from the remote node

        Raises:
            ConnectionError: If the connection is closed or fails
        """
        async with self.lock:
            if self.closed:
                raise ConnectionError("Connection is closed")

            try:
                # Write length prefix + message
                length = struct.pack("!I", len(msg))
                self.writer.write(length + msg)
                await self.writer.drain()

                # Read response length
                resp_length_bytes = await self.reader.readexactly(4)
                resp_length = struct.unpack("!I", resp_length_bytes)[0]

                # Read response
                if resp_length > 0:
                    response = await self.reader.readexactly(resp_length)
                    return response
                return b""  # Empty response

            except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError) as e:
                self.closed = True
                await self.close()
                raise ConnectionError(f"Connection failed: {e}") from e

    async def cast(self, msg: bytes) -> None:
        """Send message without waiting for response.

        Args:
            msg: Message bytes to send

        Raises:
            ConnectionError: If the connection is closed or fails
        """
        async with self.lock:
            if self.closed:
                raise ConnectionError("Connection is closed")

            try:
                # Write cast marker + length + message
                marker = struct.pack("!I", CAST_MARKER)
                length = struct.pack("!I", len(msg))
                self.writer.write(marker + length + msg)
                await self.writer.drain()

            except (ConnectionResetError, BrokenPipeError) as e:
                self.closed = True
                await self.close()
                raise ConnectionError(f"Connection failed: {e}") from e

    async def close(self) -> None:
        """Close the TCP connection."""
        if not self.closed:
            self.closed = True
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                pass  # Ignore errors during close


class TcpTransport(Transport):
    """TCP-based transport for network communication between distributed nodes."""

    def __init__(
        self,
        local_numeric_id: TcpAddress,
        all_peer_sockets: dict[TcpAddress, SocketAddress],
    ) -> None:
        """Initialize the TCP transport.

        Args:
            local_numeric_id: This node's logical numeric ID.
            all_peer_sockets: Map from logical numeric IDs to physical host/port.
        """
        self.local_numeric_id = local_numeric_id
        self.all_peer_sockets = all_peer_sockets
        self.bind_addr = self.all_peer_sockets[self.local_numeric_id]
        self.server: asyncio.Server | None = None
        self.server_task: asyncio.Task[None] | None = None

    async def connect(self, addr: Address) -> TcpConnection:
        """Establish a TCP connection to a remote node.

        Args:
            addr: Target node's logical numeric ID (as a TcpAddress).

        Returns:
            An active TCP connection to the remote node.

        Raises:
            ConnectionError: If connection establishment fails.
            TypeError: If the address is not a TcpAddress.
        """
        if not isinstance(addr, TcpAddress):
            raise TypeError(f"Address must be a TcpAddress, got {type(addr)}")

        sock_addr = self.all_peer_sockets.get(addr)
        if not sock_addr:
            raise ConnectionError(f"No physical socket address known for {addr}")

        try:
            reader, writer = await asyncio.open_connection(sock_addr.host, sock_addr.port)

            # --- KEY CHANGE ---
            # Send our logical numeric ID (as a u16) immediately upon connecting.
            # This is how the remote server knows who we are.
            writer.write(struct.pack("!H", self.local_numeric_id.id))
            await writer.drain()
            # ------------------

            return TcpConnection(reader, writer)
        except (OSError, asyncio.TimeoutError) as e:
            raise ConnectionError(f"Failed to connect to {sock_addr}: {e}") from e

    async def serve(self, server: Server, stop_event: asyncio.Event) -> None:
        """Start serving incoming TCP connections.

        Args:
            server: Server instance that will handle incoming messages
            stop_event: Event that signals when to stop serving
        """

        async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            """Handle a single client connection."""
            remote_numeric_id: TcpAddress | None = None
            try:
                # --- KEY CHANGE ---
                # Wait for the client to send its 2-byte (u16) numeric ID first.
                id_bytes = await reader.readexactly(2)
                remote_numeric_id = TcpAddress(struct.unpack("!H", id_bytes)[0])
                logger.trace(f"New connection from {remote_numeric_id}")
                # ------------------

                while not stop_event.is_set():
                    # Read first 4 bytes to check for cast marker or length
                    first_bytes = await reader.readexactly(4)
                    first_value = struct.unpack("!I", first_bytes)[0]

                    is_cast = first_value == CAST_MARKER

                    if is_cast:
                        # This is a cast message, read length and message
                        length_bytes = await reader.readexactly(4)
                        msg_length = struct.unpack("!I", length_bytes)[0]
                    else:
                        # This is a send message, first_value is the length
                        msg_length = first_value

                    msg_bytes = await reader.readexactly(msg_length)

                    # Handle message using the logical numeric ID
                    response = await server.handle(remote_numeric_id, msg_bytes)

                    if not is_cast:
                        if response is not None:
                            # Send response with length prefix
                            resp_length = struct.pack("!I", len(response))
                            writer.write(resp_length + response)
                            await writer.drain()
                        else:
                            # Send empty response
                            writer.write(struct.pack("!I", 0))
                            await writer.drain()

            except asyncio.IncompleteReadError:
                # Client disconnected
                logger.trace(f"Client {remote_numeric_id or 'unknown'} disconnected")
            except Exception as e:
                logger.error(f"Error handling client {remote_numeric_id or 'unknown'}: {e}")
            finally:
                writer.close()
                await writer.wait_closed()

        async def server_task(stop_event: asyncio.Event) -> None:
            """Internal task to run the server."""
            try:
                async with self.server:
                    await self.server.serve_forever()
            except asyncio.CancelledError:
                logger.trace("Server task cancelled")
            finally:
                logger.trace("TCP transport stopped")

        # Start the TCP server
        try:
            self.server = await asyncio.start_server(
                handle_client, self.bind_addr.host, self.bind_addr.port
            )
        except OSError as e:
            logger.error(f"Failed to bind server to {self.bind_addr}: {e}")
            raise

        addrs = ", ".join(str(sock.getsockname()) for sock in self.server.sockets or [])
        logger.trace(f"TCP transport serving on {addrs}")

        # Start the server task
        self.server_task = asyncio.create_task(server_task(stop_event))

        # Wait for stop signal
        await stop_event.wait()

        # Stop the server
        self.server.close()
        if self.server_task:
            self.server_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.server_task
