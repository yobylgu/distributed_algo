"""Connection manager for managing peer connections.

This module provides a connection manager that handles:
- Lazy connection establishment
- Connection pooling and reuse
- Automatic retry logic with exponential backoff
- Thread-safe access to connections
"""

import asyncio
import logging
from typing import Generic, TypeVar

from distbench.transport.base import Address, Connection, Transport

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Transport)
A = TypeVar("A", bound=Address)


class ConnectionManager(Generic[T, A]):
    """Manages connections to a specific peer with retry logic.

    This class handles:
    - Lazy connection establishment on first use
    - Connection caching for reuse
    - Automatic retry with exponential backoff
    - Connection invalidation on errors
    - Thread-safe access
    """

    def __init__(
        self,
        transport: T,
        address: A,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        """Initialize a connection manager for a specific peer.

        Args:
            transport: Transport instance for creating connections
            address: Target peer's address
            max_retries: Maximum number of connection attempts (default: 3)
            retry_delay: Base delay between retries in seconds (default: 1.0)
        """
        self.transport = transport
        self.address = address
        self.connection: Connection | None = None
        self.lock = asyncio.Lock()
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    async def _ensure_connected(self) -> Connection:
        """Lazily establish connection with retry logic.

        Returns:
            An active connection to the peer

        Raises:
            ConnectionError: If all connection attempts fail
        """
        async with self.lock:
            if self.connection is not None:
                return self.connection

            last_error: Exception | None = None

            for attempt in range(self.max_retries):
                try:
                    logger.trace(
                        f"Connecting to {self.address} (attempt {attempt + 1}/{self.max_retries})"
                    )
                    self.connection = await self.transport.connect(self.address)
                    logger.trace(f"Successfully connected to {self.address}")
                    return self.connection

                except Exception as e:
                    last_error = e
                    logger.warning(
                        f"Connection attempt {attempt + 1}/{self.max_retries} "
                        f"to {self.address} failed: {e}"
                    )

                    if attempt < self.max_retries - 1:
                        # Exponential backoff
                        delay = self.retry_delay * (2**attempt)
                        logger.trace(f"Retrying in {delay:.2f} seconds...")
                        await asyncio.sleep(delay)

            # All attempts failed
            error_msg = f"Failed to connect to {self.address} after {self.max_retries} attempts"
            if last_error:
                error_msg += f": {last_error}"
            raise ConnectionError(error_msg)

    async def send(self, msg: bytes) -> bytes:
        """Send message and wait for response.

        This method establishes a connection if needed, sends the message,
        and waits for a response. If the connection fails, it invalidates
        the cached connection for the next attempt.

        Args:
            msg: Message bytes to send

        Returns:
            Response bytes from the peer

        Raises:
            ConnectionError: If the send operation fails
        """
        conn = await self._ensure_connected()

        try:
            return await conn.send(msg)
        except Exception as e:
            # Invalidate connection on error
            logger.warning(f"Send to {self.address} failed, invalidating connection: {e}")
            async with self.lock:
                self.connection = None
            raise

    async def cast(self, msg: bytes) -> None:
        """Send message without waiting for response.

        This method establishes a connection if needed and sends the message
        without waiting for a response. If the connection fails, it invalidates
        the cached connection for the next attempt.

        Args:
            msg: Message bytes to send

        Raises:
            ConnectionError: If the cast operation fails
        """
        conn = await self._ensure_connected()

        try:
            await conn.cast(msg)
        except Exception as e:
            # Invalidate connection on error
            logger.warning(f"Cast to {self.address} failed, invalidating connection: {e}")
            async with self.lock:
                self.connection = None
            raise

    async def close(self) -> None:
        """Close the managed connection if it exists."""
        async with self.lock:
            if self.connection is not None:
                await self.connection.close()
                self.connection = None
