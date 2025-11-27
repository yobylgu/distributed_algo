"""Middleware transport that adds artificial latency.

This module provides a transport wrapper that injects a fixed delay
before every message send operation.
"""

import asyncio
import logging
import random
from typing import Generic, TypeVar

from distbench.transport.base import Address, Connection, Server, Transport

logger = logging.getLogger(__name__)

A = TypeVar("A", bound=Address)
T = TypeVar("T", bound=Transport)


class DelayedConnection(Connection, Generic[T, A]):
    """A connection that adds latency to outgoing messages."""

    def __init__(self, inner: Connection, delay_range: tuple[int, int]) -> None:
        """Initialize a delayed connection.

        Args:
            inner: The underlying connection to wrap
            delay_range: Tuple of (min_ms, max_ms) for random delay
        """
        self.inner = inner
        self.delay_range = delay_range

    async def _apply_delay(self) -> None:
        """Apply random delay from the configured range."""
        min_delay, max_delay = self.delay_range
        if min_delay == max_delay:
            delay_ms = min_delay
        else:
            delay_ms = random.randint(min_delay, max_delay)

        if delay_ms > 0:
            logger.trace(f"Delaying message by {delay_ms}ms")
            await asyncio.sleep(delay_ms / 1000.0)

    async def send(self, msg: bytes) -> bytes:
        """Send message and wait for response with added latency.

        Args:
            msg: Message bytes to send

        Returns:
            Response bytes from the remote node
        """
        await self._apply_delay()
        return await self.inner.send(msg)

    async def cast(self, msg: bytes) -> None:
        """Send message without waiting for response with added latency.

        Args:
            msg: Message bytes to send
        """
        await self._apply_delay()
        await self.inner.cast(msg)

    async def close(self) -> None:
        """Close the underlying connection."""
        await self.inner.close()


class DelayedTransport(Transport, Generic[T, A]):
    """A transport that adds latency to all outgoing messages.

    Wraps an inner transport and adds a delay to every connection created.
    """

    def __init__(self, inner: T, delay_range: tuple[int, int]) -> None:
        """Create a new delayed transport.

        Args:
            inner: The underlying transport to wrap
            delay_range: Tuple of (min_ms, max_ms) for random delay.
                        If min == max, uses fixed delay.
        """
        self.inner = inner
        self.delay_range = delay_range

    async def connect(self, addr: A) -> Connection:
        """Establish a delayed connection to the specified address.

        Args:
            addr: Target address to connect to

        Returns:
            A delayed connection to the remote node
        """
        inner_conn = await self.inner.connect(addr)
        return DelayedConnection(inner_conn, self.delay_range)

    async def serve(self, server: Server, stop_event: asyncio.Event) -> None:
        """Start serving incoming connections.

        Delegates to the inner transport, as delays are only applied to
        outgoing messages.

        Args:
            server: Server instance to handle incoming messages
            stop_event: Event that signals when to stop serving
        """
        await self.inner.serve(server, stop_event)
