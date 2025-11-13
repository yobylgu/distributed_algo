"""Base abstractions for transport layer.

This module defines the core interfaces for network communication:
- Address: Identifies a node's network location
- Connection: Represents an active connection to another node
- Transport: Manages overall networking and connection establishment
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Protocol, TypeVar, runtime_checkable


@runtime_checkable
class Address(Protocol):
    """Protocol for network addresses.

    Addresses must be hashable, comparable, and convertible to string.
    """

    def __hash__(self) -> int:
        """Return hash of the address for use in dictionaries and sets."""
        ...

    def __eq__(self, other: object) -> bool:
        """Check equality with another address."""
        ...

    def __str__(self) -> str:
        """Return string representation of the address."""
        ...


A = TypeVar("A", bound=Address)


class Connection(ABC):
    """Abstract base class for connections between nodes.

    A connection provides two communication patterns:
    - send: Request-response pattern (send message, wait for reply)
    - cast: Fire-and-forget pattern (send message, no reply expected)
    """

    @abstractmethod
    async def send(self, msg: bytes) -> bytes:
        """Send message and wait for response.

        Args:
            msg: Message bytes to send

        Returns:
            Response bytes from the remote node

        Raises:
            ConnectionError: If the connection fails
            asyncio.TimeoutError: If the request times out
        """
        pass

    @abstractmethod
    async def cast(self, msg: bytes) -> None:
        """Send message without waiting for response.

        Args:
            msg: Message bytes to send

        Raises:
            ConnectionError: If the connection fails
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the connection and release resources."""
        pass


class Server(Protocol):
    """Protocol for servers that handle incoming connections.

    Servers must implement a handle method to process incoming messages.
    """

    async def handle(self, addr: Address, msg: bytes) -> bytes | None:
        """Handle an incoming message.

        Args:
            addr: Source address of the message
            msg: Message bytes received

        Returns:
            Response bytes to send back, or None for cast messages
        """
        ...


class Transport(ABC):
    """Abstract base class for transport implementations.

    A transport manages the overall networking layer, including:
    - Establishing connections to remote nodes
    - Serving incoming connections from other nodes
    """

    @abstractmethod
    async def connect(self, addr: A) -> Connection:
        """Establish a connection to the specified address.

        Args:
            addr: Target address to connect to

        Returns:
            An active connection to the remote node

        Raises:
            ConnectionError: If connection establishment fails
        """
        pass

    @abstractmethod
    async def serve(self, server: Server, stop_event: asyncio.Event) -> None:
        """Start serving incoming connections.

        This method should run until the stop_event is set.

        Args:
            server: Server instance to handle incoming messages
            stop_event: Event that signals when to stop serving
        """
        pass
