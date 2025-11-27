"""Transport layer for distributed algorithms framework.

This module provides abstractions for network communication between nodes.
"""

from distbench.transport.base import Address, Connection, Transport
from distbench.transport.delayed import DelayedConnection, DelayedTransport
from distbench.transport.offline import LocalAddress, LocalConnection, LocalTransport
from distbench.transport.tcp import TcpAddress, TcpConnection, TcpTransport

__all__ = [
    "Address",
    "Connection",
    "Transport",
    "LocalAddress",
    "LocalConnection",
    "LocalTransport",
    "TcpAddress",
    "TcpConnection",
    "TcpTransport",
    "DelayedTransport",
    "DelayedConnection",
]
