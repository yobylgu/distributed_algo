"""Distbench - Framework for distributed algorithms in Python.

This package provides infrastructure for implementing and testing
distributed algorithms with support for:
- Multiple transport layers (TCP, in-memory channels)
- Pluggable serialization formats (JSON, msgpack)
- Cryptographic message signing (Ed25519)
- Offline and network execution modes
"""

from distbench.algorithm import Algorithm, Peer
from distbench.community import Community, NodeSet, NodeStatus, PeerId
from distbench.connection import ConnectionManager
from distbench.decorators import algorithm_state, config_field, handlers, message
from distbench.encoding import Format, JsonFormat, MsgpackFormat
from distbench.node import Node
from distbench.signing import KeyPair, Signed
from distbench.transport import (
    Address,
    Connection,
    TcpAddress,
    TcpTransport,
    Transport,
)

__version__ = "0.1.0"

__all__ = [
    # Core abstractions
    "Algorithm",
    "Peer",
    "Node",
    # Community and peers
    "Community",
    "PeerId",
    "NodeStatus",
    "NodeSet",
    # Connection management
    "ConnectionManager",
    # Transport layer
    "Transport",
    "Connection",
    "Address",
    "TcpTransport",
    "TcpAddress",
    # Serialization
    "Format",
    "JsonFormat",
    "MsgpackFormat",
    # Cryptography
    "Signed",
    "KeyPair",
    # Decorators
    "message",
    "algorithm_state",
    "handlers",
    "config_field",
]
