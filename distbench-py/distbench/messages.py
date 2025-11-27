"""Node-level message serialization.

This module handles the outer envelope for all messages exchanged between nodes,
including lifecycle coordination messages and algorithm-specific messages.
"""

from dataclasses import dataclass
from enum import Enum

import msgpack


class MessageType(Enum):
    """Types of node-level messages.

    These messages coordinate node lifecycle and wrap algorithm messages.
    """

    PUBKEY = 1  # Public key exchange during startup
    STARTED = 2  # Node has started and is ready
    FINISHED = 3  # Node has finished execution
    ALGORITHM = 4  # Algorithm-specific message


@dataclass
class AlgorithmMessage:
    """Concrete packaged message, used to pass messages across layers.

    Matches the Rust AlgorithmMessage struct.
    """

    type_id: str
    bytes: bytes


class NodeMessage:
    """Envelope for node-to-node messages.

    This class provides static methods for serializing and deserializing
    different types of node messages using msgpack.
    """

    @staticmethod
    def pubkey(public_key: bytes) -> bytes:
        """Serialize a public key announcement message.

        Args:
            public_key: Ed25519 public key bytes (32 bytes)

        Returns:
            Serialized message bytes
        """
        return msgpack.packb([MessageType.PUBKEY.value, public_key], use_bin_type=True)  # type: ignore

    @staticmethod
    def started() -> bytes:
        """Serialize a Started message indicating the node is ready.

        Returns:
            Serialized message bytes
        """
        return msgpack.packb([MessageType.STARTED.value], use_bin_type=True)  # type: ignore

    @staticmethod
    def finished() -> bytes:
        """Serialize a Finished message indicating the node has stopped.

        Returns:
            Serialized message bytes
        """
        return msgpack.packb([MessageType.FINISHED.value], use_bin_type=True)  # type: ignore

    @staticmethod
    def algorithm(msg_type: str, payload: bytes) -> bytes:
        """Serialize an algorithm-specific message.

        Args:
            msg_type: Type identifier for the algorithm message
            payload: Serialized algorithm message bytes

        Returns:
            Serialized message bytes
        """
        return msgpack.packb(  # type: ignore
            [MessageType.ALGORITHM.value, msg_type, payload], use_bin_type=True
        )

    @staticmethod
    def deserialize(data: bytes) -> tuple[MessageType, str | None, bytes | None]:
        """Deserialize a node message.

        Args:
            data: Serialized message bytes

        Returns:
            Tuple of (message_type, type_id, payload):
            - message_type: The type of message
            - type_id: For ALGORITHM messages, the algorithm message type ID
            - payload: For ALGORITHM messages, the algorithm message payload;
                      For PUBKEY messages, the public key bytes

        Raises:
            ValueError: If the message format is invalid
        """
        unpacked = msgpack.unpackb(data, raw=False)

        if not isinstance(unpacked, list) or len(unpacked) < 1:
            raise ValueError("Invalid message format")

        msg_type = MessageType(unpacked[0])

        if msg_type == MessageType.ALGORITHM:
            if len(unpacked) != 3:
                raise ValueError("Algorithm message must have 3 elements")
            return msg_type, unpacked[1], unpacked[2]
        elif msg_type == MessageType.PUBKEY:
            if len(unpacked) != 2:
                raise ValueError("Pubkey message must have 2 elements")
            return msg_type, None, unpacked[1]
        else:
            return msg_type, None, None
