"""Algorithm framework base classes.

This module provides the core abstractions for implementing distributed algorithms:
- Algorithm: Base class that algorithms extend
- Peer: Proxy for sending messages to other nodes
"""

import asyncio
import logging
from abc import ABC
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from distbench.community import PeerId
from distbench.connection import ConnectionManager
from distbench.encoding.format import Format
from distbench.messages import AlgorithmMessage, NodeMessage
from distbench.signing import KeyPair, Keystore, Signed, recursive_verify
from distbench.transport.base import Address, Transport

if TYPE_CHECKING:
    from distbench.community import Community

T = TypeVar("T")
A = TypeVar("A", bound=Address)
TR = TypeVar("TR", bound=Transport)


class Peer(Generic[A, TR]):
    """Proxy for sending messages to a peer node.

    This class provides methods for sending algorithm-specific messages
    to a peer, with automatic serialization and transport handling.
    """

    def __init__(
        self,
        peer_id: PeerId,
        conn_manager: ConnectionManager[TR, A],
        format: Format,
        keystore: Keystore | None = None,
    ) -> None:
        """Initialize a peer proxy.

        Args:
            peer_id: ID of the target peer
            conn_manager: Connection manager for this peer
            format: Serialization format to use for messages
            keystore: Optional keystore for verifying signed responses
        """
        self.peer_id = peer_id
        self.conn_manager = conn_manager
        self.format = format
        self.keystore = keystore
        self._handlers: dict[str, type[Any]] = {}

    def _register_handler(self, name: str, msg_class: type[Any]) -> None:
        """Register a message handler type.

        This is used internally by the decorator system to track
        message types for this peer.

        Args:
            name: Handler method name / message type ID
            msg_class: Message class type
        """
        self._handlers[name] = msg_class

    async def _send_message(
        self,
        msg_type: str,
        msg: Any,
        expect_response: bool = False,
        response_type: type[Any] | None = None,
        path: list[str] | None = None,
    ) -> Any:
        """Generic message sender.

        Args:
            msg_type: Message type identifier
            msg: Message object to send
            expect_response: Whether to wait for a response
            response_type: Type to deserialize response to (if expect_response is True)
            path: Optional path for routing to child algorithms

        Returns:
            Response object if expect_response is True, None otherwise
        """
        # Serialize the message
        payload = self.format.serialize(msg)

        # Wrap in node message
        node_msg = NodeMessage.algorithm(msg_type, payload)

        if not expect_response:
            await self.conn_manager.cast(node_msg)
            return None

        response_bytes = await self.conn_manager.send(node_msg)
        if not response_bytes or len(response_bytes) == 0:
            return None

        # Deserialize response with proper type
        if response_type is not None:
            response = self.format.deserialize(response_bytes, response_type)
        else:
            # Fallback to dict if no type specified
            response = self.format.deserialize(response_bytes, dict)

        # Verify all signatures in the response
        if self.keystore is not None and not recursive_verify(response, self.keystore):
            logging.warning(f"Verification failed for response from {self.peer_id}, dropping")
            return None

        return response


class Algorithm(ABC):
    """Base class for distributed algorithms.

    Algorithms should extend this class and implement the required methods.
    The framework handles node lifecycle, message routing, and communication.
    """

    def __init__(self) -> None:
        """Initialize algorithm with termination event."""
        self._terminated_event = asyncio.Event()
        self._node_id: PeerId | None = None
        self._total_nodes: int = 0
        self._keypair: KeyPair | None = None
        self.community: Community | None = None
        self.format: Format | None = None

        # Layering support
        self._children: dict[str, Algorithm] = {}
        self._parent: Algorithm | None = None
        self._name: str = ""

    async def on_start(self) -> None:  # noqa: B027
        """Called when all nodes are ready and synchronized.

        Override this method to implement algorithm initialization logic.
        This is called after all nodes have started and are ready to communicate.
        """
        pass

    async def on_exit(self) -> None:  # noqa: B027
        """Called during shutdown for cleanup.

        Override this method to implement algorithm cleanup logic.
        This is called when the node is terminating.
        """
        pass

    async def report(self) -> dict[str, str] | None:
        """Return algorithm-specific results or metrics.

        Override this method to provide custom output when the algorithm completes.

        Returns:
            Dictionary of key-value pairs to display, or None
        """
        return None

    async def terminate(self) -> None:
        """Signal that this algorithm has completed and should stop."""
        self._terminated_event.set()
        # Also terminate children
        for child in self._children.values():
            await child.terminate()

    async def terminated(self) -> bool:
        """Wait for termination signal.

        Returns:
            True when the algorithm has been terminated
        """
        await self._terminated_event.wait()
        return True

    def is_terminated(self) -> bool:
        """Check if algorithm has been terminated (non-blocking).

        Returns:
            True if terminated, False otherwise
        """
        return self._terminated_event.is_set()

    def id(self) -> PeerId:
        """Get this node's unique identifier.

        Returns:
            This node's peer ID

        Raises:
            RuntimeError: If called before node ID is set
        """
        if self._node_id is None:
            raise RuntimeError("Node ID not set")
        return self._node_id

    def N(self) -> int:  # noqa: N802
        """Get total number of nodes in the system.

        Returns:
            Total node count

        Raises:
            RuntimeError: If called before node count is set
        """
        if self._total_nodes == 0:
            raise RuntimeError("Total nodes not set")
        return self._total_nodes

    def sign(self, message: T) -> Signed[T]:
        """Create a cryptographically signed message.

        Args:
            message: Message to sign

        Returns:
            Signed wrapper with signature and signer ID

        Raises:
            RuntimeError: If called before key pair is set
        """
        if self._keypair is None:
            raise RuntimeError("Key pair not set")
        return self._keypair.sign(message)

    # --- Layering Support ---

    def register_child(self, name: str, child: "Algorithm") -> None:
        """Register a child algorithm.

        Args:
            name: Name of the child algorithm
            child: The child algorithm instance
        """
        self._children[name] = child
        child._parent = self
        child._name = name

        # Propagate context to child
        if self._node_id:
            child._set_node_id(self._node_id)
        if self._total_nodes:
            child._set_total_nodes(self._total_nodes)
        if self._keypair:
            child._set_keypair(self._keypair)
        if self.community:
            child.community = self.community
        if self.format:
            child._set_format(self.format)

    async def deliver(
        self,
        src: PeerId,
        msg_bytes: bytes,
        format: Format,
    ) -> bytes | None:
        """Deliver a message from a child algorithm to this parent.

        Args:
            src: Peer ID of the sender (who sent the original message to the child)
            msg_bytes: The serialized envelope (type_id, payload) from the child
            format: Serialization format

        Returns:
            Optional response bytes
        """
        # This method should be implemented by the @distbench decorator
        # to dispatch to handlers marked with @handler(from_child=...)
        raise NotImplementedError("deliver() should be implemented by @distbench decorator")

    async def deliver_message(
        self,
        src: PeerId,
        msg_bytes: bytes,
    ) -> Any:
        """Helper for child algorithms to deliver a message to their parent.

        Args:
            src: Peer ID of the sender
            msg_bytes: The content to deliver

        Returns:
            Response from parent, if any
        """
        if self._parent is None:
            return None

        if self.format is None:
            raise RuntimeError("Format not set in algorithm")

        try:
            alg_msg = self.format.deserialize(msg_bytes, AlgorithmMessage)
        except Exception as e:
            logging.error(f"SimpleBroadcast: Error relaying to parent: {e}")

        # Create envelope: (msg_type_id, payload_bytes)
        envelope = (alg_msg.type_id, alg_msg.bytes)
        envelope_bytes = self.format.serialize(envelope)

        return await self._parent.deliver(src, envelope_bytes, self.format)

    def _set_node_id(self, node_id: PeerId) -> None:
        """Internal method to set the node ID.

        Args:
            node_id: This node's peer ID
        """
        self._node_id = node_id
        for child in self._children.values():
            child._set_node_id(node_id)

    def _set_total_nodes(self, total: int) -> None:
        """Internal method to set the total node count.

        Args:
            total: Total number of nodes
        """
        self._total_nodes = total
        for child in self._children.values():
            child._set_total_nodes(total)

    def _set_keypair(self, keypair: KeyPair) -> None:
        """Internal method to set the cryptographic key pair.

        Args:
            keypair: Ed25519 key pair for this node
        """
        self._keypair = keypair
        for child in self._children.values():
            child._set_keypair(keypair)

    def _set_format(self, format: Format) -> None:
        """Internal method to set the serialization format.

        Args:
            format: The serialization format implementation
        """
        self.format = format
        for child in self._children.values():
            child._set_format(format)

    async def handle(
        self,
        src: PeerId,
        msg_type_id: str,
        msg_bytes: bytes,
        format: Format,
        path: list[str] | None = None,
    ) -> bytes | None:
        """Handle an incoming algorithm message.

        This method is called when an algorithm message is received from a peer.
        The @handlers decorator will generate an implementation that dispatches
        to the appropriate handler methods.

        Args:
            src: Peer ID of the sender
            msg_type_id: Message type identifier
            msg_bytes: Serialized message payload
            format: Serialization format to use
            path: Optional path for routing to child algorithms

        Returns:
            Serialized response bytes, or None for no response

        Raises:
            ValueError: If the message type is unknown
        """
        raise NotImplementedError("handle() method should be implemented by @distbench decorator")
