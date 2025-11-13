"""Decorators for algorithm development.

This module provides decorators that replace Rust's procedural macros:
- @message: Mark a class as a message type
- @distbench: Set up algorithm state with configuration fields and handlers
- @handler: Mark a method as a message handler
"""

import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from distbench.algorithm import Algorithm, Peer
from distbench.community import PeerId
from distbench.encoding.format import Format
from distbench.signing import Keystore, recursive_verify


def message(cls: type[Any]) -> type[Any]:
    """Decorator to mark a class as a message type.

    This automatically makes the class a dataclass if it isn't already,
    and marks it for use in the message handling system. It also adds
    a verify() method that recursively verifies all Signed fields.

    Usage:
        @message
        class MyMessage:
            value: int
            name: str

    Args:
        cls: Class to decorate

    Returns:
        Decorated class (as a dataclass with verify() method)
    """
    # Make it a dataclass if not already
    if not hasattr(cls, "__dataclass_fields__"):
        cls = dataclass(cls)

    # Add verify() method that recursively verifies all fields
    def verify(self: Any, keystore: Keystore) -> bool:
        """Verify all Signed fields in this message.

        Args:
            keystore: Object that can provide public keys for verification

        Returns:
            True if all nested Signed messages verify successfully, False otherwise
        """
        # Verify each field
        for field_name in self.__dataclass_fields__:
            field_value = getattr(self, field_name)
            if not recursive_verify(field_value, keystore):
                return False
        return True

    cls.verify = verify  # type: ignore

    # Mark as a message class
    cls.__message__ = True  # type: ignore
    return cls


def config_field(default: Any = None, required: bool = False) -> Any:
    """Mark a field as a configuration parameter.

    Configuration fields are loaded from the YAML config file at startup.

    Usage:
        @distbench
        class MyAlgorithm(Algorithm):
            max_rounds: int = config_field(required=True)
            timeout: float = config_field(default=5.0)

    Args:
        default: Default value if not specified in config
        required: Whether this field must be present in config

    Returns:
        A dataclass field with configuration metadata
    """
    metadata = {"config": True, "required": required}
    if default is not None:
        return field(default=default, metadata=metadata)
    else:
        return field(metadata=metadata)


def handler(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to mark a method as a message handler.

    Handler methods receive messages from peers and optionally return responses.

    Usage:
        @distbench
        class MyAlgorithm(Algorithm):
            @handler
            async def my_message(self, src: PeerId, msg: MyMessage) -> None:
                print(f"Received {msg} from {src}")

            @handler
            async def request(self, src: PeerId, msg: Request) -> Response:
                return Response(value=msg.value * 2)

    Args:
        func: Method to mark as a handler

    Returns:
        The same method, marked with handler metadata
    """
    func.__handler__ = True  # type: ignore
    return func


def distbench(cls: type[Algorithm]) -> type[Algorithm]:
    """Decorator to set up algorithm with configuration and message handlers.

    This decorator:
    - Extracts configuration fields from the class
    - Modifies __init__ to accept config dict and peers
    - Automatically assigns config values to instance variables
    - Collects all @handler decorated methods
    - Generates a handle() method that dispatches to the correct handler
    - Generates corresponding methods on the Peer class for sending messages

    Usage:
        @distbench
        class MyAlgorithm(Algorithm):
            max_rounds: int = config_field(required=True)
            timeout: float = config_field(default=5.0)

            @handler
            async def my_message(self, src: PeerId, msg: MyMessage) -> None:
                print(f"Received {msg} from {src}")

            @handler
            async def request(self, src: PeerId, msg: Request) -> Response:
                return Response(value=msg.value * 2)

    Args:
        cls: Algorithm class to decorate

    Returns:
        Decorated algorithm class
    """
    # Extract config fields
    config_fields = {}

    for name, field_obj in cls.__dict__.items():
        if hasattr(field_obj, "metadata") and field_obj.metadata.get("config"):
            config_fields[name] = field_obj

    # Store original __init__ if it exists
    original_init = cls.__init__ if hasattr(cls, "__init__") else None

    def new_init(self: Any, config: dict[str, Any], peers: dict[PeerId, Peer]) -> None:  # type: ignore
        """Initialize algorithm with config and peers.

        Args:
            config: Configuration dictionary from YAML file
            peers: Dictionary mapping peer IDs to Peer proxies
        """
        # Call parent class __init__
        Algorithm.__init__(self)

        # Initialize config fields from dict
        for name, field_obj in config_fields.items():
            if name in config:
                setattr(self, name, config[name])
            elif not field_obj.metadata.get("required"):
                # Use default value
                if hasattr(field_obj, "default"):
                    setattr(self, name, field_obj.default)
                elif hasattr(field_obj, "default_factory"):
                    setattr(self, name, field_obj.default_factory())
            else:
                raise ValueError(f"Required config field '{name}' missing")

        # Set peers
        self.peers = peers

        # Call original init if it existed (but not if it's just Algorithm.__init__)
        if original_init and original_init != Algorithm.__init__:
            # Try calling with config/peers, but ignore if it doesn't accept them
            from contextlib import suppress

            with suppress(TypeError):
                original_init(self, config, peers)

    cls.__init__ = new_init  # type: ignore
    cls.__config_fields__ = config_fields  # type: ignore

    # Collect all @handler decorated methods
    handler_methods: dict[str, dict[str, Any]] = {}

    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
        # Check if method is marked with @handler
        if not hasattr(method, "__handler__"):
            continue

        # Get method signature
        sig = inspect.signature(method)
        params = list(sig.parameters.values())

        # Handler methods should have signature: (self, src: PeerId, msg: MessageType)
        if len(params) >= 3:
            msg_param = params[2]
            msg_type = msg_param.annotation

            # Skip if no type annotation
            if msg_type == inspect.Parameter.empty:
                continue

            # Store handler info
            has_return = sig.return_annotation not in (None, inspect.Signature.empty, type(None))
            return_type = sig.return_annotation if has_return else None

            handler_methods[name] = {
                "method": method,
                "msg_type": msg_type,
                "returns": has_return,
                "return_type": return_type,
            }

    # Generate handle method
    async def handle(
        self: Any,
        src: PeerId,
        msg_type_id: str,
        msg_bytes: bytes,
        format: Format,
    ) -> bytes | None:
        """Handle incoming algorithm message by dispatching to the correct handler.

        Args:
            src: Peer ID of the sender
            msg_type_id: Message type identifier
            msg_bytes: Serialized message bytes
            format: Serialization format

        Returns:
            Serialized response bytes, or None
        """
        if msg_type_id not in handler_methods:
            raise ValueError(f"Unknown message type: {msg_type_id}")

        handler_info = handler_methods[msg_type_id]
        msg_obj = format.deserialize(msg_bytes, handler_info["msg_type"])

        # Verify all signatures in the message
        if not recursive_verify(msg_obj, self.community):
            logging.warning(
                f"Verification failed for {msg_type_id} from {src}, dropping message"
            )
            return None

        result = await handler_info["method"](self, src, msg_obj)

        if result is not None:
            return format.serialize(result)
        return None

    cls.handle = handle  # type: ignore

    # Generate Peer methods dynamically
    def create_peer_method(
        msg_type_id: str,
        msg_type: type[Any],
        has_return: bool,
        return_type: type[Any] | None,
    ) -> Callable[..., Any]:
        """Create a peer method for sending a specific message type.

        Args:
            msg_type_id: Message type identifier
            msg_type: Message class type
            has_return: Whether this message expects a response
            return_type: Type to deserialize response to (if has_return is True)

        Returns:
            Async method for sending this message type
        """

        async def peer_method(peer_self: Peer, msg: msg_type) -> Any:  # type: ignore
            """Send a message to this peer.

            Args:
                msg: Message to send

            Returns:
                Response if expected, None otherwise
            """
            return await peer_self._send_message(
                msg_type_id, msg, expect_response=has_return, response_type=return_type
            )

        peer_method.__name__ = msg_type_id
        peer_method.__qualname__ = f"Peer.{msg_type_id}"
        return peer_method

    # Attach methods to Peer class
    for name, info in handler_methods.items():
        peer_method = create_peer_method(
            name, info["msg_type"], info["returns"], info.get("return_type")
        )
        setattr(Peer, name, peer_method)

    # Store handler info on the class
    cls.__handlers__ = handler_methods  # type: ignore

    return cls


# Keep the old names as aliases for backward compatibility
algorithm_state = distbench
handlers = distbench
