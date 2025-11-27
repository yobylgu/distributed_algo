"""Decorators for algorithm development.

This module provides decorators that replace Rust's procedural macros:
- @message: Mark a class as a message type
- @distbench: Set up algorithm state with configuration fields and handlers
- @handler: Mark a method as a message handler
- @child_algorithm: Mark a field as a child algorithm
"""

import functools
import inspect
import logging
from collections.abc import Callable
from dataclasses import MISSING, dataclass, field
from typing import Any, get_type_hints

from distbench.algorithm import Algorithm, Peer
from distbench.community import PeerId
from distbench.encoding.format import Format
from distbench.messages import AlgorithmMessage
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


def config_field(
    default: Any = None, default_factory: Callable[[], Any] | None = None, required: bool = False
) -> Any:
    """Mark a field as a configuration parameter.

    Configuration fields are loaded from the YAML config file at startup.

    Usage:
        @distbench
        class MyAlgorithm(Algorithm):
            max_rounds: int = config_field(required=True)
            timeout: float = config_field(default=5.0)
            items: list = config_field(default_factory=list)

    Args:
        default: Default value if not specified in config
        default_factory: Factory function for default value (e.g., list, dict)
        required: Whether this field must be present in config

    Returns:
        A dataclass field with configuration metadata
    """
    metadata = {"config": True, "required": required}
    if default_factory is not None:
        metadata["default_factory"] = default_factory
        return field(default_factory=default_factory, metadata=metadata)
    elif default is not None:
        metadata["default"] = default
        return field(default=default, metadata=metadata)
    else:
        return field(metadata=metadata)


def child_algorithm(cls: type[Algorithm]) -> Any:
    """Mark a field as a child algorithm.

    Child algorithms are automatically initialized and registered.

    Usage:
        @distbench
        class MyAlgorithm(Algorithm):
            broadcast: SimpleBroadcast = child_algorithm(SimpleBroadcast)

    Args:
        cls: The algorithm class of the child

    Returns:
        A dataclass field with child algorithm metadata
    """
    metadata = {"child": True, "class": cls}
    return field(metadata=metadata)


def handler(
    func: Callable[..., Any] = None, *, from_child: str | None = None
) -> Callable[..., Any]:
    """Decorator to mark a method as a message handler.

    Handler methods receive messages from peers and optionally return responses.

    Usage:
        @distbench
        class MyAlgorithm(Algorithm):
            @handler
            async def my_message(self, src: PeerId, msg: MyMessage) -> None:
                print(f"Received {msg} from {src}")

            @handler(from_child="broadcast")
            async def on_broadcast(self, src: PeerId, msg: BroadcastMessage) -> None:
                 # Handle message delivered from child algorithm
                 pass

    Args:
        func: Method to mark as a handler (when used without args)
        from_child: Name of child algorithm this handler receives messages from

    Returns:
        The same method, marked with handler metadata
    """

    def wrapper(f: Callable[..., Any]) -> Callable[..., Any]:
        f.__handler__ = True  # type: ignore
        f.__from_child__ = from_child  # type: ignore
        return f

    if func is None:
        return wrapper
    return wrapper(func)


def interface(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to expose a method as a public interface.

    Transforms a method taking bytes into one taking a typed message,
    handling the serialization into an AlgorithmMessage envelope.

    Matches the Rust #[distbench::interface] macro behavior.

    Usage:
        @distbench
        class MyAlgorithm(Algorithm):
            @interface
            def my_public_method(self, data: bytes):
                # logic...
                pass

    Calls to my_public_method(msg_obj) will:
    1. Serialize msg_obj to bytes
    2. Create AlgorithmMessage(type_id, bytes)
    3. Serialize AlgorithmMessage to bytes
    4. Call original my_public_method with the final bytes
    """

    # Note: We access self.format inside the wrapper, assuming the method belongs to an Algorithm

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(self: Any, msg: Any) -> Any:
            if not hasattr(self, "format") or self.format is None:
                raise RuntimeError("Algorithm format not initialized")

            # 1. Serialize inner message
            inner_bytes = self.format.serialize(msg)

            # 2. Create envelope
            type_id = type(msg).__name__
            if hasattr(msg, "type_id"):
                type_id = msg.type_id()
            elif hasattr(type(msg), "type_id"):
                type_id = type(msg).type_id()

            alg_msg = AlgorithmMessage(type_id=type_id, bytes=inner_bytes)

            # 3. Serialize envelope
            outer_bytes = self.format.serialize(alg_msg)

            # 4. Call original
            return await func(self, outer_bytes)

        return async_wrapper

    else:

        @functools.wraps(func)
        def sync_wrapper(self: Any, msg: Any) -> Any:
            if not hasattr(self, "format") or self.format is None:
                raise RuntimeError("Algorithm format not initialized")

            # 1. Serialize inner message
            inner_bytes = self.format.serialize(msg)

            # 2. Create envelope
            type_id = type(msg).__name__
            if hasattr(msg, "type_id"):
                type_id = msg.type_id()
            elif hasattr(type(msg), "type_id"):
                type_id = type(msg).type_id()

            alg_msg = AlgorithmMessage(type_id=type_id, bytes=inner_bytes)

            # 3. Serialize envelope
            outer_bytes = self.format.serialize(alg_msg)

            # 4. Call original
            return func(self, outer_bytes)

        return sync_wrapper


def distbench(cls: type[Algorithm]) -> type[Algorithm]:
    """Decorator to set up algorithm with configuration and message handlers.

    This decorator:
    - Extracts configuration fields from the class
    - Modifies __init__ to accept config dict and peers
    - Automatically assigns config values to instance variables
    - Collects all @handler decorated methods
    - Generates a handle() method that dispatches to the correct handler
    - Generates corresponding methods on the Peer class for sending messages
    - Sets up child algorithms

    Args:
        cls: Algorithm class to decorate

    Returns:
        Decorated algorithm class
    """
    # Extract config fields and child algorithms
    config_fields = {}
    child_fields = {}

    for name, field_obj in cls.__dict__.items():
        if hasattr(field_obj, "metadata"):
            if field_obj.metadata.get("config"):
                config_fields[name] = field_obj
            elif field_obj.metadata.get("child"):
                child_fields[name] = field_obj

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
                if hasattr(field_obj, "default") and field_obj.default is not MISSING:
                    setattr(self, name, field_obj.default)
                elif (
                    hasattr(field_obj, "default_factory")
                    and field_obj.default_factory is not MISSING
                ):
                    setattr(self, name, field_obj.default_factory())
            else:
                raise ValueError(f"Required config field '{name}' missing")

        # Initialize child algorithms
        for name, field_obj in child_fields.items():
            child_cls = field_obj.metadata["class"]
            child_config = config.get(name, {})

            # Create child instance
            child_instance = child_cls(child_config, peers)
            self.register_child(name, child_instance)
            setattr(self, name, child_instance)

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
    child_handlers: dict[
        str, dict[str, Any]
    ] = {}  # Map child_name -> {msg_type -> handler_method_name}

    for name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
        # Check if method is marked with @handler
        if not hasattr(method, "__handler__"):
            continue

        from_child = getattr(method, "__from_child__", None)

        # Try to resolve type hints to get actual classes instead of strings
        try:
            hints = get_type_hints(method)
        except Exception:
            hints = {}

        # Get method signature
        sig = inspect.signature(method)
        params = list(sig.parameters.values())

        # If from_child is set, it's a deliver handler
        # Signature: (self, src, msg)
        if from_child:
            if len(params) >= 3:
                msg_param = params[2]
                # Use resolved hint if available
                msg_type = hints.get(msg_param.name, msg_param.annotation)

                if msg_type == inspect.Parameter.empty:
                    continue

                # Register for delivery
                if from_child not in child_handlers:
                    child_handlers[from_child] = {}

                # Use class name for routing key
                msg_type_name = (
                    msg_type.__name__ if hasattr(msg_type, "__name__") else str(msg_type)
                )

                child_handlers[from_child][msg_type_name] = {
                    "method": method,
                    "msg_type": msg_type,
                }
            continue

        # Handler methods should have signature: (self, src: PeerId, msg: MessageType)
        if len(params) >= 3:
            msg_param = params[2]
            # Use resolved hint if available
            msg_type = hints.get(msg_param.name, msg_param.annotation)

            # Skip if no type annotation
            if msg_type == inspect.Parameter.empty:
                continue

            # Store handler info
            has_return = sig.return_annotation not in (
                None,
                inspect.Signature.empty,
                type(None),
            )
            return_type = sig.return_annotation if has_return else None

            # If return_type is a string, try to resolve it?
            # Usually we don't use return_type for logic, just maybe serialization?
            # In peer_method creation we use return_type.
            if isinstance(return_type, str) and return_type in hints:
                # This is unlikely for return annotation unless we check key 'return'
                pass

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
        path: list[str] | None = None,
    ) -> bytes | None:
        """Handle incoming algorithm message by dispatching to the correct handler.

        Args:
            src: Peer ID of the sender
            msg_type_id: Message type identifier
            msg_bytes: Serialized message bytes
            format: Serialization format
            path: Optional path for routing to child algorithms

        Returns:
            Serialized response bytes, or None
        """
        # Check path for routing
        if path and len(path) > 0:
            child_name = path[0]
            remaining_path = path[1:]
            if child_name in self._children:
                child = self._children[child_name]
                return await child.handle(src, msg_type_id, msg_bytes, format, remaining_path)
            else:
                logging.warning(
                    f"Child algorithm '{child_name}' not found in {self._name or 'root'}"
                )
                return None

        # Check if we handle it directly
        if msg_type_id in handler_methods:
            handler_info = handler_methods[msg_type_id]
            # Here msg_type must be a class
            if isinstance(handler_info["msg_type"], str):
                logging.error(
                    f"Handler for {msg_type_id} has unresolved type string: {handler_info['msg_type']}"
                )
                return None

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

        # If not handled locally and no path, check if any child handles this message type
        # This is implicit routing (if supported)
        for child in self._children.values():
            # We can check if child class has handler for this.
            # Accessing `__handlers__` on child class.
            child_cls = child.__class__
            if hasattr(child_cls, "__handlers__") and msg_type_id in child_cls.__handlers__:
                return await child.handle(src, msg_type_id, msg_bytes, format, path)

        logging.warning(f"Unhandled message type: {msg_type_id} in {self.__class__.__name__}")
        return None

    cls.handle = handle  # type: ignore

    # Generate deliver method
    async def deliver(
        self: Any,
        src: PeerId,
        msg_bytes: bytes,
        format: Format,
    ) -> bytes | None:
        """Deliver a message from a child algorithm to this parent."""
        try:
            envelope = format.deserialize(msg_bytes, tuple)
            if not isinstance(envelope, list | tuple) or len(envelope) != 2:
                logging.error("Invalid envelope format for deliver()")
                return None
            msg_type_id, payload = envelope
        except Exception as e:
            logging.error(f"Failed to deserialize envelope: {e}")
            return None

        for handlers in child_handlers.values():
            if msg_type_id in handlers:
                handler_info = handlers[msg_type_id]

                if isinstance(handler_info["msg_type"], str):
                    logging.error(f"Deliver handler for {msg_type_id} has unresolved type string")
                    return None

                msg_obj = format.deserialize(payload, handler_info["msg_type"])

                # Verify
                if not recursive_verify(msg_obj, self.community):
                    return None

                return await handler_info["method"](self, src, msg_obj)

        logging.warning(f"No handler for delivered message: {msg_type_id}")
        return None

    cls.deliver = deliver  # type: ignore

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
