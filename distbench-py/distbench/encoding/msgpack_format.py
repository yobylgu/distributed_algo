"""Msgpack serialization format implementation.

This module provides a msgpack-based binary serialization format
that is compact and fast, similar to Rust's bincode.
"""

from dataclasses import asdict, fields, is_dataclass
from typing import Any, TypeVar, get_args, get_origin

import msgpack

from distbench.encoding.format import Format

T = TypeVar("T")


class MsgpackFormat(Format):
    """Msgpack binary serialization format.

    Uses msgpack for compact binary serialization with good performance.
    Similar to Rust's bincode in terms of efficiency.
    """

    def serialize(self, obj: Any) -> bytes:
        """Serialize an object to msgpack bytes.

        Args:
            obj: Object to serialize (should be a dataclass or dict)

        Returns:
            Msgpack-encoded bytes
        """
        if is_dataclass(obj):
            data = asdict(obj)
        elif isinstance(obj, dict):
            data = obj
        else:
            # Try to use __dict__ for other objects
            data = obj.__dict__ if hasattr(obj, "__dict__") else obj

        return msgpack.packb(data, use_bin_type=True)  # type: ignore

    def deserialize(self, data: bytes, cls: type[T]) -> T:
        """Deserialize msgpack bytes to an object.

        Args:
            data: Msgpack-encoded bytes
            cls: Target class (should be a dataclass or have from_dict method)

        Returns:
            Deserialized object instance
        """
        obj_dict = msgpack.unpackb(data, raw=False)

        # Use helper to recursively deserialize nested dataclasses
        return self._deserialize_obj(obj_dict, cls)

    def _deserialize_obj(
        self, obj_data: Any, cls: type[T], type_args: tuple[Any, ...] | None = None
    ) -> T:
        """Recursively deserialize an object, handling nested dataclasses, lists, and tuples.

        Args:
            obj_data: Dictionary, list, or primitive value to deserialize
            cls: Target class type
            type_args: Type arguments for generic types (e.g., [Message] for Signed[Message])

        Returns:
            Deserialized object instance
        """

        # Get origin and args from the target type
        origin = get_origin(cls)

        if type_args is None:
            type_args = get_args(cls)

        if origin in (list, tuple):
            if not isinstance(obj_data, list | tuple):
                return obj_data

            # Get the type of items in the list/tuple (e.g., Message from list[Message])
            item_type = type_args[0] if type_args else Any
            if item_type is Any:
                return obj_data  # Can't deserialize items without a type

            # Recursively deserialize each item
            deserialized_items = [self._deserialize_obj(item, item_type) for item in obj_data]
            # Return as the correct type (list or tuple)
            return origin(deserialized_items)  # type: ignore

        if isinstance(obj_data, dict):
            # If the original cls was a generic (e.g. Signed[T]),
            # origin is the base class (Signed).
            # If it was a plain dataclass, origin is None.
            target_cls = origin if origin is not None else cls

            if is_dataclass(target_cls):
                field_values = {}
                try:
                    dataclass_fields = fields(target_cls)  # type: ignore
                except TypeError:
                    # Not a dataclass, fallback
                    return obj_data  # type: ignore

                for field in dataclass_fields:
                    field_name = field.name
                    if field_name in obj_data:
                        field_type = field.type
                        field_value = obj_data[field_name]

                        # Resolve generic dataclasses like Signed[T]
                        # If this is the 'value' field and we have type args for the *parent*
                        if type_args and field_name == "value" and len(type_args) > 0:
                            # Use the type argument for this field
                            field_type = type_args[0]

                        # Recursively deserialize this field
                        field_values[field_name] = self._deserialize_obj(field_value, field_type)

                return target_cls(**field_values)  # type: ignore

            return obj_data  # type: ignore

        return obj_data  # type: ignore

    @property
    def name(self) -> str:
        """Get format name.

        Returns:
            "msgpack"
        """
        return "msgpack"
