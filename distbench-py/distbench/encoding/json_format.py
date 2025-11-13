"""JSON serialization format implementation.

This module provides a JSON-based serialization format that is
human-readable and useful for debugging.
"""

import base64
import json
from dataclasses import asdict, fields, is_dataclass
from typing import Any, TypeVar, get_args, get_origin

from distbench.encoding.format import Format

T = TypeVar("T")


def _json_encoder(obj: Any) -> Any:
    """Custom JSON encoder for non-standard types.

    Args:
        obj: Object to encode

    Returns:
        JSON-serializable representation
    """
    if isinstance(obj, bytes):
        # Encode bytes as base64 strings
        return {"__bytes__": base64.b64encode(obj).decode("ascii")}
    if is_dataclass(obj):
        return asdict(obj)
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _json_decoder(dct: dict[str, Any]) -> Any:
    """Custom JSON decoder for non-standard types.

    Args:
        dct: Dictionary from JSON

    Returns:
        Decoded object
    """
    if "__bytes__" in dct:
        return base64.b64decode(dct["__bytes__"])
    return dct


class JsonFormat(Format):
    """JSON serialization format.

    Uses the standard library json module with dataclass support.
    Provides human-readable serialization at the cost of larger message sizes.
    """

    def serialize(self, obj: Any) -> bytes:
        """Serialize an object to JSON bytes.

        Args:
            obj: Object to serialize (should be a dataclass or dict)

        Returns:
            UTF-8 encoded JSON bytes
        """
        if is_dataclass(obj):
            data = asdict(obj)
        elif isinstance(obj, dict):
            data = obj
        else:
            # Try to use __dict__ for other objects
            data = obj.__dict__ if hasattr(obj, "__dict__") else obj

        return json.dumps(data, default=_json_encoder, separators=(",", ":")).encode("utf-8")

    def deserialize(self, data: bytes, cls: type[T]) -> T:
        """Deserialize JSON bytes to an object.

        Args:
            data: UTF-8 encoded JSON bytes
            cls: Target class (should be a dataclass or have from_dict method)

        Returns:
            Deserialized object instance
        """
        obj_dict = json.loads(data.decode("utf-8"), object_hook=_json_decoder)

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
            "json"
        """
        return "json"
