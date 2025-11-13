"""Abstract serialization format interface.

This module defines the Format abstract base class for pluggable
serialization implementations.
"""

from abc import ABC, abstractmethod
from typing import Any, TypeVar

T = TypeVar("T")


class Format(ABC):
    """Abstract base class for serialization formats.

    Implementations provide specific serialization strategies (JSON, msgpack, etc.)
    for converting objects to/from bytes.
    """

    @abstractmethod
    def serialize(self, obj: Any) -> bytes:
        """Serialize an object to bytes.

        Args:
            obj: Object to serialize

        Returns:
            Serialized bytes

        Raises:
            SerializationError: If serialization fails
        """
        pass

    @abstractmethod
    def deserialize(self, data: bytes, cls: type[T]) -> T:
        """Deserialize bytes to an object of the specified type.

        Args:
            data: Serialized bytes
            cls: Target class type

        Returns:
            Deserialized object instance

        Raises:
            DeserializationError: If deserialization fails
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the format name identifier.

        Returns:
            Format name (e.g., "json", "msgpack")
        """
        pass
