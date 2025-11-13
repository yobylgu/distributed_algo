"""Cryptographic message signing using Ed25519.

This module provides support for cryptographically signed messages,
which is essential for Byzantine fault-tolerant algorithms.
"""

import hashlib
import logging
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Generic, Protocol, TypeVar

from nacl.encoding import RawEncoder
from nacl.signing import SigningKey, VerifyKey

from distbench.community import PeerId

logger = logging.getLogger(__name__)

M = TypeVar("M")


def recursive_verify(obj: Any, keystore: "Keystore") -> bool:
    """Recursively verify all Signed messages in an object graph.

    This function traverses the entire object structure and verifies any
    Signed[T] wrappers encountered at any depth. It handles primitives,
    containers (lists, dicts, tuples, sets), and objects with verify() methods.

    Args:
        obj: Object to verify (can be any type)
        keystore: Object that can provide public keys for verification

    Returns:
        True if all nested Signed messages verify successfully, False otherwise
    """
    # Handle None
    if obj is None:
        return True

    # Handle primitives (no verification needed)
    if isinstance(obj, (str, int, float, bool, bytes)):
        return True

    # Handle containers
    if isinstance(obj, (list, tuple)):
        return all(recursive_verify(item, keystore) for item in obj)

    if isinstance(obj, set):
        return all(recursive_verify(item, keystore) for item in obj)

    if isinstance(obj, dict):
        # Verify both keys and values
        for key, value in obj.items():
            if not recursive_verify(key, keystore):
                return False
            if not recursive_verify(value, keystore):
                return False
        return True

    # Handle objects with verify() method
    if hasattr(obj, "verify") and callable(obj.verify):
        try:
            return obj.verify(keystore)
        except Exception as e:
            logger.warning(f"Verification failed for {type(obj).__name__}: {e}")
            return False

    # For other objects, assume no verification needed
    return True


class Keystore(Protocol):
    """Protocol for objects that can provide public keys for verification."""

    def get_key(self, peer_id: PeerId) -> bytes | None:
        """Get the public key for a peer.

        Args:
            peer_id: The peer ID to get the key for

        Returns:
            The public key bytes (32 bytes), or None if not found
        """
        ...


@dataclass
class Signed(Generic[M]):
    """Wrapper for cryptographically signed messages.

    This class wraps a message with its Ed25519 signature and signer identity.
    It provides transparent access to the inner message through __getattr__.
    """

    value: M
    signature: bytes
    signer: PeerId

    def __str__(self) -> str:
        """Display the message with signer information.

        Returns:
            String showing the value and who signed it
        """
        return f"{self.value} (signed by {self.signer})"

    def __repr__(self) -> str:
        """Return a representation of the signed message.

        Returns:
            String in format: <value> (signed by <node>)
        """
        return f"{self.value} (signed by {self.signer})"

    def __getattr__(self, name: str) -> Any:
        """Provide transparent access to inner value's attributes.

        This implements a "deref" pattern where accessing attributes on the
        Signed wrapper delegates to the wrapped value.

        Args:
            name: Attribute name to access

        Returns:
            Attribute value from the inner message
        """
        # Avoid infinite recursion for dataclass fields
        if name in ["value", "signature", "signer"]:
            return object.__getattribute__(self, name)
        return getattr(object.__getattribute__(self, "value"), name)

    def inner(self) -> M:
        """Get the inner wrapped value.

        Returns:
            The original message without the signature wrapper
        """
        return self.value

    def verify(self, keystore: Keystore) -> bool:
        """Verify the signature and recursively verify nested signed content.

        Args:
            keystore: Object that can provide public keys for verification

        Returns:
            True if the signature is valid and all nested content verifies,
            False otherwise
        """
        # Get the signer's public key bytes
        key_bytes = keystore.get_key(self.signer)
        if key_bytes is None:
            logger.warning(f"Cannot verify signature: no public key for {self.signer}")
            return False

        # Convert bytes to VerifyKey
        try:
            verify_key = VerifyKey(key_bytes)
        except Exception as e:
            logger.warning(
                f"Invalid public key for {self.signer}: {e}"
            )
            return False

        # Verify the signature
        try:
            # Compute digest of the inner value
            digest = KeyPair._digest_static(self.value)
            # Verify signature
            verify_key.verify(digest, self.signature, encoder=RawEncoder)
        except Exception as e:
            logger.warning(
                f"Signature verification failed for message from {self.signer}: {e}"
            )
            return False

        # Recursively verify the inner value
        return recursive_verify(self.value, keystore)


class KeyPair:
    """Ed25519 key pair for signing and verifying messages.

    Each node generates a unique key pair on startup for creating
    and verifying cryptographic signatures.
    """

    def __init__(self, peer_id: PeerId) -> None:
        """Initialize a new key pair.

        Args:
            peer_id: The peer ID of the node that owns this key pair
        """
        self.peer_id = peer_id
        self.signing_key = SigningKey.generate()
        self.verify_key = self.signing_key.verify_key

    def public_key_bytes(self) -> bytes:
        """Get the public key as raw bytes.

        Returns:
            32-byte Ed25519 public key
        """
        return bytes(self.verify_key)

    def sign(self, message: M) -> Signed[M]:
        """Create a cryptographically signed message.

        Args:
            message: Message to sign (any object)

        Returns:
            Signed wrapper containing the message, signature, and signer ID
        """
        # Compute digest of the message
        digest = self._digest(message)

        # Sign the digest
        signature = self.signing_key.sign(digest, encoder=RawEncoder).signature

        # Return signed wrapper
        return Signed(value=message, signature=signature, signer=self.peer_id)

    def verify(self, signed: Signed[M], verify_key: VerifyKey) -> bool:
        """Verify a signed message against a public key.

        Args:
            signed: Signed message to verify
            verify_key: Public key to verify against

        Returns:
            True if the signature is valid, False otherwise
        """
        digest = self._digest(signed.value)
        try:
            verify_key.verify(digest, signed.signature, encoder=RawEncoder)
            return True
        except Exception:
            return False

    @staticmethod
    def verify_key_from_bytes(key_bytes: bytes) -> VerifyKey:
        """Create a VerifyKey from raw bytes.

        Args:
            key_bytes: 32-byte Ed25519 public key

        Returns:
            VerifyKey instance for signature verification
        """
        return VerifyKey(key_bytes)

    @staticmethod
    def _digest_static(obj: Any) -> bytes:
        """Compute a cryptographic digest of an object (static version).

        Args:
            obj: Object to hash

        Returns:
            32-byte BLAKE2b digest
        """
        # Convert object to bytes for hashing
        if is_dataclass(obj):
            # Use dataclass fields in a canonical order
            data = asdict(obj)
            # Sort keys for deterministic ordering
            canonical = str(sorted(data.items()))
        elif isinstance(obj, (str, int, float, bool)):
            canonical = str(obj)
        elif isinstance(obj, bytes):
            return hashlib.blake2b(obj, digest_size=32).digest()
        else:
            # Fallback to repr
            canonical = repr(obj)

        return hashlib.blake2b(canonical.encode("utf-8"), digest_size=32).digest()

    def _digest(self, obj: Any) -> bytes:
        """Compute a cryptographic digest of an object.

        Args:
            obj: Object to hash

        Returns:
            32-byte BLAKE2b digest
        """
        return KeyPair._digest_static(obj)
