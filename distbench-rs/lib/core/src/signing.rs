//! Structures and traits for handling cryptographically signed values.
//!
//! This module defines the `Signed<M>` wrapper, which associates a generic value
//! with a cryptographic signature and the ID of the signing peer. It also
//! provides the `Unverified<M>` trait for types that need signature verification
//! against a `KeyStore`.
//!
//! The main structure, `Signed<M>`, is designed for easy use by implementing
//! `Deref` and `AsRef` to the inner value `M`.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    ops::Deref,
};

use serde::{Deserialize, Serialize};

use crate::{community::KeyStore, crypto::Signature, PeerError, PeerId};

/// A trait for types that can be signed.
pub trait Digest {
    /// Returns the digest of the value.
    fn digest(&self) -> [u8; 32];
}

/// Represents a signed message or value.
///
/// This struct wraps a generic value `M`, associating it with a
/// cryptographic `Signature` and the `PeerId` of the signer.
/// It implements `Deref` and `AsRef` to allow for easy access to the inner value.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Signed<M: Digest> {
    /// The inner value that was signed.
    value: M,
    /// The signature of the `value`.
    signature: Signature,
    /// The ID of the peer that created the signature.
    peer: PeerId,
}

impl<M: Digest> Signed<M> {
    /// Creates a new `Signed` wrapper.
    pub fn new(value: M, signature: Signature, peer: PeerId) -> Self {
        Self {
            value,
            signature,
            peer,
        }
    }

    /// Borrows a reference to the wrapped value.
    pub fn inner(&self) -> &M {
        &self.value
    }
}

impl<M: Digest> AsRef<M> for Signed<M> {
    fn as_ref(&self) -> &M {
        &self.value
    }
}

impl<M: Digest> Deref for Signed<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<M: Digest> Digest for Signed<M> {
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.value.digest());
        hasher.update(&self.signature.digest());
        hasher.update(&self.peer.to_string().as_bytes());
        hasher.finalize().into()
    }
}

impl<M: Digest + fmt::Display> fmt::Display for Signed<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (signed by {})", self.value, self.peer)
    }
}

/// A trait for data structures that wrap a value `M` requiring
/// cryptographic verification.
///
/// This is implemented by types like `Signed<M>` that hold un-trusted data
/// which must be authenticated before use.
pub trait Verifiable<M: Digest> {
    /// Verifies the integrity and authenticity of the wrapped data.
    ///
    /// Implementations should use the provided `keystore` to look up the
    /// public key of the signing peer and validate the signature.
    ///
    /// # Returns
    ///
    /// Returns `Ok(M)` containing the inner value if verification succeeds.
    ///
    /// # Errors
    ///
    /// Returns `Err(PeerError)` if verification fails (e.g., invalid signature,
    /// unknown peer).
    fn verify(self, keystore: &KeyStore) -> Result<M, PeerError>;
}

impl<M: Digest + Verifiable<M>> Verifiable<Signed<M>> for Signed<M> {
    /// Verifies the signature using the public key associated with `self.peer`.
    ///
    /// The public key is retrieved from the `keystore`.
    ///
    /// # Errors
    ///
    /// Returns `Err(PeerError)` if the peer is unknown or the signature is invalid.
    fn verify(mut self, keystore: &KeyStore) -> Result<Signed<M>, PeerError> {
        let public_key = keystore.get(&self.peer).ok_or(PeerError::UnknownPeer {
            peer_id: self.peer.to_string(),
        })?;
        self.value = self.value.verify(keystore)?;

        if public_key.verify(&self.value.digest(), &self.signature) {
            Ok(self)
        } else {
            Err(PeerError::InvalidSignature {
                peer_id: self.peer.to_string(),
            })
        }
    }
}

/* Commented out because specialization is not stable yet
   and this implementation is not needed for the current use case.
   It can be uncommented when specialization becomes stable or if needed in the future.
impl<M: Digest> Verifiable<Signed<M>> for Signed<M> {
    /// Verifies the signature using the public key associated with `self.peer`.
    ///
    /// The public key is retrieved from the `keystore`.
    ///
    /// # Errors
    ///
    /// Returns `Err(PeerError)` if the peer is unknown or the signature is invalid.
    default fn verify(mut self, keystore: &KeyStore) -> Result<Signed<M>, PeerError> {
        let public_key = keystore.get(&self.peer).ok_or(PeerError::UnknownPeer {
            peer_id: self.peer.to_string(),
        })?;

        if public_key.verify(&self.value.digest(), &self.signature) {
            Ok(self)
        } else {
            Err(PeerError::InvalidSignature {
                peer_id: self.peer.to_string(),
            })
        }
    }
}
*/

impl<M: Digest> AsRef<Signed<M>> for Signed<M> {
    fn as_ref(&self) -> &Signed<M> {
        self
    }
}

macro_rules! impl_hashable_primitive {
    ($($t:ty),*) => {
        $(
            impl Digest for $t {
                fn digest(&self) -> [u8; 32] {
                    blake3::hash(&self.to_le_bytes()).into()
                }
            }

            impl Verifiable<$t> for $t {
                /// For primitive types, verification is a no-op.
                /// This requires the type to be `Digest`.
                fn verify(self, _keystore: &KeyStore) -> Result<$t, PeerError> {
                    Ok(self)
                }
            }
        )*
    };
}

impl_hashable_primitive!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

impl Digest for bool {
    fn digest(&self) -> [u8; 32] {
        blake3::hash(&[if *self { 1 } else { 0 }]).into()
    }
}

impl Verifiable<bool> for bool {
    fn verify(self, _keystore: &KeyStore) -> Result<bool, PeerError> {
        Ok(self)
    }
}

impl Digest for String {
    fn digest(&self) -> [u8; 32] {
        blake3::hash(self.as_bytes()).into()
    }
}

impl<T: Digest> Digest for Vec<T> {
    /// Hashes a `Vec` by hashing its elements in order.
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for item in self {
            hasher.update(&item.digest());
        }
        hasher.finalize().into()
    }
}

impl<T: Digest + Ord> Digest for BTreeSet<T> {
    /// Hashes a `BTreeSet` by hashing its elements in sorted order.
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for item in self {
            hasher.update(&item.digest());
        }
        hasher.finalize().into()
    }
}

impl<K: Digest + Ord, V: Digest> Digest for BTreeMap<K, V> {
    /// Hashes a `BTreeMap` by hashing its (key, value) pairs in sorted key order.
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        for (key, value) in self {
            hasher.update(&key.digest());
            hasher.update(&value.digest());
        }
        hasher.finalize().into()
    }
}

impl<K, V> Digest for HashMap<K, V>
where
    K: Digest + Eq + std::hash::Hash + Ord,
    V: Digest,
{
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        let mut keys: Vec<&K> = self.keys().collect();
        keys.sort();

        for key in keys {
            hasher.update(&key.digest());
            hasher.update(&self.get(key).expect("Key not found").digest());
        }
        hasher.finalize().into()
    }
}

impl Verifiable<String> for String {
    /// For `String`, verification is a no-op.
    fn verify(self, _keystore: &KeyStore) -> Result<String, PeerError> {
        Ok(self)
    }
}

impl<T, M> Verifiable<Vec<M>> for Vec<T>
where
    T: Verifiable<M>, // The inner type T can be verified into M
    M: Digest,        // The resulting type M must be Digest
{
    /// Verifies all items in a `Vec`.
    ///
    /// This will transform a `Vec<Signed<T>>` into a `Result<Vec<T>, _>`.
    /// It will also handle `Vec<String>` as a no-op, returning `Result<Vec<String>, _>`.
    fn verify(self, keystore: &KeyStore) -> Result<Vec<M>, PeerError> {
        self.into_iter().map(|item| item.verify(keystore)).collect()
    }
}

impl<T, M> Verifiable<BTreeSet<M>> for BTreeSet<T>
where
    T: Verifiable<M>,
    M: Digest + Ord, // The verified type M must be Ord for BTreeSet
{
    /// Verifies all items in a `BTreeSet`.
    fn verify(self, keystore: &KeyStore) -> Result<BTreeSet<M>, PeerError> {
        self.into_iter().map(|item| item.verify(keystore)).collect()
    }
}

impl<Ku, Vu, K, V> Verifiable<BTreeMap<K, V>> for BTreeMap<Ku, Vu>
where
    Ku: Verifiable<K>, // Unverified Key -> Verified Key
    Vu: Verifiable<V>, // Unverified Value -> Verified Value
    K: Digest + Ord,   // Verified Key must be Digest and Ord
    V: Digest,
{
    /// Verifies all keys and values in a `BTreeMap`.
    fn verify(self, keystore: &KeyStore) -> Result<BTreeMap<K, V>, PeerError> {
        self.into_iter()
            .map(|(k_unverified, v_unverified)| {
                let k_verified = k_unverified.verify(keystore)?;
                let v_verified = v_unverified.verify(keystore)?;
                Ok((k_verified, v_verified))
            })
            .collect()
    }
}

impl<Ku, Vu, K, V> Verifiable<HashMap<K, V>> for HashMap<Ku, Vu>
where
    Ku: Verifiable<K>,                      // Unverified Key -> Verified Key
    Vu: Verifiable<V>,                      // Unverified Value -> Verified Value
    K: Digest + Eq + std::hash::Hash + Ord, // Verified Key must be Digest, Eq, and std::hash::Hash
    V: Digest,
{
    /// Verifies all keys and values in a `HashMap`.
    fn verify(self, keystore: &KeyStore) -> Result<HashMap<K, V>, PeerError> {
        self.into_iter()
            .map(|(k_unverified, v_unverified)| {
                let k_verified = k_unverified.verify(keystore)?;
                let v_verified = v_unverified.verify(keystore)?;
                Ok((k_verified, v_verified))
            })
            .collect()
    }
}

impl<T: Digest> Digest for Option<T> {
    /// Hashes an `Option` by hashing a discriminant (`[0]` for `None`, `[1]` for `Some`)
    /// followed by the hash of the inner value if it exists.
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        match self {
            Some(value) => {
                hasher.update(&[1]); // Discriminant for Some
                hasher.update(&value.digest());
            }
            None => {
                hasher.update(&[0]); // Discriminant for None
            }
        }
        hasher.finalize().into()
    }
}

impl<T: Digest, E> Digest for Result<T, E> {
    /// Hashes a `Result` by hashing a discriminant (`[0]` for `Err`, `[1]` for `Ok`)
    /// followed by the hash of the inner value.
    fn digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        match self {
            Ok(value) => {
                hasher.update(&[1]); // Discriminant for Ok
                hasher.update(&value.digest());
            }
            Err(_) => {
                hasher.update(&[0]); // Discriminant for Err
            }
        }
        hasher.finalize().into()
    }
}

impl<T, M> Verifiable<Option<M>> for Option<T>
where
    T: Verifiable<M>, // The unverified inner T can be verified into M
    M: Digest,        // The resulting M must be Digest
{
    /// Verifies the inner value if it is `Some`.
    ///
    /// This transforms an `Option<Signed<T>>` into a `Result<Option<T>, _>`.
    fn verify(self, keystore: &KeyStore) -> Result<Option<M>, PeerError> {
        match self {
            Some(unverified_value) => {
                let verified_value = unverified_value.verify(keystore)?;
                Ok(Some(verified_value))
            }
            None => Ok(None),
        }
    }
}

impl<T, E, M> Verifiable<Result<M, E>> for Result<T, E>
where
    T: Verifiable<M>, // Unverified Ok(T) -> Verified Ok(M)
    M: Digest,        // Verified Ok(M) must be Digest
    E: Digest,        // The Err(E) type must still be Digest (to match the Digest impl)
{
    /// Verifies only the `Ok` variant, passing the `Err` variant through unchanged.
    ///
    /// This transforms a `Result<Signed<T>, E>` into a `Result<Result<T, E>, _>`.
    fn verify(self, keystore: &KeyStore) -> Result<Result<M, E>, PeerError> {
        match self {
            Ok(unverified_value) => {
                // Verify the Ok value
                let verified_value = unverified_value.verify(keystore)?;
                Ok(Ok(verified_value))
            }
            Err(error_value) => {
                // Pass the Err value through directly
                Ok(Err(error_value))
            }
        }
    }
}
