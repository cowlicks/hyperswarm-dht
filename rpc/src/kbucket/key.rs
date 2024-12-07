#![allow(
    clippy::ptr_offset_with_cast,
    clippy::assign_op_pattern,
    clippy::transmute_ptr_to_ptr,
    clippy::manual_range_contains
)]
// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::{
    borrow::Borrow,
    convert::TryInto,
    hash::{Hash, Hasher},
};

use sha2::{
    digest::generic_array::{typenum::U32, GenericArray},
    Digest, Sha256,
};
use uint::construct_uint;

use crate::{util::debug_vec, IdBytes, PeerId};

construct_uint! {
    /// 256-bit unsigned integer.
    pub(super) struct U256(4);
}

/// TODO simplify this. `Key` is just the preimage now. Simpify it
/// A `Key` in the DHT keyspace with preserved preimage.
///
/// Keys in the DHT keyspace identify both the participating nodes, as well as
/// the records stored in the DHT.
///
/// `Key`s have an XOR metric as defined in the Kademlia paper, i.e. the bitwise
/// XOR of the hash digests, interpreted as an integer. See [`Key::distance`].
#[derive(Clone, Debug)]
pub struct Key<T> {
    preimage: T,
    bytes: KeyBytes,
}

fn to_keybytes<T: Borrow<[u8]>>(x: &T) -> KeyBytes {
    KeyBytes(*GenericArray::from_slice(x.borrow()))
}

impl<T: Borrow<[u8]>> Key<T> {
    /// Constructs a new `Key` by running the given value through a random
    /// oracle.
    ///
    /// The preimage of type `T` is preserved. See [`Key::preimage`] and
    /// [`Key::into_preimage`].
    pub fn new(preimage: T) -> Key<T> {
        let bytes = to_keybytes(&preimage);
        Key { preimage, bytes }
    }

    /// Borrows the preimage of the key.
    pub fn preimage(&self) -> &T {
        &self.preimage
    }

    /// Converts the key into its preimage.
    pub fn into_preimage(self) -> T {
        self.preimage
    }

    /// Computes the distance of the keys according to the XOR metric.
    pub fn distance<U>(&self, other: &U) -> Distance
    where
        U: AsRef<KeyBytes>,
    {
        let x: &[u8] = self.preimage.borrow();
        distance(x, other.as_ref().0.as_slice())
    }
}

impl<T> From<Key<T>> for KeyBytes {
    fn from(val: Key<T>) -> Self {
        val.bytes
    }
}

impl From<IdBytes> for Key<IdBytes> {
    fn from(n: IdBytes) -> Self {
        Key::new(n)
    }
}

impl From<PeerId> for Key<PeerId> {
    fn from(p: PeerId) -> Self {
        let bytes = to_keybytes(&p);
        Key { preimage: p, bytes }
    }
}

impl<T> AsRef<KeyBytes> for Key<T> {
    fn as_ref(&self) -> &KeyBytes {
        &self.bytes
    }
}

impl<T, U> PartialEq<Key<U>> for Key<T> {
    fn eq(&self, other: &Key<U>) -> bool {
        self.bytes == other.bytes
    }
}

impl<T> Eq for Key<T> {}

impl<T> Hash for Key<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes.0.hash(state);
    }
}

/// The raw bytes of a key in the DHT keyspace.
#[derive(PartialEq, Eq, Clone)]
pub struct KeyBytes(pub GenericArray<u8, U32>);
impl std::fmt::Debug for KeyBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("KeyBytes")
            .field(&debug_vec(&self.0))
            .finish()
    }
}

pub fn distance(a: &[u8], b: &[u8]) -> Distance {
    let a = U256::from(a);
    let b = U256::from(b);
    Distance(a ^ b)
}

impl KeyBytes {
    /// Creates a new key in the DHT keyspace by running the given
    /// value through a random oracle.
    pub fn new<T>(value: T) -> Self
    where
        T: Borrow<[u8]>,
    {
        let x: &[u8] = value.borrow();
        KeyBytes(*GenericArray::from_slice(x))
    }

    /// Computes the distance of the keys according to the XOR metric.
    pub fn distance<U>(&self, other: &U) -> Distance
    where
        U: AsRef<KeyBytes>,
    {
        distance(&self.0, other.as_ref().0.as_slice())
    }

    /// Returns the uniquely determined key with the given distance to `self`.
    ///
    /// This implements the following equivalence:
    ///
    /// `self xor other = distance <==> other = self xor distance`
    pub fn for_distance(&self, d: Distance) -> KeyBytes {
        let key_int = U256::from(self.0.as_slice()) ^ d.0;
        KeyBytes(GenericArray::from(<[u8; 32]>::from(key_int)))
    }
}

impl AsRef<KeyBytes> for KeyBytes {
    fn as_ref(&self) -> &KeyBytes {
        self
    }
}

/// A distance between two keys in the DHT keyspace.
#[derive(Copy, Clone, PartialEq, Eq, Default, PartialOrd, Ord, Debug)]
pub struct Distance(pub(super) U256);
