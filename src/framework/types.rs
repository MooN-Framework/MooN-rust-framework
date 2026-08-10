//! This module defines the `PeerMask` type, which is a bitmask representing the set of peer slots in a distributed system.
//! Each bit in the mask corresponds to a peer slot, allowing efficient tracking of which peers are active or have been seen in error.
//! The `PeerMask` type provides methods for setting, clearing, and checking bits, as well as iterating over the set bits.
//! Also this module provides space for future types that may be used in the framework.

use crate::framework::config::MAX_PEERS;

/// Bitmask over peer slots. Bit `k` refers to the `k`-th peer in the owner's
/// peer list (own node excluded, sorted by id).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PeerMask(pub u8);

impl PeerMask {
    pub const EMPTY: Self = Self(0);

    /// Return the mask of all peers in the roster, excluding the own peer.
    /// This works because self.0 accesses the first bit of the u8, which corresponds to the first peer slot in the roster.
    #[inline]
    pub fn as_u8(&self) -> u8 {
        self.0
    }

    #[inline]
    pub const fn from_u8(b: u8) -> Self {
        PeerMask(b)
    }

    #[inline]
    pub fn set(&mut self, slot: usize) {
        debug_assert!(slot < MAX_PEERS);
        self.0 |= 1 << slot;
    }

    #[inline]
    pub fn clear(&mut self, slot: usize) {
        debug_assert!(slot < MAX_PEERS);
        self.0 &= !(1 << slot);
    }

    #[inline]
    pub fn contains(self, slot: usize) -> bool {
        debug_assert!(slot < MAX_PEERS);
        (self.0 >> slot) & 1 == 1
    }

    #[inline]
    pub fn count(self) -> u32 {
        self.0.count_ones()
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Iterate set slot indices in ascending order.
    pub fn iter(self) -> impl Iterator<Item = usize> {
        (0..MAX_PEERS).filter(move |i| self.contains(*i))
    }
}
