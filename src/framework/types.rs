//! This module defines the `PeerMask` type, which is a bitmask representing the set of peer slots in a distributed system.
//! Each bit in the mask corresponds to a peer slot, allowing efficient tracking of which peers are active or have been seen in error.
//! The `PeerMask` type provides methods for setting, clearing, and checking bits, as well as iterating over the set bits.
//! Also this module provides space for future types that may be used in the framework.

use crate::framework::config::{MAX_PEERS, MAX_TOTAL_NODES};

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

/// Bitmask over node ids, as opposed to `PeerMask`, whose bit order is
/// relative to the owner's own peer list.
///
/// The rejoin vote needs this: every node ANDs its own mask with the
/// masks of its peers, and that only works if all of them index the
/// same way. A `PeerMask` would have to be position-translated per
/// sender first (see `observation::sender_observed`).
///
/// Only ids `0..MAX_TOTAL_NODES` are representable. `set` reports
/// whether the id fit, so a caller can surface the limitation instead
/// of dropping a vote silently.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NodeIdMask(pub u8);

impl NodeIdMask {
    pub const EMPTY: Self = Self(0);

    #[inline]
    pub fn as_u8(&self) -> u8 {
        self.0
    }

    #[inline]
    pub const fn from_u8(b: u8) -> Self {
        NodeIdMask(b)
    }

    /// Set the bit for `node_id`. Returns false when the id is outside
    /// the representable range.
    #[inline]
    pub fn set(&mut self, node_id: u8) -> bool {
        if (node_id as usize) >= MAX_TOTAL_NODES {
            return false;
        }
        self.0 |= 1 << node_id;
        true
    }

    #[inline]
    pub fn contains(self, node_id: u8) -> bool {
        if (node_id as usize) >= MAX_TOTAL_NODES {
            return false;
        }
        (self.0 >> node_id) & 1 == 1
    }

    #[inline]
    pub fn count(self) -> u32 {
        self.0.count_ones()
    }

    #[inline]
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

#[cfg(test)]
mod node_id_mask_tests {
    //! `NodeIdMask` exists because the rejoin vote is ANDed across
    //! nodes without position translation. These tests pin the id
    //! indexing and the out-of-range behaviour that previously tripped
    //! a `debug_assert` inside `PeerMask`.
    use super::*;

    #[test]
    fn set_and_contains_use_the_node_id_directly() {
        let mut m = NodeIdMask::EMPTY;
        assert!(m.set(2));
        assert!(m.contains(2));
        assert!(!m.contains(1));
        assert_eq!(m.as_u8(), 0b0000_0100);
    }

    #[test]
    fn highest_representable_id_fits() {
        let mut m = NodeIdMask::EMPTY;
        assert!(m.set((MAX_TOTAL_NODES - 1) as u8));
        assert_eq!(m.count(), 1);
    }

    #[test]
    fn out_of_range_id_is_reported_not_panicked() {
        let mut m = NodeIdMask::EMPTY;
        assert!(!m.set(MAX_TOTAL_NODES as u8));
        assert!(m.is_empty());
        assert!(!m.contains(MAX_TOTAL_NODES as u8));
    }
}
