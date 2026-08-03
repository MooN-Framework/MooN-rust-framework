use crate::framework::config::MAX_PEERS;

/// Bitmask over peer slots. Bit `k` refers to the `k`-th peer in the owner's
/// peer list (own node excluded, sorted by id).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PeerMask(pub u8);

impl PeerMask {
    pub const EMPTY: Self = Self(0);

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
