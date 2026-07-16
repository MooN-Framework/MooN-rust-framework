#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeMask(u8);

impl NodeMask {
    pub const EMPTY: Self = NodeMask(0);

    /// Mask containing exactly one node.
    pub const fn only(id: u8) -> Self {
        NodeMask(1 << id)
    }

    /// Add a node id to the mask.
    pub fn insert(&mut self, id: u8) {
        self.0 |= 1 << id;
    }

    /// True if the given node id is in the mask.
    pub fn contains(&self, id: u8) -> bool {
        (self.0 >> id) & 1 == 1
    }

    /// How many nodes are represented in the mask.
    pub fn count(&self) -> u32 {
        self.0.count_ones()
    }

    /// Raw byte for CRC/serialization.
    pub fn as_u8(&self) -> u8 {
        self.0
    }

    pub const fn from_u8(b: u8) -> Self {
        NodeMask(b)
    }
}
