#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeMask(u8);

// achtung nur
impl NodeMask {
    pub const EMPTY: Self = NodeMask(0);

    pub const fn only(id: u8) -> Self {
        assert!(id < 8);
        NodeMask(1u8 << id)
    }

    pub fn insert(&mut self, id: u8) {
        assert!(id < 8);
        self.0 |= 1u8 << id;
    }

    pub fn contains(&self, id: u8) -> bool {
        assert!(id < 8);
        (self.0 & (1u8 << id)) != 0
    }

    pub fn count(&self) -> u32 {
        self.0.count_ones()
    }

    pub fn as_u8(&self) -> u8 {
        self.0
    }

    pub const fn from_u8(b: u8) -> Self { NodeMask(b) }
}
