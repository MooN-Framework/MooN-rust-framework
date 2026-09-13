//! Wire format.
//!
//! `codec` is the byte-level reader and writer, `frame` the
//! datagram layout on top of it. Both are fixed-size by design: a
//! payload declares its wire size as a constant, so the cyclic path
//! never allocates and a receive buffer can be sized once at startup.
//!
//! Frame layout, little-endian throughout:
//!
//! ```text
//!   0       node_id           u8
//!   1..9    session_id        u64
//!   9..13   seq_num           u32
//!   13      node_state        u8
//!   14..22  timestamp         u64   monotonic ns, sender clock
//!   22      discriminator     u8    selects the Payload variant
//!   23..n   payload body
//!   n..n+4  crc32             u32   IEEE, over bytes 0..n
//! ```

mod codec;
mod frame;

pub use codec::{PayloadError, WireReader, WireWriter};
pub use frame::{
    FailsafeReason, FrameError, Payload, SnapshotEntry, UdpFrame, MAX_PAYLOAD_WIRE_SIZE,
    STAGING_SIZE,
};
