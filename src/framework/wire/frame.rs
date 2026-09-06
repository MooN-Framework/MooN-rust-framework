//! Framed UDP payload for peer-to-peer traffic.
//!
//! All multi-byte fields are little-endian. `offset` is the byte
//! position from the start of the frame and `size` the field's length,
//! so a field occupies the bytes `offset` through `offset + size - 1`.
//!
//! ```text
//!   offset  size  field
//!   ------  ----  -----------------------------------------------
//!        0     1  node_id
//!        1     8  session_id
//!        9     4  seq_num
//!       13     1  node_state_wire
//!       14     8  timestamp         sender local monotonic ns
//!       22     1  payload_disc
//!       23     B  payload_body      variant-dependent, B bytes
//!   23 + B     4  crc32             over bytes 0 to 22 + B
//! ```
//!
//! The header is 23 bytes (`HEADER_SIZE`) and the CRC trailer 4 bytes
//! (`CRC_SIZE`), so a frame is `23 + B + 4` bytes long. The CRC covers
//! the header and the payload body, not itself.

use crate::framework::config::{MAX_APPLICATION_DATA_SIZE, MAX_TOTAL_NODES};
use crate::framework::state_machine::NodeState;
use crate::framework::traits::CyclePayload;
use crate::framework::types::{NodeIdMask, PeerMask};
use crate::framework::wire::codec::{PayloadError, WireReader, WireWriter};
use crc32fast::Hasher;

const HEADER_SIZE: usize = 23;
const CRC_SIZE: usize = 4;

// seen_mask(1) + active_count(1) + cycle_seq(4) = 6
const STATE_BODY: usize = 6;
// received_from(1) + publisher_candidate(1) + rejoin_vote(1) = 3
const ACK_BODY: usize = 3;
const EXCLUSION_PROPOSAL_BODY: usize = 1;
const TIME_SYNC_REQ_BODY: usize = 8;
const TIME_SYNC_RESP_BODY: usize = 24;
const SYSTEM_STATE_CRC_BODY: usize = 4;
const SNAPSHOT_SCALARS: usize = 1 + 1 + 4 + 4;
const SNAPSHOT_SLOT_SIZE: usize = 1 + 1 + 1 + 4;
// Trailer: 1 byte length prefix + fixed-size buffer for ApplicationData.
// The framework serializes A::Data via `A::Data::to_wire` into the first
// `app_data_len` bytes of this buffer; the remainder is zero-filled and
// contributes to neither the CRC of the framework snapshot nor decoding
// (bounded by the length prefix). Sized once at compile time so
// `MAX_FRAME_SIZE` stays a const.
const SNAPSHOT_APP_DATA_TRAILER: usize = 1 + MAX_APPLICATION_DATA_SIZE;
const SYSTEM_STATE_SNAPSHOT_BODY: usize =
    SNAPSHOT_SCALARS + SNAPSHOT_SLOT_SIZE * MAX_TOTAL_NODES + SNAPSHOT_APP_DATA_TRAILER;
const SYSTEM_STATE_SNAPSHOT_ACK_BODY: usize = 4;
const GO_FAILSAFE_BODY: usize = 1;

const DISC_STATE: u8 = 0x00;
const DISC_RESULT: u8 = 0x01;
const DISC_ACK: u8 = 0x02;
const DISC_TIMESYNC_REQ: u8 = 0x03;
const DISC_TIMESYNC_RESP: u8 = 0x04;
const DISC_EXCLUSION_PROPOSAL: u8 = 0x05;
const DISC_SYSTEM_STATE_CRC: u8 = 0x06;
const DISC_SYSTEM_STATE_SNAPSHOT: u8 = 0x07;
const DISC_SYSTEM_STATE_SNAPSHOT_ACK: u8 = 0x08;
const DISC_INPUT: u8 = 0x09;
const DISC_GO_FAILSAFE: u8 = 0x0A;

/// Bound for user-supplied `CyclePayload::WIRE_SIZE` (I / R). Enforced
/// at compile time via `UdpFrame::_ASSERT_FITS`.
pub const MAX_PAYLOAD_WIRE_SIZE: usize = 128;

const fn max_usize(a: usize, b: usize) -> usize {
    if a > b {
        a
    } else {
        b
    }
}

/// Staging-buffer size used by `UdpFrame::encode` / `compute_crc`, and
/// exported so `transport` can size its receive buffer accordingly.
/// Must fit the largest possible body — either a user payload or an
/// internal framework variant. `SystemStateSnapshot` is currently the
/// largest framework body because it carries the fixed-size
/// `ApplicationData` trailer. Kept as a plain top-level const so the
/// staging arrays inside `compute_crc` and `encode` are non-generic.
pub const STAGING_SIZE: usize = max_usize(MAX_PAYLOAD_WIRE_SIZE, SYSTEM_STATE_SNAPSHOT_BODY);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotEntry {
    pub valid: bool,
    pub id: u8,
    pub health: u8,
    pub probation_cycles_ok: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    TooShort,
    UnknownDiscriminator,
    InvalidPayload,
    CrcMismatch,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailsafeReason {
    Unspecified = 0x00,
    QuorumLost = 0x01,
    StateDivergence = 0x02,
    SelfTestFailed = 0x03,
    SinkSafetyViolation = 0x04,
    LocalFault = 0x05,
    PeerBroadcast = 0x06,
}

impl FailsafeReason {
    #[inline]
    pub fn to_wire(self) -> u8 {
        self as u8
    }
}

impl From<PayloadError> for FrameError {
    fn from(e: PayloadError) -> Self {
        match e {
            PayloadError::TooShort => FrameError::TooShort,
            PayloadError::Invalid => FrameError::InvalidPayload,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Payload<I: CyclePayload, R: CyclePayload> {
    /// State beacon with attested observation mask. Bit `k` = sender saw
    /// its k-th peer this phase, indexed against the sender's peer order
    /// (all nodes except sender, sorted by id). A rejoin request is
    /// signalled implicitly via `node_state == ResyncLostPeer` in the
    /// frame header, not in this payload.
    State {
        seen_mask: PeerMask,
        active_count: u8,
        /// The sender's `current_seq` at the time the beacon was sent,
        /// i.e. which cycle this beacon belongs to.
        ///
        /// Beacons carry no other identity, and a node emits one on
        /// entering the CycleSync barrier and one more on leaving it.
        /// The trailing one routinely arrives at a peer that has already
        /// left, which makes it indistinguishable from a beacon for the
        /// next barrier unless the cycle is named explicitly. Since
        /// `current_seq` is part of the system-state CRC, every node in
        /// step carries the same value here.
        cycle_seq: u32,
    },
    /// Sensor input attested this cycle. Shared during ShareInputs so
    /// every node can gate for input divergence.
    Input(I),
    Result(R),
    /// Ack beacon. `received_from` attests which peer results this node
    /// ingested this cycle. `publisher_candidate` is the sender's pick.
    /// `rejoin_vote` carries the sender's vote to admit lost peers back:
    /// bit `k` set = sender confirms rejoin for the node with id `k`.
    /// Empty mask means no rejoin endorsed this cycle. Note the
    /// different index space from `received_from`: the rejoin vote is
    /// id-indexed (`NodeIdMask`) because every node ANDs it with its
    /// peers' votes directly, without position translation.
    Ack {
        received_from: PeerMask,
        publisher_candidate: u8,
        rejoin_vote: NodeIdMask,
    },
    ExclusionProposal {
        propose_exclude: PeerMask,
    },
    TimeSyncReq {
        t1: u64,
    },
    TimeSyncResp {
        t1: u64,
        t2: u64,
        t3: u64,
    },
    SystemStateCrc {
        crc: u32,
    },
    SystemStateSnapshot {
        nominal_participants: u8,
        min_participants: u8,
        probation_cycles: u32,
        current_seq: u32,
        entries: [SnapshotEntry; MAX_TOTAL_NODES],
        /// Application-data trailer. `app_data_len` is the number of
        /// valid bytes at the start of `app_data`; the rest is
        /// zero-padded. Layers above deserialize via
        /// `ApplicationData::from_wire` on `&app_data[..app_data_len]`.
        /// For `NoApplicationData` this is always `(0, [0u8; N])`.
        app_data_len: u8,
        app_data: [u8; MAX_APPLICATION_DATA_SIZE],
    },
    /// Receiver's attestation of the snapshot it adopted. `adopted_crc`
    /// is the receiver-side CRC over the applied state — allows senders
    /// to detect if their own snapshot was in the minority.
    SystemStateSnapshotAck {
        adopted_crc: u32,
    },
    GoFailsafe {
        reason: u8,
    },
}

impl<I: CyclePayload, R: CyclePayload> Payload<I, R> {
    /// One-byte discriminator written after the header.
    pub fn discriminator(&self) -> u8 {
        match self {
            Payload::State { .. } => DISC_STATE,
            Payload::Input(_) => DISC_INPUT,
            Payload::Result(_) => DISC_RESULT,
            Payload::Ack { .. } => DISC_ACK,
            Payload::ExclusionProposal { .. } => DISC_EXCLUSION_PROPOSAL,
            Payload::TimeSyncReq { .. } => DISC_TIMESYNC_REQ,
            Payload::TimeSyncResp { .. } => DISC_TIMESYNC_RESP,
            Payload::SystemStateCrc { .. } => DISC_SYSTEM_STATE_CRC,
            Payload::SystemStateSnapshot { .. } => DISC_SYSTEM_STATE_SNAPSHOT,
            Payload::SystemStateSnapshotAck { .. } => DISC_SYSTEM_STATE_SNAPSHOT_ACK,
            Payload::GoFailsafe { .. } => DISC_GO_FAILSAFE,
        }
    }

    /// Serialize the payload body (everything after the discriminator).
    /// Shared by `UdpFrame::encode` and `UdpFrame::compute_crc`.
    pub fn write_body(&self, w: &mut WireWriter<'_>) {
        match self {
            Payload::State {
                seen_mask,
                active_count,
                cycle_seq,
            } => {
                w.push_u8(seen_mask.as_u8());
                w.push_u8(*active_count);
                w.push_u32(*cycle_seq);
            }
            Payload::Input(i) => {
                i.to_wire(w);
            }
            Payload::Result(r) => {
                r.to_wire(w);
            }
            Payload::Ack {
                received_from,
                publisher_candidate,
                rejoin_vote,
            } => {
                w.push_u8(received_from.as_u8());
                w.push_u8(*publisher_candidate);
                w.push_u8(rejoin_vote.as_u8());
            }
            Payload::ExclusionProposal { propose_exclude } => {
                w.push_u8(propose_exclude.as_u8());
            }
            Payload::TimeSyncReq { t1 } => {
                w.push_u64(*t1);
            }
            Payload::TimeSyncResp { t1, t2, t3 } => {
                w.push_u64(*t1);
                w.push_u64(*t2);
                w.push_u64(*t3);
            }
            Payload::SystemStateCrc { crc } => {
                w.push_u32(*crc);
            }
            Payload::SystemStateSnapshot {
                nominal_participants,
                min_participants,
                probation_cycles,
                current_seq,
                entries,
                app_data_len,
                app_data,
            } => {
                w.push_u8(*nominal_participants);
                w.push_u8(*min_participants);
                w.push_u32(*probation_cycles);
                w.push_u32(*current_seq);
                for e in entries.iter() {
                    w.push_bool(e.valid);
                    w.push_u8(e.id);
                    w.push_u8(e.health);
                    w.push_u32(e.probation_cycles_ok);
                }
                w.push_u8(*app_data_len);
                for b in app_data.iter() {
                    w.push_u8(*b);
                }
            }
            Payload::SystemStateSnapshotAck { adopted_crc } => {
                w.push_u32(*adopted_crc);
            }
            Payload::GoFailsafe { reason } => {
                w.push_u8(*reason);
            }
        }
    }

    /// Deserialize the payload body given its discriminator. Callers
    /// pass a reader positioned at the start of the body.
    pub fn read_body(disc: u8, r: &mut WireReader<'_>) -> Result<Self, FrameError> {
        Ok(match disc {
            DISC_STATE => Payload::State {
                seen_mask: PeerMask::from_u8(r.read_u8()?),
                active_count: r.read_u8()?,
                cycle_seq: r.read_u32()?,
            },
            DISC_INPUT => Payload::Input(I::from_wire(r)?),
            DISC_RESULT => Payload::Result(R::from_wire(r)?),
            DISC_ACK => Payload::Ack {
                received_from: PeerMask::from_u8(r.read_u8()?),
                publisher_candidate: r.read_u8()?,
                rejoin_vote: NodeIdMask::from_u8(r.read_u8()?),
            },
            DISC_EXCLUSION_PROPOSAL => Payload::ExclusionProposal {
                propose_exclude: PeerMask::from_u8(r.read_u8()?),
            },
            DISC_TIMESYNC_REQ => Payload::TimeSyncReq { t1: r.read_u64()? },
            DISC_TIMESYNC_RESP => Payload::TimeSyncResp {
                t1: r.read_u64()?,
                t2: r.read_u64()?,
                t3: r.read_u64()?,
            },
            DISC_SYSTEM_STATE_CRC => Payload::SystemStateCrc { crc: r.read_u32()? },
            DISC_SYSTEM_STATE_SNAPSHOT => {
                let nominal_participants = r.read_u8()?;
                let min_participants = r.read_u8()?;
                let probation_cycles = r.read_u32()?;
                let current_seq = r.read_u32()?;
                let mut entries = [SnapshotEntry::default(); MAX_TOTAL_NODES];
                for e in entries.iter_mut() {
                    e.valid = r.read_bool()?;
                    e.id = r.read_u8()?;
                    e.health = r.read_u8()?;
                    e.probation_cycles_ok = r.read_u32()?;
                }
                let app_data_len = r.read_u8()?;
                if app_data_len as usize > MAX_APPLICATION_DATA_SIZE {
                    return Err(FrameError::InvalidPayload);
                }
                let mut app_data = [0u8; MAX_APPLICATION_DATA_SIZE];
                for b in app_data.iter_mut() {
                    *b = r.read_u8()?;
                }
                Payload::SystemStateSnapshot {
                    nominal_participants,
                    min_participants,
                    probation_cycles,
                    current_seq,
                    entries,
                    app_data_len,
                    app_data,
                }
            }
            DISC_SYSTEM_STATE_SNAPSHOT_ACK => Payload::SystemStateSnapshotAck {
                adopted_crc: r.read_u32()?,
            },
            DISC_GO_FAILSAFE => Payload::GoFailsafe {
                reason: r.read_u8()?,
            },
            _ => return Err(FrameError::UnknownDiscriminator),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UdpFrame<I: CyclePayload, R: CyclePayload> {
    node_id: u8,
    session_id: u64,
    seq_num: u32,
    node_state_wire: u8,
    timestamp: u64,
    payload: Payload<I, R>,
    crc32: u32,
}

impl<I: CyclePayload, R: CyclePayload> UdpFrame<I, R> {
    const _ASSERT_FITS: () = assert!(
        I::WIRE_SIZE <= MAX_PAYLOAD_WIRE_SIZE && R::WIRE_SIZE <= MAX_PAYLOAD_WIRE_SIZE,
        "CyclePayload::WIRE_SIZE exceeds MAX_PAYLOAD_WIRE_SIZE"
    );

    pub const MAX_FRAME_SIZE: usize = {
        let body = max_usize(I::WIRE_SIZE, R::WIRE_SIZE);
        let body = max_usize(body, ACK_BODY);
        let body = max_usize(body, TIME_SYNC_REQ_BODY);
        let body = max_usize(body, TIME_SYNC_RESP_BODY);
        let body = max_usize(body, STATE_BODY);
        let body = max_usize(body, EXCLUSION_PROPOSAL_BODY);
        let body = max_usize(body, SYSTEM_STATE_CRC_BODY);
        let body = max_usize(body, SYSTEM_STATE_SNAPSHOT_BODY);
        let body = max_usize(body, SYSTEM_STATE_SNAPSHOT_ACK_BODY);
        let body = max_usize(body, GO_FAILSAFE_BODY);
        HEADER_SIZE + body + CRC_SIZE
    };

    /// Construct a frame around any payload. The CRC is computed once at
    /// construction and cached.
    pub fn new(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        payload: Payload<I, R>,
    ) -> Self {
        let mut f = Self {
            node_id,
            session_id,
            seq_num,
            node_state_wire: node_state.to_wire(),
            timestamp,
            payload,
            crc32: 0,
        };
        f.crc32 = f.compute_crc();
        f
    }

    pub fn node_id(&self) -> u8 {
        self.node_id
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
    pub fn seq_num(&self) -> u32 {
        self.seq_num
    }
    pub fn node_state_wire(&self) -> u8 {
        self.node_state_wire
    }
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
    pub fn payload(&self) -> Payload<I, R> {
        self.payload
    }

    fn compute_crc(&self) -> u32 {
        let _: () = Self::_ASSERT_FITS;
        let mut h = Hasher::new();
        h.update(&[self.node_id]);
        h.update(&self.session_id.to_le_bytes());
        h.update(&self.seq_num.to_le_bytes());
        h.update(&[self.node_state_wire]);
        h.update(&self.timestamp.to_le_bytes());
        h.update(&[self.payload.discriminator()]);
        let mut staging = [0u8; STAGING_SIZE];
        let n = {
            let mut w = WireWriter::new(&mut staging);
            self.payload.write_body(&mut w);
            w.written()
        };
        h.update(&staging[..n]);
        h.finalize()
    }

    pub fn verify(&self) -> bool {
        self.crc32 == self.compute_crc()
    }

    pub fn encode(&self) -> Vec<u8> {
        let _: () = Self::_ASSERT_FITS;

        let mut buf = Vec::with_capacity(Self::MAX_FRAME_SIZE);
        buf.push(self.node_id);
        buf.extend_from_slice(&self.session_id.to_le_bytes());
        buf.extend_from_slice(&self.seq_num.to_le_bytes());
        buf.push(self.node_state_wire);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        buf.push(self.payload.discriminator());
        let mut staging = [0u8; STAGING_SIZE];
        let n = {
            let mut w = WireWriter::new(&mut staging);
            self.payload.write_body(&mut w);
            w.written()
        };
        buf.extend_from_slice(&staging[..n]);
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        let _: () = Self::_ASSERT_FITS;

        if bytes.len() < HEADER_SIZE + CRC_SIZE {
            return Err(FrameError::TooShort);
        }
        let node_id = bytes[0];
        let session_id = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let seq_num = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
        let node_state_wire = bytes[13];
        let timestamp = u64::from_le_bytes(bytes[14..22].try_into().unwrap());
        let disc = bytes[22];

        let body_end = bytes.len() - CRC_SIZE;
        let mut r = WireReader::new(&bytes[HEADER_SIZE..body_end]);
        let payload = Payload::<I, R>::read_body(disc, &mut r)?;
        let body_size = r.consumed();

        let crc_off = HEADER_SIZE + body_size;
        if bytes.len() < crc_off + CRC_SIZE {
            return Err(FrameError::TooShort);
        }
        let crc32 = u32::from_le_bytes(bytes[crc_off..crc_off + CRC_SIZE].try_into().unwrap());

        let frame = UdpFrame {
            node_id,
            session_id,
            seq_num,
            node_state_wire,
            timestamp,
            payload,
            crc32,
        };
        if !frame.verify() {
            return Err(FrameError::CrcMismatch);
        }
        Ok(frame)
    }
}

#[cfg(test)]
mod tests {
    //! Wire-level regression tests for the SystemStateSnapshot layout
    //! after the `ApplicationData` trailer was added. These are the
    //! tests that would have failed loudly on the `STAGING_SIZE`
    //! overflow (the panic in `encode` when the snapshot body exceeds
    //! `MAX_PAYLOAD_WIRE_SIZE`) — worth having a fast unit-level
    //! guard for it since the failure mode at runtime was subtle
    //! (rejoin tests hang instead of erroring cleanly).

    use super::*;
    use crate::framework::state_machine::NodeState;
    use crate::framework::traits::CyclePayload;
    use crate::framework::wire::codec::{PayloadError, WireReader, WireWriter};

    /// Minimal `CyclePayload` for wiring up a generic frame — the
    /// SystemStateSnapshot variant doesn't carry I or R, but the frame
    /// type still needs concrete types to instantiate.
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct DummyPayload;
    impl CyclePayload for DummyPayload {
        const WIRE_SIZE: usize = 0;
        fn to_wire(&self, _w: &mut WireWriter<'_>) {}
        fn from_wire(_r: &mut WireReader<'_>) -> Result<Self, PayloadError> {
            Ok(Self)
        }
    }

    fn full_entries() -> [SnapshotEntry; MAX_TOTAL_NODES] {
        let mut e = [SnapshotEntry::default(); MAX_TOTAL_NODES];
        // Populate every slot so we serialize a maximum-size snapshot
        // — this is what triggers the STAGING_SIZE ceiling.
        for (i, slot) in e.iter_mut().enumerate() {
            slot.valid = true;
            slot.id = i as u8;
            slot.health = (i % 3) as u8;
            slot.probation_cycles_ok = i as u32 * 17;
        }
        e
    }

    fn full_app_data() -> [u8; MAX_APPLICATION_DATA_SIZE] {
        let mut buf = [0u8; MAX_APPLICATION_DATA_SIZE];
        // Deterministic pattern so byte-for-byte comparison after
        // decode catches truncation or misalignment.
        for (i, b) in buf.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        buf
    }

    /// The critical regression test: encoding a fully-populated
    /// SystemStateSnapshot (max entries + max-length app_data trailer)
    /// must NOT panic on staging-buffer overflow. Before the
    /// `STAGING_SIZE` fix this panicked inside `WireWriter::take` with
    /// an index-out-of-bounds on the 128-byte staging slice.
    #[test]
    fn full_size_snapshot_encodes_without_panic() {
        let payload = Payload::<DummyPayload, DummyPayload>::SystemStateSnapshot {
            nominal_participants: 8,
            min_participants: 2,
            probation_cycles: 10,
            current_seq: 42,
            entries: full_entries(),
            app_data_len: MAX_APPLICATION_DATA_SIZE as u8,
            app_data: full_app_data(),
        };

        let frame = UdpFrame::<DummyPayload, DummyPayload>::new(
            0,
            0xDEAD_BEEF,
            1,
            NodeState::SystemStateSync,
            123_456_789,
            payload,
        );

        // `encode` runs the same write_body path as `compute_crc` — if
        // one panics, so does the other. Not asserting on length here
        // (that would be brittle across body-layout changes); just
        // asserting the call returns.
        let bytes = frame.encode();
        assert!(
            !bytes.is_empty(),
            "encoded frame must produce at least the header"
        );
    }

    /// End-to-end wire round-trip: encode → decode → compare. If the
    /// app_data trailer is lost, misaligned, or truncated anywhere in
    /// the codec pipeline this test catches it.
    #[test]
    fn snapshot_with_app_data_round_trips() {
        let entries = full_entries();
        let app_data = full_app_data();

        let payload_in = Payload::<DummyPayload, DummyPayload>::SystemStateSnapshot {
            nominal_participants: 5,
            min_participants: 3,
            probation_cycles: 7,
            current_seq: 99,
            entries,
            app_data_len: 40, // partial fill — length prefix must survive
            app_data,
        };

        let frame_in = UdpFrame::<DummyPayload, DummyPayload>::new(
            2,
            0x1234_5678_9ABC_DEF0,
            99,
            NodeState::SystemStateSync,
            555,
            payload_in,
        );

        let bytes = frame_in.encode();
        let frame_out = UdpFrame::<DummyPayload, DummyPayload>::decode(&bytes)
            .expect("decode must succeed on a well-formed frame");

        // Header fields survive.
        assert_eq!(frame_out.node_id(), 2);
        assert_eq!(frame_out.session_id(), 0x1234_5678_9ABC_DEF0);
        assert_eq!(frame_out.seq_num(), 99);

        // Payload survives with every field intact.
        match frame_out.payload() {
            Payload::SystemStateSnapshot {
                nominal_participants,
                min_participants,
                probation_cycles,
                current_seq,
                entries: got_entries,
                app_data_len,
                app_data: got_app_data,
            } => {
                assert_eq!(nominal_participants, 5);
                assert_eq!(min_participants, 3);
                assert_eq!(probation_cycles, 7);
                assert_eq!(current_seq, 99);
                assert_eq!(got_entries, entries);
                assert_eq!(app_data_len, 40);
                assert_eq!(got_app_data, app_data);
            }
            other => panic!("expected SystemStateSnapshot, got {:?}", other),
        }
    }

    /// The decoder must reject a snapshot whose length prefix claims
    /// more bytes than the trailer can hold. This is a defensive check
    /// against a hostile or corrupted frame — the buffer itself is
    /// always exactly `MAX_APPLICATION_DATA_SIZE` bytes on the wire,
    /// but the length prefix could theoretically be up to 255.
    #[test]
    fn decoder_rejects_oversized_app_data_len() {
        // Build a byte stream by hand — start from a valid snapshot,
        // then overwrite the len prefix byte with 0xFF.
        let payload = Payload::<DummyPayload, DummyPayload>::SystemStateSnapshot {
            nominal_participants: 3,
            min_participants: 2,
            probation_cycles: 10,
            current_seq: 0,
            entries: full_entries(),
            app_data_len: 0,
            app_data: [0u8; MAX_APPLICATION_DATA_SIZE],
        };
        let frame = UdpFrame::<DummyPayload, DummyPayload>::new(
            0,
            0,
            0,
            NodeState::SystemStateSync,
            0,
            payload,
        );
        let mut bytes = frame.encode();

        // Locate the app_data_len byte: header (23) + disc (1) +
        // SNAPSHOT_SCALARS (10) + SNAPSHOT_SLOT_SIZE * MAX_TOTAL_NODES.
        let len_byte_off = 23 + 1 + SNAPSHOT_SCALARS + SNAPSHOT_SLOT_SIZE * MAX_TOTAL_NODES;
        bytes[len_byte_off] = 0xFF;

        // The frame CRC will also be wrong now (we tampered with the
        // body), so decode should reject — either as CrcMismatch or
        // as InvalidPayload depending on which check fires first.
        // Both are acceptable failures; the point is decode does NOT
        // return Ok.
        match UdpFrame::<DummyPayload, DummyPayload>::decode(&bytes) {
            Err(_) => {}
            Ok(_) => panic!("decode must reject an oversized app_data_len"),
        }
    }
}
