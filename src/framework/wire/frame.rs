//! Framed UDP payload for peer-to-peer traffic.
//!
//! Wire layout (little-endian):
//! ```text
//!   0        node_id           (1)
//!   1..9     session_id        (8)
//!   9..13    seq_num           (4)
//!   13       node_state_wire   (1)
//!   14..22   timestamp         (8)   sender local monotonic ns
//!   22       payload_disc      (1)
//!   23..X    payload_body      variant-dependent
//!   X..X+4   crc32             (4)
//! ```

use crate::framework::config::MAX_TOTAL_NODES;
use crate::framework::state_machine::NodeState;
use crate::framework::traits::CyclePayload;
use crate::framework::types::PeerMask;
use crate::framework::wire::codec::{PayloadError, WireReader, WireWriter};
use crc32fast::Hasher;

const HEADER_SIZE: usize = 23;
const CRC_SIZE: usize = 4;

const STATE_BODY: usize = 2;
// received_from(1) + publisher_candidate(1) + rejoin_vote(1) = 3
const ACK_BODY: usize = 3;
const EXCLUSION_PROPOSAL_BODY: usize = 1;
const TIME_SYNC_REQ_BODY: usize = 8;
const TIME_SYNC_RESP_BODY: usize = 24;
const SYSTEM_STATE_CRC_BODY: usize = 4;
const SNAPSHOT_SCALARS: usize = 1 + 1 + 4 + 4;
const SNAPSHOT_SLOT_SIZE: usize = 1 + 1 + 1 + 4;
const SYSTEM_STATE_SNAPSHOT_BODY: usize = SNAPSHOT_SCALARS + SNAPSHOT_SLOT_SIZE * MAX_TOTAL_NODES;
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

pub const MAX_PAYLOAD_WIRE_SIZE: usize = 128;

const fn max_usize(a: usize, b: usize) -> usize {
    if a > b {
        a
    } else {
        b
    }
}

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
    },
    /// Sensor input attested this cycle. Shared during ShareInputs so
    /// every node can gate for input divergence.
    Input(I),
    Result(R),
    /// Ack beacon. `received_from` attests which peer results this node
    /// ingested this cycle. `publisher_candidate` is the sender's pick.
    /// `rejoin_vote` carries the sender's vote to admit lost peers back:
    /// bit `k` set = sender confirms rejoin for the peer with id `k`.
    /// Empty mask means no rejoin endorsed this cycle.
    Ack {
        received_from: PeerMask,
        publisher_candidate: u8,
        rejoin_vote: PeerMask,
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
            } => {
                w.push_u8(seen_mask.as_u8());
                w.push_u8(*active_count);
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
            },
            DISC_INPUT => Payload::Input(I::from_wire(r)?),
            DISC_RESULT => Payload::Result(R::from_wire(r)?),
            DISC_ACK => Payload::Ack {
                received_from: PeerMask::from_u8(r.read_u8()?),
                publisher_candidate: r.read_u8()?,
                rejoin_vote: PeerMask::from_u8(r.read_u8()?),
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
                Payload::SystemStateSnapshot {
                    nominal_participants,
                    min_participants,
                    probation_cycles,
                    current_seq,
                    entries,
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
        let mut staging = [0u8; MAX_PAYLOAD_WIRE_SIZE];
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
        let mut staging = [0u8; MAX_PAYLOAD_WIRE_SIZE];
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
