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

const DISC_STATE: u8 = 0x00;
const DISC_RESULT: u8 = 0x01;
const DISC_ACK: u8 = 0x02;
const DISC_TIMESYNC_REQ: u8 = 0x03;
const DISC_TIMESYNC_RESP: u8 = 0x04;
const DISC_EXCLUSION_PROPOSAL: u8 = 0x05;
const DISC_SYSTEM_STATE_CRC: u8 = 0x06;
const DISC_SYSTEM_STATE_SNAPSHOT: u8 = 0x07;
const DISC_SYSTEM_STATE_SNAPSHOT_ACK: u8 = 0x08;

pub const MAX_PAYLOAD_WIRE_SIZE: usize = 64;

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

impl From<PayloadError> for FrameError {
    fn from(e: PayloadError) -> Self {
        match e {
            PayloadError::TooShort => FrameError::TooShort,
            PayloadError::Invalid => FrameError::InvalidPayload,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Payload<P: CyclePayload> {
    /// State beacon with attested observation mask. Bit `k` = sender saw
    /// its k-th peer this phase, indexed against the sender's peer order
    /// (all nodes except sender, sorted by id). A rejoin request is
    /// signalled implicitly via `node_state == ResyncLostPeer` in the
    /// frame header, not in this payload.
    State {
        seen_mask: PeerMask,
        active_count: u8,
    },
    Result(P),
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
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UdpFrame<P: CyclePayload> {
    node_id: u8,
    session_id: u64,
    seq_num: u32,
    node_state_wire: u8,
    timestamp: u64,
    payload: Payload<P>,
    crc32: u32,
}

impl<P: CyclePayload> UdpFrame<P> {
    const _ASSERT_FITS: () = assert!(
        P::WIRE_SIZE <= MAX_PAYLOAD_WIRE_SIZE,
        "CyclePayload::WIRE_SIZE exceeds MAX_PAYLOAD_WIRE_SIZE"
    );

    pub const MAX_FRAME_SIZE: usize = {
        let body = max_usize(P::WIRE_SIZE, ACK_BODY);
        let body = max_usize(body, TIME_SYNC_REQ_BODY);
        let body = max_usize(body, TIME_SYNC_RESP_BODY);
        let body = max_usize(body, STATE_BODY);
        let body = max_usize(body, EXCLUSION_PROPOSAL_BODY);
        let body = max_usize(body, SYSTEM_STATE_CRC_BODY);
        let body = max_usize(body, SYSTEM_STATE_SNAPSHOT_BODY);
        let body = max_usize(body, SYSTEM_STATE_SNAPSHOT_ACK_BODY);
        HEADER_SIZE + body + CRC_SIZE
    };

    fn new(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        payload: Payload<P>,
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

    pub fn state_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        seen_mask: PeerMask,
        active_count: u8,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::State {
                seen_mask,
                active_count,
            },
        )
    }

    pub fn result_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        result: P,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::Result(result),
        )
    }

    pub fn ack_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        received_from: PeerMask,
        publisher_candidate: u8,
        rejoin_vote: PeerMask,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::Ack {
                received_from,
                publisher_candidate,
                rejoin_vote,
            },
        )
    }

    pub fn exclusion_proposal_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        propose_exclude: PeerMask,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::ExclusionProposal { propose_exclude },
        )
    }

    pub fn time_sync_req_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        t1: u64,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::TimeSyncReq { t1 },
        )
    }

    pub fn time_sync_resp_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        t1: u64,
        t2: u64,
        t3: u64,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::TimeSyncResp { t1, t2, t3 },
        )
    }

    pub fn system_state_crc_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        crc: u32,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::SystemStateCrc { crc },
        )
    }

    pub fn system_state_snapshot_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        nominal_participants: u8,
        min_participants: u8,
        probation_cycles: u32,
        current_seq: u32,
        entries: [SnapshotEntry; MAX_TOTAL_NODES],
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::SystemStateSnapshot {
                nominal_participants,
                min_participants,
                probation_cycles,
                current_seq,
                entries,
            },
        )
    }

    pub fn system_state_snapshot_ack_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        adopted_crc: u32,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::SystemStateSnapshotAck { adopted_crc },
        )
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
    pub fn payload(&self) -> Payload<P> {
        self.payload
    }

    fn compute_crc(&self) -> u32 {
        let _ = Self::_ASSERT_FITS;
        let mut h = Hasher::new();
        h.update(&[self.node_id]);
        h.update(&self.session_id.to_le_bytes());
        h.update(&self.seq_num.to_le_bytes());
        h.update(&[self.node_state_wire]);
        h.update(&self.timestamp.to_le_bytes());
        match &self.payload {
            Payload::State {
                seen_mask,
                active_count,
            } => {
                h.update(&[DISC_STATE, seen_mask.as_u8(), *active_count]);
            }
            Payload::Result(r) => {
                h.update(&[DISC_RESULT]);
                let mut staging = [0u8; MAX_PAYLOAD_WIRE_SIZE];
                {
                    let mut w = WireWriter::new(&mut staging[..P::WIRE_SIZE]);
                    r.to_wire(&mut w);
                }
                h.update(&staging[..P::WIRE_SIZE]);
            }
            Payload::Ack {
                received_from,
                publisher_candidate,
                rejoin_vote,
            } => {
                h.update(&[
                    DISC_ACK,
                    received_from.as_u8(),
                    *publisher_candidate,
                    rejoin_vote.as_u8(),
                ]);
            }
            Payload::ExclusionProposal { propose_exclude } => {
                h.update(&[DISC_EXCLUSION_PROPOSAL, propose_exclude.as_u8()]);
            }
            Payload::TimeSyncReq { t1 } => {
                h.update(&[DISC_TIMESYNC_REQ]);
                h.update(&t1.to_le_bytes());
            }
            Payload::TimeSyncResp { t1, t2, t3 } => {
                h.update(&[DISC_TIMESYNC_RESP]);
                h.update(&t1.to_le_bytes());
                h.update(&t2.to_le_bytes());
                h.update(&t3.to_le_bytes());
            }
            Payload::SystemStateCrc { crc } => {
                h.update(&[DISC_SYSTEM_STATE_CRC]);
                h.update(&crc.to_le_bytes());
            }
            Payload::SystemStateSnapshot {
                nominal_participants,
                min_participants,
                probation_cycles,
                current_seq,
                entries,
            } => {
                h.update(&[
                    DISC_SYSTEM_STATE_SNAPSHOT,
                    *nominal_participants,
                    *min_participants,
                ]);
                h.update(&probation_cycles.to_le_bytes());
                h.update(&current_seq.to_le_bytes());
                for e in entries.iter() {
                    h.update(&[if e.valid { 1 } else { 0 }, e.id, e.health]);
                    h.update(&e.probation_cycles_ok.to_le_bytes());
                }
            }
            Payload::SystemStateSnapshotAck { adopted_crc } => {
                h.update(&[DISC_SYSTEM_STATE_SNAPSHOT_ACK]);
                h.update(&adopted_crc.to_le_bytes());
            }
        }
        h.finalize()
    }

    pub fn verify(&self) -> bool {
        self.crc32 == self.compute_crc()
    }

    pub fn encode(&self) -> Vec<u8> {
        let _ = Self::_ASSERT_FITS;

        let mut buf = Vec::with_capacity(Self::MAX_FRAME_SIZE);
        buf.push(self.node_id);
        buf.extend_from_slice(&self.session_id.to_le_bytes());
        buf.extend_from_slice(&self.seq_num.to_le_bytes());
        buf.push(self.node_state_wire);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        match self.payload {
            Payload::State {
                seen_mask,
                active_count,
            } => {
                buf.extend_from_slice(&[DISC_STATE, seen_mask.as_u8(), active_count]);
            }
            Payload::Result(r) => {
                buf.push(DISC_RESULT);
                let mut staging = [0u8; MAX_PAYLOAD_WIRE_SIZE];
                {
                    let mut w = WireWriter::new(&mut staging[..P::WIRE_SIZE]);
                    r.to_wire(&mut w);
                }
                buf.extend_from_slice(&staging[..P::WIRE_SIZE]);
            }
            Payload::Ack {
                received_from,
                publisher_candidate,
                rejoin_vote,
            } => {
                buf.extend_from_slice(&[
                    DISC_ACK,
                    received_from.as_u8(),
                    publisher_candidate,
                    rejoin_vote.as_u8(),
                ]);
            }
            Payload::ExclusionProposal { propose_exclude } => {
                buf.extend_from_slice(&[DISC_EXCLUSION_PROPOSAL, propose_exclude.as_u8()]);
            }
            Payload::TimeSyncReq { t1 } => {
                buf.push(DISC_TIMESYNC_REQ);
                buf.extend_from_slice(&t1.to_le_bytes());
            }
            Payload::TimeSyncResp { t1, t2, t3 } => {
                buf.push(DISC_TIMESYNC_RESP);
                buf.extend_from_slice(&t1.to_le_bytes());
                buf.extend_from_slice(&t2.to_le_bytes());
                buf.extend_from_slice(&t3.to_le_bytes());
            }
            Payload::SystemStateCrc { crc } => {
                buf.push(DISC_SYSTEM_STATE_CRC);
                buf.extend_from_slice(&crc.to_le_bytes());
            }
            Payload::SystemStateSnapshot {
                nominal_participants,
                min_participants,
                probation_cycles,
                current_seq,
                entries,
            } => {
                buf.push(DISC_SYSTEM_STATE_SNAPSHOT);
                buf.push(nominal_participants);
                buf.push(min_participants);
                buf.extend_from_slice(&probation_cycles.to_le_bytes());
                buf.extend_from_slice(&current_seq.to_le_bytes());
                for e in entries.iter() {
                    buf.push(if e.valid { 1 } else { 0 });
                    buf.push(e.id);
                    buf.push(e.health);
                    buf.extend_from_slice(&e.probation_cycles_ok.to_le_bytes());
                }
            }
            Payload::SystemStateSnapshotAck { adopted_crc } => {
                buf.push(DISC_SYSTEM_STATE_SNAPSHOT_ACK);
                buf.extend_from_slice(&adopted_crc.to_le_bytes());
            }
        }
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        let _ = Self::_ASSERT_FITS;

        if bytes.len() < HEADER_SIZE + CRC_SIZE {
            return Err(FrameError::TooShort);
        }
        let node_id = bytes[0];
        let session_id = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let seq_num = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
        let node_state_wire = bytes[13];
        let timestamp = u64::from_le_bytes(bytes[14..22].try_into().unwrap());
        let disc = bytes[22];

        let (payload, body_size) = match disc {
            DISC_STATE => {
                if bytes.len() < HEADER_SIZE + STATE_BODY + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let seen_mask = PeerMask::from_u8(bytes[HEADER_SIZE]);
                let active_count = bytes[HEADER_SIZE + 1];
                (
                    Payload::State {
                        seen_mask,
                        active_count,
                    },
                    STATE_BODY,
                )
            }
            DISC_RESULT => {
                let end = HEADER_SIZE + P::WIRE_SIZE;
                if bytes.len() < end + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let mut r = WireReader::new(&bytes[HEADER_SIZE..end]);
                (Payload::Result(P::from_wire(&mut r)?), P::WIRE_SIZE)
            }
            DISC_ACK => {
                if bytes.len() < HEADER_SIZE + ACK_BODY + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                (
                    Payload::Ack {
                        received_from: PeerMask::from_u8(bytes[HEADER_SIZE]),
                        publisher_candidate: bytes[HEADER_SIZE + 1],
                        rejoin_vote: PeerMask::from_u8(bytes[HEADER_SIZE + 2]),
                    },
                    ACK_BODY,
                )
            }
            DISC_EXCLUSION_PROPOSAL => {
                if bytes.len() < HEADER_SIZE + EXCLUSION_PROPOSAL_BODY + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                (
                    Payload::ExclusionProposal {
                        propose_exclude: PeerMask::from_u8(bytes[HEADER_SIZE]),
                    },
                    EXCLUSION_PROPOSAL_BODY,
                )
            }
            DISC_TIMESYNC_REQ => {
                let end = HEADER_SIZE + TIME_SYNC_REQ_BODY;
                if bytes.len() < end + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let t1 = u64::from_le_bytes(bytes[HEADER_SIZE..end].try_into().unwrap());
                (Payload::TimeSyncReq { t1 }, TIME_SYNC_REQ_BODY)
            }
            DISC_TIMESYNC_RESP => {
                let end = HEADER_SIZE + TIME_SYNC_RESP_BODY;
                if bytes.len() < end + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let t1 =
                    u64::from_le_bytes(bytes[HEADER_SIZE..HEADER_SIZE + 8].try_into().unwrap());
                let t2 = u64::from_le_bytes(
                    bytes[HEADER_SIZE + 8..HEADER_SIZE + 16].try_into().unwrap(),
                );
                let t3 = u64::from_le_bytes(
                    bytes[HEADER_SIZE + 16..HEADER_SIZE + 24]
                        .try_into()
                        .unwrap(),
                );
                (Payload::TimeSyncResp { t1, t2, t3 }, TIME_SYNC_RESP_BODY)
            }
            DISC_SYSTEM_STATE_CRC => {
                let end = HEADER_SIZE + SYSTEM_STATE_CRC_BODY;
                if bytes.len() < end + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let crc = u32::from_le_bytes(bytes[HEADER_SIZE..end].try_into().unwrap());
                (Payload::SystemStateCrc { crc }, SYSTEM_STATE_CRC_BODY)
            }
            DISC_SYSTEM_STATE_SNAPSHOT => {
                let end = HEADER_SIZE + SYSTEM_STATE_SNAPSHOT_BODY;
                if bytes.len() < end + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let nominal_participants = bytes[HEADER_SIZE];
                let min_participants = bytes[HEADER_SIZE + 1];
                let probation_cycles =
                    u32::from_le_bytes(bytes[HEADER_SIZE + 2..HEADER_SIZE + 6].try_into().unwrap());
                let current_seq = u32::from_le_bytes(
                    bytes[HEADER_SIZE + 6..HEADER_SIZE + 10].try_into().unwrap(),
                );
                let mut entries = [SnapshotEntry::default(); MAX_TOTAL_NODES];
                let mut off = HEADER_SIZE + SNAPSHOT_SCALARS;
                for e in entries.iter_mut() {
                    e.valid = bytes[off] != 0;
                    e.id = bytes[off + 1];
                    e.health = bytes[off + 2];
                    e.probation_cycles_ok =
                        u32::from_le_bytes(bytes[off + 3..off + 7].try_into().unwrap());
                    off += SNAPSHOT_SLOT_SIZE;
                }
                (
                    Payload::SystemStateSnapshot {
                        nominal_participants,
                        min_participants,
                        probation_cycles,
                        current_seq,
                        entries,
                    },
                    SYSTEM_STATE_SNAPSHOT_BODY,
                )
            }
            DISC_SYSTEM_STATE_SNAPSHOT_ACK => {
                let end = HEADER_SIZE + SYSTEM_STATE_SNAPSHOT_ACK_BODY;
                if bytes.len() < end + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let adopted_crc = u32::from_le_bytes(bytes[HEADER_SIZE..end].try_into().unwrap());
                (
                    Payload::SystemStateSnapshotAck { adopted_crc },
                    SYSTEM_STATE_SNAPSHOT_ACK_BODY,
                )
            }
            _ => return Err(FrameError::UnknownDiscriminator),
        };

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
