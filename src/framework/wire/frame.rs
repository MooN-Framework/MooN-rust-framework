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

use crate::framework::state_machine::NodeState;
use crate::framework::traits::CyclePayload;
use crate::framework::types::PeerMask;
use crate::framework::wire::codec::{PayloadError, WireReader, WireWriter};
use crc32fast::Hasher;

const HEADER_SIZE: usize = 23;
const CRC_SIZE: usize = 4;

const STATE_BODY: usize = 1;
const ACK_BODY: usize = 2;
const EXCLUSION_PROPOSAL_BODY: usize = 1;
const TIME_SYNC_REQ_BODY: usize = 8;
const TIME_SYNC_RESP_BODY: usize = 24;

const DISC_STATE: u8 = 0x00;
const DISC_RESULT: u8 = 0x01;
const DISC_ACK: u8 = 0x02;
const DISC_TIMESYNC_REQ: u8 = 0x03;
const DISC_TIMESYNC_RESP: u8 = 0x04;
const DISC_EXCLUSION_PROPOSAL: u8 = 0x05;

/// Upper bound on `CyclePayload::WIRE_SIZE`. Sizes the stack-allocated
/// staging buffer used during serialization and CRC.
pub const MAX_PAYLOAD_WIRE_SIZE: usize = 64;

const fn max_usize(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
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

/// Payload variants carried inside a `UdpFrame`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Payload<P: CyclePayload> {
    /// State beacon with attested observation mask. Bit `k` = sender saw
    /// its k-th peer this phase, indexed against the sender's peer order
    /// (all nodes except sender, sorted by id).
    State { seen_mask: PeerMask },
    Result(P),
    Ack {
        received_from: PeerMask,
        publisher_candidate: u8,
    },
    /// Exclusion vote in ErrorManagement. Bit interpretation as `State`.
    ExclusionProposal { propose_exclude: PeerMask },
    /// Cristian request; `t1` is the requester send time (ns) on its
    /// local clock.
    TimeSyncReq { t1: u64 },
    /// Cristian response; `t2`, `t3` on the responder clock.
    TimeSyncResp { t1: u64, t2: u64, t3: u64 },
}

/// A framed UDP message.
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

    /// Largest possible encoded frame for payload type `P`.
    pub const MAX_FRAME_SIZE: usize = {
        let body = max_usize(P::WIRE_SIZE, ACK_BODY);
        let body = max_usize(body, TIME_SYNC_REQ_BODY);
        let body = max_usize(body, TIME_SYNC_RESP_BODY);
        let body = max_usize(body, STATE_BODY);
        let body = max_usize(body, EXCLUSION_PROPOSAL_BODY);
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
    ) -> Self {
        Self::new(node_id, session_id, seq_num, node_state, timestamp, Payload::State { seen_mask })
    }

    pub fn result_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        result: P,
    ) -> Self {
        Self::new(node_id, session_id, seq_num, node_state, timestamp, Payload::Result(result))
    }

    pub fn ack_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        timestamp: u64,
        received_from: PeerMask,
        publisher_candidate: u8,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            timestamp,
            Payload::Ack { received_from, publisher_candidate },
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
        Self::new(node_id, session_id, seq_num, node_state, timestamp, Payload::TimeSyncReq { t1 })
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

    pub fn node_id(&self) -> u8 { self.node_id }
    pub fn session_id(&self) -> u64 { self.session_id }
    pub fn seq_num(&self) -> u32 { self.seq_num }
    pub fn node_state_wire(&self) -> u8 { self.node_state_wire }
    pub fn timestamp(&self) -> u64 { self.timestamp }
    pub fn payload(&self) -> Payload<P> { self.payload }

    fn compute_crc(&self) -> u32 {
        let _ = Self::_ASSERT_FITS;
        let mut h = Hasher::new();
        h.update(&[self.node_id]);
        h.update(&self.session_id.to_le_bytes());
        h.update(&self.seq_num.to_le_bytes());
        h.update(&[self.node_state_wire]);
        h.update(&self.timestamp.to_le_bytes());
        match &self.payload {
            Payload::State { seen_mask } => {
                h.update(&[DISC_STATE, seen_mask.as_u8()]);
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
            Payload::Ack { received_from, publisher_candidate } => {
                h.update(&[DISC_ACK, received_from.as_u8(), *publisher_candidate]);
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
        }
        h.finalize()
    }

    /// Recompute the CRC and compare against the stored one.
    pub fn verify(&self) -> bool {
        self.crc32 == self.compute_crc()
    }

    /// Serialize into a fresh byte vector.
    pub fn encode(&self) -> Vec<u8> {
        let _ = Self::_ASSERT_FITS;

        let mut buf = Vec::with_capacity(Self::MAX_FRAME_SIZE);
        buf.push(self.node_id);
        buf.extend_from_slice(&self.session_id.to_le_bytes());
        buf.extend_from_slice(&self.seq_num.to_le_bytes());
        buf.push(self.node_state_wire);
        buf.extend_from_slice(&self.timestamp.to_le_bytes());
        match self.payload {
            Payload::State { seen_mask } => {
                buf.extend_from_slice(&[DISC_STATE, seen_mask.as_u8()]);
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
            Payload::Ack { received_from, publisher_candidate } => {
                buf.extend_from_slice(&[DISC_ACK, received_from.as_u8(), publisher_candidate]);
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
        }
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    /// Parse from raw bytes and verify the CRC.
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
                (Payload::State { seen_mask: PeerMask::from_u8(bytes[HEADER_SIZE]) }, STATE_BODY)
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
                let t1 = u64::from_le_bytes(bytes[HEADER_SIZE..HEADER_SIZE + 8].try_into().unwrap());
                let t2 = u64::from_le_bytes(bytes[HEADER_SIZE + 8..HEADER_SIZE + 16].try_into().unwrap());
                let t3 = u64::from_le_bytes(bytes[HEADER_SIZE + 16..HEADER_SIZE + 24].try_into().unwrap());
                (Payload::TimeSyncResp { t1, t2, t3 }, TIME_SYNC_RESP_BODY)
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
