// -------------------------------------------------------------
// udp_frame.rs
// -------------------------------------------------------------
use crate::input::braking_curve::BrakeResult;
use crate::net::node_mask::NodeMask;
use crate::sys_state::state_machine::NodeState;
use crc32fast::Hasher;
use std::convert::TryFrom;

// -------------------------------------------------------------
// Wire-Kategorie: field-less Kopie von NodeState nur fürs Frame-Byte.
// -------------------------------------------------------------
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireState {
    Startup = 0,
    Sync = 1,
    Probation = 2,
    Operational = 3,
    Degraded = 4,
    Failsafe = 5,
}

impl WireState {
    pub fn from_state(s: &NodeState) -> Self {
        match s {
            NodeState::Startup => WireState::Startup,
            NodeState::Sync => WireState::Sync,
            NodeState::Probation { .. } => WireState::Probation,
            NodeState::Operational { .. } => WireState::Operational,
            NodeState::Degraded { .. } => WireState::Degraded,
            NodeState::Failsafe => WireState::Failsafe,
        }
    }
}

impl TryFrom<u8> for WireState {
    type Error = ();
    fn try_from(b: u8) -> Result<Self, ()> {
        Ok(match b {
            0 => Self::Startup,
            1 => Self::Sync,
            2 => Self::Probation,
            3 => Self::Operational,
            4 => Self::Degraded,
            5 => Self::Failsafe,
            _ => return Err(()),
        })
    }
}

// -------------------------------------------------------------
// Byte-Layout (little-endian):
//   0        node_id           (1)
//   1..9     session_id        (8)
//   9..13    seq_num           (4)
//   13       wire_state        (1)
//   14       payload_disc      (1)
//   15..X    payload_body      (0 | 10 | 2)
//   X..X+4   crc32             (4)
// -------------------------------------------------------------
const HEADER_SIZE: usize = 15;
const CRC_SIZE: usize = 4;
const RESULT_BODY: usize = 10;
const ACK_BODY: usize = 2;
pub const MAX_FRAME_SIZE: usize = HEADER_SIZE + RESULT_BODY + CRC_SIZE;

const DISC_STATE: u8 = 0x00;
const DISC_RESULT: u8 = 0x01;
const DISC_ACK: u8 = 0x02;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    TooShort,
    UnknownDiscriminator,
    InvalidState,
    CrcMismatch,
}

// -------------------------------------------------------------
// Payload
// -------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Payload {
    State,
    Result(BrakeResult),
    Ack {
        received_from: NodeMask,
        publisher_candidate: u8,
    },
}

// -------------------------------------------------------------
// Frame
// -------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UdpFrame {
    node_id: u8,
    session_id: u64,
    seq_num: u32,
    node_state: WireState,
    payload: Payload,
    crc32: u32,
}

impl UdpFrame {
    fn new(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        payload: Payload,
    ) -> Self {
        let mut f = Self {
            node_id,
            session_id,
            seq_num,
            node_state: WireState::from_state(&node_state),
            payload,
            crc32: 0,
        };
        f.crc32 = f.compute_crc();
        f
    }

    pub fn state_frame(node_id: u8, session_id: u64, seq_num: u32, node_state: NodeState) -> Self {
        Self::new(node_id, session_id, seq_num, node_state, Payload::State)
    }

    pub fn result_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        result: BrakeResult,
    ) -> Self {
        Self::new(node_id, session_id, seq_num, node_state, Payload::Result(result))
    }

    pub fn ack_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        received_from: NodeMask,
        publisher_candidate: u8,
    ) -> Self {
        Self::new(
            node_id,
            session_id,
            seq_num,
            node_state,
            Payload::Ack {
                received_from,
                publisher_candidate,
            },
        )
    }

    // ---- Getter ----
    pub fn node_id(&self) -> u8 { self.node_id }
    pub fn session_id(&self) -> u64 { self.session_id }
    pub fn seq_num(&self) -> u32 { self.seq_num }
    pub fn node_state(&self) -> WireState { self.node_state }
    pub fn payload(&self) -> Payload { self.payload }

    // ---- CRC ----
    fn compute_crc(&self) -> u32 {
        let mut h = Hasher::new();
        h.update(&[self.node_id]);
        h.update(&self.session_id.to_le_bytes());
        h.update(&self.seq_num.to_le_bytes());
        h.update(&[self.node_state as u8]);

        match &self.payload {
            Payload::State => {
                h.update(&[DISC_STATE]);
            }
            Payload::Result(r) => {
                h.update(&[DISC_RESULT]);
                h.update(&r.total_distance.to_le_bytes());
                h.update(&[r.emergency_brake as u8]);
                h.update(&[r.valid_entry as u8]);
            }
            Payload::Ack {
                received_from,
                publisher_candidate,
            } => {
                h.update(&[DISC_ACK]);
                h.update(&[received_from.as_u8()]);
                h.update(&[*publisher_candidate]);
            }
        }
        h.finalize()
    }

    pub fn verify(&self) -> bool {
        self.crc32 == self.compute_crc()
    }

    // ---- Serialisierung ----
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(MAX_FRAME_SIZE);
        buf.push(self.node_id);
        buf.extend_from_slice(&self.session_id.to_le_bytes());
        buf.extend_from_slice(&self.seq_num.to_le_bytes());
        buf.push(self.node_state as u8);
        match self.payload {
            Payload::State => buf.push(DISC_STATE),
            Payload::Result(r) => {
                buf.push(DISC_RESULT);
                buf.extend_from_slice(&r.total_distance.to_le_bytes());
                buf.push(r.emergency_brake as u8);
                buf.push(r.valid_entry as u8);
            }
            Payload::Ack {
                received_from,
                publisher_candidate,
            } => {
                buf.push(DISC_ACK);
                buf.push(received_from.as_u8());
                buf.push(publisher_candidate);
            }
        }
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < HEADER_SIZE + CRC_SIZE {
            return Err(FrameError::TooShort);
        }

        // Länge oben geprüft — try_into().unwrap() ist panic-frei.
        let node_id = bytes[0];
        let session_id = u64::from_le_bytes(bytes[1..9].try_into().unwrap());
        let seq_num = u32::from_le_bytes(bytes[9..13].try_into().unwrap());
        let state_b = bytes[13];
        let disc = bytes[14];

        let (payload, body_size) = match disc {
            DISC_STATE => (Payload::State, 0usize),
            DISC_RESULT => {
                if bytes.len() < HEADER_SIZE + RESULT_BODY + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let td = f64::from_le_bytes(
                    bytes[HEADER_SIZE..HEADER_SIZE + 8].try_into().unwrap(),
                );
                let eb = bytes[HEADER_SIZE + 8] != 0;
                let ve = bytes[HEADER_SIZE + 9] != 0;
                (
                    Payload::Result(BrakeResult {
                        total_distance: td,
                        emergency_brake: eb,
                        valid_entry: ve,
                    }),
                    RESULT_BODY,
                )
            }
            DISC_ACK => {
                if bytes.len() < HEADER_SIZE + ACK_BODY + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let mask = NodeMask::from_u8(bytes[HEADER_SIZE]);
                let cand = bytes[HEADER_SIZE + 1];
                (
                    Payload::Ack {
                        received_from: mask,
                        publisher_candidate: cand,
                    },
                    ACK_BODY,
                )
            }
            _ => return Err(FrameError::UnknownDiscriminator),
        };

        let crc_off = HEADER_SIZE + body_size;
        if bytes.len() < crc_off + CRC_SIZE {
            return Err(FrameError::TooShort);
        }
        let crc32 = u32::from_le_bytes(bytes[crc_off..crc_off + CRC_SIZE].try_into().unwrap());

        let node_state = WireState::try_from(state_b).map_err(|_| FrameError::InvalidState)?;

        let frame = UdpFrame {
            node_id,
            session_id,
            seq_num,
            node_state,
            payload,
            crc32,
        };

        if !frame.verify() {
            return Err(FrameError::CrcMismatch);
        }
        Ok(frame)
    }
}
