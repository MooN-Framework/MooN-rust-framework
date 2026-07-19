use crate::sys_state::traits::CyclePayload;
use crate::sys_state::wire::{PayloadError, WireReader, WireWriter};
use crate::sys_state::state_machine::NodeState;
use crate::sys_state::types::PeerMask;
use crc32fast::Hasher;

// -------------------------------------------------------------
// Byte-Layout (little-endian):
//   0        node_id           (1)
//   1..9     session_id        (8)
//   9..13    seq_num           (4)
//   13       node_state_wire   (1)
//   14       payload_disc      (1)
//   15..X    payload_body      (0 | P::WIRE_SIZE | 2)
//   X..X+4   crc32             (4)
// -------------------------------------------------------------
const HEADER_SIZE: usize = 15;
const CRC_SIZE: usize = 4;
const ACK_BODY: usize = 2;

const DISC_STATE: u8 = 0x00;
const DISC_RESULT: u8 = 0x01;
const DISC_ACK: u8 = 0x02;

/// Obere Grenze fuer `CyclePayload::WIRE_SIZE`.
///
/// Bestimmt die Groesse des stack-allokierten Staging-Puffers fuer
/// Serialisierung und CRC-Berechnung. Wird zur Compile-Zeit geprueft:
/// eine Instantiierung mit `P::WIRE_SIZE > MAX_PAYLOAD_WIRE_SIZE` schlaegt
/// die Kompilierung ab.
///
/// 64 Byte deckt typische Voter-Payloads ab (mehrere `f64` + Flags + Reserve),
/// bleibt aber klein genug fuer allokationsfreien Betrieb auf Embedded-Targets.
pub const MAX_PAYLOAD_WIRE_SIZE: usize = 64;

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

// -------------------------------------------------------------
// Payload
// -------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Payload<P: CyclePayload> {
    State,
    Result(P),
    Ack {
        received_from: PeerMask,
        publisher_candidate: u8,
    },
}

// -------------------------------------------------------------
// Frame
// -------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UdpFrame<P: CyclePayload> {
    node_id: u8,
    session_id: u64,
    seq_num: u32,
    node_state_wire: u8,
    payload: Payload<P>,
    crc32: u32,
}

impl<P: CyclePayload> UdpFrame<P> {
    /// Compile-time Assertion: post-monomorphization error, sobald jemand
    /// versucht `UdpFrame<P>` mit `P::WIRE_SIZE > MAX_PAYLOAD_WIRE_SIZE` zu
    /// instanziieren. Wird in encode/decode referenziert, um die Auswertung
    /// zu erzwingen.
    const _ASSERT_FITS: () = assert!(
        P::WIRE_SIZE <= MAX_PAYLOAD_WIRE_SIZE,
        "CyclePayload::WIRE_SIZE ueberschreitet MAX_PAYLOAD_WIRE_SIZE"
    );

    /// Groesster moeglicher Frame fuer diesen Payload-Typ.
    pub const MAX_FRAME_SIZE: usize = {
        let body = if P::WIRE_SIZE > ACK_BODY { P::WIRE_SIZE } else { ACK_BODY };
        HEADER_SIZE + body + CRC_SIZE
    };

    fn new(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        payload: Payload<P>,
    ) -> Self {
        let mut f = Self {
            node_id,
            session_id,
            seq_num,
            node_state_wire: node_state.to_wire(),
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
        result: P,
    ) -> Self {
        Self::new(node_id, session_id, seq_num, node_state, Payload::Result(result))
    }

    pub fn ack_frame(
        node_id: u8,
        session_id: u64,
        seq_num: u32,
        node_state: NodeState,
        received_from: PeerMask,
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

    pub fn node_id(&self) -> u8 { self.node_id }
    pub fn session_id(&self) -> u64 { self.session_id }
    pub fn seq_num(&self) -> u32 { self.seq_num }
    pub fn node_state_wire(&self) -> u8 { self.node_state_wire }
    pub fn payload(&self) -> Payload<P> { self.payload }

    fn compute_crc(&self) -> u32 {
        // Erzwingt Auswertung der Compile-Time-Assertion.
        let _ = Self::_ASSERT_FITS;

        let mut h = Hasher::new();
        h.update(&[self.node_id]);
        h.update(&self.session_id.to_le_bytes());
        h.update(&self.seq_num.to_le_bytes());
        h.update(&[self.node_state_wire]);

        match &self.payload {
            Payload::State => {
                h.update(&[DISC_STATE]);
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
        let _ = Self::_ASSERT_FITS;

        let mut buf = Vec::with_capacity(Self::MAX_FRAME_SIZE);
        buf.push(self.node_id);
        buf.extend_from_slice(&self.session_id.to_le_bytes());
        buf.extend_from_slice(&self.seq_num.to_le_bytes());
        buf.push(self.node_state_wire);
        match self.payload {
            Payload::State => buf.push(DISC_STATE),
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
        let _ = Self::_ASSERT_FITS;

        if bytes.len() < HEADER_SIZE + CRC_SIZE {
            return Err(FrameError::TooShort);
        }
        let node_id = bytes[0];
        let session_id = u64::from_le_bytes(
            bytes[1..9].try_into().expect("slice length checked above"),
        );
        let seq_num = u32::from_le_bytes(
            bytes[9..13].try_into().expect("slice length checked above"),
        );
        let node_state_wire = bytes[13];
        let disc = bytes[14];

        let (payload, body_size) = match disc {
            DISC_STATE => (Payload::State, 0usize),
            DISC_RESULT => {
                let end = HEADER_SIZE + P::WIRE_SIZE;
                if bytes.len() < end + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let mut r = WireReader::new(&bytes[HEADER_SIZE..end]);
                let value = P::from_wire(&mut r)?;
                (Payload::Result(value), P::WIRE_SIZE)
            }
            DISC_ACK => {
                if bytes.len() < HEADER_SIZE + ACK_BODY + CRC_SIZE {
                    return Err(FrameError::TooShort);
                }
                let mask = PeerMask::from_u8(bytes[HEADER_SIZE]);
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
        let crc32 = u32::from_le_bytes(
            bytes[crc_off..crc_off + CRC_SIZE]
                .try_into()
                .expect("slice length checked above"),
        );

        let frame = UdpFrame {
            node_id,
            session_id,
            seq_num,
            node_state_wire,
            payload,
            crc32,
        };

        if !frame.verify() {
            return Err(FrameError::CrcMismatch);
        }
        Ok(frame)
    }
}