//! This module implements the transport layer of the framework,
//! which is responsible for sending and receiving messages between nodes in a distributed system.
//! The transport layer uses UDP multicast to communicate with peers, and it provides mechanisms for framing, sequencing,
//! and session management.
//! The transport layer is designed to be efficient and reliable, handling common issues such as packet loss, duplication, and reordering.

use crate::framework::config::TransportConfig;
use crate::framework::state_machine::NodeState;
use crate::framework::traits::CyclePayload;
use crate::framework::wire::{FrameError, Payload, UdpFrame, STAGING_SIZE};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::OnceLock;
use std::time::Instant;

const HEADER_SIZE: usize = 23;
const CRC_SIZE: usize = 4;

/// Upper bound on receive buffer size across all payload variants.
/// Uses `STAGING_SIZE` (max body across all variants, including the
/// framework's `SystemStateSnapshot` with its `ApplicationData`
/// trailer) — not `MAX_PAYLOAD_WIRE_SIZE`, which only bounds
/// user-supplied `CyclePayload` types and would silently truncate
/// larger framework bodies.
pub const RECV_BUFFER_SIZE: usize = HEADER_SIZE + STAGING_SIZE + CRC_SIZE;

/// Classification of an incoming frame after decode + peer-cursor checks.
#[derive(Debug)]
pub enum RecvOutcome<I: CyclePayload, R: CyclePayload> {
    /// Well-formed frame from a known peer with the expected next seq.
    Valid(UdpFrame<I, R>),
    /// No frame arrived within the socket's read timeout.
    Timeout,
    /// Our own frame looped back over multicast.
    SelfLoopback,
    /// Frame with a bad CRC.
    CrcError,
    /// Frame is structurally malformed.
    Malformed(FrameError),
    /// Same or older seq than we last saw for this peer/session.
    Duplicate { peer_id: u8, seen: u32, last: u32 },
    /// Peer's session id changed — likely rebooted.
    NewSession {
        peer_id: u8,
        previous_session: u64,
        new_session: u64,
        frame: UdpFrame<I, R>,
    },
    /// Gap detected between last-seen and this seq.
    SeqGap {
        peer_id: u8,
        gap: u32,
        frame: UdpFrame<I, R>,
    },
    /// Time-sync frame; bypasses seq-num classification.
    /// `local_recv_ns` is the receive timestamp (t2 or t4).
    TimeSync {
        peer_id: u8,
        frame: UdpFrame<I, R>,
        local_recv_ns: u64,
    },
}

#[derive(Debug)]
pub enum TransportError {
    Io(io::Error),
    InterfaceNotFound(String),
}

impl From<io::Error> for TransportError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Debug, Clone, Copy)]
struct PeerCursor {
    session_id: u64,
    last_seq: u32,
}

/// Multicast UDP endpoint with per-peer seq/session tracking.
pub struct UdpTransport<I: CyclePayload, R: CyclePayload> {
    socket: UdpSocket,
    group_addr: SocketAddrV4,
    self_node_id: u8,
    self_session_id: u64,
    next_seq_num: u32,
    peers: HashMap<u8, PeerCursor>,
    _payload: core::marker::PhantomData<(I, R)>,
}

impl<I: CyclePayload, R: CyclePayload> UdpTransport<I, R> {
    /// Bind to the multicast group on the given interface.
    pub fn new(cfg: TransportConfig) -> Result<Self, TransportError> {
        let iface_ip = interface_ipv4(&cfg.interface_name)?;

        let s = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        s.set_reuse_address(true)?;

        #[cfg(target_os = "linux")]
        s.bind_device(Some(cfg.interface_name.as_bytes()))?;

        s.bind(&SockAddr::from(SocketAddrV4::new(
            Ipv4Addr::UNSPECIFIED,
            cfg.port,
        )))?;
        s.join_multicast_v4(&cfg.multicast_group, &iface_ip)?;
        s.set_multicast_loop_v4(true)?;
        s.set_multicast_ttl_v4(1)?;

        let socket: UdpSocket = s.into();
        let group_addr = SocketAddrV4::new(cfg.multicast_group, cfg.port);

        Ok(Self {
            socket,
            group_addr,
            self_node_id: cfg.self_node_id,
            self_session_id: cfg.self_session_id,
            peers: HashMap::new(),
            next_seq_num: cfg.initial_seq_num,
            _payload: core::marker::PhantomData,
        })
    }

    /// Wrap `payload` in a frame with the current sender identity and a
    /// fresh monotonic timestamp, then send it. Returns the allocated seq.
    ///
    /// The specialised `send_time_sync_req` / `send_time_sync_resp` paths
    /// carry timestamps that are semantically identical to the frame's
    /// `timestamp` field and therefore build the frame directly.
    pub fn send(
        &mut self,
        node_state: NodeState,
        payload: Payload<I, R>,
    ) -> Result<u32, TransportError> {
        let ts = now_monotonic_ns();
        self.send_at(node_state, ts, payload)
    }

    /// Send a Cristian request. Returns the sent seq and `t1` for
    /// correlation. `t1` is both the frame timestamp and the payload's t1.
    pub fn send_time_sync_req(
        &mut self,
        node_state: NodeState,
    ) -> Result<(u32, u64), TransportError> {
        let t1 = now_monotonic_ns();
        let seq = self.send_at(node_state, t1, Payload::TimeSyncReq { t1 })?;
        Ok((seq, t1))
    }

    /// Send a Cristian response echoing t1 and t2, with t3 taken as late
    /// as possible. `t3` is both the frame timestamp and the payload's t3.
    pub fn send_time_sync_resp(
        &mut self,
        node_state: NodeState,
        t1_echo: u64,
        t2_local: u64,
    ) -> Result<u32, TransportError> {
        let t3 = now_monotonic_ns();
        self.send_at(
            node_state,
            t3,
            Payload::TimeSyncResp {
                t1: t1_echo,
                t2: t2_local,
                t3,
            },
        )
    }

    /// Blocking receive: waits until a frame arrives or the socket's read
    /// timeout fires.
    pub fn recv(&mut self) -> RecvOutcome<I, R> {
        let mut buf = [0u8; RECV_BUFFER_SIZE];
        let (n, _) = match self.socket.recv_from(&mut buf) {
            Ok(x) => x,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                return RecvOutcome::Timeout;
            }
            Err(_) => return RecvOutcome::Timeout,
        };
        self.classify_bytes(&buf[..n])
    }

    /// Non-blocking receive. Returns `Timeout` if nothing is pending.
    pub fn try_recv(&mut self) -> RecvOutcome<I, R> {
        if self.socket.set_nonblocking(true).is_err() {
            return RecvOutcome::Timeout;
        }
        let mut buf = [0u8; RECV_BUFFER_SIZE];
        let (n, _) = match self.socket.recv_from(&mut buf) {
            Ok(x) => x,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return RecvOutcome::Timeout,
            Err(_) => return RecvOutcome::Timeout,
        };
        self.classify_bytes(&buf[..n])
    }

    /// Accept a `NewSession` or `SeqGap` frame: update the cursor to the
    /// frame's session/seq. State machine's call to signal it took the
    /// frame after policy review.
    pub fn accept(&mut self, frame: &UdpFrame<I, R>) {
        self.peers.insert(
            frame.node_id(),
            PeerCursor {
                session_id: frame.session_id(),
                last_seq: frame.seq_num(),
            },
        );
    }

    /// Drop a peer's cursor entirely.
    pub fn forget(&mut self, peer_id: u8) {
        self.peers.remove(&peer_id);
    }

    /// Common send path with an explicit timestamp. All public send methods
    /// funnel through here.
    fn send_at(
        &mut self,
        node_state: NodeState,
        ts: u64,
        payload: Payload<I, R>,
    ) -> Result<u32, TransportError> {
        let seq = self.next_seq_num;
        let frame = UdpFrame::<I, R>::new(
            self.self_node_id,
            self.self_session_id,
            seq,
            node_state,
            ts,
            payload,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    /// Common decode + classification path shared by `recv` and `try_recv`.
    fn classify_bytes(&mut self, bytes: &[u8]) -> RecvOutcome<I, R> {
        let local_recv_ns = now_monotonic_ns();
        let frame = match UdpFrame::<I, R>::decode(bytes) {
            Ok(f) => f,
            Err(FrameError::CrcMismatch) => return RecvOutcome::CrcError,
            Err(e) => return RecvOutcome::Malformed(e),
        };
        if frame.node_id() == self.self_node_id {
            return RecvOutcome::SelfLoopback;
        }
        if matches!(
            frame.payload(),
            Payload::TimeSyncReq { .. } | Payload::TimeSyncResp { .. }
        ) {
            return RecvOutcome::TimeSync {
                peer_id: frame.node_id(),
                frame,
                local_recv_ns,
            };
        }
        self.classify(frame)
    }

    /// Session/seq classification against the peer cursor table.
    fn classify(&mut self, frame: UdpFrame<I, R>) -> RecvOutcome<I, R> {
        let peer_id = frame.node_id();
        match self.peers.get(&peer_id).copied() {
            None => {
                self.peers.insert(
                    peer_id,
                    PeerCursor {
                        session_id: frame.session_id(),
                        last_seq: frame.seq_num(),
                    },
                );
                RecvOutcome::Valid(frame)
            }
            Some(cur) if frame.session_id() != cur.session_id => RecvOutcome::NewSession {
                peer_id,
                previous_session: cur.session_id,
                new_session: frame.session_id(),
                frame,
            },
            Some(cur) if frame.seq_num() <= cur.last_seq => RecvOutcome::Duplicate {
                peer_id,
                seen: frame.seq_num(),
                last: cur.last_seq,
            },
            Some(cur) if frame.seq_num() == cur.last_seq + 1 => {
                self.peers.insert(
                    peer_id,
                    PeerCursor {
                        session_id: frame.session_id(),
                        last_seq: frame.seq_num(),
                    },
                );
                RecvOutcome::Valid(frame)
            }
            Some(cur) => {
                let gap = frame.seq_num() - cur.last_seq - 1;
                RecvOutcome::SeqGap {
                    peer_id,
                    gap,
                    frame,
                }
            }
        }
    }
}

/// Look up the first IPv4 address bound to the named interface.
fn interface_ipv4(name: &str) -> Result<Ipv4Addr, TransportError> {
    for iface in if_addrs::get_if_addrs().map_err(TransportError::Io)? {
        if iface.name == name {
            if let IpAddr::V4(ip) = iface.ip() {
                return Ok(ip);
            }
        }
    }
    Err(TransportError::InterfaceNotFound(name.to_string()))
}

/// Node-local monotonic clock in nanoseconds. Each node has its own epoch
/// (first call); time-sync exchanges only differences and offsets, not a
/// shared wall clock.
pub fn now_monotonic_ns() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_nanos() as u64
}
