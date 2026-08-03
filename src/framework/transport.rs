use crate::framework::config::TransportConfig;
use crate::framework::state_machine::NodeState;
use crate::framework::traits::CyclePayload;
use crate::framework::types::PeerMask;
use crate::framework::wire::{FrameError, MAX_PAYLOAD_WIRE_SIZE, Payload, UdpFrame};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::OnceLock;
use std::time::Instant;

const HEADER_SIZE: usize = 23;
const CRC_SIZE: usize = 4;

/// Upper bound on receive buffer size across all payload variants.
pub const RECV_BUFFER_SIZE: usize = HEADER_SIZE + MAX_PAYLOAD_WIRE_SIZE + CRC_SIZE;

/// Classification of an incoming frame after decode + peer-cursor checks.
#[derive(Debug)]
pub enum RecvOutcome<P: CyclePayload> {
    /// Well-formed frame from a known peer with the expected next seq.
    Valid(UdpFrame<P>),
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
    NewSession { peer_id: u8, previous_session: u64, new_session: u64, frame: UdpFrame<P> },
    /// Gap detected between last-seen and this seq.
    SeqGap { peer_id: u8, gap: u32, frame: UdpFrame<P> },
    /// Time-sync frame; bypasses seq-num classification.
    /// `local_recv_ns` is the receive timestamp (t2 or t4).
    TimeSync { peer_id: u8, frame: UdpFrame<P>, local_recv_ns: u64 },
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
pub struct UdpTransport<P: CyclePayload> {
    socket: UdpSocket,
    group_addr: SocketAddrV4,
    self_node_id: u8,
    self_session_id: u64,
    next_seq_num: u32,
    peers: HashMap<u8, PeerCursor>,
    _payload: core::marker::PhantomData<P>,
}

impl<P: CyclePayload> UdpTransport<P> {
    /// Bind to the multicast group on the given interface.
    pub fn new(cfg: TransportConfig) -> Result<Self, TransportError> {
        let iface_ip = interface_ipv4(&cfg.interface_name)?;

        let s = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        s.set_reuse_address(true)?;

        #[cfg(target_os = "linux")]
        s.bind_device(Some(cfg.interface_name.as_bytes()))?;

        s.bind(&SockAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, cfg.port)))?;
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

    /// Send a State beacon carrying the current observation mask.
    pub fn send_state(&mut self, node_state: NodeState, seen_mask: PeerMask) -> Result<u32, TransportError> {
        self.send_payload(node_state, |id, sid, seq, ns, ts| {
            UdpFrame::<P>::state_frame(id, sid, seq, ns, ts, seen_mask)
        })
    }

    /// Send the current cycle result.
    pub fn send_result(&mut self, node_state: NodeState, result: P) -> Result<u32, TransportError> {
        self.send_payload(node_state, |id, sid, seq, ns, ts| {
            UdpFrame::<P>::result_frame(id, sid, seq, ns, ts, result)
        })
    }

    /// Send an ack for received results plus our publisher pick.
    pub fn send_ack(
        &mut self,
        node_state: NodeState,
        received_from: PeerMask,
        publisher_candidate: u8,
    ) -> Result<u32, TransportError> {
        self.send_payload(node_state, |id, sid, seq, ns, ts| {
            UdpFrame::<P>::ack_frame(id, sid, seq, ns, ts, received_from, publisher_candidate)
        })
    }

    /// Send an exclusion vote for the ErrorManagement phase.
    pub fn send_exclusion_proposal(
        &mut self,
        node_state: NodeState,
        propose_exclude: PeerMask,
    ) -> Result<u32, TransportError> {
        self.send_payload(node_state, |id, sid, seq, ns, ts| {
            UdpFrame::<P>::exclusion_proposal_frame(id, sid, seq, ns, ts, propose_exclude)
        })
    }

    /// Send a Cristian request. Returns the sent seq and `t1` for
    /// correlation.
    pub fn send_time_sync_req(&mut self, node_state: NodeState) -> Result<(u32, u64), TransportError> {
        let seq_marker = self.next_seq_num;
        let t1 = now_monotonic_ns();
        let frame = UdpFrame::<P>::time_sync_req_frame(
            self.self_node_id,
            self.self_session_id,
            seq_marker,
            node_state,
            t1,
            t1,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        Ok((seq_marker, t1))
    }

    /// Send a Cristian response echoing t1 and t2, with t3 taken as late
    /// as possible.
    pub fn send_time_sync_resp(
        &mut self,
        node_state: NodeState,
        t1_echo: u64,
        t2_local: u64,
    ) -> Result<u32, TransportError> {
        let seq_marker = self.next_seq_num;
        let t3 = now_monotonic_ns();
        let frame = UdpFrame::<P>::time_sync_resp_frame(
            self.self_node_id,
            self.self_session_id,
            seq_marker,
            node_state,
            t3,
            t1_echo,
            t2_local,
            t3,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        Ok(seq_marker)
    }

    /// Blocking receive: waits until a frame arrives or the socket's read
    /// timeout fires.
    pub fn recv(&mut self) -> RecvOutcome<P> {
        let mut buf = [0u8; RECV_BUFFER_SIZE];
        let (n, _) = match self.socket.recv_from(&mut buf) {
            Ok(x) => x,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
                return RecvOutcome::Timeout;
            }
            Err(_) => return RecvOutcome::Timeout,
        };
        self.classify_bytes(&buf[..n])
    }

    /// Non-blocking receive. Returns `Timeout` if nothing is pending.
    pub fn try_recv(&mut self) -> RecvOutcome<P> {
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
    pub fn accept(&mut self, frame: &UdpFrame<P>) {
        self.peers.insert(
            frame.node_id(),
            PeerCursor { session_id: frame.session_id(), last_seq: frame.seq_num() },
        );
    }

    /// Drop a peer's cursor entirely.
    pub fn forget(&mut self, peer_id: u8) {
        self.peers.remove(&peer_id);
    }

    /// Common send path: allocate seq, timestamp, encode, send, advance seq.
    fn send_payload<F>(&mut self, node_state: NodeState, build: F) -> Result<u32, TransportError>
    where
        F: FnOnce(u8, u64, u32, NodeState, u64) -> UdpFrame<P>,
    {
        let seq = self.next_seq_num;
        let ts = now_monotonic_ns();
        let frame = build(self.self_node_id, self.self_session_id, seq, node_state, ts);
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    /// Common decode + classification path shared by `recv` and `try_recv`.
    fn classify_bytes(&mut self, bytes: &[u8]) -> RecvOutcome<P> {
        let local_recv_ns = now_monotonic_ns();
        let frame = match UdpFrame::<P>::decode(bytes) {
            Ok(f) => f,
            Err(FrameError::CrcMismatch) => return RecvOutcome::CrcError,
            Err(e) => return RecvOutcome::Malformed(e),
        };
        if frame.node_id() == self.self_node_id {
            return RecvOutcome::SelfLoopback;
        }
        if matches!(frame.payload(), Payload::TimeSyncReq { .. } | Payload::TimeSyncResp { .. }) {
            return RecvOutcome::TimeSync {
                peer_id: frame.node_id(),
                frame,
                local_recv_ns,
            };
        }
        self.classify(frame)
    }

    /// Session/seq classification against the peer cursor table.
    fn classify(&mut self, frame: UdpFrame<P>) -> RecvOutcome<P> {
        let peer_id = frame.node_id();
        match self.peers.get(&peer_id).copied() {
            None => {
                self.peers.insert(
                    peer_id,
                    PeerCursor { session_id: frame.session_id(), last_seq: frame.seq_num() },
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
                    PeerCursor { session_id: frame.session_id(), last_seq: frame.seq_num() },
                );
                RecvOutcome::Valid(frame)
            }
            Some(cur) => {
                let gap = frame.seq_num() - cur.last_seq - 1;
                RecvOutcome::SeqGap { peer_id, gap, frame }
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
