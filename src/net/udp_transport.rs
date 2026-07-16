use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4, UdpSocket};
use std::time::{Duration, Instant};
use crate::net::node_mask::NodeMask;
use crate::net::udp_frame::{FrameError, MAX_FRAME_SIZE, UdpFrame};
use crate::state_machine::NodeState;
use crate::braking_curve::BrakeResult;

pub struct TransportConfig {
    pub interface_name: String,      // z.B. "eth0"
    pub multicast_group: Ipv4Addr,   // z.B. Ipv4Addr::new(239, 10, 0, 1)
    pub port: u16,                   // z.B. 5555
    pub self_node_id: u8,
    pub self_session_id: u64,
    pub initial_sequenz_num : u32,
}

#[derive(Debug)]
pub enum RecvOutcome {
    /// Frame vom erwarteten Peer mit passender seq_num — kann direkt genutzt werden.
    Valid(UdpFrame),

    /// Timeout beim Empfang (kein Frame innerhalb read_timeout).
    Timeout,

    /// Eigener Frame per Multicast-Loopback zurückgekommen — ignorieren.
    SelfLoopback,

    /// CRC schlägt fehl — Frame verwerfen.
    CrcError,

    /// Frame ist strukturell kaputt (zu kurz, ungültiger Discriminator, ...).
    Malformed(FrameError),

    /// Selbe oder ältere seq_num vom selben Peer (in derselben Session).
    /// Klassischer Replay/Duplikat, verwerfen.
    Duplicate {
        peer_id: u8,
        seen: u32,
        last: u32,
    },

    /// Peer hat eine andere Session-ID als beim letzten Frame → er hat rebootet
    /// oder ist neu gestartet. State-Machine entscheidet, ob Resync/Rejoin.
    NewSession {
        peer_id: u8,
        previous_session: u64,
        new_session: u64,
        frame: UdpFrame,
    },

    /// seq_num ist größer als erwartet — Lücke erkannt (Pakete verloren).
    /// State-Machine entscheidet, ob akzeptieren (Resync) oder ignorieren.
    SeqGap {
        peer_id: u8,
        gap: u32,
        frame: UdpFrame,
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

pub struct UdpTransport {
    socket: UdpSocket,
    group_addr: SocketAddrV4,
    self_node_id: u8,
    self_session_id : u64,
    next_seq_num: u32,
    peers: HashMap<u8, PeerCursor>,
}

impl UdpTransport {
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
        s.set_multicast_loop_v4(false)?; // eigene Frames nicht per Multicast zurück
        s.set_multicast_ttl_v4(1)?;      // nur lokales Segment

        let socket: UdpSocket = s.into();
        let group_addr = SocketAddrV4::new(cfg.multicast_group, cfg.port);

        Ok(Self {
            socket,
            group_addr,
            self_node_id: cfg.self_node_id,
            self_session_id: cfg.self_session_id,
            peers: HashMap::new(),
            next_seq_num : cfg.initial_sequenz_num,
        })
    }

    /// Sendet einen State-Frame mit automatisch hochgezählter seq_num.
    pub fn send_state(&mut self, node_state: NodeState) -> Result<u32, TransportError> {
        let seq = self.next_seq_num;
        let frame = UdpFrame::state_frame(
            self.self_node_id, self.self_session_id, seq, node_state,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    pub fn send_result(
        &mut self, node_state: NodeState, result: BrakeResult,
    ) -> Result<u32, TransportError> {
        let seq = self.next_seq_num;
        let frame = UdpFrame::result_frame(
            self.self_node_id, self.self_session_id, seq, node_state, result,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    pub fn send_ack(
        &mut self, node_state: NodeState,
        received_from: NodeMask, publisher_candidate: u8,
    ) -> Result<u32, TransportError> {
        let seq = self.next_seq_num;
        let frame = UdpFrame::ack_frame(
            self.self_node_id, self.self_session_id, seq, node_state,
            received_from, publisher_candidate,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    pub fn recv(&mut self) -> RecvOutcome {
        let mut buf = [0u8; MAX_FRAME_SIZE];
        let (n, _src) = match self.socket.recv_from(&mut buf) {
            Ok(x) => x,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                return RecvOutcome::Timeout;
            }
            Err(_) => return RecvOutcome::Timeout,
        };

        let frame = match UdpFrame::decode(&buf[..n]) {
            Ok(f) => f,
            Err(FrameError::CrcMismatch) => return RecvOutcome::CrcError,
            Err(e) => return RecvOutcome::Malformed(e),
        };

        if frame.node_id() == self.self_node_id {
            return RecvOutcome::SelfLoopback;
        }

        self.classify(frame)
    }

    pub fn recv_before(&mut self, deadline: Instant) -> RecvOutcome {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);

        if remaining.is_zero() {
            return RecvOutcome::Timeout;
        }

        if self.socket.set_read_timeout(Some(remaining)).is_err() {
            return RecvOutcome::Timeout;
        }

        let mut buf = [0u8; MAX_FRAME_SIZE];
        let (n, _src) = match self.socket.recv_from(&mut buf) {
            Ok(x) => x,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                return RecvOutcome::Timeout;
            }
            Err(_) => return RecvOutcome::Timeout,
        };

        let frame = match UdpFrame::decode(&buf[..n]) {
            Ok(f) => f,
            Err(FrameError::CrcMismatch) => return RecvOutcome::CrcError,
            Err(e) => return RecvOutcome::Malformed(e),
        };

        if frame.node_id() == self.self_node_id {
            return RecvOutcome::SelfLoopback;
        }

        self.classify(frame)
    }

    fn classify(&mut self, frame: UdpFrame) -> RecvOutcome {
        let peer_id = frame.node_id();
        match self.peers.get(&peer_id).copied() {
            // Erster Frame von diesem Peer überhaupt → einfach akzeptieren.
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

            // Peer hat eine neue Session-ID → er hat rebootet.
            // Cursor NICHT automatisch aktualisieren — die State-Machine soll
            // entscheiden, ob sie den neuen Peer akzeptiert (via accept()).
            Some(cur) if frame.session_id() != cur.session_id => RecvOutcome::NewSession {
                peer_id,
                previous_session: cur.session_id,
                new_session: frame.session_id(),
                frame,
            },

            // Selbe Session, aber alte oder identische seq_num → Duplikat/Replay.
            Some(cur) if frame.seq_num() <= cur.last_seq => RecvOutcome::Duplicate {
                peer_id,
                seen: frame.seq_num(),
                last: cur.last_seq,
            },

            // Selbe Session, seq_num genau um 1 höher → normaler Fall.
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

            // Selbe Session, seq_num > last_seq + 1 → Pakete verloren.
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

    /// Von der State-Machine aufzurufen, wenn sie einen `NewSession`- oder
    /// `SeqGap`-Frame nach Prüfung akzeptiert (z.B. Peer-Rejoin in Probation).
    /// Damit übernimmt der Transport die neuen Cursor-Werte.
    pub fn accept(&mut self, frame: &UdpFrame) {
        self.peers.insert(
            frame.node_id(),
            PeerCursor {
                session_id: frame.session_id(),
                last_seq: frame.seq_num(),
            },
        );
    }

    /// Peer explizit aus der Cursor-Tabelle entfernen (z.B. wenn die
    /// State-Machine entscheidet, den Peer nicht wieder aufzunehmen).
    pub fn forget(&mut self, peer_id: u8) {
        self.peers.remove(&peer_id);
    }
}

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