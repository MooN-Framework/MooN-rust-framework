use crate::framework::state_machine::NodeState;
use crate::framework::traits::CyclePayload;
use crate::framework::types::PeerMask;
use crate::framework::udp_frame::{FrameError, MAX_PAYLOAD_WIRE_SIZE, Payload, UdpFrame};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::OnceLock;
use std::time::Instant;

/// Obere Grenze fuer die Puffergroesse beim Empfang.
///
/// Deckt jeden `UdpFrame<P>` mit `P::WIRE_SIZE <= MAX_PAYLOAD_WIRE_SIZE` ab,
/// unabhaengig vom konkreten Payload-Typ. Damit bleibt der Empfangspuffer
/// stack-allokiert und typ-agnostisch.
const HEADER_SIZE: usize = 23;
const CRC_SIZE: usize = 4;
pub const RECV_BUFFER_SIZE: usize = HEADER_SIZE + MAX_PAYLOAD_WIRE_SIZE + CRC_SIZE;

pub struct TransportConfig {
    pub interface_name: String,    // z.B. "eth0"
    pub multicast_group: Ipv4Addr, // z.B. Ipv4Addr::new(239, 10, 0, 1)
    pub port: u16,                 // z.B. 5555
    pub self_node_id: u8,
    pub self_session_id: u64,
    pub initial_sequenz_num: u32,
}

#[derive(Debug)]
pub enum RecvOutcome<P: CyclePayload> {
    /// Frame vom erwarteten Peer mit passender seq_num — kann direkt genutzt werden.
    Valid(UdpFrame<P>),

    /// Timeout beim Empfang (kein Frame innerhalb read_timeout).
    Timeout,

    /// Eigener Frame per Multicast-Loopback zurueckgekommen — ignorieren.
    SelfLoopback,

    /// CRC schlaegt fehl — Frame verwerfen.
    CrcError,

    /// Frame ist strukturell kaputt (zu kurz, ungueltiger Discriminator, ...).
    Malformed(FrameError),

    /// Selbe oder aeltere seq_num vom selben Peer (in derselben Session).
    /// Klassischer Replay/Duplikat, verwerfen.
    Duplicate { peer_id: u8, seen: u32, last: u32 },

    /// Peer hat eine andere Session-ID als beim letzten Frame -> er hat rebootet
    /// oder ist neu gestartet. State-Machine entscheidet, ob Resync/Rejoin.
    NewSession {
        peer_id: u8,
        previous_session: u64,
        new_session: u64,
        frame: UdpFrame<P>,
    },

    /// seq_num ist groesser als erwartet — Luecke erkannt (Pakete verloren).
    /// State-Machine entscheidet, ob akzeptieren (Resync) oder ignorieren.
    SeqGap {
        peer_id: u8,
        gap: u32,
        frame: UdpFrame<P>,
    },

    /// Zeitsync-Frame (Request oder Response). Umgeht die seq_num-
    /// Klassifikation, da Sync-Frames einen eigenen asynchronen
    /// Nachrichtenfluss darstellen.
    ///
    /// `local_recv_ns` ist der Zeitstempel, der unmittelbar nach `recv_from`
    /// genommen wurde. Er entspricht T2 (bei Request) bzw. T4 (bei Response)
    /// aus der Cristian-Nomenklatur.
    TimeSync {
        peer_id: u8,
        frame: UdpFrame<P>,
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
        s.set_multicast_loop_v4(true)?; // eigene Frames nicht per Multicast zurueck
        s.set_multicast_ttl_v4(1)?; // nur lokales Segment

        let socket: UdpSocket = s.into();
        let group_addr = SocketAddrV4::new(cfg.multicast_group, cfg.port);

        Ok(Self {
            socket,
            group_addr,
            self_node_id: cfg.self_node_id,
            self_session_id: cfg.self_session_id,
            peers: HashMap::new(),
            next_seq_num: cfg.initial_sequenz_num,
            _payload: core::marker::PhantomData,
        })
    }

    /// Sendet einen State-Frame mit automatisch hochgezaehlter seq_num.
    pub fn send_state(&mut self, node_state: NodeState) -> Result<u32, TransportError> {
        let seq = self.next_seq_num;
        let timestamp = now_monotonic_ns();
        let frame = UdpFrame::<P>::state_frame(
            self.self_node_id,
            self.self_session_id,
            seq,
            node_state,
            timestamp,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    pub fn send_result(&mut self, node_state: NodeState, result: P) -> Result<u32, TransportError> {
        let seq = self.next_seq_num;
        let timestamp = now_monotonic_ns();
        let frame = UdpFrame::<P>::result_frame(
            self.self_node_id,
            self.self_session_id,
            seq,
            node_state,
            timestamp,
            result,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    pub fn send_ack(
        &mut self,
        node_state: NodeState,
        received_from: PeerMask,
        publisher_candidate: u8,
    ) -> Result<u32, TransportError> {
        let seq = self.next_seq_num;
        let timestamp = now_monotonic_ns();
        let frame = UdpFrame::<P>::ack_frame(
            self.self_node_id,
            self.self_session_id,
            seq,
            node_state,
            timestamp,
            received_from,
            publisher_candidate,
        );
        self.socket.send_to(&frame.encode(), self.group_addr)?;
        self.next_seq_num = self.next_seq_num.wrapping_add(1);
        Ok(seq)
    }

    /// Sendet einen TimeSyncReq. Der zurueckgegebene `t1` ist der
    /// Sendezeitstempel, den die PeerSync-Logik als pending_t1 vermerken muss,
    /// um die passende Response zuordnen zu koennen.
    ///
    /// Sync-Frames tragen die aktuelle Position im Voting-seq_num-Strom
    /// als Marker mit, INKREMENTIEREN diese aber nicht: aus Sicht des
    /// Voting-Protokolls sind Sync-Frames unsichtbar (Read ohne Write auf
    /// den Strom). Die Deduplizierung von Sync-Traffic passiert nicht ueber
    /// seq_num, sondern ueber pending_t1 im PeerSync-Modul.
    ///
    /// Der Header-`timestamp` ist identisch mit `t1` — beide werden aus
    /// demselben `now_monotonic_ns()`-Aufruf befuellt.
    pub fn send_time_sync_req(
        &mut self,
        node_state: NodeState,
    ) -> Result<(u32, u64), TransportError> {
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

    /// Sendet einen TimeSyncResp. `t1_echo` und `t2_local` stammen aus dem
    /// verarbeiteten Request. `t3` wird intern erzeugt, moeglichst kurz vor
    /// dem eigentlichen `send_to`, damit die Responder-Verarbeitungszeit
    /// (t3 - t2) klein bleibt.
    ///
    /// Wie `send_time_sync_req`: seq_num wird als Marker mitgetragen, aber
    /// nicht inkrementiert. Header-`timestamp` ist identisch mit `t3`.
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

    pub fn recv(&mut self) -> RecvOutcome<P> {
        let mut buf = [0u8; RECV_BUFFER_SIZE];
        let (n, _src) = match self.socket.recv_from(&mut buf) {
            Ok(x) => x,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                return RecvOutcome::Timeout;
            }
            Err(_) => return RecvOutcome::Timeout,
        };

        // Zeitstempel unmittelbar nach recv_from — Software-Aequivalent zu
        // Hardware-Timestamping. Wird ausschliesslich fuer Sync-Frames genutzt,
        // aber unbedingt VOR jeglicher Verarbeitung genommen.
        let local_recv_ns = now_monotonic_ns();

        let frame = match UdpFrame::<P>::decode(&buf[..n]) {
            Ok(f) => f,
            Err(FrameError::CrcMismatch) => return RecvOutcome::CrcError,
            Err(e) => return RecvOutcome::Malformed(e),
        };

        if frame.node_id() == self.self_node_id {
            return RecvOutcome::SelfLoopback;
        }

        // Sync-Frames umgehen die seq_num-Klassifikation.
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

    pub fn try_recv(&mut self) -> RecvOutcome<P> {
        // Non-blocking Modus setzen; falls das fehlschlaegt, gibt's nichts zu tun.
        if self.socket.set_nonblocking(true).is_err() {
            return RecvOutcome::Timeout;
        }

        let mut buf = [0u8; RECV_BUFFER_SIZE];
        let (n, _src) = match self.socket.recv_from(&mut buf) {
            Ok(x) => x,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                return RecvOutcome::Timeout;
            }
            Err(_) => return RecvOutcome::Timeout,
        };

        let local_recv_ns = now_monotonic_ns();

        let frame = match UdpFrame::<P>::decode(&buf[..n]) {
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

    fn classify(&mut self, frame: UdpFrame<P>) -> RecvOutcome<P> {
        let peer_id = frame.node_id();
        match self.peers.get(&peer_id).copied() {
            // Erster Frame von diesem Peer ueberhaupt -> einfach akzeptieren.
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

            // Peer hat eine neue Session-ID -> er hat rebootet.
            // Cursor NICHT automatisch aktualisieren — die State-Machine soll
            // entscheiden, ob sie den neuen Peer akzeptiert (via accept()).
            Some(cur) if frame.session_id() != cur.session_id => RecvOutcome::NewSession {
                peer_id,
                previous_session: cur.session_id,
                new_session: frame.session_id(),
                frame,
            },

            // Selbe Session, aber alte oder identische seq_num -> Duplikat/Replay.
            Some(cur) if frame.seq_num() <= cur.last_seq => RecvOutcome::Duplicate {
                peer_id,
                seen: frame.seq_num(),
                last: cur.last_seq,
            },

            // Selbe Session, seq_num genau um 1 hoeher -> normaler Fall.
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

            // Selbe Session, seq_num > last_seq + 1 -> Pakete verloren.
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
    /// `SeqGap`-Frame nach Pruefung akzeptiert (z.B. Peer-Rejoin in Probation).
    /// Damit uebernimmt der Transport die neuen Cursor-Werte.
    pub fn accept(&mut self, frame: &UdpFrame<P>) {
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

/// Knoten-lokale monotone Uhr in Nanosekunden.
///
/// Jeder Knoten hat seine eigene Referenz-Epoche (erster Aufruf). Fuer die
/// Zeitsynchronisation werden nur Differenzen und Offsets zwischen Knoten
/// benoetigt, keine gemeinsame Wall-Clock.
///
/// Hinweis: `Instant` nutzt auf Linux `CLOCK_MONOTONIC`, das durch NTP
/// geslewt wird. Fuer engere Fehlerbaender waere ein direkter Aufruf von
/// `clock_gettime(CLOCK_MONOTONIC_RAW)` via `libc` sauberer, weil er
/// unabhaengig von jeglicher Wall-Clock-Korrektur laeuft.
pub fn now_monotonic_ns() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_nanos() as u64
}
