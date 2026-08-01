//! Diagnose-Schnittstelle.
//!
//! Multicast-basiert: GUI und Nodes wissen nichts voneinander, sondern
//! nutzen eine gemeinsame Multicast-Gruppe. GUI sendet Telegramme,
//! Nodes hoeren mit und antworten auf dieselbe Gruppe. GUI empfaengt
//! die Antworten und ordnet sie ueber `source_node_id` zu.
//!
//! Zwei Telegramm-Typen:
//! - Input-Telegramme: gehen immer an alle Nodes. Neue Input-Daten fuer
//!   die Computation. Werden gestagt und beim naechsten Zyklus wirksam.
//! - Command-Telegramme: gehen an spezifische Nodes (targets-Liste).
//!   Fault Injection, Status-Abfragen. Zustandsaendernde Kommandos
//!   werden gestagt, Abfragen wie GetStatus werden sofort beantwortet.
//!
//! Staging-Regel: alle zustandsaendernden Aenderungen werden fuer den
//! Beginn des naechsten Zyklus geplant, damit sie synchron an allen
//! Nodes wirksam werden (CycleSync-Barrier garantiert Simultanitaet).
//!
//! Mehrfache Kommandos gleicher Art vor dem Zyklusstart: letztes gewinnt
//! (Overwrite-Semantik).

use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use tracing::{debug, error, info, warn};

// -------------------------------------------------------------
// Config
// -------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct DiagnosticConfig {
    pub enabled: bool,
    pub interface_name: String,
    pub multicast_group: Ipv4Addr,
    pub port: u16,
}

impl Default for DiagnosticConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interface_name: "lo".into(),
            multicast_group: Ipv4Addr::new(239, 10, 0, 2),
            port: 6666,
        }
    }
}

// -------------------------------------------------------------
// Injection State (aktiv)
// -------------------------------------------------------------

/// Aktive Fault-Injection-Zaehler. Wird vom Runner an relevanten Stellen
/// konsultiert und dekrementiert.
#[derive(Debug, Default, Clone, Copy)]
pub struct InjectionState {
    pub drop_next_n_results: u32,
    pub drop_next_n_acks: u32,
}

// -------------------------------------------------------------
// Wire-Format: eingehende Telegramme
// -------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomingTelegram {
    /// Neuer Input-Wert fuer die Computation. Geht an alle Nodes.
    /// `value` wird als opaque JSON gespeichert; der Runner
    /// deserialisiert erst beim Anwenden gegen `C::Input`.
    Input { value: serde_json::Value },

    /// Kommando an spezifische Nodes.
    Command {
        targets: Vec<u8>,
        #[serde(flatten)]
        payload: Command,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Sofort-Antwort mit aktuellem Node-Zustand.
    GetStatus,

    /// Die naechsten `count` Result-Sendungen droppen.
    InjectDropResults { count: u32 },

    /// Die naechsten `count` Ack-Sendungen droppen.
    InjectDropAcks { count: u32 },

    /// Alle aktiven Injections zuruecksetzen.
    ClearInjection,
    // Erweiterbar: SetHealth, InjectValueOffset, ...
}

// -------------------------------------------------------------
// Wire-Format: ausgehende Telegramme
// -------------------------------------------------------------

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutgoingTelegram<'a> {
    /// Bestaetigung eines gestagten Kommandos oder Input-Telegramms.
    /// Wird angewendet zu Beginn des naechsten Zyklus.
    Staged {
        source_node_id: u8,
        staged_kind: &'a str,
    },

    /// Sofortige Antwort auf GetStatus mit vollstaendigem Node-Zustand.
    Status {
        source_node_id: u8,
        data: StatusResponse,
    },

    /// Fehler bei Verarbeitung eines Telegramms (z.B. Deserialisierung).
    Error {
        source_node_id: u8,
        message: String,
    },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct StatusResponse {
    pub node_id: u8,
    pub session_id: u64,
    pub node_state: String,
    pub current_seq: u32,
    pub last_cycle_us: Option<u128>,
    pub sync_valid: bool,
    pub sync_epsilon_ns: i64,
    pub cycles_since_last_sync: u32,
    pub peers: Vec<PeerStatus>,
    pub injection: InjectionSnapshot,
    pub pending_input: bool,
    pub pending_injection_update: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PeerStatus {
    pub id: u8,
    pub health: String,
    pub consecutive_faults: u32,
    pub consecutive_healthy_cycles: u32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct InjectionSnapshot {
    pub drop_next_n_results: u32,
    pub drop_next_n_acks: u32,
}

// -------------------------------------------------------------
// Staging Buffer
// -------------------------------------------------------------

/// Gestagete Aenderungen, die beim naechsten Zyklusstart wirksam werden.
/// Alle Felder Overwrite-Semantik: neues Kommando gleicher Art ersetzt
/// das vorherige.
#[derive(Debug, Default)]
pub struct PendingChanges {
    /// Neuer Input-Wert als opaque JSON. Runner deserialisiert beim
    /// Anwenden gegen `C::Input`.
    pub input_json: Option<serde_json::Value>,

    /// Update auf die Injection-Zaehler. `Some(0)` = Injection loeschen,
    /// `Some(n)` = auf n setzen, `None` = kein Update.
    pub drop_next_n_results: Option<u32>,
    pub drop_next_n_acks: Option<u32>,
    pub clear_injection: bool,
}

impl PendingChanges {
    pub fn has_input(&self) -> bool {
        self.input_json.is_some()
    }

    pub fn has_injection_update(&self) -> bool {
        self.drop_next_n_results.is_some()
            || self.drop_next_n_acks.is_some()
            || self.clear_injection
    }
}

// -------------------------------------------------------------
// Diagnostic
// -------------------------------------------------------------

pub struct Diagnostic {
    socket: UdpSocket,
    multicast_addr: SocketAddrV4,
    node_id: u8,

    /// Aktiver Injection-Zustand, den der Runner konsultiert.
    pub injection: InjectionState,

    /// Gestagete Aenderungen fuer den naechsten Zyklus.
    pub pending: PendingChanges,
}

impl Diagnostic {
    pub fn new(cfg: &DiagnosticConfig, node_id: u8) -> Result<Self, io::Error> {
        let iface_ip = interface_ipv4(&cfg.interface_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no IPv4 address on interface {}", cfg.interface_name),
            )
        })?;

        let s = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        s.set_reuse_address(true)?;
        s.bind(&SocketAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, cfg.port)).into())?;
        s.join_multicast_v4(&cfg.multicast_group, &iface_ip)?;
        s.set_multicast_loop_v4(true)?;
        s.set_multicast_if_v4(&iface_ip)?;
        s.set_nonblocking(true)?;

        let socket: UdpSocket = s.into();
        let multicast_addr = SocketAddrV4::new(cfg.multicast_group, cfg.port);

        info!(
            iface = %cfg.interface_name,
            group = %cfg.multicast_group,
            port = cfg.port,
            node_id,
            "diagnostic multicast interface bound"
        );

        Ok(Self {
            socket,
            multicast_addr,
            node_id,
            injection: InjectionState::default(),
            pending: PendingChanges::default(),
        })
    }

    /// Non-blocking Empfang und Verarbeitung eingehender Telegramme.
    /// Filtert nach Ziel: Input geht immer durch, Command nur wenn eigene
    /// node_id in `targets` steht.
    ///
    /// Gibt Some(Command::GetStatus) zurueck, damit der Runner die
    /// Status-Antwort mit vollem Zustand bauen kann — alle anderen
    /// Kommandos werden hier direkt in `pending` gestagt.
    pub fn try_recv(&mut self) -> Option<Command> {
        let mut buf = [0u8; 2048];
        let (n, _src) = match self.socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return None,
            Err(e) => {
                warn!(error = ?e, "diagnostic recv failed");
                return None;
            }
        };

        // Direktversuch, das Telegramm zu parsen. Nodes senden auch
        // OutgoingTelegrams in dieselbe Multicast-Gruppe und empfangen
        // sie via Loopback zurueck — die sind aus Sicht des Incoming-
        // Parsers ungueltig und werden hier still verworfen, statt eine
        // Error-Antwort zu produzieren (das wuerde einen Feedback-Loop
        // ausloesen).
        let telegram = match serde_json::from_slice::<IncomingTelegram>(&buf[..n]) {
            Ok(t) => t,
            Err(e) => {
                debug!(
                    error = ?e,
                    raw = %String::from_utf8_lossy(&buf[..n]),
                    "dropped unparseable diagnostic frame"
                );
                return None;
            }
        };

        match telegram {
            IncomingTelegram::Input { value } => {
                debug!("input telegram received, staging for next cycle");
                self.pending.input_json = Some(value); // overwrite
                self.send(&OutgoingTelegram::Staged {
                    source_node_id: self.node_id,
                    staged_kind: "input",
                });
                None
            }
            IncomingTelegram::Command { targets, payload } => {
                if !targets.contains(&self.node_id) {
                    debug!(targets = ?targets, "command not targeted at us, ignored");
                    return None;
                }
                self.handle_command(payload)
            }
        }
    }

    fn handle_command(&mut self, cmd: Command) -> Option<Command> {
        match cmd {
            Command::GetStatus => {
                // Sofort — Runner baut die Antwort mit vollem Zustand.
                Some(Command::GetStatus)
            }
            Command::InjectDropResults { count } => {
                self.pending.drop_next_n_results = Some(count);
                info!(count, "injection drop_results staged for next cycle");
                self.send(&OutgoingTelegram::Staged {
                    source_node_id: self.node_id,
                    staged_kind: "inject_drop_results",
                });
                None
            }
            Command::InjectDropAcks { count } => {
                self.pending.drop_next_n_acks = Some(count);
                info!(count, "injection drop_acks staged for next cycle");
                self.send(&OutgoingTelegram::Staged {
                    source_node_id: self.node_id,
                    staged_kind: "inject_drop_acks",
                });
                None
            }
            Command::ClearInjection => {
                self.pending.clear_injection = true;
                info!("injection clear staged for next cycle");
                self.send(&OutgoingTelegram::Staged {
                    source_node_id: self.node_id,
                    staged_kind: "clear_injection",
                });
                None
            }
        }
    }

    /// Sendet ein Telegramm zurueck in die Multicast-Gruppe.
    pub fn send(&self, telegram: &OutgoingTelegram) {
        let bytes = match serde_json::to_vec(telegram) {
            Ok(b) => b,
            Err(e) => {
                error!(error = ?e, "failed to serialize outgoing telegram");
                return;
            }
        };
        if let Err(e) = self.socket.send_to(&bytes, self.multicast_addr) {
            warn!(error = ?e, "failed to send diagnostic response");
        }
    }

    /// Wendet die Injection-Updates aus `pending` auf `injection` an.
    /// Wird vom Runner am Anfang von `handle_read_inputs` aufgerufen.
    ///
    /// Input wird vom Runner selbst deserialisiert (weil generisch), hier
    /// nur die Injection-Zaehler.
    pub fn apply_pending_injection(&mut self) {
        if self.pending.clear_injection {
            self.injection = InjectionState::default();
            debug!("cleared injection state at cycle start");
        }
        if let Some(n) = self.pending.drop_next_n_results.take() {
            self.injection.drop_next_n_results = n;
            debug!(count = n, "applied drop_results at cycle start");
        }
        if let Some(n) = self.pending.drop_next_n_acks.take() {
            self.injection.drop_next_n_acks = n;
            debug!(count = n, "applied drop_acks at cycle start");
        }
        self.pending.clear_injection = false;
    }

    /// Nimmt das gestagete Input-JSON heraus, damit der Runner es
    /// gegen `C::Input` deserialisieren kann.
    pub fn take_pending_input(&mut self) -> Option<serde_json::Value> {
        self.pending.input_json.take()
    }

    pub fn should_drop_result(&mut self) -> bool {
        if self.injection.drop_next_n_results > 0 {
            self.injection.drop_next_n_results -= 1;
            true
        } else {
            false
        }
    }

    pub fn should_drop_ack(&mut self) -> bool {
        if self.injection.drop_next_n_acks > 0 {
            self.injection.drop_next_n_acks -= 1;
            true
        } else {
            false
        }
    }

    pub fn node_id(&self) -> u8 {
        self.node_id
    }
}

// -------------------------------------------------------------
// Interface-Helper
// -------------------------------------------------------------

fn interface_ipv4(iface_name: &str) -> Option<Ipv4Addr> {
    use std::process::Command;

    let out = Command::new("ip")
        .args(["-4", "-o", "addr", "show", iface_name])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines() {
        if let Some(idx) = line.find("inet ") {
            let rest = &line[idx + 5..];
            if let Some(slash) = rest.find('/') {
                if let Ok(ip) = rest[..slash].parse::<Ipv4Addr>() {
                    return Some(ip);
                }
            }
        }
    }
    None
}