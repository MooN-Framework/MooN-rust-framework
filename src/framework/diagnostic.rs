//! Multicast diagnostic side-channel.
//!
//! GUI and nodes share a multicast group; the GUI publishes telegrams and
//! nodes reply on the same group. Incoming input values and state-changing
//! commands are staged and applied at the next cycle boundary — the
//! CycleSync barrier guarantees synchronous activation across nodes.
//! `GetStatus` bypasses staging and is answered immediately.

use crate::framework::config::DiagnosticConfig;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use tracing::{debug, error, info, warn};

/// Active fault-injection counters. Consulted by the runner at the
/// relevant sites and decremented on effect.
#[derive(Debug, Default, Clone, Copy)]
pub struct InjectionState {
    pub drop_next_n_results: u32,
    pub drop_next_n_acks: u32,
}

/// Incoming telegram from the GUI.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomingTelegram {
    /// New computation input for all nodes. Opaque JSON — the runner
    /// deserialises against `C::Input` on apply.
    Input { value: serde_json::Value },
    /// Command targeted at specific nodes.
    Command {
        targets: Vec<u8>,
        #[serde(flatten)]
        payload: Command,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    GetStatus,
    InjectDropResults { count: u32 },
    InjectDropAcks { count: u32 },
    ClearInjection,
}

/// Outgoing telegram sent back to the diagnostic group.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutgoingTelegram<'a> {
    Staged {
        source_node_id: u8,
        staged_kind: &'a str,
    },
    Status {
        source_node_id: u8,
        data: StatusResponse,
    },
    Error {
        source_node_id: u8,
        message: String,
    },
}

/// Snapshot of node runtime state returned on `GetStatus`.
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

/// Staged changes to apply at the next cycle boundary. All fields have
/// overwrite semantics.
#[derive(Debug, Default)]
pub struct PendingChanges {
    pub input_json: Option<serde_json::Value>,
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

pub struct Diagnostic {
    socket: UdpSocket,
    multicast_addr: SocketAddrV4,
    node_id: u8,
    pub injection: InjectionState,
    pub pending: PendingChanges,
}

impl Diagnostic {
    /// Bind to the diagnostic multicast group on the given interface.
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
            "diagnostic bound"
        );

        Ok(Self {
            socket,
            multicast_addr,
            node_id,
            injection: InjectionState::default(),
            pending: PendingChanges::default(),
        })
    }

    /// Non-blocking receive + dispatch. Returns `Some(GetStatus)` so the
    /// runner can build the status response with its full context; all
    /// other commands are staged in-place.
    pub fn try_recv(&mut self) -> Option<Command> {
        let mut buf = [0u8; 2048];
        let (n, _) = match self.socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return None,
            Err(e) => {
                warn!(error = ?e, "diagnostic recv failed");
                return None;
            }
        };
        let telegram = match serde_json::from_slice::<IncomingTelegram>(&buf[..n]) {
            Ok(t) => t,
            Err(_) => return None,
        };
        match telegram {
            IncomingTelegram::Input { value } => {
                self.pending.input_json = Some(value);
                self.stage_ack("input");
                None
            }
            IncomingTelegram::Command { targets, payload } => {
                if !targets.contains(&self.node_id) {
                    return None;
                }
                self.handle_command(payload)
            }
        }
    }

    /// Send a telegram back to the diagnostic multicast group.
    pub fn send(&self, telegram: &OutgoingTelegram) {
        let bytes = match serde_json::to_vec(telegram) {
            Ok(b) => b,
            Err(e) => {
                error!(error = ?e, "failed to serialise outgoing telegram");
                return;
            }
        };
        if let Err(e) = self.socket.send_to(&bytes, self.multicast_addr) {
            warn!(error = ?e, "failed to send diagnostic response");
        }
    }

    /// Apply staged injection updates to the active state. Called by the
    /// runner at cycle start.
    pub fn apply_pending_injection(&mut self) {
        if self.pending.clear_injection {
            self.injection = InjectionState::default();
        }
        if let Some(n) = self.pending.drop_next_n_results.take() {
            self.injection.drop_next_n_results = n;
        }
        if let Some(n) = self.pending.drop_next_n_acks.take() {
            self.injection.drop_next_n_acks = n;
        }
        self.pending.clear_injection = false;
    }

    /// Take the staged input JSON. The runner deserialises against
    /// `C::Input` before applying.
    pub fn take_pending_input(&mut self) -> Option<serde_json::Value> {
        self.pending.input_json.take()
    }

    /// Decrement the drop-results counter and return whether to suppress.
    pub fn should_drop_result(&mut self) -> bool {
        if self.injection.drop_next_n_results > 0 {
            self.injection.drop_next_n_results -= 1;
            true
        } else {
            false
        }
    }

    /// Decrement the drop-acks counter and return whether to suppress.
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

    /// Stage a command. GetStatus is handled synchronously by the runner.
    fn handle_command(&mut self, cmd: Command) -> Option<Command> {
        match cmd {
            Command::GetStatus => Some(Command::GetStatus),
            Command::InjectDropResults { count } => {
                self.pending.drop_next_n_results = Some(count);
                self.stage_ack("inject_drop_results");
                None
            }
            Command::InjectDropAcks { count } => {
                self.pending.drop_next_n_acks = Some(count);
                self.stage_ack("inject_drop_acks");
                None
            }
            Command::ClearInjection => {
                self.pending.clear_injection = true;
                self.stage_ack("clear_injection");
                None
            }
        }
    }

    fn stage_ack(&self, staged_kind: &'static str) {
        debug!(kind = staged_kind, "staged for next cycle");
        self.send(&OutgoingTelegram::Staged {
            source_node_id: self.node_id,
            staged_kind,
        });
    }
}

/// Look up the first IPv4 address of `iface_name` via `ip -4 -o addr show`.
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
