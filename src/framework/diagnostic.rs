//! Multicast diagnostic side-channel.
//!
//! GUI and nodes share a multicast group; the GUI publishes telegrams and
//! nodes reply on the same group. Incoming input values and state-changing
//! commands are staged and applied at the next cycle boundary — the
//! CycleSync barrier guarantees synchronous activation across nodes.
//! `GetStatus` bypasses staging and is answered immediately.
//!
//! Injection catalog (see the fault-taxonomy document for the mapping to
//! test cases T1..T17):
//!
//! - drop_next_n_inputs         → per-cycle suppression of Input frames
//! - drop_next_n_results        → per-cycle suppression of Result frames
//! - drop_next_n_acks           → per-cycle suppression of Ack frames
//! - drop_next_n_cyclesync      → per-cycle suppression of CycleSync State
//! - drop_next_n_crc            → per-cycle suppression of SystemStateCrc
//! - drop_next_n_votes          → per-cycle suppression of ExclusionProposal
//! - fake_crc_remaining         → send a bogus CRC value (T7)
//! - divergent_publisher_remaining → send Ack with a wrong publisher pick
//! - cycle_delay_ms/remaining   → sleep additional ms in ReadInputs (T8)
//! - mute_cycles_remaining      → soft shutdown, reversible via clear
//! - shutdown (one-shot)        → hard process exit at cycle boundary
//! - targeted_input             → apply a distinct input only to this node

use crate::framework::config::DiagnosticConfig;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use tracing::{debug, error, info, warn};

/// Active fault-injection counters. Consulted by the runner at the
/// relevant send/receive sites and decremented on effect.
#[derive(Debug, Default, Clone, Copy)]
pub struct InjectionState {
    // Per-frame-type drop counters. Each `should_drop_*` call decrements.
    pub drop_next_n_inputs: u32,
    pub drop_next_n_results: u32,
    pub drop_next_n_acks: u32,
    pub drop_next_n_cyclesync: u32,
    pub drop_next_n_crc: u32,
    pub drop_next_n_votes: u32,

    // Value corruption counters.
    pub fake_crc_remaining: u32,
    pub divergent_publisher_remaining: u32,

    // Whole-node behaviour.
    pub mute_cycles_remaining: u32,
    pub cycle_delay_ms: u32,
    pub cycle_delay_remaining: u32,
}

/// Incoming telegram from the GUI.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomingTelegram {
    /// New computation input broadcast to all nodes. Opaque JSON — the
    /// runner deserialises against `C::Input` on apply.
    Input { value: serde_json::Value },
    /// Command targeted at specific nodes. `Command::TargetedInput`
    /// carries a value that only the targeted nodes will pick up,
    /// enabling per-node input divergence tests (T5).
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

    // Frame-type suppression.
    InjectDropInputs { count: u32 },
    InjectDropResults { count: u32 },
    InjectDropAcks { count: u32 },
    InjectDropCyclesync { count: u32 },
    InjectDropCrc { count: u32 },
    InjectDropVotes { count: u32 },

    // Value corruption.
    InjectFakeCrc { count: u32 },
    InjectDivergentPublisher { count: u32 },

    // Whole-node behaviour.
    InjectShutdown,
    InjectMute { cycles: u32 },
    InjectCycleDelay { ms: u32, count: u32 },
    InjectTargetedInput { value: serde_json::Value },

    ClearInjection,
}

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
    pub drop_next_n_inputs: u32,
    pub drop_next_n_results: u32,
    pub drop_next_n_acks: u32,
    pub drop_next_n_cyclesync: u32,
    pub drop_next_n_crc: u32,
    pub drop_next_n_votes: u32,
    pub fake_crc_remaining: u32,
    pub divergent_publisher_remaining: u32,
    pub mute_cycles_remaining: u32,
    pub cycle_delay_ms: u32,
    pub cycle_delay_remaining: u32,
}

/// Staged changes to apply at the next cycle boundary. All optional
/// fields have overwrite semantics.
#[derive(Debug, Default)]
pub struct PendingChanges {
    // Broadcast input (applied to any node that receives it).
    pub input_json: Option<serde_json::Value>,
    // Targeted input (applied only if the receiving node was in the
    // command's target list — that check happens in try_recv).
    pub targeted_input_json: Option<serde_json::Value>,

    pub drop_next_n_inputs: Option<u32>,
    pub drop_next_n_results: Option<u32>,
    pub drop_next_n_acks: Option<u32>,
    pub drop_next_n_cyclesync: Option<u32>,
    pub drop_next_n_crc: Option<u32>,
    pub drop_next_n_votes: Option<u32>,

    pub fake_crc_remaining: Option<u32>,
    pub divergent_publisher_remaining: Option<u32>,

    pub mute_cycles_remaining: Option<u32>,
    pub cycle_delay: Option<(u32, u32)>, // (ms, count)

    pub clear_injection: bool,
    /// One-shot hard shutdown flag. Not affected by ClearInjection.
    pub shutdown: bool,
}

impl PendingChanges {
    pub fn has_input(&self) -> bool {
        self.input_json.is_some() || self.targeted_input_json.is_some()
    }

    pub fn has_injection_update(&self) -> bool {
        self.drop_next_n_inputs.is_some()
            || self.drop_next_n_results.is_some()
            || self.drop_next_n_acks.is_some()
            || self.drop_next_n_cyclesync.is_some()
            || self.drop_next_n_crc.is_some()
            || self.drop_next_n_votes.is_some()
            || self.fake_crc_remaining.is_some()
            || self.divergent_publisher_remaining.is_some()
            || self.mute_cycles_remaining.is_some()
            || self.cycle_delay.is_some()
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

    pub fn try_recv(&mut self) -> Option<Command> {
        let mut buf = [0u8; 4096];
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

    /// Apply staged injection updates. Called by the runner at cycle start.
    /// Also ticks per-cycle counters (mute, cycle_delay).
    pub fn apply_pending_injection(&mut self) {
        if self.pending.clear_injection {
            // Note: shutdown is NOT cleared here — it fires regardless.
            let shutdown = self.pending.shutdown;
            self.injection = InjectionState::default();
            self.pending = PendingChanges {
                shutdown,
                ..Default::default()
            };
            return;
        }

        // Simple setters.
        if let Some(n) = self.pending.drop_next_n_inputs.take() {
            self.injection.drop_next_n_inputs = n;
        }
        if let Some(n) = self.pending.drop_next_n_results.take() {
            self.injection.drop_next_n_results = n;
        }
        if let Some(n) = self.pending.drop_next_n_acks.take() {
            self.injection.drop_next_n_acks = n;
        }
        if let Some(n) = self.pending.drop_next_n_cyclesync.take() {
            self.injection.drop_next_n_cyclesync = n;
        }
        if let Some(n) = self.pending.drop_next_n_crc.take() {
            self.injection.drop_next_n_crc = n;
        }
        if let Some(n) = self.pending.drop_next_n_votes.take() {
            self.injection.drop_next_n_votes = n;
        }
        if let Some(n) = self.pending.fake_crc_remaining.take() {
            self.injection.fake_crc_remaining = n;
        }
        if let Some(n) = self.pending.divergent_publisher_remaining.take() {
            self.injection.divergent_publisher_remaining = n;
        }
        if let Some(n) = self.pending.mute_cycles_remaining.take() {
            self.injection.mute_cycles_remaining = n;
        }
        if let Some((ms, count)) = self.pending.cycle_delay.take() {
            self.injection.cycle_delay_ms = ms;
            self.injection.cycle_delay_remaining = count;
        }

        // Per-cycle decrements for time-based counters happen inside the
        // relevant `should_*`/`take_*` accessors so that the count reflects
        // remaining EFFECTS, not remaining cycle boundaries.
    }

    pub fn take_pending_input(&mut self) -> Option<serde_json::Value> {
        // Targeted takes precedence — a per-node override wins over a
        // broadcast in the same cycle.
        self.pending
            .targeted_input_json
            .take()
            .or_else(|| self.pending.input_json.take())
    }

    pub fn take_pending_shutdown(&mut self) -> bool {
        core::mem::replace(&mut self.pending.shutdown, false)
    }

    // --- Drop counters ---------------------------------------------------

    pub fn should_drop_input(&mut self) -> bool { dec(&mut self.injection.drop_next_n_inputs) }
    pub fn should_drop_result(&mut self) -> bool { dec(&mut self.injection.drop_next_n_results) }
    pub fn should_drop_ack(&mut self) -> bool { dec(&mut self.injection.drop_next_n_acks) }
    pub fn should_drop_cyclesync(&mut self) -> bool { dec(&mut self.injection.drop_next_n_cyclesync) }
    pub fn should_drop_crc(&mut self) -> bool { dec(&mut self.injection.drop_next_n_crc) }
    pub fn should_drop_vote(&mut self) -> bool { dec(&mut self.injection.drop_next_n_votes) }

    // --- Value corruption ------------------------------------------------

    /// Returns Some(bogus) if we should replace our CRC this cycle.
    pub fn should_fake_crc(&mut self) -> Option<u32> {
        if self.injection.fake_crc_remaining > 0 {
            self.injection.fake_crc_remaining -= 1;
            Some(0xDEADBEEF)
        } else {
            None
        }
    }

    /// Returns true if we should send an ack with a bogus publisher pick.
    pub fn should_send_divergent_publisher(&mut self) -> bool {
        dec(&mut self.injection.divergent_publisher_remaining)
    }

    // --- Whole-node behaviour -------------------------------------------

    /// Called once per cycle in apply_pending_diagnostic. Ticks mute and
    /// cycle_delay counters. Returns (mute_active, delay_ms_this_cycle).
    pub fn tick_cycle_effects(&mut self) -> (bool, u32) {
        let mute_active = self.injection.mute_cycles_remaining > 0;
        if mute_active {
            self.injection.mute_cycles_remaining -= 1;
        }

        let delay_ms = if self.injection.cycle_delay_remaining > 0 {
            self.injection.cycle_delay_remaining -= 1;
            self.injection.cycle_delay_ms
        } else {
            0
        };

        (mute_active, delay_ms)
    }

    pub fn node_id(&self) -> u8 {
        self.node_id
    }

    fn handle_command(&mut self, cmd: Command) -> Option<Command> {
        match cmd {
            Command::GetStatus => Some(Command::GetStatus),

            Command::InjectDropInputs { count } => {
                self.pending.drop_next_n_inputs = Some(count);
                self.stage_ack("inject_drop_inputs");
                None
            }
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
            Command::InjectDropCyclesync { count } => {
                self.pending.drop_next_n_cyclesync = Some(count);
                self.stage_ack("inject_drop_cyclesync");
                None
            }
            Command::InjectDropCrc { count } => {
                self.pending.drop_next_n_crc = Some(count);
                self.stage_ack("inject_drop_crc");
                None
            }
            Command::InjectDropVotes { count } => {
                self.pending.drop_next_n_votes = Some(count);
                self.stage_ack("inject_drop_votes");
                None
            }

            Command::InjectFakeCrc { count } => {
                self.pending.fake_crc_remaining = Some(count);
                self.stage_ack("inject_fake_crc");
                None
            }
            Command::InjectDivergentPublisher { count } => {
                self.pending.divergent_publisher_remaining = Some(count);
                self.stage_ack("inject_divergent_publisher");
                None
            }

            Command::InjectShutdown => {
                self.pending.shutdown = true;
                self.stage_ack("inject_shutdown");
                None
            }
            Command::InjectMute { cycles } => {
                self.pending.mute_cycles_remaining = Some(cycles);
                self.stage_ack("inject_mute");
                None
            }
            Command::InjectCycleDelay { ms, count } => {
                self.pending.cycle_delay = Some((ms, count));
                self.stage_ack("inject_cycle_delay");
                None
            }
            Command::InjectTargetedInput { value } => {
                self.pending.targeted_input_json = Some(value);
                self.stage_ack("inject_targeted_input");
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

/// Decrement helper: if the counter is >0, decrement it and return true.
#[inline]
fn dec(counter: &mut u32) -> bool {
    if *counter > 0 {
        *counter -= 1;
        true
    } else {
        false
    }
}

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