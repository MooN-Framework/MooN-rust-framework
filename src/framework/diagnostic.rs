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
//! - corrupt_result_remaining   → perturb own Result before send (T6)
//! - drop_from_peers_mask       → drop all incoming frames from the given
//!                                 peer node ids (mask bit N ⇔ node id N).
//!                                 Persistent until ClearInjection; used
//!                                 by T10 to force an asymmetric-view
//!                                 scenario without a real partitioner.
//! - fake_phase_header_remaining / _value → for N sends, replace the
//!                                 outgoing frame's node_state_wire byte
//!                                 with `wire_value`. Verifies the
//!                                 phase-header guards in CycleSync,
//!                                 Resync, and the ingest rendezvous
//!                                 path (T16). `wire_value` must map to
//!                                 a valid NodeState variant.

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
    /// Remaining Input frames to suppress.
    pub drop_next_n_inputs: u32,
    /// Remaining Result frames to suppress.
    pub drop_next_n_results: u32,
    /// Remaining Ack frames to suppress.
    pub drop_next_n_acks: u32,
    /// Remaining CycleSync beacons to suppress.
    pub drop_next_n_cyclesync: u32,
    /// Remaining CRC attestations to suppress.
    pub drop_next_n_crc: u32,
    /// Remaining exclusion proposals to suppress.
    pub drop_next_n_votes: u32,

    /// Remaining cycles to report a bogus system-state CRC.
    pub fake_crc_remaining: u32,
    /// Remaining cycles to nominate the wrong publisher.
    pub divergent_publisher_remaining: u32,

    /// Remaining cycles of soft shutdown. Reversible, unlike a real
    /// process exit.
    pub mute_cycles_remaining: u32,
    /// Extra milliseconds to sleep in ReadInputs while the delay is
    /// active.
    pub cycle_delay_ms: u32,
    /// Remaining cycles to apply `cycle_delay_ms`.
    pub cycle_delay_remaining: u32,
    /// Remaining cycles to corrupt the own result before sending.
    pub corrupt_result_remaining: u32,

    /// Bit N set means: drop every incoming frame whose sender node id
    /// is N. Persistent until ClearInjection; NOT a per-cycle decrement.
    pub drop_from_peers_mask: u8,

    /// For the next `fake_phase_header_remaining` outbound frames, the
    /// runner replaces the node_state_wire byte with
    /// `fake_phase_header_value`. Decremented once per send.
    pub fake_phase_header_remaining: u32,
    /// State byte written in place of the real one.
    pub fake_phase_header_value: u8,
}

/// Incoming telegram from the GUI.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IncomingTelegram {
    /// New computation input broadcast to all nodes. Opaque JSON — the
    /// runner deserialises against `C::Input` on apply.
    Input {
        /// Opaque input value, deserialized against `C::Input` on apply.
        value: serde_json::Value,
    },
    /// Command targeted at specific nodes. `Command::TargetedInput`
    /// carries a value that only the targeted nodes will pick up,
    /// enabling per-node input divergence tests (T5).
    Command {
        /// Node ids this command applies to.
        targets: Vec<u8>,
        /// The command itself.
        #[serde(flatten)]
        payload: Command,
    },
}

/// A command addressed to one or more nodes.
///
/// Everything except `GetStatus` is staged and applied at the next
/// cycle boundary, so an injection takes effect on every targeted node
/// in the same cycle.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Command {
    /// Ask for a status response. Answered immediately, not staged.
    GetStatus,

    /// Suppress the next `count` Input frames.
    InjectDropInputs {
        /// Frames to suppress.
        count: u32,
    },
    /// Suppress the next `count` Result frames.
    InjectDropResults {
        /// Frames to suppress.
        count: u32,
    },
    /// Suppress the next `count` Ack frames.
    InjectDropAcks {
        /// Frames to suppress.
        count: u32,
    },
    /// Suppress the next `count` CycleSync beacons.
    InjectDropCyclesync {
        /// Frames to suppress.
        count: u32,
    },
    /// Suppress the next `count` CRC attestations.
    InjectDropCrc {
        /// Frames to suppress.
        count: u32,
    },
    /// Suppress the next `count` exclusion proposals.
    InjectDropVotes {
        /// Frames to suppress.
        count: u32,
    },

    /// Report a bogus system-state CRC for `count` cycles.
    InjectFakeCrc {
        /// Cycles to stay active.
        count: u32,
    },
    /// Nominate the wrong publisher for `count` cycles.
    InjectDivergentPublisher {
        /// Cycles to stay active.
        count: u32,
    },

    /// Exit the process at the next cycle boundary.
    InjectShutdown,
    /// Go silent for `cycles` cycles without exiting. Reversible.
    InjectMute {
        /// Cycles to stay muted.
        cycles: u32,
    },
    /// Sleep `ms` extra milliseconds in ReadInputs for `count` cycles.
    InjectCycleDelay {
        /// Extra sleep per affected cycle.
        ms: u32,
        /// Cycles to stay active.
        count: u32,
    },
    /// Apply an input only on the targeted nodes, which produces input
    /// divergence without touching the other nodes.
    InjectTargetedInput {
        /// Opaque input value for the targeted nodes.
        value: serde_json::Value,
    },
    /// Corrupt the own result before sending, for `count` cycles.
    InjectCorruptResult {
        /// Cycles to stay active.
        count: u32,
    },

    /// T10 — asymmetric view. `peers_mask` bit N = drop frames whose
    /// sender node id is N. Persistent until ClearInjection.
    InjectDropFromPeer {
        /// Bit N set means drop frames from node id N.
        peers_mask: u8,
    },

    /// T16 — phase-header spoof. For `count` outgoing frames, replace
    /// the node_state_wire byte with `wire_value`. `wire_value` should
    /// decode to a valid NodeState variant, otherwise the runner logs
    /// a warning and falls back to the real state.
    InjectFakePhaseHeader {
        /// Frames to spoof.
        count: u32,
        /// State byte to write instead of the real one.
        wire_value: u8,
    },

    /// One-shot: on the next `handle_read_inputs`, skip the
    /// `record_own_input` call so the following ShareInputs sees no
    /// own input and takes the `LocalFault` failsafe path. Self-clearing.
    InjectInputProviderFail,

    /// One-shot: on the next `handle_share_inputs`, the compute step
    /// returns Fault without invoking `Computation::compute`, forcing
    /// the `LocalFault` failsafe path. Self-clearing.
    InjectComputationFail,

    /// Clear every active injection, persistent ones included.
    ClearInjection,
}

/// A node's reply on the diagnostic channel.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutgoingTelegram<'a> {
    /// Acknowledges that a command was accepted and will take effect at
    /// the next cycle boundary.
    Staged {
        /// Replying node.
        source_node_id: u8,
        /// Which command was staged.
        staged_kind: &'a str,
    },
    /// Answer to `GetStatus`.
    Status {
        /// Replying node.
        source_node_id: u8,
        /// The status itself.
        data: StatusResponse,
    },
    /// A command could not be accepted.
    Error {
        /// Replying node.
        source_node_id: u8,
        /// What went wrong.
        message: String,
    },
}

/// One node's view of itself, as reported to the diagnostic tool.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct StatusResponse {
    /// Reporting node.
    pub node_id: u8,
    /// Session id of the current process run.
    pub session_id: u64,
    /// Current phase, `Debug`-formatted.
    pub node_state: String,
    /// Current cycle counter.
    pub current_seq: u32,
    /// Duration of the last completed cycle, in microseconds.
    pub last_cycle_us: Option<u128>,
    /// Whether the clock offsets are currently usable.
    pub sync_valid: bool,
    /// Current clock uncertainty, in nanoseconds.
    pub sync_epsilon_ns: i64,
    /// Cycles since the last clock-sync round.
    pub cycles_since_last_sync: u32,
    /// Peers as this node sees them.
    pub peers: Vec<PeerStatus>,
    /// Injections currently active on this node.
    pub injection: InjectionSnapshot,
    /// Whether an input is staged for the next cycle boundary.
    pub pending_input: bool,
    /// Whether an injection change is staged for the next boundary.
    pub pending_injection_update: bool,

    /// Failsafe reason latched by the first `mark_failsafe` call this
    /// process lifetime, as the decimal `u8` discriminant of
    /// `FailsafeReason`. `None` while the node has not yet marked
    /// itself for failsafe.
    pub last_failsafe_reason: Option<u8>,

    /// Ring buffer (up to 32) of the most recent FSM transitions
    /// observed by this process, oldest first. Each entry is
    /// `(from_state, event, to_state)` formatted with `Debug`.
    pub recent_transitions: Vec<TransitionRecord>,
}

/// One recorded state-machine transition.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct TransitionRecord {
    /// State the node was in.
    pub from: String,
    /// Event the phase handler returned.
    pub event: String,
    /// State the node moved to.
    pub to: String,
}

/// One peer as seen by the reporting node.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct PeerStatus {
    /// Peer node id.
    pub id: u8,
    /// Peer health, `Debug`-formatted.
    pub health: String,
    /// Consecutive cycles this peer was faulted.
    pub consecutive_faults: u32,
    /// Consecutive healthy cycles, i.e. probation progress.
    pub consecutive_healthy_cycles: u32,
}

/// Serializable copy of [`InjectionState`] for the status reply.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct InjectionSnapshot {
    /// Remaining Input frames to suppress.
    pub drop_next_n_inputs: u32,
    /// Remaining Result frames to suppress.
    pub drop_next_n_results: u32,
    /// Remaining Ack frames to suppress.
    pub drop_next_n_acks: u32,
    /// Remaining CycleSync beacons to suppress.
    pub drop_next_n_cyclesync: u32,
    /// Remaining CRC attestations to suppress.
    pub drop_next_n_crc: u32,
    /// Remaining exclusion proposals to suppress.
    pub drop_next_n_votes: u32,
    /// Remaining cycles reporting a bogus CRC.
    pub fake_crc_remaining: u32,
    /// Remaining cycles nominating the wrong publisher.
    pub divergent_publisher_remaining: u32,
    /// Remaining muted cycles.
    pub mute_cycles_remaining: u32,
    /// Extra sleep per affected cycle, in milliseconds.
    pub cycle_delay_ms: u32,
    /// Remaining cycles applying the delay.
    pub cycle_delay_remaining: u32,
    /// Remaining cycles corrupting the own result.
    pub corrupt_result_remaining: u32,
    /// Senders currently being dropped, one bit per node id.
    pub drop_from_peers_mask: u8,
    /// Remaining frames with a spoofed state byte.
    pub fake_phase_header_remaining: u32,
    /// State byte written in place of the real one.
    pub fake_phase_header_value: u8,
}

/// Staged changes to apply at the next cycle boundary. All optional
/// fields have overwrite semantics.
#[derive(Debug, Default)]
pub struct PendingChanges {
    /// Input broadcast to every node.
    pub input_json: Option<serde_json::Value>,
    /// Input for this node only.
    pub targeted_input_json: Option<serde_json::Value>,

    /// Staged Input suppression count.
    pub drop_next_n_inputs: Option<u32>,
    /// Staged Result suppression count.
    pub drop_next_n_results: Option<u32>,
    /// Staged Ack suppression count.
    pub drop_next_n_acks: Option<u32>,
    /// Staged CycleSync suppression count.
    pub drop_next_n_cyclesync: Option<u32>,
    /// Staged CRC suppression count.
    pub drop_next_n_crc: Option<u32>,
    /// Staged exclusion-proposal suppression count.
    pub drop_next_n_votes: Option<u32>,

    /// Staged bogus-CRC cycle count.
    pub fake_crc_remaining: Option<u32>,
    /// Staged divergent-publisher cycle count.
    pub divergent_publisher_remaining: Option<u32>,

    /// Staged mute duration in cycles.
    pub mute_cycles_remaining: Option<u32>,
    /// Staged cycle delay as `(milliseconds, cycles)`.
    pub cycle_delay: Option<(u32, u32)>,

    /// Clear every active injection at the next boundary.
    pub clear_injection: bool,
    /// Exit the process at the next boundary.
    pub shutdown: bool,
    /// Staged result-corruption cycle count.
    pub corrupt_result_remaining: Option<u32>,

    /// T10 staging.
    pub drop_from_peers_mask: Option<u8>,
    /// T16 staging: (count, wire_value).
    pub fake_phase_header: Option<(u32, u8)>,

    /// One-shot: skip `record_own_input` in the next ReadInputs.
    pub inject_input_provider_fail: bool,
    /// One-shot: fail the next compute step without calling the
    /// application's `compute`.
    pub inject_computation_fail: bool,
}

impl PendingChanges {
    /// True when an input is staged, broadcast or targeted.
    pub fn has_input(&self) -> bool {
        self.input_json.is_some() || self.targeted_input_json.is_some()
    }

    /// True when any injection change is staged.
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
            || self.corrupt_result_remaining.is_some()
            || self.drop_from_peers_mask.is_some()
            || self.fake_phase_header.is_some()
            || self.inject_input_provider_fail
            || self.inject_computation_fail
            || self.clear_injection
    }
}

/// The node's end of the diagnostic channel.
///
/// Owns the socket, the active injections and the changes staged for
/// the next cycle boundary.
pub struct Diagnostic {
    socket: UdpSocket,
    multicast_addr: SocketAddrV4,
    node_id: u8,
    /// Injections currently in effect.
    pub injection: InjectionState,
    /// Changes waiting for the next cycle boundary.
    pub pending: PendingChanges,
}

impl Diagnostic {
    /// Bind the diagnostic socket and join its multicast group.
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

    /// Poll the socket once, non-blocking. Staging commands are
    /// applied to `pending` and return `None`; `GetStatus` is returned
    /// to the caller, which has the state needed to answer it.
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

    /// Broadcast a reply. Failures are logged, never propagated: the
    /// diagnostic channel must not affect the cycle.
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
        if let Some(n) = self.pending.corrupt_result_remaining.take() {
            self.injection.corrupt_result_remaining = n;
        }
        if let Some(m) = self.pending.drop_from_peers_mask.take() {
            self.injection.drop_from_peers_mask = m;
        }
        if let Some((count, value)) = self.pending.fake_phase_header.take() {
            self.injection.fake_phase_header_remaining = count;
            self.injection.fake_phase_header_value = value;
        }
    }

    /// Take the staged input, if any. Targeted input wins over a
    /// broadcast one.
    pub fn take_pending_input(&mut self) -> Option<serde_json::Value> {
        self.pending
            .targeted_input_json
            .take()
            .or_else(|| self.pending.input_json.take())
    }

    /// Take the staged shutdown flag.
    pub fn take_pending_shutdown(&mut self) -> bool {
        core::mem::replace(&mut self.pending.shutdown, false)
    }

    // --- Drop counters ---------------------------------------------------

    /// True when an Input frame should be suppressed this cycle.
    /// Decrements the counter.
    pub fn should_drop_input(&mut self) -> bool {
        dec(&mut self.injection.drop_next_n_inputs)
    }
    /// True when a Result frame should be suppressed this cycle.
    /// Decrements the counter.
    pub fn should_drop_result(&mut self) -> bool {
        dec(&mut self.injection.drop_next_n_results)
    }
    /// True when an Ack frame should be suppressed this cycle.
    /// Decrements the counter.
    pub fn should_drop_ack(&mut self) -> bool {
        dec(&mut self.injection.drop_next_n_acks)
    }
    /// True when a CycleSync beacon should be suppressed this cycle.
    /// Decrements the counter.
    pub fn should_drop_cyclesync(&mut self) -> bool {
        dec(&mut self.injection.drop_next_n_cyclesync)
    }
    /// True when a CRC attestation should be suppressed this cycle.
    /// Decrements the counter.
    pub fn should_drop_crc(&mut self) -> bool {
        dec(&mut self.injection.drop_next_n_crc)
    }
    /// True when an exclusion proposal should be suppressed this cycle.
    /// Decrements the counter.
    pub fn should_drop_vote(&mut self) -> bool {
        dec(&mut self.injection.drop_next_n_votes)
    }
    /// True when the own result should be corrupted before sending.
    /// Decrements the counter.
    pub fn should_corrupt_result(&mut self) -> bool {
        dec(&mut self.injection.corrupt_result_remaining)
    }

    /// T10 — persistent, not decremented. Cleared only via ClearInjection.
    /// `peer_node_id` is the frame sender's node id.
    pub fn should_drop_from(&self, peer_node_id: u8) -> bool {
        if peer_node_id > 7 {
            return false;
        }
        (self.injection.drop_from_peers_mask & (1u8 << peer_node_id)) != 0
    }

    /// T16 — returns Some(wire_value) if we should replace the outgoing
    /// frame's node_state_wire this cycle. Decremented on consumption.
    pub fn should_fake_phase_header(&mut self) -> Option<u8> {
        if self.injection.fake_phase_header_remaining > 0 {
            self.injection.fake_phase_header_remaining -= 1;
            Some(self.injection.fake_phase_header_value)
        } else {
            None
        }
    }

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

    /// Node id this diagnostic instance belongs to.
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

            Command::InjectCorruptResult { count } => {
                self.pending.corrupt_result_remaining = Some(count);
                self.stage_ack("inject_corrupt_result");
                None
            }
            Command::InjectDropFromPeer { peers_mask } => {
                self.pending.drop_from_peers_mask = Some(peers_mask);
                self.stage_ack("inject_drop_from_peer");
                None
            }
            Command::InjectFakePhaseHeader { count, wire_value } => {
                self.pending.fake_phase_header = Some((count, wire_value));
                self.stage_ack("inject_fake_phase_header");
                None
            }
            Command::InjectInputProviderFail => {
                self.pending.inject_input_provider_fail = true;
                self.stage_ack("inject_input_provider_fail");
                None
            }
            Command::InjectComputationFail => {
                self.pending.inject_computation_fail = true;
                self.stage_ack("inject_computation_fail");
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
