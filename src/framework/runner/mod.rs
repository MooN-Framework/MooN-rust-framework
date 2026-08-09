mod collect;
mod diag;
mod ingest;
mod phases;

pub use collect::PhaseOutcome;

use crate::framework::config::{CycleTiming, DiagnosticConfig};
use crate::framework::diagnostic::Diagnostic;
use crate::framework::state::RunState;
use crate::framework::state_machine::NodeState;
use crate::framework::traits::{Computation, DecisionSink, SelfTest, Voter};
use crate::framework::transport::UdpTransport;
use serde::Deserialize;
use std::time::Instant;
use tracing::{debug, error, info};

/// Orchestrates the per-cycle state machine over transport, state, sink,
/// computation, self-test, and optional diagnostic. One runner instance
/// per node.
pub struct Runner<C, V, S, T>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    T: SelfTest,
{
    pub(super) state: RunState<V, C::Input>,
    pub(super) transport: UdpTransport<C::Input, V::Payload>,
    pub(super) computation: C,
    pub(super) input: C::Input,
    pub(super) sink: S,
    pub(super) self_test: T,
    pub(super) timing: CycleTiming,
    pub(super) last_cycle_start: Option<Instant>,
    pub(super) last_cycle_us: Option<u128>,
    pub(super) next_cycle_deadline: Option<Instant>,
    pub(super) cycles_since_last_sync: u32,
    pub(super) diagnostic: Option<Diagnostic>,
    /// Rendezvous flag: set by ingest paths when a healthy peer's frame
    /// arrives with `node_state == ErrorManagement`. Read + cleared by
    /// the in-cycle phase handlers via `take_peer_in_error`.
    pub(super) peer_in_error_seen: bool,
}

impl<C, V, S, T> Runner<C, V, S, T>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    T: SelfTest,
    C::Input: for<'de> Deserialize<'de>,
{
    /// Construct a runner. Attempts to bring up the diagnostic side-channel
    /// when enabled; failure is non-fatal.
    pub fn new(
        state: RunState<V, C::Input>,
        transport: UdpTransport<C::Input, V::Payload>,
        computation: C,
        input: C::Input,
        sink: S,
        self_test: T,
        timing: CycleTiming,
        diag_cfg: DiagnosticConfig,
    ) -> Self {
        info!(own_id = state.own_id(), "runner constructed");
        let diagnostic = if diag_cfg.enabled {
            match Diagnostic::new(&diag_cfg, state.own_id()) {
                Ok(d) => Some(d),
                Err(e) => {
                    error!(error = ?e, "diagnostic init failed, continuing without");
                    None
                }
            }
        } else {
            None
        };
        Self {
            state,
            transport,
            computation,
            input,
            sink,
            self_test,
            timing,
            last_cycle_start: None,
            last_cycle_us: None,
            next_cycle_deadline: None,
            cycles_since_last_sync: 0,
            diagnostic,
            peer_in_error_seen: false,
        }
    }

    /// Overwrite the runtime input, bypassing diagnostic staging.
    pub fn set_input(&mut self, input: C::Input) {
        self.input = input;
    }

    /// Cycle-global deadline anchor. Falls back to `Instant::now()` for
    /// the first cycle before `handle_read_inputs` has set `last_cycle_start`.
    #[inline]
    pub(super) fn cycle_anchor(&self) -> Instant {
        self.last_cycle_start.unwrap_or_else(Instant::now)
    }

    /// Read + clear the rendezvous flag. Callers use this at the entry of
    /// an in-cycle phase to discard a stale flag from a previous phase,
    /// and again before the phase returns to check whether a peer has
    /// entered ErrorManagement during this phase.
    #[inline]
    pub(super) fn take_peer_in_error(&mut self) -> bool {
        core::mem::replace(&mut self.peer_in_error_seen, false)
    }

    /// Main loop. Returns once the node enters `Failsafe`.
    pub fn run(&mut self) {
        info!(own_id = self.state.own_id(), "runner started");
        loop {
            let current = self.state.node_state();
            debug!(state = ?current, seq = self.state.current_seq(), "phase enter");

            let event = match current {
                NodeState::Startup => self.handle_startup(),
                NodeState::InitSync => self.handle_init_sync(),
                NodeState::PeerSync => self.handle_peer_sync(),
                NodeState::CycleSync => self.handle_cycle_sync(),
                NodeState::ReadInputs => self.handle_read_inputs(),
                NodeState::ShareInputs => self.handle_share_inputs(),
                NodeState::ShareResult => self.handle_share_result(),
                NodeState::SendAck => self.handle_send_ack(),
                NodeState::PublishResult => self.handle_publish(),
                NodeState::ErrorManagement => self.handle_error_management(),
                NodeState::Isolation => self.handle_isolation(),
                NodeState::ResyncLostPeer => self.handle_resync_lost_node(),
                NodeState::SystemStateCrcExchange => self.handle_system_state_crc(),
                NodeState::SystemStateSync => self.handle_system_state_sync(),
                NodeState::Failsafe => {
                    self.enter_failsafe();
                    return;
                }
            };

            self.poll_diagnostic();

            let next = current.next(event);
            info!(from = ?current, event = ?event, to = ?next, "transition");
            self.state.set_node_state(next);
        }
    }

    fn enter_failsafe(&mut self) {
        error!(own_id = self.state.own_id(), "failsafe entered");
    }
}