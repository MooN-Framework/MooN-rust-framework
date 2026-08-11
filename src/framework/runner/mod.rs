mod collect;
mod diag;
mod ingest;
mod phases;

pub use collect::PhaseOutcome;

use crate::framework::config::{CycleTiming, DiagnosticConfig};
use crate::framework::diagnostic::Diagnostic;
use crate::framework::state::RunState;
use crate::framework::state_machine::{NodeState, StateEvent};
use crate::framework::wire::{FailsafeReason, Payload};
use crate::framework::traits::{Computation, DecisionSink, SelfTest, Voter};
use crate::framework::transport::UdpTransport;
use serde::Deserialize;
use std::time::Instant;
use tracing::{debug, error, info, warn};

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
    /// arrives with `node_state == ErrorManagement`.
    pub(super) peer_in_error_seen: bool,

    /// True for the current cycle if InjectMute is active. Send paths
    /// consult this and suppress transmit while true. Refreshed once per
    /// cycle in `apply_pending_diagnostic`.
    pub(super) mute_active: bool,

    /// Extra sleep (ms) to add to `wait_for_next_cycle_tick` this cycle.
    /// Used by InjectCycleDelay to test cycle-skew handling. Refreshed
    /// once per cycle in `apply_pending_diagnostic`.
    pub(super) pending_cycle_delay_ms: u32,
    pub(super) peer_failsafe_seen: bool,
    pub(super) pending_failsafe_reason: Option<FailsafeReason>,
}

impl<C, V, S, T> Runner<C, V, S, T>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    T: SelfTest,
    C::Input: for<'de> Deserialize<'de>,
{
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
            mute_active: false,
            pending_cycle_delay_ms: 0,
            peer_failsafe_seen: false,
            pending_failsafe_reason: None,
        }
    }

    pub fn set_input(&mut self, input: C::Input) {
        self.input = input;
    }

    /// Cycle-global deadline anchor.
    #[inline]
    pub(super) fn cycle_anchor(&self) -> Instant {
        self.last_cycle_start.unwrap_or_else(Instant::now)
    }

    /// Read + clear the rendezvous flag.
    #[inline]
    pub(super) fn take_peer_in_error(&mut self) -> bool {
        core::mem::replace(&mut self.peer_in_error_seen, false)
    }

    /// Consult mute state. If true, send paths return without transmitting.
    #[inline]
    pub(super) fn is_muted(&self) -> bool {
        self.mute_active
    }

    /// Consume the pending cycle delay (only applied once).
    #[inline]
    pub(super) fn take_pending_cycle_delay_ms(&mut self) -> u32 {
        core::mem::replace(&mut self.pending_cycle_delay_ms, 0)
    }

pub fn run(&mut self) {
    info!(own_id = self.state.own_id(), "runner started");
    loop {
        let current = self.state.node_state();
        debug!(state = ?current, seq = self.state.current_seq(), "phase enter");
 
        let mut event = match current {
            NodeState::Startup                => self.handle_startup(),
            NodeState::InitSync               => self.handle_init_sync(),
            NodeState::PeerSync               => self.handle_peer_sync(),
            NodeState::CycleSync              => self.handle_cycle_sync(),
            NodeState::ReadInputs             => self.handle_read_inputs(),
            NodeState::ShareInputs            => self.handle_share_inputs(),
            NodeState::ShareResult            => self.handle_share_result(),
            NodeState::SendAck                => self.handle_send_ack(),
            NodeState::PublishResult          => self.handle_publish(),
            NodeState::ErrorManagement        => self.handle_error_management(),
            NodeState::Isolation              => self.handle_isolation(),
            NodeState::ResyncLostPeer         => self.handle_resync_lost_node(),
            NodeState::SystemStateCrcExchange => self.handle_system_state_crc(),
            NodeState::SystemStateSync        => self.handle_system_state_sync(),
            NodeState::Failsafe => {
                let reason = self.pending_failsafe_reason
                    .unwrap_or(FailsafeReason::LocalFault);
                self.enter_failsafe(reason);
                return;
            }
        };
 
        self.poll_diagnostic();
 
        if self.peer_failsafe_seen {
            warn!("peer_failsafe_seen set, forcing Fault");
            self.mark_failsafe(FailsafeReason::PeerBroadcast);
            event = StateEvent::Fault;
        }
 
        let next = current.next(event);
        info!(from = ?current, event = ?event, to = ?next, "transition");
        self.state.set_node_state(next);
    }
}

    #[inline]
    pub(super) fn mark_failsafe(&mut self, reason: FailsafeReason) {
        // Erster Grund gewinnt — spaetere Ueberschreibungen sind meist
        // Folgefehler, die weniger informativ sind.
        if self.pending_failsafe_reason.is_none() {
            self.pending_failsafe_reason = Some(reason);
        }
    }

    fn enter_failsafe(&mut self, reason: FailsafeReason) {
        error!(
            own_id = self.state.own_id(),
            reason = ?reason,
            "failsafe entered"
        );
    
        // Best-effort GoFailsafe-Broadcast an alle Peers.
        let node_state = self.state.node_state();
        for _ in 0..3 {
            if let Err(e) = self.transport.send(
                node_state,
                Payload::GoFailsafe { reason: reason.to_wire() },
            ) {
                error!(error = ?e, "GoFailsafe send failed");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    
        // Aktor in sicheren Zustand.
        self.sink.on_failsafe();
    }
}
