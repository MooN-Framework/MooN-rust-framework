//! The cyclic driver.
//!
//! [`Runner`] owns the application implementations and the transport
//! and executes one phase per iteration: call the handler for the
//! current [`NodeState`], feed the returned [`StateEvent`] through
//! `NodeState::next`, repeat until a phase lands in failsafe.
//!
//! The submodules split that work by concern. `phases` holds the
//! handlers, `collect` the generic send-and-wait loop they all use,
//! `ingest` the routing of received frames into the run state, and
//! `diag` the diagnostic interface, compiled only with the
//! `diagnostic` feature.
//!
//! Deadlines inside a cycle are offsets from the cycle anchor, not
//! durations from phase entry. Every healthy node therefore sees the
//! same deadline at the same wall-clock instant, which is what keeps
//! two survivors from timing out in different phases when a third goes
//! silent mid-cycle.

mod collect;
#[cfg(feature = "diagnostic")]
mod diag;
mod ingest;
mod phases;

pub use collect::PhaseOutcome;
use crate::framework::config::CycleTiming;
use crate::framework::state::RunState;
use crate::framework::state_machine::{NodeState, StateEvent};
use crate::framework::traits::{
    ApplicationStateProvider, Computation, DecisionSink, InputSource, SelfTest, Voter,
};
use crate::framework::transport::{TransportError, UdpTransport};
use crate::framework::wire::{FailsafeReason, Payload};
use serde::Deserialize;
use std::time::Instant;
use tracing::{debug, error, info, warn};
#[cfg(feature = "diagnostic")]
use crate::framework::config::DiagnosticConfig;
#[cfg(feature = "diagnostic")]
use crate::framework::diagnostic::Diagnostic;

macro_rules! diag_bool_helper {
    ($name:ident, $call:ident) => {
        #[cfg(feature = "diagnostic")]
        #[inline]
        pub(super) fn $name(&mut self) -> bool {
            self.diagnostic.as_mut().map(|d| d.$call()).unwrap_or(false)
        }
        #[cfg(not(feature = "diagnostic"))]
        #[inline]
        pub(super) fn $name(&mut self) -> bool { false }
    };
}

/// Owns the application implementations and drives the phase loop.
///
/// Construct with [`Runner::new`], then call [`Runner::run`], which
/// only returns once the node has reached failsafe.
pub struct Runner<C, V, S, T, IS, A>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    T: SelfTest,
    IS: InputSource<Input = C::Input>,
    A: ApplicationStateProvider,
{
    pub(super) state: RunState<V, C::Input>,
    pub(super) transport: UdpTransport<C::Input, V::Payload>,
    pub(super) computation: C,
    pub(super) input_source: IS,
    pub(super) app_state: A,
    pub(super) sink: S,
    pub(super) self_test: T,
    pub(super) timing: CycleTiming,
    pub(super) last_cycle_start: Option<Instant>,
    pub(super) last_cycle_us: Option<u128>,
    pub(super) next_cycle_deadline: Option<Instant>,
    /// Timing marks between the cycle anchor and the first send of the
    /// cycle. Reported as fields on the ShareInputs entry-offset line so
    /// a lost budget can be attributed to a specific step instead of
    /// guessed at. Deliberately no log lines of their own: on a node
    /// where the synchronous stdout writer is the suspect, extra lines
    /// would distort the very measurement they serve.
    pub(super) mark_after_cycle_log: Option<Instant>,
    pub(super) mark_after_input: Option<Instant>,
    pub(super) mark_after_poll: Option<Instant>,
    /// Frames discarded during the current collect phase, by reason.
    /// Reset on entry to every phase and reported when its deadline is
    /// missed: without them, "the peer sent nothing" and "the peer sent
    /// something and we threw it away" produce an identical log, and
    /// both discard paths below are `debug!`, so at INFO they are
    /// invisible.
    pub(super) drops_stale: u32,
    pub(super) drops_lost_peer: u32,
    pub(super) cycles_since_last_sync: u32,
    #[cfg(feature = "diagnostic")]
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

    /// Ring buffer of the last N `(from, event, to)` transitions,
    /// exposed via the diagnostic StatusResponse so external test
    /// harnesses can assert on exact FSM paths without parsing logs.
    /// Only compiled under `feature = "diagnostic"`.
    #[cfg(feature = "diagnostic")]
    pub(super) recent_transitions:
        heapless::Deque<(NodeState, StateEvent, NodeState), 32>,

    /// One-shot: when true, the next `handle_read_inputs` skips the
    /// `record_own_input` call so the next ShareInputs sees `None`
    /// and takes the `LocalFault` failsafe path. Set via
    /// `InjectInputProviderFail`, cleared on effect.
    #[cfg(feature = "diagnostic")]
    pub(super) inject_input_provider_fail_next: bool,

    /// One-shot: when true, the compute step in `handle_share_inputs`
    /// returns `Fault` with `LocalFault` instead of calling the real
    /// `Computation::compute`. Set via `InjectComputationFail`,
    /// cleared on effect.
    #[cfg(feature = "diagnostic")]
    pub(super) inject_computation_fail_next: bool,
}

impl<C, V, S, T, IS, A> Runner<C, V, S, T, IS, A>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    T: SelfTest,
    IS: InputSource<Input = C::Input>,
    A: ApplicationStateProvider,
    C::Input: for<'de> Deserialize<'de>,
{
    /// Assemble a runner from the run state, the transport and the six
    /// application implementations.
    pub fn new(
        state: RunState<V, C::Input>,
        transport: UdpTransport<C::Input, V::Payload>,
        computation: C,
        input_source: IS,
        app_state: A,
        sink: S,
        self_test: T,
        timing: CycleTiming,
        #[cfg(feature = "diagnostic")]
        diag_cfg: DiagnosticConfig,
    ) -> Self {
        info!(own_id = state.own_id(), "runner constructed");
        #[cfg(feature = "diagnostic")]
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
            input_source,
            app_state,
            sink,
            self_test,
            timing,
            last_cycle_start: None,
            last_cycle_us: None,
            next_cycle_deadline: None,
            mark_after_cycle_log: None,
            mark_after_input: None,
            mark_after_poll: None,
            drops_stale: 0,
            drops_lost_peer: 0,
            cycles_since_last_sync: 0,
            #[cfg(feature = "diagnostic")]
            diagnostic,
            peer_in_error_seen: false,
            mute_active: false,
            pending_cycle_delay_ms: 0,
            peer_failsafe_seen: false,
            pending_failsafe_reason: None,
            #[cfg(feature = "diagnostic")]
            recent_transitions: heapless::Deque::new(),
            #[cfg(feature = "diagnostic")]
            inject_input_provider_fail_next: false,
            #[cfg(feature = "diagnostic")]
            inject_computation_fail_next: false,
        }
    }

    diag_bool_helper!(should_drop_input, should_drop_input);
    diag_bool_helper!(should_drop_result, should_drop_result);
    diag_bool_helper!(should_drop_ack, should_drop_ack);
    diag_bool_helper!(should_drop_cyclesync, should_drop_cyclesync);
    diag_bool_helper!(should_drop_crc, should_drop_crc);
    diag_bool_helper!(should_drop_vote, should_drop_vote);
    diag_bool_helper!(should_send_divergent_publisher, should_send_divergent_publisher);


    #[cfg(feature = "diagnostic")]
    #[inline]
    pub(super) fn should_fake_crc(&mut self) -> Option<u32> {
        self.diagnostic.as_mut().and_then(|d| d.should_fake_crc())
    }

    #[cfg(not(feature = "diagnostic"))]
    #[inline]
    pub(super) fn should_fake_crc(&mut self) -> Option<u32> { None }

    #[cfg(feature = "diagnostic")]
    #[inline]
    pub(super) fn should_corrupt_result(&mut self) -> bool {
        self.diagnostic.as_mut().map(|d| d.should_corrupt_result()).unwrap_or(false)
    }

    #[cfg(feature = "diagnostic")]
    #[inline]
    pub(super) fn maybe_corrupt_result(&mut self, own: V::Payload) -> V::Payload
    where
        V::Payload: crate::framework::traits::Corruptible,
    {
        use crate::framework::traits::Corruptible;
        let mut own = own;
        if self.should_corrupt_result() {
            tracing::warn!("injection: corrupting own result before send");
            Corruptible::corrupt(&mut own);
        }
        own
    }

    #[cfg(not(feature = "diagnostic"))]
    #[inline]
    pub(super) fn maybe_corrupt_result(&mut self, own: V::Payload) -> V::Payload {
        own
    }

    // ---------------------------------------------------------------------
    // T10 — asymmetric view. Called from ingest_frame before any recording.
    // ---------------------------------------------------------------------
    #[cfg(feature = "diagnostic")]
    #[inline]
    pub(super) fn should_drop_frame_from_peer(&self, peer_node_id: u8) -> bool {
        self.diagnostic
            .as_ref()
            .map(|d| d.should_drop_from(peer_node_id))
            .unwrap_or(false)
    }

    #[cfg(not(feature = "diagnostic"))]
    #[inline]
    pub(super) fn should_drop_frame_from_peer(&self, _peer_node_id: u8) -> bool {
        false
    }

    // ---------------------------------------------------------------------
    // T16 — phase-header spoof. `send_frame` is the single choke point
    // through which every phase's outgoing frame flows. Without the
    // feature it's a plain forward to `transport.send`.
    // ---------------------------------------------------------------------
    #[cfg(feature = "diagnostic")]
    #[inline]
    pub(super) fn send_frame(
        &mut self,
        real_state: NodeState,
        payload: Payload<C::Input, V::Payload>,
    ) -> Result<u32, TransportError> {
        let fake = self.diagnostic.as_mut().and_then(|d| d.should_fake_phase_header());
        if let Some(wire) = fake {
            match NodeState::from_wire(wire) {
                Ok(faked_state) => {
                    warn!(
                        real = ?real_state,
                        faked = ?faked_state,
                        "injection: sending with faked phase header"
                    );
                    return self.transport.send(faked_state, payload);
                }
                Err(_) => {
                    warn!(
                        wire,
                        "fake_phase_header value does not decode to a NodeState, falling back to real state"
                    );
                }
            }
        }
        self.transport.send(real_state, payload)
    }

    #[cfg(not(feature = "diagnostic"))]
    #[inline]
    pub(super) fn send_frame(
        &mut self,
        real_state: NodeState,
        payload: Payload<C::Input, V::Payload>,
    ) -> Result<u32, TransportError> {
        self.transport.send(real_state, payload)
    }

    /// Push an input into the input source from outside the cycle,
    /// e.g. from the diagnostic interface.
    pub fn set_input(&mut self, input: C::Input) {
        self.input_source.set(input);
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

    /// Read the rendezvous flag without consuming it.
    ///
    /// The four in-cycle phases use this in their completion predicate
    /// so the collect loop stops as soon as a peer signals
    /// ErrorManagement, rather than waiting out a deadline for answers
    /// from nodes that have demonstrably moved on. The flag itself is
    /// still consumed once, by `take_peer_in_error` after the loop,
    /// which is what turns the outcome into `PeerInError`.
    #[inline]
    pub(super) fn peer_in_error(&self) -> bool {
        self.peer_in_error_seen
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

        /// Run until the node reaches failsafe.
        ///
        /// Each iteration runs the handler for the current phase, feeds
        /// its event through the transition function and logs the
        /// transition. The sink's `on_failsafe` hook fires once, right
        /// before this returns.
        pub fn run(&mut self)
        where
            V::Payload: crate::framework::traits::Corruptible,
        {
        info!(own_id = self.state.own_id(), "runner started");
        loop {
            let current = self.state.node_state();
            debug!(state = ?current, seq = self.state.current_seq(), "phase enter");

            let mut event = match current {
                NodeState::Startup => self.handle_startup(),
                NodeState::InitSync => self.handle_init_sync(),
                NodeState::ClockSync => self.handle_clock_sync(),
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
                    let reason = self
                        .pending_failsafe_reason
                        .unwrap_or(FailsafeReason::LocalFault);
                    self.enter_failsafe(reason);
                    return;
                }
            };

            #[cfg(feature = "diagnostic")]
            self.poll_diagnostic();
            // Outside the feature gate: without the diagnostic this just
            // marks the point right before the transition log, which is
            // what the ShareInputs breakdown needs either way.
            self.mark_after_poll = Some(Instant::now());

            if self.peer_failsafe_seen {
                warn!("peer_failsafe_seen set, forcing Fault");
                self.mark_failsafe(FailsafeReason::PeerBroadcast);
                event = StateEvent::Fault;
            }

            let next = current.next(event);
            info!(from = ?current, event = ?event, to = ?next, "transition");
            #[cfg(feature = "diagnostic")]
            {
                if self.recent_transitions.is_full() {
                    let _ = self.recent_transitions.pop_front();
                }
                let _ = self.recent_transitions.push_back((current, event, next));
            }
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

        // Best-effort GoFailsafe-Broadcast an alle Peers. Bypasst
        // absichtlich send_frame — im Failsafe-Pfad wollen wir keine
        // Injection-Effekte auf die GoFailsafe-Semantik.
        let node_state = self.state.node_state();
        for _ in 0..3 {
            if let Err(e) = self.transport.send(
                node_state,
                Payload::GoFailsafe {
                    reason: reason.to_wire(),
                },
            ) {
                error!(error = ?e, "GoFailsafe send failed");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        // Aktor in sicheren Zustand.
        self.sink.on_failsafe();
    }
}
