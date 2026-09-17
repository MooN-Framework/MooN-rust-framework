//! The generic phase loop.
//!
//! Every waiting phase is the same shape: broadcast periodically, watch
//! for a completion condition, give up at a deadline. `collect_phase`
//! implements that once and takes the three differing parts as
//! closures.
//!
//! The loop also answers incoming time-sync requests transparently, so
//! a peer that is still synchronising does not stall while everyone
//! else is busy in a cycle phase.
//!
//! A `GoFailsafe` from any peer ends the phase at once, before the
//! phase-specific ingest closure sees the frame. Several closures only
//! look at their own payload type, so leaving the check to them would
//! silently drop the broadcast in exactly those phases.

use crate::framework::clock_sync::{extract_sync_fields, SyncFields};
use crate::framework::state::PeerHealth;
use crate::framework::traits::{
    ApplicationStateProvider, Computation, DecisionSink, InputSource, SelfTest, Voter,
};
use crate::framework::transport::RecvOutcome;
use crate::framework::wire::{FailsafeReason, Payload, UdpFrame};
use serde::Deserialize;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// Outcome of a phase-collection loop.
pub enum PhaseOutcome {
    /// Termination condition met before the deadline.
    Complete,
    /// Deadline reached without meeting the termination condition.
    Timeout,
    /// Send path failed hard, or a peer broadcast `GoFailsafe`. The
    /// caller should transition to failsafe.
    Fault,
}

impl<C, V, S, T, IS, A> super::Runner<C, V, S, T, IS, A>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    T: SelfTest,
    IS: InputSource<Input = C::Input>,
    A: ApplicationStateProvider,
    C::Input: for<'de> Deserialize<'de>,
{
    /// Generic phase driver: periodically send until the completion
    /// predicate holds or the deadline expires. Sync-request frames from
    /// peers are answered transparently.
    pub(super) fn collect_phase<Snd, Done, Ing>(
        &mut self,
        phase: &'static str,
        deadline: Instant,
        resend_interval: Duration,
        mut send: Snd,
        mut done: Done,
        mut ingest: Ing,
    ) -> PhaseOutcome
    where
        Snd: FnMut(&mut Self) -> Result<(), ()>,
        Done: FnMut(&Self) -> bool,
        Ing: FnMut(&mut Self, UdpFrame<C::Input, V::Payload>),
    {
        self.drops_stale = 0;
        self.drops_lost_peer = 0;
        let mut next_send = Instant::now();
        loop {
            if Instant::now() >= next_send {
                if send(self).is_err() {
                    error!(phase, "send failed, aborting phase");
                    return PhaseOutcome::Fault;
                }
                next_send = Instant::now() + resend_interval;
            }

            if done(self) {
                let _ = send(self);
                debug!(phase, "phase complete");
                return PhaseOutcome::Complete;
            }

            let now = Instant::now();
            if now > deadline {
                // `overshoot_us` is how late the deadline was noticed.
                // The check runs once per loop iteration, so a large
                // value means this loop did not get scheduled, not that
                // the phase legitimately used up its budget. Read it
                // together with the entry offset logged by the phase
                // handlers: entry offset small plus overshoot large
                // points at CPU starvation rather than a silent peer.
                warn!(
                    phase,
                    overshoot_us = now.saturating_duration_since(deadline).as_micros(),
                    drops_stale = self.drops_stale,
                    drops_lost_peer = self.drops_lost_peer,
                    "phase deadline exceeded"
                );
                return PhaseOutcome::Timeout;
            }

            match self.transport.try_recv() {
                RecvOutcome::Valid(frame) => {
                    if self.take_go_failsafe(phase, &frame) {
                        return PhaseOutcome::Fault;
                    }
                    ingest(self, frame)
                }
                RecvOutcome::SeqGap {
                    peer_id,
                    gap,
                    frame,
                } => {
                    warn!(peer_id, gap, "seq gap, advancing cursor");
                    self.transport.accept(&frame);
                    if self.take_go_failsafe(phase, &frame) {
                        return PhaseOutcome::Fault;
                    }
                    ingest(self, frame);
                }
                RecvOutcome::NewSession {
                    peer_id,
                    previous_session,
                    new_session,
                    frame,
                } => {
                    // A restarted peer that fails straight away still
                    // takes the fabric with it: fail-safe beats
                    // availability, same as for a Lost sender.
                    if self.take_go_failsafe(phase, &frame) {
                        return PhaseOutcome::Fault;
                    }
                    let was_lost = self
                        .state
                        .peer_index(peer_id)
                        .map(|idx| self.state.peers()[idx].health == PeerHealth::Lost)
                        .unwrap_or(false);
                    if was_lost {
                        warn!(
                            peer_id,
                            previous_session,
                            new_session,
                            "Rejoin request from Lost peer, accepting and continuing phase"
                        );
                        self.transport.accept(&frame);
                        ingest(self, frame);
                    } else {
                        warn!(
                            peer_id,
                            previous_session,
                            new_session,
                            "Peer either rebooted mid phase or wasn't in peer list, ignoring"
                        );
                    }
                }
                RecvOutcome::TimeSync {
                    frame,
                    local_recv_ns,
                    ..
                } => {
                    if let Some(SyncFields::Request { t1, t2_local, .. }) =
                        extract_sync_fields(&frame, local_recv_ns)
                    {
                        let ns = self.state.node_state();
                        if let Err(e) = self.transport.send_time_sync_resp(ns, t1, t2_local) {
                            warn!(error = ?e, "send_time_sync_resp failed in phase");
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Check a received frame for `GoFailsafe`. On a hit the reason is
    /// latched as `PeerBroadcast` and the flag for the run loop is set.
    /// Latching here, before the caller maps `Fault` to its own reason,
    /// keeps the peer broadcast as the recorded cause.
    ///
    /// Honours the asymmetric-view injection, so a peer that is being
    /// ignored on purpose is ignored for this frame too.
    fn take_go_failsafe(&mut self, phase: &'static str, frame: &UdpFrame<C::Input, V::Payload>) -> bool {
        let Payload::GoFailsafe { reason } = frame.payload() else {
            return false;
        };
        let peer_id = frame.node_id();
        if self.should_drop_frame_from_peer(peer_id) {
            debug!(peer_id, phase, "injection: dropping GoFailsafe from peer");
            return false;
        }
        warn!(peer_id, reason, phase, "peer broadcast GoFailsafe, aborting phase");
        self.peer_failsafe_seen = true;
        self.mark_failsafe(FailsafeReason::PeerBroadcast);
        true
    }
}
