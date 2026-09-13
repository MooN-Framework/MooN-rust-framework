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

use crate::framework::clock_sync::{extract_sync_fields, SyncFields};
use crate::framework::state::PeerHealth;
use crate::framework::traits::{
    ApplicationStateProvider, Computation, DecisionSink, InputSource, SelfTest, Voter,
};
use crate::framework::transport::RecvOutcome;
use crate::framework::wire::UdpFrame;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tracing::{debug, error, warn};

/// Outcome of a phase-collection loop.
pub enum PhaseOutcome {
    /// Termination condition met before the deadline.
    Complete,
    /// Deadline reached without meeting the termination condition.
    Timeout,
    /// Send path failed hard; caller should transition to failsafe.
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
                RecvOutcome::Valid(frame) => ingest(self, frame),
                RecvOutcome::SeqGap {
                    peer_id,
                    gap,
                    frame,
                } => {
                    warn!(peer_id, gap, "seq gap, advancing cursor");
                    self.transport.accept(&frame);
                    ingest(self, frame);
                }
                RecvOutcome::NewSession {
                    peer_id,
                    previous_session,
                    new_session,
                    frame,
                } => {
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
}
