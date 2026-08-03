use crate::framework::peer_sync::{SyncFields, extract_sync_fields};
use crate::framework::state::PeerHealth;
use crate::framework::traits::{Computation, DecisionSink, Voter};
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

impl<C, V, S> super::Runner<C, V, S>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
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
        Ing: FnMut(&mut Self, UdpFrame<V::Payload>),
    {
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

            if Instant::now() > deadline {
                warn!(phase, "phase deadline exceeded");
                return PhaseOutcome::Timeout;
            }

            match self.transport.try_recv() {
                RecvOutcome::Valid(frame) => ingest(self, frame),
                RecvOutcome::SeqGap { peer_id, gap, frame } => {
                    warn!(peer_id, gap, "seq gap, advancing cursor");
                    self.transport.accept(&frame);
                    ingest(self, frame);
                }
                RecvOutcome::NewSession { peer_id, previous_session, new_session, frame } => {
                    let was_lost = self
                        .state
                        .peer_index(peer_id)
                        .map(|idx| self.state.peers()[idx].health == PeerHealth::Lost)
                        .unwrap_or(false);
                    if was_lost {
                        warn!(peer_id, previous_session, new_session, "rejoin from lost peer ignored");
                    } else {
                        warn!(peer_id, previous_session, new_session, "peer rebooted mid-phase");
                        self.transport.accept(&frame);
                        ingest(self, frame);
                    }
                }
                RecvOutcome::TimeSync { frame, local_recv_ns, .. } => {
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
