//! Routing received frames into the run state.
//!
//! Everything that arrives passes the same gauntlet before it reaches a
//! buffer: unknown senders are rejected once discovery is closed,
//! excluded peers are dropped, and frames older than the staleness
//! threshold are discarded. Only then is the payload filed by variant.
//!
//! This is also where the rendezvous flag is raised. A frame whose
//! header says the sender is already in error management pulls this
//! node forward into the same phase, so two survivors reach the
//! exclusion vote in the same cycle instead of timing out separately.

use crate::framework::state::{AckInfo, PeerHealth};
use crate::framework::state_machine::NodeState;
use crate::framework::traits::{
    ApplicationStateProvider, Computation, DecisionSink, InputSource, SelfTest, Voter,
};
use crate::framework::transport::now_monotonic_ns;
use crate::framework::types::PeerMask;
use crate::framework::wire::{Payload, UdpFrame};
use serde::Deserialize;
use tracing::{debug, warn};

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
    /// Route a validated frame into `RunState`. Drops frames from unknown
    /// or lost peers and stale frames (post time-sync).
    ///
    /// Also detects the rendezvous condition: a frame from a healthy peer
    /// whose header carries `node_state == ErrorManagement`. The flag is
    /// consumed by the four in-cycle phase handlers.
    ///
    /// T10 hook: `should_drop_frame_from_peer` is consulted first. When
    /// active, the frame is dropped silently before any state mutation,
    /// realising the asymmetric-view scenario without touching the
    /// network stack.
    pub(super) fn ingest_frame(&mut self, frame: UdpFrame<C::Input, V::Payload>) {
        let peer_id = frame.node_id();

        // T10: asymmetric-view injection. Silent drop, no state effect.
        if self.should_drop_frame_from_peer(peer_id) {
            debug!(
                peer_id,
                "injection: dropping frame from peer (drop_from_peers_mask)"
            );
            return;
        }

        if let Payload::GoFailsafe { reason } = frame.payload() {
            warn!(
                peer_id,
                reason, "Peer broadcast GoFailsafe, entering failsafe"
            );
            self.peer_failsafe_seen = true;
            return;
        }

        if self.state.discovery_locked() && self.state.peer_index(peer_id).is_none() {
            warn!(peer_id, "frame from unknown peer post-discovery, dropped");
            return;
        }
        if let Some(idx) = self.state.peer_index(peer_id) {
            if self.state.peers()[idx].health == PeerHealth::Lost {
                self.drops_lost_peer = self.drops_lost_peer.saturating_add(1);
                debug!(peer_id, "frame from lost peer dropped");
                return;
            }
        }
        if let Some(age) = self.frame_age_if_stale(&frame) {
            self.drops_stale = self.drops_stale.saturating_add(1);
            debug!(peer_id, age_ns = age, "stale frame dropped");
            return;
        }

        // Rendezvous detection: healthy peer already in ErrorManagement.
        // Health check re-uses the checks above — if we got here the peer
        // exists and is non-Lost.
        if frame.node_state_wire() == NodeState::ErrorManagement.to_wire() {
            debug!(peer_id, "peer in ErrorManagement, rendezvous flag raised");
            self.peer_in_error_seen = true;
        }

        match frame.payload() {
            Payload::Input(value) => {
                if let Err(e) = self.state.record_peer_input(peer_id, value) {
                    warn!(peer_id, error = ?e, "record_peer_input failed");
                }
            }
            Payload::Result(value) => {
                if let Err(e) = self.state.record_peer_result(peer_id, value) {
                    warn!(peer_id, error = ?e, "record_peer_result failed");
                }
            }
            Payload::Ack {
                received_from,
                publisher_candidate,
                ..
            } => {
                let ack = AckInfo {
                    received_from: received_from.as_u8(),
                    publisher_candidate,
                };
                if let Err(e) = self.state.record_peer_ack(peer_id, ack) {
                    warn!(peer_id, error = ?e, "record_peer_ack failed");
                }
            }
            Payload::ExclusionProposal { propose_exclude } => {
                if let Err(e) = self
                    .state
                    .record_peer_exclusion_proposal(peer_id, propose_exclude)
                {
                    warn!(peer_id, error = ?e, "record_peer_exclusion_proposal failed");
                }
            }
            Payload::State { .. } => {
                debug!(peer_id, "state frame outside CycleSync, dropped");
            }
            Payload::TimeSyncReq { .. } | Payload::TimeSyncResp { .. } => {
                debug!(peer_id, "sync frame outside ClockSync, dropped");
            }
            Payload::SystemStateCrc { .. } => {
                debug!(
                    peer_id,
                    "system state crc frame outside CrcExchange, dropped"
                );
            }
            Payload::SystemStateSnapshot { .. } => {
                debug!(peer_id, "snapshot frame outside state sync, dropped");
            }
            Payload::SystemStateSnapshotAck { .. } => {
                debug!(peer_id, "snapshot ack frame outside state sync, dropped");
            }
            Payload::GoFailsafe { .. } => { /* unreachable, handled early */ }
        }
    }

    /// True when every non-Lost peer has a value in the slice this cycle.
    fn all_non_lost_peers_have<F>(&self, mut check: F) -> bool
    where
        F: FnMut(usize) -> bool,
    {
        self.state
            .peers()
            .iter()
            .enumerate()
            .filter(|(_, p)| p.health != PeerHealth::Lost)
            .all(|(idx, _)| check(idx))
    }

    /// True when every non-Lost peer has recorded a result this cycle.
    pub(super) fn all_peer_results_in(&self) -> bool {
        self.all_non_lost_peers_have(|idx| self.state.cycle().peer_results[idx].is_some())
    }

    /// True when every non-Lost peer has recorded an input this cycle.
    pub(super) fn all_peer_inputs_in(&self) -> bool {
        self.all_non_lost_peers_have(|idx| self.state.cycle().peer_inputs[idx].is_some())
    }

    /// True when every non-Lost peer has recorded an ack this cycle.
    pub(super) fn all_peer_acks_in(&self) -> bool {
        self.all_non_lost_peers_have(|idx| self.state.cycle().peer_acks[idx].is_some())
    }

    /// Bitmask of peers whose result we have this cycle. Attested to peers
    /// via the ack frame so cross-observed evidence can be aggregated.
    pub(super) fn received_mask(&self) -> PeerMask {
        let mut mask = PeerMask::EMPTY;
        for (idx, slot) in self.state.cycle().peer_results.iter().enumerate() {
            if slot.is_some() {
                mask.set(idx);
            }
        }
        mask
    }

    /// Age of a frame against local time if a translation is available and
    /// the age exceeds the stale threshold. Returns `None` when either the
    /// clock offset is unknown or the frame is fresh.
    fn frame_age_if_stale(&self, frame: &UdpFrame<C::Input, V::Payload>) -> Option<u64> {
        if !self.state.sync_valid() {
            return None;
        }
        let peer_id = frame.node_id();
        let local_send = self.state.peer_ts_to_local(peer_id, frame.timestamp())?;
        let now = now_monotonic_ns();
        let age = now.saturating_sub(local_send);
        let threshold_ns = self.timing.stale_frame_threshold.as_nanos() as u64;
        if age > threshold_ns {
            Some(age)
        } else {
            None
        }
    }
}
