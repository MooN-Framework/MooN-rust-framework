use crate::framework::peer_sync::{PeerSync, SAMPLES_PER_PEER, SyncFields, extract_sync_fields};
use crate::framework::state::PeerHealth;
use crate::framework::state_machine::{StateEvent, SystemState, NodeState};
use crate::framework::traits::{Computation, DecisionSink, Voter, VotingOutcome};
use crate::framework::transport::RecvOutcome;
use crate::framework::types::PeerMask;
use crate::framework::wire::Payload;
use serde::Deserialize;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use std::cell::Cell;

use super::PhaseOutcome;

impl<C, V, S> super::Runner<C, V, S>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    C::Input: for<'de> Deserialize<'de>,
{
    /// Startup: self-test passes unconditionally today.
    pub(super) fn handle_startup(&mut self) -> StateEvent {
        self.state.set_system_state(SystemState::Startup);
        StateEvent::SelfTestOk
    }

    /// Isolation: park until external intervention.
    pub(super) fn handle_isolation(&mut self) -> StateEvent {
        loop {
            warn!("isolation entered, waiting for intervention");
            sleep(Duration::from_secs(1));
            self.poll_diagnostic();
        }
    }

pub(super) fn handle_resync_lost_node(&mut self) -> StateEvent {
    let is_returning = self.state.was_lost();
    info!(is_returning, "resync entered");

    let node_state = self.state.node_state();

    // Healthy peer: expected size = current active + peers we're readmitting.
    // Lost peer: unknown until we hear from someone.
    let expected_size: Cell<Option<u8>> = Cell::new(if is_returning {
        None
    } else {
        let pending = self.state.pending_rejoin().as_u8().count_ones() as u8;
        Some(self.state.active_count_including_self() + pending)
    });

    let deadline = Instant::now() + if is_returning {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(5)
    };

let outcome = self.collect_phase(
    "resync",
    deadline,
    Duration::from_millis(10),
    |this| {
        // send closure — unchanged
        let mask = if is_returning {
            PeerMask::EMPTY
        } else {
            this.state.own_seen_mask()
        };
        let ac = if is_returning {
            0
        } else {
            let pending = this.state.pending_rejoin().as_u8().count_ones() as u8;
            this.state.active_count_including_self() + pending
        };
        if let Err(e) = this.transport.send_state(node_state, mask, ac) {
            error!(error = ?e, "send_state failed in resync");
            return Err(());
        }
        Ok(())
    },
    |this| match expected_size.get() {
        Some(n) => {
            let present = if is_returning {
                this.state.peers().len() as u8 + 1
            } else {
                this.state.active_count_including_self()
            };
            present >= n
        }
        None => false,
    },
    |this, frame| {
        if frame.node_state_wire() != NodeState::ResyncLostPeer.to_wire() {
            warn!(peer_id = frame.node_id(), "non-resync frame in resync phase, ignoring");
            return;
        }
        let peer_id = frame.node_id();

        if is_returning {
            let _ = this.state.on_peer_discovered(peer_id);
        } else if this.state.readmit_peer(peer_id) {
            info!(peer_id, "peer readmitted");
        }

        if let Payload::State { active_count, .. } = frame.payload() {
            if active_count > 0 && expected_size.get().is_none() {
                expected_size.set(Some(active_count));
                info!(size = active_count, "learned expected system size");
            }
        }
    },
);

    match outcome {
        PhaseOutcome::Complete => {
            if is_returning {
                if let Err(e) = self.state.finalize_discovery() {
                    error!(error = ?e, "finalize_discovery failed");
                    return StateEvent::SelfTestErr;
                }
                self.state.set_was_lost(false);
            }
            self.state.clear_pending_rejoin();
            self.state.start_new_cycle(self.next_cycle_tick());
            self.state.set_system_state(SystemState::Operational);
            StateEvent::ResyncLostPeerOk
        }
        PhaseOutcome::Timeout => {
            self.state.clear_pending_rejoin();
            warn!(is_returning, expected = ?expected_size.get(), "resync deadline exceeded");
            StateEvent::ResyncLostPeerTimeout
        }
        PhaseOutcome::Fault => {
            self.state.clear_pending_rejoin();
            StateEvent::Fault
        }
    }
}

    /// InitSync: broadcast state until the nominal peer set is discovered
    /// or the discovery window elapses.
    pub(super) fn handle_init_sync(&mut self) -> StateEvent {
        let nominal = self.state.participants().nominal_participants;
        info!(expected = nominal, "init sync entered");

        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.init_sync_timeout;
        let outcome = self.collect_phase(
            "init_sync",
            deadline,
            Duration::from_millis(10),
            |this| {
                if let Err(e) = this.transport.send_state(node_state, PeerMask::EMPTY, 0) {
                    error!(error = ?e, "send_state failed in init sync");
                    return Err(());
                }
                Ok(())
            },
            |this| this.discovery_complete(),
            |this, frame| {
                if frame.node_state_wire() == NodeState::InitSync.to_wire() {
                    debug!(peer_id = frame.node_id(), "init sync frame received");
                    let _ = this.state.on_peer_discovered(frame.node_id());
                } else
                {
                    warn!("Non init sync frame received, i was lost, going into resync state.");
                    this.state.set_was_lost(true);
                }
            },
        );

        match outcome {
            PhaseOutcome::Complete => {
                if self.state.was_lost() {
                    warn!("I was lost, going into resync state.");
                    return StateEvent::GoResyncLostPeer;
                }

                if let Err(e) = self.state.finalize_discovery() {
                    error!(error = ?e, "finalize_discovery failed");
                    return StateEvent::SelfTestErr;
                }

                self.state.start_new_cycle(self.next_cycle_tick());
                self.state.set_system_state(SystemState::Operational);
                StateEvent::InitialSyncOk
            }
            PhaseOutcome::Timeout => {
                warn!(
                    found = self.state.peers().len(),
                    expected = nominal,
                    "discovery window elapsed"
                );
                StateEvent::InitialSyncTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    /// PeerSync: run Cristian rounds with each non-Lost peer until every
    /// peer has enough samples or the window elapses.
    pub(super) fn handle_peer_sync(&mut self) -> StateEvent {
        if self.state.sync_valid() {
            self.state.invalidate_sync();
        }

        let peer_ids: Vec<u8> = self
            .state
            .peers()
            .iter()
            .filter(|p| p.health != PeerHealth::Lost)
            .map(|p| p.id)
            .collect();
        if peer_ids.is_empty() {
            error!("peer sync entered without active peers");
            return StateEvent::Fault;
        }

        let mut peer_sync = PeerSync::new(&peer_ids);
        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.peer_sync_timeout;
        let mut next_request = Instant::now();

        loop {
            if peer_sync.is_complete() {
                let clocks = peer_sync.finalize();
                let epsilon = peer_sync.max_error_bound().unwrap_or(0);
                info!(epsilon_ns = epsilon, count = clocks.len(), "peer sync complete");
                self.state.set_peer_clocks(&clocks);
                self.state.set_sync_epsilon(epsilon);
                self.state.mark_sync_valid();
                self.cycles_since_last_sync = 0;
                self.next_cycle_deadline = None;
                self.last_cycle_start = None;
                self.state.start_new_cycle(self.next_cycle_tick());
                return StateEvent::PeerSyncOk;
            }

            if Instant::now() > deadline {
                warn!(
                    found = peer_sync.finalize().len(),
                    total = peer_ids.len(),
                    target = SAMPLES_PER_PEER,
                    "peer sync deadline exceeded"
                );
                return StateEvent::PeerSyncTimeout;
            }

            if Instant::now() >= next_request {
                let any_needs_sync = peer_ids.iter().any(|&id| !peer_sync.has_pending(id));
                if any_needs_sync {
                    match self.transport.send_time_sync_req(node_state) {
                        Ok((_seq, t1)) => {
                            for &peer_id in &peer_ids {
                                if !peer_sync.has_pending(peer_id) {
                                    peer_sync.record_outgoing_request(peer_id, t1);
                                }
                            }
                            next_request = Instant::now() + self.timing.peer_sync_request_interval;
                        }
                        Err(e) => {
                            error!(error = ?e, "send_time_sync_req failed");
                            return StateEvent::Fault;
                        }
                    }
                }
            }

            if let RecvOutcome::TimeSync { frame, local_recv_ns, .. } = self.transport.try_recv() {
                match extract_sync_fields(&frame, local_recv_ns) {
                    Some(SyncFields::Request { t1, t2_local, .. }) => {
                        if let Err(e) = self.transport.send_time_sync_resp(node_state, t1, t2_local)
                        {
                            warn!(error = ?e, "send_time_sync_resp failed");
                        }
                    }
                    Some(SyncFields::Response { peer_id, t1, t2, t3, t4_local }) => {
                        peer_sync.on_response(peer_id, t1, t2, t3, t4_local);
                    }
                    None => {}
                }
            }
        }
    }

/// CycleSync: peers exchange State beacons with attested seen-masks
    /// until each side has observed all non-Lost peers, or the phase times
    /// out.
    pub(super) fn handle_cycle_sync(&mut self) -> StateEvent {
        self.state.reset_cycle_sync_evidence();

        let node_state = self.state.node_state();
        let expected_mask = self.state.expected_sync_mask();
        let deadline = Instant::now() + self.timing.cycle_sync_timeout;

        let outcome = self.collect_phase(
            "cycle_sync",
            deadline,
            Duration::from_millis(1),
            |this| {
                let mask = this.state.own_seen_mask();
                if let Err(e) = this.transport.send_state(node_state, mask, this.state.active_count_including_self()) {
                    error!(error = ?e, "send_state failed in cycle sync");
                    return Err(());
                }
                Ok(())
            },
            |this| this.state.own_seen_mask().as_u8() == expected_mask,
            |this, frame| {
                if frame.node_state_wire() == NodeState::ResyncLostPeer.to_wire() {
                    this.state.set_rejoin_seen(frame.node_id());
                }

                let peer_id = frame.node_id();
                if let Payload::State { seen_mask, .. } = frame.payload() {
                    if let Some(idx) = this.state.peer_index(peer_id) {
                        this.state.set_own_seen_bit(idx);
                        let _ = this.state.record_peer_seen_mask(peer_id, seen_mask);
                    }
                }
            },
        );

        self.state.attribute_cycle_sync_missing();

        match outcome {
            PhaseOutcome::Complete => {
                self.state.start_new_cycle(self.next_cycle_tick());
                StateEvent::CycleSyncOk
            }
            PhaseOutcome::Timeout => {
                warn!(
                    got = self.state.own_seen_mask().as_u8(),
                    expected = expected_mask,
                    "cycle sync deadline exceeded"
                );
                StateEvent::CycleSyncTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    /// ReadInputs: sleep to the next cycle tick, then run the computation
    /// on the current input.
    pub(super) fn handle_read_inputs(&mut self) -> StateEvent {
        self.apply_pending_diagnostic();
        self.wait_for_next_cycle_tick();

        let now = Instant::now();
        if let Some(prev) = self.last_cycle_start {
            let elapsed = now.duration_since(prev);
            self.last_cycle_us = Some(elapsed.as_micros());
            info!(cycle_us = elapsed.as_micros(), "cycle duration");
        }
        self.last_cycle_start = Some(now);

        match self.computation.compute(self.input) {
            Ok(payload) => {
                self.state.record_own_result(payload);
                StateEvent::InputsRead
            }
            Err(e) => {
                error!(error = ?e, "computation failed");
                StateEvent::Fault
            }
        }
    }

/// ShareResult: broadcast our result until every non-Lost peer has
    /// delivered theirs.
    pub(super) fn handle_share_result(&mut self) -> StateEvent {
        let suppress_send = self
            .diagnostic
            .as_mut()
            .map(|d| d.should_drop_result())
            .unwrap_or(false);
        if suppress_send {
            warn!("injection: suppressing send_result");
        }

        let own = match self.state.cycle().own_result {
            Some(r) => r,
            None => {
                error!("share_result entered without own result");
                return StateEvent::Fault;
            }
        };

        let deadline = Instant::now() + self.timing.share_timeout;
        let node_state = self.state.node_state();

        let outcome = self.collect_phase(
            "share_result",
            deadline,
            Duration::from_millis(1),
            |this| {
                if suppress_send {
                    return Ok(());
                }
                if let Err(e) = this.transport.send_result(node_state, own) {
                    error!(error = ?e, "send_result failed");
                }
                Ok(())
            },
            |this| this.all_peer_results_in(),
            |this, frame| {
                if frame.node_state_wire() == NodeState::ResyncLostPeer.to_wire() {
                    this.state.set_rejoin_seen(frame.node_id());
                } else {
                    this.ingest_frame(frame);
                }
            },
        );

        match outcome {
            PhaseOutcome::Complete => StateEvent::ResultShared,
            PhaseOutcome::Timeout => {
                self.state.attribute_result_missing();
                StateEvent::ShareResultTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    /// SendAck: broadcast our attested received-mask, publisher pick, and
    /// rejoin vote until every non-Lost peer has done the same.
    pub(super) fn handle_send_ack(&mut self) -> StateEvent {
        let suppress_send = self
            .diagnostic
            .as_mut()
            .map(|d| d.should_drop_ack())
            .unwrap_or(false);
        if suppress_send {
            warn!("injection: suppressing send_ack");
        }

        let mask = self.received_mask();
        let candidate = self.pick_publisher_candidate();
        let rejoin_vote = self.state.own_rejoin_vote();
        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.ack_timeout;

        let outcome = self.collect_phase(
            "send_ack",
            deadline,
            Duration::from_millis(1),
            |this| {
                if suppress_send {
                    return Ok(());
                }
                if let Err(e) = this.transport.send_ack(node_state, mask, candidate, rejoin_vote) {
                    error!(error = ?e, "send_ack failed");
                }
                Ok(())
            },
            |this| this.all_peer_acks_in(),
            |this, frame| {
                if let Payload::Ack { rejoin_vote, .. } = frame.payload() {
                    let _ = this.state.record_peer_rejoin_vote(frame.node_id(), rejoin_vote);
                }
                this.ingest_frame(frame);
            },
        );

        match outcome {
            PhaseOutcome::Complete => {
                self.state.attribute_result_missing();
                StateEvent::AckReceived
            }
            PhaseOutcome::Timeout => {
                self.state.attribute_result_missing();
                self.fault_peers_missing_ack_unilateral();
                StateEvent::AckTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    /// PublishResult: run the vote, publish the decision if we're the
    /// designated publisher, route dissenters through ErrorManagement.
    pub(super) fn handle_publish(&mut self) -> StateEvent {
        let outcome = self.state.run_vote();
        match outcome {
            VotingOutcome::Consensus(decision) => {
                let own_result = self.state.cycle().own_result;
                let dissenter_analysis = own_result.map(|own| {
                    self.state
                        .voter()
                        .find_dissenters(&own, &self.state.cycle().peer_results, &decision)
                });

                if let Some((own_dissented, peer_dissenter_indices)) = dissenter_analysis {
                    if own_dissented {
                        error!(own_id = self.state.own_id(), "own value dissented, failsafe");
                        return StateEvent::Fault;
                    }
                    if !peer_dissenter_indices.is_empty() {
                        let dissenter_ids: Vec<u8> = peer_dissenter_indices
                            .iter()
                            .filter_map(|idx| {
                                self.state.peers().get(*idx as usize).map(|p| p.id)
                            })
                            .collect();

                        let publisher = self.pick_publisher_candidate();
                        if publisher == self.state.own_id() {
                            warn!(publisher, "consensus with dissenters, publishing then reconfig");
                            self.sink.publish(&decision);
                        }
                        for peer_id in dissenter_ids {
                            warn!(peer_id, "peer value diverged from consensus");
                            let _ = self.state.propose_exclude(peer_id);
                        }
                        return StateEvent::DissenterDetected;
                    }
                }

                let publisher = self.pick_publisher_candidate();
                let own_id = self.state.own_id();
                if publisher == own_id {
                    info!(publisher, "consensus reached, publishing");
                    self.sink.publish(&decision);
                } else {
                    debug!(publisher, own_id, "consensus reached, peer publishes");
                }

                let confirmed_rejoin = self.state.aggregate_rejoin_votes();
                if confirmed_rejoin.as_u8() != 0 {
                    warn!(
                        rejoin_mask = confirmed_rejoin.as_u8(),
                        "rejoin confirmed by all healthy peers, going into resync"
                    );
                    self.state.set_pending_rejoin(confirmed_rejoin);
                    return StateEvent::GoResyncLostPeer;
                }

                self.cycles_since_last_sync = self.cycles_since_last_sync.saturating_add(1);
                if self.cycles_since_last_sync >= self.timing.resync_interval_cycles {
                    info!(
                        cycles = self.cycles_since_last_sync,
                        interval = self.timing.resync_interval_cycles,
                        "resync interval reached"
                    );
                    StateEvent::ResyncDue
                } else {
                    StateEvent::ResultPublished
                }
            }
            VotingOutcome::Disagreement => {
                warn!("vote disagreement");
                StateEvent::StateDiverged
            }
            VotingOutcome::InsufficientQuorum => {
                error!(peer_results = self.state.cycle().peer_results.len(), "insufficient quorum");
                StateEvent::Fault
            }
        }
    }

    /// ErrorManagement: exchange exclusion proposals, apply confirmed
    /// transitions, then decide on the next system state.
    ///
    /// Failsafe conditions:
    /// - Vote timed out with a healthy peer silent (Rule 2b).
    /// - Quorum lost after applying transitions.
    /// - Fabric now in degraded mode AND no peer evidence this cycle:
    ///   'we are blind' and 'peer is dead' are indistinguishable without a
    ///   third witness, both mean stop.
    pub(super) fn handle_error_management(&mut self) -> StateEvent {
        self.state.reset_exclusion_proposals();

        let own_proposal = self.state.proposed_exclusions();
        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.error_management_vote_timeout;

        debug!(proposed = own_proposal.as_u8(), "exclusion vote entered");

        let outcome = self.collect_phase(
            "exclusion_vote",
            deadline,
            Duration::from_millis(1),
            |this| {
                if let Err(e) = this.transport.send_exclusion_proposal(node_state, own_proposal) {
                    error!(error = ?e, "send_exclusion_proposal failed");
                    return Err(());
                }
                Ok(())
            },
            |this| this.state.healthy_peers_missing_vote().is_empty(),
            |this, frame| this.ingest_frame(frame),
        );

        match outcome {
            PhaseOutcome::Complete => {
                let no_buffer_before_vote = self.state.tolerable_failures_remaining() == 0;

                let confirmed = self.state.aggregate_exclusion_votes();
                let transitions = self.state.apply_confirmed_exclusions(confirmed);
                if transitions > 0 {
                    info!(
                        transitions,
                        confirmed = confirmed.as_u8(),
                        active_peers = self.state.active_peer_count(),
                        "health transitions applied"
                    );
                }

                self.state.start_new_cycle(self.next_cycle_tick());

                if !self.state.quorum_available() {
                    error!(
                        active_peers = self.state.active_peer_count(),
                        minimum = self.state.participants().min_participants,
                        "quorum lost"
                    );
                    return StateEvent::TooFewNodes;
                }

                if no_buffer_before_vote {
                    error!(
                        active_peers = self.state.active_peer_count(),
                        nominal = self.state.participants().nominal_participants,
                        minimum = self.state.participants().min_participants,
                        "error in no-tolerance mode, failsafe"
                    );
                    return StateEvent::TooFewNodes;
                }

                StateEvent::StateOk
            }
            PhaseOutcome::Timeout => {
                let missing = self.state.healthy_peers_missing_vote();
                error!(missing = ?missing.as_slice(), "exclusion vote timed out, failsafe");
                StateEvent::StateTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    /// Attribute missing acks unilaterally. Cross-observation is not
    /// available here — ack frames don't attest which acks their sender
    /// received. The vote layer absorbs the resulting unilateral counter.
    ///
    /// Guard: attribute only when at least one ack arrived, otherwise our
    /// own inbound is suspect.
    fn fault_peers_missing_ack_unilateral(&mut self) {
        let any_ack = self.state.cycle().peer_acks.iter().any(|a| a.is_some());
        if !any_ack {
            warn!("no acks received, skipping unilateral MissedAck attribution");
            return;
        }
        let missing_ids: Vec<u8> = self
            .state
            .peers()
            .iter()
            .enumerate()
            .filter_map(|(idx, p)| {
                if p.health == PeerHealth::Lost {
                    return None;
                }
                if self.state.cycle().peer_acks[idx].is_none() {
                    Some(p.id)
                } else {
                    None
                }
            })
            .collect();
        for peer_id in missing_ids {
            let _ = self.state.propose_exclude(peer_id);
        }
    }

    /// True when discovery has found the nominal total node count or we going to resync because of received frames.
    fn discovery_complete(&self) -> bool {
        if self.state.was_lost() {
            return true;
        }
        let found = self.state.peers().len() as u8 + 1;
        found == self.state.participants().nominal_participants
    }

    /// Sleep until the next cycle tick. Logs an overrun and skips sleeping
    /// on lateness.
    fn wait_for_next_cycle_tick(&mut self) {
        let now = Instant::now();
        match self.next_cycle_deadline {
            None => {
                self.next_cycle_deadline = Some(now + self.timing.cycle_duration);
            }
            Some(deadline) => {
                if now < deadline {
                    sleep(deadline - now);
                } else {
                    warn!(overrun_us = (now - deadline).as_micros(), "cycle overrun");
                }
                self.next_cycle_deadline = Some(deadline + self.timing.cycle_duration);
            }
        }
    }

    /// Publisher pick: lowest-id Alive node, defaulting to self.
    fn pick_publisher_candidate(&self) -> u8 {
        self.state.lowest_alive_id()
    }

    /// Placeholder for the next-cycle epoch; the barrier is at the
    /// application layer for now.
    fn next_cycle_tick(&self) -> u64 {
        0
    }
}