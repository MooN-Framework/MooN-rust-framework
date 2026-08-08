use crate::framework::peer_sync::{extract_sync_fields, PeerSync, SyncFields, SAMPLES_PER_PEER};
use crate::framework::state::{PeerHealth, StoredSnapshot, crc_from_snapshot_fields};
use crate::framework::state_machine::{NodeState, StateEvent, SystemState};
use crate::framework::traits::{Computation, DecisionSink, SelfTest, Voter, VotingOutcome};
use crate::framework::transport::RecvOutcome;
use crate::framework::types::PeerMask;
use crate::framework::wire::Payload;
use serde::Deserialize;
use std::cell::Cell;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use super::PhaseOutcome;

impl<C, V, S, T> super::Runner<C, V, S, T>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    T: SelfTest,
    C::Input: for<'de> Deserialize<'de>,
{
    /// Startup: delegate to the user-supplied `SelfTest`. On `Ok` the
    /// node proceeds to `InitSync`; on `Err` it goes straight to Failsafe
    /// via `SelfTestErr`.
    pub(super) fn handle_startup(&mut self) -> StateEvent {
        self.state.set_system_state(SystemState::Startup);
        match self.self_test.run() {
            Ok(()) => {
                info!("self test passed");
                StateEvent::SelfTestOk
            }
            Err(e) => {
                error!(error = ?e, "self test failed");
                StateEvent::SelfTestErr
            }
        }
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

        let deadline = Instant::now()
            + if is_returning {
                self.timing.resync_lost_peer_returning_timeout
            } else {
                self.timing.resync_lost_peer_healthy_timeout
            };

        let outcome = self.collect_phase(
            "resync",
            deadline,
            self.timing.resync_send_interval,
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
                if let Err(e) = this.transport.send(
                    node_state,
                    Payload::State {
                        seen_mask: mask,
                        active_count: ac,
                    },
                ) {
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
                    warn!(
                        peer_id = frame.node_id(),
                        "non-resync frame in resync phase, ignoring"
                    );
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
                    self.state.set_needs_state_sync(true);   // NEU: markiere für nächste Phase
                    // enter_self_probation() ENTFÄLLT — Snapshot setzt self_probation_remaining
                }
                self.state.clear_pending_rejoin();
                self.state.start_new_cycle(self.next_cycle_tick());
                StateEvent::ResyncLostPeerOk   // statt InitialSyncOk
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

    pub(super) fn handle_system_state_crc(&mut self) -> StateEvent {
        self.state.reset_crc_evidence();

        let own_crc = self.state.compute_system_state_crc();
        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.system_state_crc_timeout;

        debug!(own_crc, "system state crc exchange entered");

        let outcome = self.collect_phase(
            "system_state_crc",
            deadline,
            self.timing.system_state_crc_send_interval,
            |this| {
                if let Err(e) = this
                    .transport
                    .send(node_state, Payload::SystemStateCrc { crc: own_crc })
                {
                    error!(error = ?e, "send_system_state_crc failed");
                    return Err(());
                }
                Ok(())
            },
            |this| this.state.healthy_peers_missing_crc().is_empty(),
            |this, frame| {
                if let Payload::SystemStateCrc { crc } = frame.payload() {
                    let _ = this.state.record_peer_crc(frame.node_id(), crc);
                }
            },
        );

        match outcome {
            PhaseOutcome::Complete => {
                if self.state.crc_unanimous(own_crc) {
                    debug!(own_crc, "crc unanimous");
                    StateEvent::CrcOk
                } else {
                    let picks: Vec<(u8, Option<u32>)> = self
                        .state
                        .peers()
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, p)| {
                            if p.health == PeerHealth::Lost {
                                return None;
                            }
                            Some((p.id, self.state.cycle_peer_crc(idx)))
                        })
                        .collect();
                    error!(own_crc, peer_crcs = ?picks, "crc divergence, failsafe");
                    StateEvent::CrcDivergent
                }
            }
            PhaseOutcome::Timeout => {
                let missing = self.state.healthy_peers_missing_crc();
                error!(missing = ?missing.as_slice(), "crc exchange timeout, failsafe");
                StateEvent::CrcDivergent
            }
            PhaseOutcome::Fault => StateEvent::Fault,
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
            self.timing.init_sync_send_interval,
            |this| {
                if let Err(e) = this.transport.send(
                    node_state,
                    Payload::State {
                        seen_mask: PeerMask::EMPTY,
                        active_count: 0,
                    },
                ) {
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
                } else {
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
                info!(
                    epsilon_ns = epsilon,
                    count = clocks.len(),
                    "peer sync complete"
                );
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

            if let RecvOutcome::TimeSync {
                frame,
                local_recv_ns,
                ..
            } = self.transport.try_recv()
            {
                match extract_sync_fields(&frame, local_recv_ns) {
                    Some(SyncFields::Request { t1, t2_local, .. }) => {
                        if let Err(e) = self.transport.send_time_sync_resp(node_state, t1, t2_local)
                        {
                            warn!(error = ?e, "send_time_sync_resp failed");
                        }
                    }
                    Some(SyncFields::Response {
                        peer_id,
                        t1,
                        t2,
                        t3,
                        t4_local,
                    }) => {
                        peer_sync.on_response(peer_id, t1, t2, t3, t4_local);
                    }
                    None => {}
                }
            }
        }
    }

    pub(super) fn handle_system_state_sync(&mut self) -> StateEvent {
    let is_receiver = self.state.needs_state_sync();
    info!(is_receiver, "system state sync entered");

    self.state.reset_state_sync_evidence();

    let node_state = self.state.node_state();
    let deadline = Instant::now() + self.timing.system_state_sync_timeout;

    // Sender-Rolle: eigener Snapshot bleibt konstant während der Phase.
    // Receiver-Rolle: eigener Snapshot unbekannt bis Anwendung, nichts zu senden.
    let (nom, min, pc, cs, entries) = self.state.build_snapshot();

    // Für den Sender: Empfänger-Liste = alle non-Lost Peers (der Receiver
    // ist einer davon; die anderen Sender ackn nicht, aber schicken auch
    // keinen Snapshot der ein Ack erwartet, weil sie nicht Empfänger sind).
    // Vereinfachung: wir warten auf Acks von allen non-Lost, aber
    // ignorieren fehlende Acks von Sendern (die brauchen wir nicht).
    // Sauberer: der Sender weiß nicht wer Empfänger ist. Er sendet einfach
    // und wartet auf mindestens einen Ack. Bei mehreren Empfängern:
    // Ack von jedem der needs_state_sync=true hat. Aber das weiß der
    // Sender lokal nicht. Pragmatisch:
    //   Sender-Abschluss = mindestens ein Ack mit passendem CRC empfangen
    //                    UND alle non-Lost haben entweder Snapshot ODER Ack gesendet
    //   Receiver-Abschluss = Snapshot angewandt + Ack gesendet
    // Für den Rückkehrer-Fall (1 Empfänger, N-1 Sender) reicht: mindestens
    // 1 Ack mit unserem CRC → wir sind Mehrheit, weiter.

    let outcome = self.collect_phase(
        "system_state_sync",
        deadline,
        self.timing.system_state_sync_send_interval,
        |this| {
            if is_receiver {
                // Receiver: sobald wir Snapshots haben, wenden wir Mehrheit an
                // und senden Ack. Vorher nichts.
                if !this.state.sync_snapshots().is_empty() {
                    if let Some((winner, _minority)) = this.state.majority_snapshot() {
                        if let Err(e) = this.state.apply_snapshot(
                            winner.nominal, winner.min, winner.probation_cycles,
                            winner.current_seq, &winner.entries,
                        ) {
                            error!(error = ?e, "apply_snapshot failed");
                            return Err(());
                        }
                        let adopted_crc = this.state.compute_system_state_crc();
                        if let Err(e) = this.transport.send(
                            node_state,
                            Payload::SystemStateSnapshotAck { adopted_crc },
                        ) {
                            error!(error = ?e, "send_snapshot_ack failed");
                            return Err(());
                        }
                    }
                }
            } else {
                // Sender: broadcast snapshot.
                if let Err(e) = this.transport.send(
                    node_state,
                    Payload::SystemStateSnapshot {
                        nominal_participants: nom,
                        min_participants: min,
                        probation_cycles: pc,
                        current_seq: cs,
                        entries,
                    },
                ) {
                    error!(error = ?e, "send_snapshot failed");
                    return Err(());
                }
            }
            Ok(())
        },
        |this| {
            if is_receiver {
                // Fertig wenn wir Snapshots von allen non-Lost haben und
                // (den Snapshot bereits angewandt haben, angezeigt durch
                // needs_state_sync=false in apply-Erfolg — aber das setzen
                // wir erst nach Handler-Abschluss). Alternative:
                // Snapshot-Set komplett + kein Ausstand.
                this.state.peers_missing_snapshot().is_empty()
            } else {
                // Sender: Ack von mindestens einem Peer der needs_state_sync
                // hatte, und CRC stimmt mit unserem überein.
                let own_crc = crc_from_snapshot_fields(nom, min, pc, cs, &entries);
                this.state.sync_acks().iter().any(|(_, c)| *c == own_crc)
            }
        },
        |this, frame| {
            let peer_id = frame.node_id();
            match frame.payload() {
                Payload::SystemStateSnapshot {
                    nominal_participants, min_participants,
                    probation_cycles, current_seq, entries,
                } => {
                    let snap = StoredSnapshot {
                        nominal: nominal_participants,
                        min: min_participants,
                        probation_cycles,
                        current_seq,
                        entries,
                    };
                    this.state.record_sync_snapshot(peer_id, snap);
                }
                Payload::SystemStateSnapshotAck { adopted_crc } => {
                    this.state.record_sync_ack(peer_id, adopted_crc);
                }
                _ => {
                    debug!(peer_id, "non-sync frame in state sync phase, dropped");
                }
            }
        },
    );

    match outcome {
        PhaseOutcome::Complete => {
            if is_receiver {
                self.state.set_needs_state_sync(false);
                info!("state sync complete as receiver");
            } else {
                // Prüfen: wurde unser CRC von den Empfängern akzeptiert?
                let own_crc = crc_from_snapshot_fields(nom, min, pc, cs, &entries);
                let all_match = self.state.sync_acks().iter().all(|(_, c)| *c == own_crc);
                if !all_match {
                    error!(own_crc, "our snapshot was minority, failsafe");
                    return StateEvent::SystemStateSyncMinority;
                }
                info!("state sync complete as sender");
            }
            StateEvent::SystemStateSyncOk
        }
        PhaseOutcome::Timeout => {
            let missing_snap = self.state.peers_missing_snapshot();
            let missing_ack = self.state.peers_missing_sync_ack();
            error!(
                missing_snap = ?missing_snap.as_slice(),
                missing_ack = ?missing_ack.as_slice(),
                "state sync timeout"
            );
            StateEvent::SystemStateSyncTimeout
        }
        PhaseOutcome::Fault => StateEvent::Fault,
    }
}

    /// CycleSync: peers exchange State beacons with attested seen-masks
    /// until each side has observed all non-Lost peers, or the phase times
    /// out.
    pub(super) fn handle_cycle_sync(&mut self) -> StateEvent {
        self.state.reset_cycle_sync_evidence();

        self.state.log_system_state_crc_contents();

        let node_state = self.state.node_state();
        let expected_mask = self.state.expected_sync_mask();
        let deadline = Instant::now() + self.timing.cycle_sync_timeout;

        let outcome = self.collect_phase(
            "cycle_sync",
            deadline,
            self.timing.cycle_sync_send_interval,
            |this| {
                let mask = this.state.own_seen_mask();
                if let Err(e) = this.transport.send(
                    node_state,
                    Payload::State {
                        seen_mask: mask,
                        active_count: this.state.active_count_including_self(),
                    },
                ) {
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

    /// ReadInputs: sleep to the next cycle tick, then latch the current
    /// input into the cycle state. Actual compute is deferred until after
    /// ShareInputs so every node has verified all peers' inputs are within
    /// tolerance before spending the cycle on a computation that would be
    /// discarded on divergence.
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

        self.state.record_own_input(self.input);
        StateEvent::InputsRead
    }

    /// ShareInputs: broadcast our sensor input until every non-Lost peer's
    /// input has arrived. Then gate on `Computation::inputs_agree` — any
    /// disagreement routes the cycle to ErrorManagement so the divergent
    /// sensor can be excluded. On agreement the local computation runs
    /// and its result is latched for ShareResult.
    pub(super) fn handle_share_inputs(&mut self) -> StateEvent {
        let own = match self.state.own_input() {
            Some(i) => i,
            None => {
                error!("share_inputs entered without own input");
                return StateEvent::Fault;
            }
        };

        let deadline = Instant::now() + self.timing.share_inputs_timeout;
        let node_state = self.state.node_state();

        let outcome = self.collect_phase(
            "share_inputs",
            deadline,
            self.timing.share_inputs_send_interval,
            |this| {
                if let Err(e) = this.transport.send(node_state, Payload::Input(own)) {
                    error!(error = ?e, "send_input failed");
                }
                Ok(())
            },
            |this| this.all_peer_inputs_in(),
            |this, frame| {
                if frame.node_state_wire() == NodeState::ResyncLostPeer.to_wire() {
                    this.state.set_rejoin_seen(frame.node_id());
                } else {
                    this.ingest_frame(frame);
                }
            },
        );

        match outcome {
            PhaseOutcome::Timeout => {
                warn!("share_inputs timeout");
                return StateEvent::ShareInputsTimeout;
            }
            PhaseOutcome::Fault => return StateEvent::Fault,
            PhaseOutcome::Complete => {}
        }

        // Divergence gate on collected peer inputs.
        for (idx, slot) in self.state.peer_inputs().iter().enumerate() {
            if let Some(peer_input) = slot {
                if !self.computation.inputs_agree(&own, peer_input) {
                    let peer_id = self.state.peers()[idx].id;
                    warn!(peer_id, "input divergence detected");
                    return StateEvent::InputsDivergent;
                }
            }
        }

        // All peer inputs within tolerance — run the domain computation.
        match self.computation.compute(own) {
            Ok(payload) => {
                self.state.record_own_result(payload);
                StateEvent::InputsShared
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

        let deadline = Instant::now() + self.timing.share_result_timeout;
        let node_state = self.state.node_state();

        let outcome = self.collect_phase(
            "share_result",
            deadline,
            self.timing.share_result_send_interval,
            |this| {
                if suppress_send {
                    return Ok(());
                }
                if let Err(e) = this.transport.send(node_state, Payload::Result(own)) {
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
        let deadline = Instant::now() + self.timing.send_ack_timeout;

        let outcome = self.collect_phase(
            "send_ack",
            deadline,
            self.timing.send_ack_send_interval,
            |this| {
                if suppress_send {
                    return Ok(());
                }
                if let Err(e) = this.transport.send(
                    node_state,
                    Payload::Ack {
                        received_from: mask,
                        publisher_candidate: candidate,
                        rejoin_vote,
                    },
                ) {
                    error!(error = ?e, "send_ack failed");
                }
                Ok(())
            },
            |this| this.all_peer_acks_in(),
            |this, frame| {
                if let Payload::Ack { rejoin_vote, .. } = frame.payload() {
                    let _ = this
                        .state
                        .record_peer_rejoin_vote(frame.node_id(), rejoin_vote);
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
                let own_pick = self.pick_publisher_candidate();
                let publisher = match self.state.publisher_consensus(own_pick) {
                    Some(id) => id,
                    None => {
                        let picks: Vec<(u8, u8)> = self
                            .state
                            .peers()
                            .iter()
                            .enumerate()
                            .filter_map(|(idx, p)| {
                                if p.health == PeerHealth::Lost {
                                    return None;
                                }
                                self.state.cycle().peer_acks[idx]
                                    .map(|a| (p.id, a.publisher_candidate))
                            })
                            .collect();
                        error!(
                            own_id = self.state.own_id(),
                            own_pick,
                            peer_picks = ?picks,
                            "publisher pick divergence, failsafe"
                        );
                        return StateEvent::Fault;
                    }
                };
                let own_id = self.state.own_id();

                let own_result = self.state.cycle().own_result;
                let dissenter_analysis = own_result.map(|own| {
                    self.state.voter().find_dissenters(
                        &own,
                        &self.state.cycle().peer_results,
                        &decision,
                    )
                });

                if let Some((own_dissented, peer_dissenter_indices)) = dissenter_analysis {
                    if own_dissented {
                        error!(own_id, "own value dissented, failsafe");
                        return StateEvent::Fault;
                    }
                    if !peer_dissenter_indices.is_empty() {
                        let dissenter_ids: Vec<u8> = peer_dissenter_indices
                            .iter()
                            .filter_map(|idx| self.state.peers().get(*idx as usize).map(|p| p.id))
                            .collect();

                        if publisher == own_id {
                            warn!(
                                publisher,
                                "consensus with dissenters, publishing then reconfig"
                            );
                            self.sink.publish(&decision);
                        }
                        for peer_id in dissenter_ids {
                            warn!(peer_id, "peer value diverged from consensus");
                            let _ = self.state.propose_exclude(peer_id);
                        }
                        return StateEvent::DissenterDetected;
                    }
                }

                if publisher == own_id {
                    info!(publisher, "consensus reached, publishing");
                    self.sink.publish(&decision);
                } else {
                    debug!(publisher, own_id, "consensus reached, peer publishes");
                }

                let promoted = self.state.tick_probation();
                if promoted > 0 {
                    info!(promoted, "peers promoted from Probation to Alive");
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
                error!(
                    peer_results = self.state.cycle().peer_results.len(),
                    "insufficient quorum"
                );
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
        let deadline = Instant::now() + self.timing.error_management_timeout;

        debug!(proposed = own_proposal.as_u8(), "exclusion vote entered");

        let outcome = self.collect_phase(
            "exclusion_vote",
            deadline,
            self.timing.error_management_send_interval,
            |this| {
                if let Err(e) = this.transport.send(
                    node_state,
                    Payload::ExclusionProposal {
                        propose_exclude: own_proposal,
                    },
                ) {
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
        if self.state.self_in_probation() {
            // Self in probation must not pick itself. Fall back to the
            // lowest-id Alive peer. If none exists we return self anyway —
            // publisher_consensus will then flag divergence and we'll fault.
            self.state
                .lowest_alive_peer_id()
                .unwrap_or_else(|| self.state.own_id())
        } else {
            self.state.lowest_alive_id()
        }
    }

    /// Placeholder for the next-cycle epoch; the barrier is at the
    /// application layer for now.
    fn next_cycle_tick(&self) -> u64 {
        0
    }
}