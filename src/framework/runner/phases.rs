use super::PhaseOutcome;
use crate::framework::config::{MAX_PEERS, MAX_TOTAL_NODES};
use crate::framework::clock_sync::{extract_sync_fields, ClockSync, SyncFields, SAMPLES_PER_PEER};
use crate::framework::state::{
    crc_from_snapshot_fields, serialize_app_data, PeerHealth, StoredSnapshot,
};
use crate::framework::state_machine::{NodeState, StateEvent, SystemState};
use crate::framework::traits::SinkVerdict;
use crate::framework::traits::{
    ApplicationStateProvider, Computation, DecisionSink, InputSource, SelfTest, Voter,
    VotingOutcome,
};
use crate::framework::transport::RecvOutcome;
use crate::framework::types::PeerMask;
use crate::framework::wire::FailsafeReason;
use crate::framework::wire::Payload;
use serde::Deserialize;
use std::cell::Cell;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

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
    /// Startup: delegate to the user-supplied `SelfTest`. On `Ok` the
    /// node proceeds to `InitSync`; on `Err` it goes straight to Failsafe
    /// via `SelfTestErr`.
    pub(super) fn handle_startup(&mut self) -> StateEvent {
        self.state.set_system_state(SystemState::Startup);

        // Test hook: `MOON_INJECT_SELFTEST_FAIL=1` in the environment
        // forces this to take the SelfTestFailed path. Only compiled
        // under `feature = "diagnostic"`; production builds ignore
        // the variable entirely.
        #[cfg(feature = "diagnostic")]
        {
            if std::env::var("MOON_INJECT_SELFTEST_FAIL")
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
            {
                error!("injection: MOON_INJECT_SELFTEST_FAIL set, forcing self-test failure");
                self.mark_failsafe(FailsafeReason::SelfTestFailed);
                return StateEvent::SelfTestErr;
            }
        }

        match self.self_test.run() {
            Ok(()) => {
                info!("self test passed");
                StateEvent::SelfTestOk
            }
            Err(e) => {
                error!(error = ?e, "self test failed");
                self.mark_failsafe(FailsafeReason::SelfTestFailed);
                StateEvent::SelfTestErr
            }
        }
    }

    /// Isolation: park until external intervention. The sink hook fires
    /// exactly once so the local wiring can log / release its actuator
    /// claim. No `transport.recv()` here: the node is already out of the
    /// ack round and the publisher election, so a GoFailsafe from the
    /// remaining fabric would not change the local actuator state. Only a
    /// restart brings the node back.
    pub(super) fn handle_isolation(&mut self) -> StateEvent {
        warn!("isolation entered, notifying sink");
        self.sink.on_isolation();
        loop {
            sleep(Duration::from_secs(1));
            #[cfg(feature = "diagnostic")]
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
                self.timing.resync_returning_timeout
            } else {
                self.timing.resync_healthy_timeout
            };

        let outcome = self.collect_phase(
            "resync",
            deadline,
            self.timing.send_interval,
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
                if let Err(e) = this.send_frame(
                    node_state,
                    Payload::State {
                        seen_mask: mask,
                        active_count: ac,
                        cycle_seq: this.state.current_seq(),
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
                    // The snapshot adopted in SystemStateSync carries our
                    // own probation entry, so no local probation seeding
                    // is needed here.
                    self.state.set_needs_state_sync(true);
                }
                self.state.clear_pending_rejoin();
                self.state.start_new_cycle();
                StateEvent::ResyncLostPeerOk // statt InitialSyncOk
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
            self.timing.send_interval,
            |this| {
                if let Err(e) = this.send_frame(
                    node_state,
                    Payload::State {
                        seen_mask: PeerMask::EMPTY,
                        active_count: 0,
                        cycle_seq: this.state.current_seq(),
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

                self.state.start_new_cycle();
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

    /// ClockSync: run Cristian rounds with each non-Lost peer until every
    /// peer has enough samples or the window elapses.
    pub(super) fn handle_clock_sync(&mut self) -> StateEvent {
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
            error!("clock sync entered without active peers");
            return StateEvent::Fault;
        }

        // Liveness threshold for excluding silent peers. Set to
        // ~10% of the phase deadline so a peer that dies mid-phase
        // (e.g. rejoin race: probation node killed shortly after
        // readmit — see T21) gets flagged well before the whole phase
        // has to time out, while still tolerating short bursts of
        // packet loss on multicast. Clamped to a floor of 50 ms so
        // very tight configs don't false-positive.
        let unreachable_threshold =
            self.timing.clock_sync_timeout / 10;
        let unreachable_threshold = unreachable_threshold.max(Duration::from_millis(50));
        let unreachable_threshold_ns = unreachable_threshold.as_nanos() as u64;

        let phase_start_ns = crate::framework::transport::now_monotonic_ns();
        let mut clock_sync = ClockSync::new(&peer_ids, phase_start_ns);
        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.clock_sync_timeout;
        let mut next_request = Instant::now();

        loop {
            // Cheap to call every iteration — it's a linear scan of
            // at most MAX_PEERS entries with a saturating subtract.
            clock_sync.mark_unreachable_if_stale(
                crate::framework::transport::now_monotonic_ns(),
                unreachable_threshold_ns,
            );

            if clock_sync.is_complete() {
                let clocks = clock_sync.finalize();
                let epsilon = clock_sync.max_error_bound().unwrap_or(0);
                let unreachable = clock_sync.unreachable_peers();
                if !unreachable.is_empty() {
                    warn!(
                        unreachable = ?unreachable.as_slice(),
                        "clock sync complete with unreachable peers — they'll be dropped by the normal missed-frame path next cycle"
                    );
                }
                info!(
                    epsilon_ns = epsilon,
                    count = clocks.len(),
                    "clock sync complete"
                );
                // Diagnostic view of the actual clock offsets. Split out
                // from the completion `info!` because these are the
                // numbers you look at when investigating drift: the
                // group median (what we'd correct our local clock by
                // to follow the fabric) plus each peer's individual
                // offset relative to us. Logged at warn so it stands
                // out in a log dominated by info-level cycle chatter,
                // even though a healthy sync is not itself a problem.
                let median_offset_ns = clock_sync.convergence_correction().unwrap_or(0);
                let per_peer_offsets_ns: Vec<(u8, i64)> = clocks
                    .iter()
                    .map(|c| (c.peer_id, c.offset_ns))
                    .collect();
                warn!(
                    median_offset_ns,
                    epsilon_ns = epsilon,
                    per_peer_offsets_ns = ?per_peer_offsets_ns.as_slice(),
                    "clock divergence snapshot"
                );
                self.state.set_peer_clocks(&clocks);
                self.state.set_sync_epsilon(epsilon);
                self.state.mark_sync_valid();
                self.cycles_since_last_sync = 0;
                self.next_cycle_deadline = None;
                self.last_cycle_start = None;
                self.state.start_new_cycle();
                return StateEvent::ClockSyncOk;
            }

            if Instant::now() > deadline {
                warn!(
                    found = clock_sync.finalize().len(),
                    total = peer_ids.len(),
                    target = SAMPLES_PER_PEER,
                    unreachable = ?clock_sync.unreachable_peers().as_slice(),
                    "clock sync deadline exceeded"
                );
                return StateEvent::ClockSyncTimeout;
            }

            if Instant::now() >= next_request {
                let any_needs_sync = peer_ids.iter().any(|&id| !clock_sync.has_pending(id));
                if any_needs_sync {
                    match self.transport.send_time_sync_req(node_state) {
                        Ok((_seq, t1)) => {
                            for &peer_id in &peer_ids {
                                if !clock_sync.has_pending(peer_id) {
                                    clock_sync.record_outgoing_request(peer_id, t1);
                                }
                            }
                            next_request = Instant::now() + self.timing.send_interval;
                        }
                        Err(e) => {
                            error!(error = ?e, "send_time_sync_req failed");
                            return StateEvent::Fault;
                        }
                    }
                }
            }

            match self.transport.try_recv() {
                RecvOutcome::TimeSync {
                    frame,
                    local_recv_ns,
                    ..
                } => match extract_sync_fields(&frame, local_recv_ns) {
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
                        clock_sync.on_response(peer_id, t1, t2, t3, t4_local);
                    }
                    None => {}
                },
                // A peer can decide to fail-stop while we are still
                // syncing clocks. This phase does not go through
                // `collect_phase`, so without this arm the broadcast is
                // dropped and we keep running until our own deadline
                // fires — the fabric-wide stop would be delayed by a
                // full phase for no reason.
                RecvOutcome::Valid(frame)
                | RecvOutcome::SeqGap { frame, .. }
                | RecvOutcome::NewSession { frame, .. } => {
                    if let Payload::GoFailsafe { reason } = frame.payload() {
                        warn!(
                            peer_id = frame.node_id(),
                            reason, "Peer broadcast GoFailsafe during clock sync"
                        );
                        self.peer_failsafe_seen = true;
                        return StateEvent::Fault;
                    }
                }
                _ => {}
            }
        }
    }

    pub(super) fn handle_system_state_sync(&mut self) -> StateEvent {
        let is_receiver = self.state.needs_state_sync();
        info!(is_receiver, "system state sync entered");

        self.state.reset_state_sync_evidence();

        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.state_sync_timeout;

        // Sender-Rolle: eigener Snapshot bleibt konstant während der Phase.
        // Receiver-Rolle: eigener Snapshot unbekannt bis Anwendung, nichts zu senden.
        let (nom, min, pc, cs, entries) = self.state.build_snapshot();

        // Application-data snapshot for the sender. Frozen at phase
        // entry so the same bytes ride in every retransmit and match
        // the CRC we compare against incoming acks. Receivers ignore
        // this pair — their own app_state will be overwritten via
        // `ApplicationStateProvider::apply` once the majority
        // snapshot has been picked.
        let (own_app_len, own_app_buf) = serialize_app_data(&self.app_state.snapshot());

        // Role split for this phase:
        //   Sender   completes on at least one ack carrying our own CRC.
        //            A sender does not know locally which nodes are
        //            receivers, so it cannot wait for a specific set.
        //   Receiver completes once it has adopted the majority
        //            snapshot (see the latch below).
        // Receiver-side latch: set once the majority snapshot has been
        // adopted. Also the receiver's completion condition — the phase
        // must not end just because all snapshots arrived, since without
        // a strict majority none of them is adopted and leaving with
        // `needs_state_sync = false` would silently skip the sync.
        let applied: Cell<bool> = Cell::new(false);

        let outcome = self.collect_phase(
            "system_state_sync",
            deadline,
            self.timing.send_interval,
            |this| {
                if is_receiver {
                    // Adopt only once every non-Lost peer's snapshot is
                    // in and one of them holds a strict majority.
                    // Applying on the first arrival would let a single
                    // sender drive our roster and application state, and
                    // re-applying every send interval would let it flip
                    // us back and forth.
                    if !applied.get() && this.state.peers_missing_snapshot().is_empty() {
                        if let Some((winner, _minority)) = this.state.majority_snapshot() {
                            if let Err(e) = this.state.apply_snapshot(
                                winner.nominal,
                                winner.min,
                                winner.probation_cycles,
                                winner.current_seq,
                                &winner.entries,
                            ) {
                                error!(error = ?e, "apply_snapshot failed");
                                return Err(());
                            }
                            // Adopt the winner's application state.
                            // Decoding must succeed — the bytes were
                            // produced by an `ApplicationData` impl of
                            // the same type on the sender. A decode
                            // failure means wire corruption slipped
                            // past the frame CRC or the sender
                            // shipped a different `A::Data` type, both
                            // of which are unrecoverable here.
                            match winner.decode_app_data::<A::Data>() {
                                Ok(decoded) => this.app_state.apply(&decoded),
                                Err(e) => {
                                    error!(
                                        error = ?e,
                                        "decode_app_data on winner snapshot failed"
                                    );
                                    return Err(());
                                }
                            }
                            // Re-serialize the now-applied application
                            // state so the ack CRC folds in the
                            // adopted bytes, not our pre-sync ones.
                            let (adopted_app_len, adopted_app_buf) =
                                serialize_app_data(&this.app_state.snapshot());
                            let adopted_crc = this
                                .state
                                .compute_system_state_crc(adopted_app_len, &adopted_app_buf);
                            info!(adopted_crc, "majority snapshot adopted");
                            applied.set(true);
                        } else {
                            warn!(
                                senders = this.state.sync_snapshots().len(),
                                "no strict majority among collected snapshots, not adopting"
                            );
                        }
                    }

                    // Keep re-attesting the adopted state until a sender
                    // has seen our ack or the deadline runs out.
                    if applied.get() {
                        let (adopted_app_len, adopted_app_buf) =
                            serialize_app_data(&this.app_state.snapshot());
                        let adopted_crc = this
                            .state
                            .compute_system_state_crc(adopted_app_len, &adopted_app_buf);
                        if let Err(e) = this
                            .transport
                            .send(node_state, Payload::SystemStateSnapshotAck { adopted_crc })
                        {
                            error!(error = ?e, "send_snapshot_ack failed");
                            return Err(());
                        }
                    }
                } else {
                    // Sender: broadcast snapshot including the frozen
                    // application-data trailer.
                    if let Err(e) = this.send_frame(
                        node_state,
                        Payload::SystemStateSnapshot {
                            nominal_participants: nom,
                            min_participants: min,
                            probation_cycles: pc,
                            current_seq: cs,
                            entries,
                            app_data_len: own_app_len,
                            app_data: own_app_buf,
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
                    // Complete only once the majority snapshot was
                    // actually adopted, not merely once all snapshots
                    // arrived.
                    let _ = this;
                    applied.get()
                } else {
                    // Sender: Ack von mindestens einem Peer der needs_state_sync
                    // hatte, und CRC stimmt mit unserem überein.
                    let own_crc = crc_from_snapshot_fields(
                        nom, min, pc, cs, &entries, own_app_len, &own_app_buf,
                    );
                    this.state.sync_acks().iter().any(|(_, c)| *c == own_crc)
                }
            },
            |this, frame| {
                let peer_id = frame.node_id();
                match frame.payload() {
                    Payload::SystemStateSnapshot {
                        nominal_participants,
                        min_participants,
                        probation_cycles,
                        current_seq,
                        entries,
                        app_data_len,
                        app_data,
                    } => {
                        let snap = StoredSnapshot {
                            nominal: nominal_participants,
                            min: min_participants,
                            probation_cycles,
                            current_seq,
                            entries,
                            app_data_len,
                            app_data,
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
                    let own_crc = crc_from_snapshot_fields(
                        nom, min, pc, cs, &entries, own_app_len, &own_app_buf,
                    );
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
        let (dbg_app_len, dbg_app_buf) = serialize_app_data(&self.app_state.snapshot());
        self.state
            .log_system_state_crc_contents(dbg_app_len, &dbg_app_buf);

        let suppress = self.is_muted() || self.should_drop_cyclesync();
        if suppress {
            warn!("injection: suppressing cyclesync send");
        }

        let node_state = self.state.node_state();
        let expected_mask = self.state.expected_sync_mask();
        let deadline = std::time::Instant::now() + self.timing.cycle_sync_timeout;

        let outcome = self.collect_phase(
            "cycle_sync",
            deadline,
            self.timing.send_interval,
            |this| {
                if suppress {
                    return Ok(());
                }
                let mask = this.state.own_seen_mask();
                if let Err(e) = this.send_frame(
                    node_state,
                    Payload::State {
                        seen_mask: mask,
                        active_count: this.state.active_count_including_self(),
                        cycle_seq: this.state.current_seq(),
                    },
                ) {
                    error!(error = ?e, "send_state failed in cycle sync");
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
            super::PhaseOutcome::Complete => {
                self.state.start_new_cycle();
                StateEvent::CycleSyncOk
            }
            super::PhaseOutcome::Timeout => StateEvent::CycleSyncTimeout,
            super::PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    /// ReadInputs: sleep to the next cycle tick, then latch the current
    /// input into the cycle state. Actual compute is deferred until after
    /// ShareInputs so every node has verified all peers' inputs are within
    /// tolerance before spending the cycle on a computation that would be
    /// discarded on divergence.
    ///
    /// `last_cycle_start` is set here — this is the cycle anchor used by
    /// the in-cycle phases for their deadlines. All healthy nodes reach
    /// this point via the CycleSync barrier, so the anchor is aligned to
    /// within the peer-sync epsilon across the fabric.
    pub(super) fn handle_read_inputs(&mut self) -> StateEvent {
        #[cfg(feature = "diagnostic")]
        self.apply_pending_diagnostic();

        // Optional per-cycle extra sleep, taken from InjectCycleDelay.
        let extra_ms = self.take_pending_cycle_delay_ms();
        if extra_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(extra_ms as u64));
        }

        self.wait_for_next_cycle_tick();

        let now = std::time::Instant::now();
        if let Some(prev) = self.last_cycle_start {
            let elapsed = now.duration_since(prev);
            self.last_cycle_us = Some(elapsed.as_micros());
            info!(cycle_us = elapsed.as_micros(), "cycle duration");
        }
        self.last_cycle_start = Some(now);
        // After the cycle-duration line has been written. The gap to
        // `now` is the cost of that single synchronous log write.
        self.mark_after_cycle_log = Some(std::time::Instant::now());

        #[cfg(feature = "diagnostic")]
        {
            if self.inject_input_provider_fail_next {
                self.inject_input_provider_fail_next = false;
                warn!("injection: skipping record_own_input (InjectInputProviderFail)");
                return StateEvent::InputsRead;
            }
        }

        match self.input_source.read() {
            Ok(input) => {
                self.state.record_own_input(input);
                self.mark_after_input = Some(std::time::Instant::now());
                StateEvent::InputsRead
            }
            Err(e) => {
                // Domain-level sensor failure. The trait contract routes
                // this to Failsafe by design — the sink's `on_failsafe`
                // hook will drive the actuator into its safe state.
                // Impls that want softer semantics (last-known-good on
                // transient miss) have to return `Ok(...)` themselves;
                // we never guess.
                error!(error = ?e, "input source read failed, going to Failsafe");
                StateEvent::InputSourceFailed
            }
        }
    }

    /// ShareInputs: broadcast our sensor input until every non-Lost peer's
    /// input has arrived. Then gate on `Computation::inputs_agree` — any
    /// disagreement routes the cycle to ErrorManagement so the divergent
    /// sensor can be excluded. On agreement the local computation runs
    /// and its result is latched for ShareResult.
    ///
    /// Rendezvous: if a healthy peer already advanced to ErrorManagement
    /// (typically because its input-arrival timeout fired a few μs earlier)
    /// we follow it forward via `PeerInError` instead of waiting for our
    /// own deadline and going to Failsafe in isolation.
    pub(super) fn handle_share_inputs(&mut self) -> StateEvent {
        let _ = self.take_peer_in_error();

        let own = match self.state.own_input() {
            Some(i) => i,
            None => return StateEvent::Fault,
        };

        let deadline = self.cycle_anchor() + self.timing.share_inputs_offset;
        let node_state = self.state.node_state();

        // Instrumentation for the in-cycle budget. The deadline is an
        // offset from the cycle anchor set in ReadInputs, so everything
        // consumed between the anchor and this point is already gone
        // from this phase's budget before a single frame was sent. If
        // the remaining budget is regularly below one send interval,
        // the phase times out for local scheduling reasons rather than
        // because a peer was silent, and the resulting exclusion says
        // nothing about the peers.
        let entry = Instant::now();
        let entry_offset = entry.saturating_duration_since(self.cycle_anchor());
        let remaining = self.timing.share_inputs_offset.saturating_sub(entry_offset);

        // Breakdown of the entry offset, in order of occurrence:
        //   log_us       writing the cycle-duration line
        //   input_us     InputSource::read plus record_own_input
        //   poll_us      the diagnostic poll in the run loop
        //   dispatch_us  transition logging and the hop into this phase
        // They sum to entry_offset_us. Whichever dominates is the thing
        // to fix; two of the four are synchronous log writes.
        let anchor = self.cycle_anchor();
        let m_log = self.mark_after_cycle_log.unwrap_or(anchor);
        let m_input = self.mark_after_input.unwrap_or(m_log);
        let m_poll = self.mark_after_poll.unwrap_or(m_input);
        let log_us = m_log.saturating_duration_since(anchor).as_micros();
        let input_us = m_input.saturating_duration_since(m_log).as_micros();
        let poll_us = m_poll.saturating_duration_since(m_input).as_micros();
        let dispatch_us = entry.saturating_duration_since(m_poll).as_micros();

        if remaining < self.timing.send_interval {
            warn!(
                entry_offset_us = entry_offset.as_micros(),
                remaining_us = remaining.as_micros(),
                budget_us = self.timing.share_inputs_offset.as_micros(),
                send_interval_us = self.timing.send_interval.as_micros(),
                log_us,
                input_us,
                poll_us,
                dispatch_us,
                "share_inputs entered with less than one send interval of budget left"
            );
        } else {
            info!(
                entry_offset_us = entry_offset.as_micros(),
                remaining_us = remaining.as_micros(),
                log_us,
                input_us,
                poll_us,
                dispatch_us,
                "share_inputs entry offset"
            );
        }

        // Injection decisions latched per-phase (not per-send-attempt) so
        // that a `count=1` drops the entire cycle's transmit, not just the
        // first retransmit.
        let suppress = self.is_muted() || self.should_drop_input();
        if suppress {
            warn!("injection: suppressing input send");
        }

        let outcome = self.collect_phase(
            "share_inputs",
            deadline,
            self.timing.send_interval,
            |this| {
                if suppress {
                    return Ok(());
                }
                if let Err(e) = this.send_frame(node_state, Payload::Input(own)) {
                    error!(error = ?e, "send_input failed");
                }
                Ok(())
            },
            // Stop early on rendezvous: a peer already in
            // ErrorManagement will not answer this phase, and waiting
            // out the deadline costs the whole budget. By the time we
            // reach EM the peers may have finished their round, and an
            // EM phase that collects no votes ends in Failsafe.
            |this| this.all_peer_inputs_in() || this.peer_in_error(),
            |this, frame| {
                if frame.node_state_wire() == NodeState::ResyncLostPeer.to_wire() {
                    this.state.set_rejoin_seen(frame.node_id());
                } else {
                    this.ingest_frame(frame);
                }
            },
        );

        if self.take_peer_in_error() {
            warn!("share_inputs: peer already in ErrorManagement, rendezvous");
            self.state.attribute_input_missing();
            return StateEvent::PeerInError;
        }

        match outcome {
            super::PhaseOutcome::Timeout => {
                // Name the peers whose input never landed. Together with
                // the drop counters on the deadline warning this says
                // whether they were silent or whether we discarded what
                // they sent.
                warn!(
                    missing = ?self.state.peers_missing_input(),
                    "share_inputs timed out, peers without input"
                );
                self.state.attribute_input_missing();
                return StateEvent::ShareInputsTimeout;
            }
            super::PhaseOutcome::Fault => return StateEvent::Fault,
            super::PhaseOutcome::Complete => {}
        }

        let mut agree_count: usize = 1; // own zaehlt sich mit
        let mut divergent_peers: heapless::Vec<u8, MAX_PEERS> = heapless::Vec::new();
        // Peer inputs that passed the tolerance gate, fed into
        // `Computation::consolidate` below. Lost peers are filtered out
        // (their slots are never written by `record_peer_input`, the
        // check is belt and braces); Probation peers count, unlike in
        // `run_vote`, because their sensor reading is still a valid
        // measurement of the same physical quantity.
        let mut valid_inputs: heapless::Vec<C::Input, MAX_PEERS> = heapless::Vec::new();
        for (idx, slot) in self.state.peer_inputs().iter().enumerate() {
            if let Some(peer_input) = slot {
                if self.computation.inputs_agree(&own, peer_input) {
                    agree_count += 1;
                    if self.state.peers()[idx].health != PeerHealth::Lost {
                        let _ = valid_inputs.push(*peer_input);
                    }
                } else {
                    let _ = divergent_peers.push(self.state.peers()[idx].id);
                }
            }
        }

        if !divergent_peers.is_empty() {
            let n_alive: usize = 1
                + self
                    .state
                    .peers()
                    .iter()
                    .filter(|p| p.health != PeerHealth::Lost)
                    .count();
            let others = n_alive - agree_count;

            if 2 * agree_count > n_alive {
                // Own in Majority: alle divergenten Peers proposen.
                for peer_id in divergent_peers.iter().copied() {
                    warn!(peer_id, "input divergence detected, proposing exclude");
                    let _ = self.state.propose_exclude(peer_id);
                }
                return StateEvent::InputsDivergent;
            }

            if 2 * others > n_alive {
                // Alt: return InputsDivergent
                warn!(
                    agree_count,
                    n_alive,
                    "own input in minority, going straight to isolation"
                );
                return StateEvent::SelfExcluded;
            }

            // Tie: kein klarer Konsens irgendwo. Byzantine → Failsafe.
            error!(
                agree_count,
                others,
                n_alive,
                "input divergence without recoverable majority (tie split), failsafe"
            );
            self.mark_failsafe(FailsafeReason::StateDivergence);
            return StateEvent::Fault;
        }

        #[cfg(feature = "diagnostic")]
        {
            if self.inject_computation_fail_next {
                self.inject_computation_fail_next = false;
                error!("injection: forcing computation failure (InjectComputationFail)");
                self.mark_failsafe(FailsafeReason::LocalFault);
                return StateEvent::Fault;
            }
        }

        // Consolidation: every non-Lost peer has delivered and every
        // delivered input passed the tolerance gate, so all nodes hold
        // the same input set here. The computation runs on the reduced
        // value (for the brake domain: the median speed) instead of the
        // local sensor value alone, which keeps a single drifting sensor
        // inside its tolerance band from biasing the curve.
        let consolidated = self.computation.consolidate(&own, &valid_inputs);
        self.state.record_consolidated_input(consolidated);
        debug!(
            n_inputs = valid_inputs.len() + 1,
            "computing on consolidated input"
        );

        match self.computation.compute(consolidated) {
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
    ///
    /// Rendezvous: same as ShareInputs — a peer already in ErrorManagement
    /// pulls us forward. We attribute both input- and result-phase evidence
    /// before following.
    ///
    /// Test-hook: `Corruptible::corrupt` on the outgoing payload is applied
    /// once per phase when `should_corrupt_result` returns true. Only
    /// compiled under `feature = "diagnostic"`.
    pub(super) fn handle_share_result(&mut self) -> StateEvent
    where
        V::Payload: crate::framework::traits::Corruptible,
    {
        let _ = self.take_peer_in_error();

        let suppress = self.is_muted() || self.should_drop_result();
        if suppress {
            warn!("injection: suppressing result send");
        }

        let own = match self.state.cycle().own_result {
            Some(r) => r,
            None => return StateEvent::Fault,
        };

        let own = self.maybe_corrupt_result(own);

        let deadline = self.cycle_anchor() + self.timing.share_result_offset;
        let node_state = self.state.node_state();

        let outcome = self.collect_phase(
            "share_result",
            deadline,
            self.timing.send_interval,
            |this| {
                if suppress {
                    return Ok(());
                }
                if let Err(e) = this.send_frame(node_state, Payload::Result(own)) {
                    error!(error = ?e, "send_result failed");
                }
                Ok(())
            },
            |this| this.all_peer_results_in() || this.peer_in_error(),
            |this, frame| {
                if frame.node_state_wire() == NodeState::ResyncLostPeer.to_wire() {
                    this.state.set_rejoin_seen(frame.node_id());
                } else {
                    this.ingest_frame(frame);
                }
            },
        );

        if self.take_peer_in_error() {
            warn!("share_result: peer already in ErrorManagement, rendezvous");
            self.state.attribute_input_missing();
            self.state.attribute_result_missing();
            return StateEvent::PeerInError;
        }

        match outcome {
            super::PhaseOutcome::Complete => StateEvent::ResultShared,
            super::PhaseOutcome::Timeout => {
                self.state.attribute_result_missing();
                StateEvent::ShareResultTimeout
            }
            super::PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    /// SendAck: broadcast our attested received-mask, publisher pick, and
    /// rejoin vote until every non-Lost peer has done the same.
    ///
    /// Rendezvous: as in the earlier phases, we follow a peer already in
    /// ErrorManagement and attribute what we have from all cycle buffers.
    pub(super) fn handle_send_ack(&mut self) -> StateEvent {
        let _ = self.take_peer_in_error();

        let suppress = self.is_muted() || self.should_drop_ack();
        let corrupt_pub = self.should_send_divergent_publisher();
        if suppress {
            warn!("injection: suppressing ack send");
        }
        if corrupt_pub {
            warn!("injection: sending ack with divergent publisher");
        }

        let mask = self.received_mask();
        let mut candidate = self.pick_publisher_candidate();
        if corrupt_pub {
            candidate = candidate.wrapping_add(99);
        }
        let rejoin_vote = self.state.own_rejoin_vote();
        let node_state = self.state.node_state();
        let deadline = self.cycle_anchor() + self.timing.send_ack_offset;

        let outcome = self.collect_phase(
            "send_ack",
            deadline,
            self.timing.send_interval,
            |this| {
                if suppress {
                    return Ok(());
                }
                if let Err(e) = this.send_frame(
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
            |this| this.all_peer_acks_in() || this.peer_in_error(),
            |this, frame| {
                if let Payload::Ack { rejoin_vote, .. } = frame.payload() {
                    let _ = this
                        .state
                        .record_peer_rejoin_vote(frame.node_id(), rejoin_vote);
                }
                this.ingest_frame(frame);
            },
        );

        if self.take_peer_in_error() {
            warn!("send_ack: peer already in ErrorManagement, rendezvous");
            self.state.attribute_input_missing();
            self.state.attribute_result_missing();
            self.fault_peers_missing_ack_unilateral();
            return StateEvent::PeerInError;
        }

        match outcome {
            super::PhaseOutcome::Complete => {
                self.state.attribute_result_missing();
                StateEvent::AckReceived
            }
            super::PhaseOutcome::Timeout => {
                self.state.attribute_result_missing();
                self.fault_peers_missing_ack_unilateral();
                StateEvent::AckTimeout
            }
            super::PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    pub(super) fn handle_system_state_crc(&mut self) -> StateEvent {
        self.state.reset_crc_evidence();
        let _ = self.take_peer_in_error();

        // Snapshot the application state once at phase entry — same
        // bytes must flow into both the CRC we broadcast and any later
        // consistency check within this handler.
        let (app_len, app_buf) = serialize_app_data(&self.app_state.snapshot());
        let real_crc = self.state.compute_system_state_crc(app_len, &app_buf);
        let fake = self.should_fake_crc();
        let own_crc_on_wire = fake.unwrap_or(real_crc);
        if fake.is_some() {
            warn!(real_crc, own_crc_on_wire, "injection: sending fake crc");
        }

        let suppress = self.is_muted() || self.should_drop_crc();
        if suppress {
            warn!("injection: suppressing crc send");
        }

        let node_state = self.state.node_state();
        let deadline = self.cycle_anchor() + self.timing.crc_offset;

        let outcome = self.collect_phase(
            "system_state_crc",
            deadline,
            self.timing.send_interval,
            |this| {
                if suppress {
                    return Ok(());
                }
                if let Err(e) = this.send_frame(
                    node_state,
                    Payload::SystemStateCrc {
                        crc: own_crc_on_wire,
                    },
                ) {
                    error!(error = ?e, "send_system_state_crc failed");
                }
                Ok(())
            },
            |this| this.state.healthy_peers_missing_crc().is_empty() || this.peer_in_error(),
            |this, frame| {
                if let Payload::SystemStateCrc { crc } = frame.payload() {
                    let _ = this.state.record_peer_crc(frame.node_id(), crc);
                }
            },
        );

        if self.take_peer_in_error() {
            warn!("system_state_crc: peer already in ErrorManagement, rendezvous");
            self.state.attribute_input_missing();
            self.state.attribute_result_missing();
            self.fault_peers_missing_ack_unilateral();
            return StateEvent::PeerInError;
        }

        match outcome {
            super::PhaseOutcome::Complete => {
                if self.state.crc_unanimous(real_crc) {
                    return StateEvent::CrcOk;
                }
                // Divergenz: Mehrheit ueber own + peer CRCs ermitteln.
                match self.state.identify_crc_majority(real_crc) {
                    Some(majority_crc) => {
                        if real_crc == majority_crc {
                            // Wir sind in Majority — Minority-Peers zum
                            // Ausschluss proposen. EM erledigt den Rest.
                            let divergent = self.state.peers_with_crc_other_than(majority_crc);
                            for peer_id in divergent {
                                warn!(peer_id, "peer crc divergent, proposing exclude");
                                let _ = self.state.propose_exclude(peer_id);
                            }
                        } else {
                            // Alt: warn + StateEvent::CrcDivergent → EM → StateTimeout → Failsafe
                            warn!(
                                own_crc = real_crc,
                                majority_crc,
                                "own crc in minority, going straight to isolation"
                            );
                            return StateEvent::SelfExcluded;
                        }
                        StateEvent::CrcDivergent
                    }
                    None => {
                        error!(
                            real_crc,
                            "crc divergence without recoverable majority, failsafe"
                        );
                        self.mark_failsafe(FailsafeReason::StateDivergence);
                        StateEvent::Fault
                    }
                }
            }
            super::PhaseOutcome::Timeout => {
                error!("crc phase timeout, failsafe");
                self.mark_failsafe(FailsafeReason::QuorumLost);
                StateEvent::Fault
            }
            super::PhaseOutcome::Fault => {
                self.mark_failsafe(FailsafeReason::LocalFault);
                StateEvent::Fault
            }
        }
    }

    /// PublishResult: run the vote, publish the decision if we're the
    /// designated publisher, route dissenters through ErrorManagement.
    ///
    /// Failsafe conditions handled here:
    /// - Publisher-pick divergence across the fabric.
    /// - Own value dissents from consensus.
    /// - Sink evaluates the voted decision as unsafe (e.g. brake distance
    ///   exceeded). In this case the publisher pushes the decision to the
    ///   actuator one last time so the physical safe reaction (emergency
    ///   brake) actually engages, then the node faults.
    /// - Insufficient quorum on the vote.
    pub(super) fn handle_publish(&mut self) -> StateEvent {
        let outcome = self.state.run_vote();
        match outcome {
            VotingOutcome::Consensus(decision) => {
                let own_pick = self.pick_publisher_candidate();
                let publisher = match self.state.publisher_consensus(own_pick) {
                    Some(id) => id,
                    None => {
                        // Sammle alle picks (own + healthy peers) fuer das Bucketing.
                        let picks_with_ids: heapless::Vec<(u8, u8), MAX_TOTAL_NODES> = {
                            let mut v = heapless::Vec::new();
                            let _ = v.push((self.state.own_id(), own_pick));
                            for (idx, p) in self.state.peers().iter().enumerate() {
                                if p.health == PeerHealth::Lost {
                                    continue;
                                }
                                if let Some(ack) = self.state.cycle().peer_acks[idx] {
                                    let _ = v.push((p.id, ack.publisher_candidate));
                                }
                            }
                            v
                        };
                        let pick_values: heapless::Vec<u8, MAX_TOTAL_NODES> =
                            picks_with_ids.iter().map(|(_, pick)| *pick).collect();

                        match crate::framework::state::strict_majority(&pick_values) {
                            Some(majority_pick) => {
                                if own_pick == majority_pick {
                                    // Wir sind in Majority: propose alle Peers mit
                                    // abweichendem Pick.
                                    for (peer_id, pick) in picks_with_ids.iter() {
                                        if *peer_id == self.state.own_id() {
                                            continue;
                                        }
                                        if *pick != majority_pick {
                                            warn!(
                                                peer_id,
                                                peer_pick = *pick,
                                                majority_pick,
                                                "peer publisher pick divergent, proposing exclude"
                                            );
                                            let _ = self.state.propose_exclude(*peer_id);
                                        }
                                    }
                                    return StateEvent::DissenterDetected;
                                } else {
                                    // Own pick ist Minority — wir werden per EM-Detektor
                                    // isoliert werden.
                                    warn!(
                                        own_id = self.state.own_id(),
                                        own_pick,
                                        majority_pick,
                                        "own publisher pick in minority, expect isolation via EM"
                                    );
                                    return StateEvent::SelfExcluded;
                                }
                            }
                            None => {
                                // Keine Publisher-Mehrheit ermittelbar → echter
                                // Byzantine → Failsafe wie bisher.
                                error!(
                                    own_id = self.state.own_id(),
                                    own_pick,
                                    peer_picks = ?picks_with_ids,
                                    "publisher pick divergence without recoverable majority, failsafe"
                                );
                                self.mark_failsafe(FailsafeReason::StateDivergence);
                                return StateEvent::Fault;
                            }
                        }
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
                        // Own value diverges from the majority consensus.
                        // We are the sole outlier against a healthy
                        // majority — self-quarantine via Isolation
                        // instead of broadcasting GoFailsafe, which
                        // would needlessly tear down a working cluster.
                        warn!(
                            own_id,
                            "own value dissented from consensus, going straight to isolation"
                        );
                        return StateEvent::SelfExcluded;
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

                // Domain safety gate. Runs on every node, not only the
                // publisher, so all nodes agree on the failsafe transition
                // even before the GoFailsafe broadcast races through the
                // fabric.
                if self.sink.evaluate(&decision) == SinkVerdict::Failsafe {
                    error!(
                        own_id,
                        "sink rejected decision as unsafe, publishing then failsafe"
                    );
                    if publisher == own_id {
                        self.sink.publish(&decision);
                    }
                    self.mark_failsafe(FailsafeReason::SinkSafetyViolation);
                    return StateEvent::Fault;
                }

                if publisher == own_id {
                    info!(publisher, "consensus reached, publishing");
                    self.sink.publish(&decision);
                } else {
                    debug!(publisher, own_id, "consensus reached, peer publishes");
                }

                // Probation promotion is not ticked here any more: it is
                // derived from `current_seq` in `start_new_cycle`, so a
                // cycle that ends in ErrorManagement instead of here can
                // no longer desynchronise the counters across nodes.
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
                self.mark_failsafe(FailsafeReason::QuorumLost);
                StateEvent::Fault
            }
        }
    }

    /// ErrorManagement: exchange exclusion proposals, apply confirmed
    /// transitions, then decide on the next system state.
    ///
    /// Failsafe conditions:
    /// - Vote timed out with a healthy peer silent (Rule 2b) — tagged as
    ///   `QuorumLost` because a silent healthy peer is indistinguishable
    ///   from a lost one at this point.
    /// - Quorum lost after applying transitions.
    /// - Fabric now in degraded mode AND no peer evidence this cycle:
    ///   'we are blind' and 'peer is dead' are indistinguishable without a
    ///   third witness, both mean stop.
    pub(super) fn handle_error_management(&mut self) -> StateEvent {
        self.state.reset_exclusion_proposals();
        let _ = self.take_peer_in_error();

        let suppress = self.is_muted() || self.should_drop_vote();
        if suppress {
            warn!("injection: suppressing exclusion vote send");
        }

        let own_proposal = self.state.proposed_exclusions();
        let node_state = self.state.node_state();
        let deadline = std::time::Instant::now() + self.timing.error_mgmt_timeout;
        let voting_pool_before = 1 + self.state.active_peer_count_alive_only();
        let outcome = self.collect_phase(
            "exclusion_vote",
            deadline,
            self.timing.send_interval,
            |this| {
                if suppress {
                    return Ok(());
                }
                if let Err(e) = this.send_frame(
                    node_state,
                    Payload::ExclusionProposal {
                        propose_exclude: own_proposal,
                    },
                ) {
                    error!(error = ?e, "send_exclusion_proposal failed");
                }
                Ok(())
            },
            |this| this.state.healthy_peers_missing_vote().is_empty(),
            |this, frame| this.ingest_frame(frame),
        );

        let _ = self.take_peer_in_error();

        match outcome {
            super::PhaseOutcome::Complete => {
                let no_buffer_before_vote = self.state.tolerable_failures_remaining() == 0;

                if self.state.self_excluded_by_peers() {
                    warn!(
                        own_id = self.state.own_id(),
                        "peer majority excluded self, entering isolation"
                    );
                    // Keine confirmed exclusions anwenden — die peer roster
                    // aus unserer Sicht ist gleich, wir bewegen uns nur in
                    // Isolation. mark_failsafe NICHT setzen; Isolation ist
                    // kein Failsafe.
                    return StateEvent::SelfExcluded;
                }

                let confirmed = self.state.aggregate_exclusion_votes();
                let transitions = self.state.apply_confirmed_exclusions(confirmed);
                if transitions > 0 {
                    info!(
                        transitions,
                        confirmed = confirmed.as_u8(),
                        "health transitions"
                    );
                }

                self.state.start_new_cycle();
                if !self.state.quorum_available() {
                    self.mark_failsafe(FailsafeReason::QuorumLost);
                    return StateEvent::TooFewNodes;
                }
                
                let voting_pool_after = 1 + self.state.active_peer_count_alive_only();
                let voting_pool_shrank = voting_pool_after < voting_pool_before;
                if no_buffer_before_vote && voting_pool_shrank {
                     self.mark_failsafe(FailsafeReason::QuorumLost);
                     return StateEvent::TooFewNodes;
                }
                StateEvent::StateOk
            }
            super::PhaseOutcome::Timeout => {
                self.mark_failsafe(FailsafeReason::QuorumLost);
                StateEvent::StateTimeout
            }
            super::PhaseOutcome::Fault => {
                self.mark_failsafe(FailsafeReason::LocalFault);
                StateEvent::Fault
            }
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

    /// Sleep until the next cycle tick. On lateness the schedule skips
    /// forward by whole periods instead of sleeping.
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
                self.next_cycle_deadline =
                    Some(next_raster_point(deadline, now, self.timing.cycle_duration));
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

}
/// First raster point strictly after `now`, keeping the phase of the
/// original schedule.
///
/// The distinction matters after an overrun. Re-anchoring on `now`
/// would shift this node's raster against its peers permanently: their
/// in-cycle deadlines are offsets from their own anchors, so a node
/// whose anchor sits a few milliseconds off sends inside a window the
/// others have already closed, gets attributed as missing, and is voted
/// out of a fabric it is perfectly able to serve. Advancing by a single
/// period is no good either, because after a long stall the result can
/// still be in the past and the node then races through the backlog
/// without ever sleeping. Skipping whole periods keeps the phase and
/// lands in the future in one step.
fn next_raster_point(deadline: Instant, now: Instant, period: Duration) -> Instant {
    if now < deadline {
        return deadline + period;
    }
    let period_ns = period.as_nanos().max(1);
    let missed = ((now - deadline).as_nanos() / period_ns) + 1;
    let missed = u32::try_from(missed).unwrap_or(u32::MAX);
    deadline + period.saturating_mul(missed)
}

#[cfg(test)]
mod raster_tests {
    //! The cycle raster has to keep its phase across an overrun. A node
    //! whose anchor drifts against its peers falls outside their
    //! in-cycle windows and gets excluded even though it is healthy.
    use super::next_raster_point;
    use std::time::{Duration, Instant};

    const PERIOD: Duration = Duration::from_millis(20);

    #[test]
    fn on_time_advances_by_one_period() {
        let deadline = Instant::now();
        let now = deadline - Duration::from_millis(5);
        assert_eq!(next_raster_point(deadline, now, PERIOD), deadline + PERIOD);
    }

    #[test]
    fn small_overrun_keeps_the_original_phase() {
        // The exact case from the field log: a 6.2 ms overrun must not
        // move the raster by 6.2 ms.
        let deadline = Instant::now();
        let now = deadline + Duration::from_micros(6235);
        assert_eq!(next_raster_point(deadline, now, PERIOD), deadline + PERIOD);
    }

    #[test]
    fn long_stall_skips_whole_periods_and_lands_in_the_future() {
        let deadline = Instant::now();
        let now = deadline + Duration::from_millis(95); // 4.75 periods late
        let next = next_raster_point(deadline, now, PERIOD);
        assert_eq!(next, deadline + PERIOD * 5);
        assert!(next > now);
    }

    #[test]
    fn result_is_always_a_whole_number_of_periods_from_the_anchor() {
        let deadline = Instant::now();
        for late_us in [0u64, 1, 19_999, 20_000, 20_001, 250_000] {
            let now = deadline + Duration::from_micros(late_us);
            let next = next_raster_point(deadline, now, PERIOD);
            let offset = next.duration_since(deadline).as_nanos();
            assert_eq!(offset % PERIOD.as_nanos(), 0, "late_us={late_us}");
            assert!(next > now, "late_us={late_us}");
        }
    }
}
