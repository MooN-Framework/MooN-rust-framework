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
                    self.state.set_needs_state_sync(true); // NEU: markiere für nächste Phase
                                                           // enter_self_probation() ENTFÄLLT — Snapshot setzt self_probation_remaining
                }
                self.state.clear_pending_rejoin();
                self.state.start_new_cycle(self.next_cycle_tick());
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
                self.state.start_new_cycle(self.next_cycle_tick());
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
                        clock_sync.on_response(peer_id, t1, t2, t3, t4_local);
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
            self.timing.send_interval,
            |this| {
                if is_receiver {
                    // Receiver: sobald wir Snapshots haben, wenden wir Mehrheit an
                    // und senden Ack. Vorher nichts.
                    if !this.state.sync_snapshots().is_empty() {
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
                            if let Err(e) = this
                                .transport
                                .send(node_state, Payload::SystemStateSnapshotAck { adopted_crc })
                            {
                                error!(error = ?e, "send_snapshot_ack failed");
                                return Err(());
                            }
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
                    // Fertig wenn wir Snapshots von allen non-Lost haben und
                    // (den Snapshot bereits angewandt haben, angezeigt durch
                    // needs_state_sync=false in apply-Erfolg — aber das setzen
                    // wir erst nach Handler-Abschluss). Alternative:
                    // Snapshot-Set komplett + kein Ausstand.
                    this.state.peers_missing_snapshot().is_empty()
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
                self.state.start_new_cycle(self.next_cycle_tick());
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
            |this| this.all_peer_inputs_in(),
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
                self.state.attribute_input_missing();
                return StateEvent::ShareInputsTimeout;
            }
            super::PhaseOutcome::Fault => return StateEvent::Fault,
            super::PhaseOutcome::Complete => {}
        }

        let mut agree_count: usize = 1; // own zaehlt sich mit
        let mut divergent_peers: heapless::Vec<u8, MAX_PEERS> = heapless::Vec::new();
        for (idx, slot) in self.state.peer_inputs().iter().enumerate() {
            if let Some(peer_input) = slot {
                if self.computation.inputs_agree(&own, peer_input) {
                    agree_count += 1;
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
            |this| this.all_peer_results_in(),
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
            |this| this.state.healthy_peers_missing_crc().is_empty(),
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

                self.state.start_new_cycle(self.next_cycle_tick());
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