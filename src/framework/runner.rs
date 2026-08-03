use serde::Deserialize;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::framework::diagnostic::{
    Command, Diagnostic, DiagnosticConfig, InjectionSnapshot, OutgoingTelegram,
    PeerStatus, StatusResponse,
};
use crate::framework::peer_sync::{PeerSync, SyncFields, extract_sync_fields};
use crate::framework::run_state::{AckInfo, FaultKind, PeerHealth, RunState};
use crate::framework::state_machine::{NodeState, StateEvent};
use crate::framework::traits::{Computation, DecisionSink, Voter, VotingOutcome};
use crate::framework::types::PeerMask;
use crate::framework::udp_frame::{Payload, UdpFrame};
use crate::framework::udp_transport::{RecvOutcome, UdpTransport, now_monotonic_ns};

enum PhaseOutcome {
    Complete,
    Timeout,
    Fault,
}

pub struct CycleTiming {
    pub cycle_duration: Duration,
    pub init_sync_timeout: Duration,
    pub peer_sync_timeout: Duration,
    pub peer_sync_request_interval: Duration,
    pub cycle_sync_timeout: Duration,
    pub share_timeout: Duration,
    pub ack_timeout: Duration,
    /// Zeitfenster fuer die Exclusion-Voting-Phase in ErrorManagement.
    /// Kurze Nachricht (1 Byte Mask) von jedem gesunden Peer — sollte
    /// in der Groessenordnung von `ack_timeout` liegen. Ueberschreitung
    /// mit gesundem Silence triggert Regel 2 (b): Failsafe.
    pub error_management_vote_timeout: Duration,
    pub stale_threshold: Duration,
    pub resync_interval_cycles: u32,
}

pub struct Runner<C, V, S>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
{
    state: RunState<V>,
    transport: UdpTransport<V::Payload>,
    computation: C,
    input: C::Input,
    sink: S,
    timing: CycleTiming,
    last_cycle_start: Option<Instant>,
    last_cycle_us: Option<u128>,
    next_cycle_deadline: Option<Instant>,
    cycles_since_last_sync: u32,

    diagnostic: Option<Diagnostic>,
}

impl<C, V, S> Runner<C, V, S>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    C::Input: for<'de> Deserialize<'de>,
{
    pub fn new(
        state: RunState<V>,
        transport: UdpTransport<V::Payload>,
        computation: C,
        input: C::Input,
        sink: S,
        timing: CycleTiming,
        diag_cfg: DiagnosticConfig,
    ) -> Self {
        info!(own_id = state.own_id(), "Runner constructed");

        let diagnostic = if diag_cfg.enabled {
            match Diagnostic::new(&diag_cfg, state.own_id()) {
                Ok(d) => Some(d),
                Err(e) => {
                    error!(
                        error = ?e,
                        "failed to init diagnostic interface, continuing without"
                    );
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
            timing,
            last_cycle_start: None,
            last_cycle_us: None,
            next_cycle_deadline: None,
            cycles_since_last_sync: 0,
            diagnostic,
        }
    }

    pub fn set_input(&mut self, input: C::Input) {
        debug!("input updated directly (bypassing staging)");
        self.input = input;
    }

    pub fn run(&mut self) {
        info!(own_id = self.state.own_id(), "runner loop starting");
        loop {
            let current = self.state.node_state();
            debug!(state = ?current, seq = self.state.current_seq(), "entering state");

            let event = match current {
                NodeState::Startup => self.handle_startup(),
                NodeState::InitSync => self.handle_init_sync(),
                NodeState::PeerSync => self.handle_peer_sync(),
                NodeState::CycleSync => self.handle_cycle_sync(),
                NodeState::ReadInputs => self.handle_read_inputs(),
                NodeState::ShareResult => self.handle_share_result(),
                NodeState::SendACK => self.handle_send_ack(),
                NodeState::PublishResult => self.handle_publish(),
                NodeState::ErrorManagement => self.handle_error_management(),
                NodeState::Isolation => self.handle_isolation(),
                NodeState::Failsafe => {
                    self.enter_failsafe();
                    return;
                }
            };

            self.poll_diagnostic();

            let next = current.next(event);
            info!(from = ?current, event = ?event, to = ?next, "state transition");
            self.state.set_node_state(next);
        }
    }

    // -------------------------------------------------------------
    // Diagnose
    // -------------------------------------------------------------

    fn poll_diagnostic(&mut self) {
        let Some(mut diag) = self.diagnostic.take() else {
            return;
        };

        while let Some(immediate) = diag.try_recv() {
            match immediate {
                Command::GetStatus => {
                    let status = self.build_status_response(&diag);
                    diag.send(&OutgoingTelegram::Status {
                        source_node_id: diag.node_id(),
                        data: status,
                    });
                }
                _ => {
                    debug!("unexpected immediate command returned from diagnostic");
                }
            }
        }

        self.diagnostic = Some(diag);
    }

    fn apply_pending_diagnostic(&mut self) {
        let Some(diag) = self.diagnostic.as_mut() else {
            return;
        };

        if let Some(json) = diag.take_pending_input() {
            match serde_json::from_value::<C::Input>(json) {
                Ok(new_input) => {
                    info!("applying staged input at cycle start");
                    self.input = new_input;
                }
                Err(e) => {
                    warn!(
                        error = ?e,
                        "failed to deserialize staged input, keeping previous"
                    );
                }
            }
        }

        diag.apply_pending_injection();
    }

    fn build_status_response(&self, diag: &Diagnostic) -> StatusResponse {
        StatusResponse {
            node_id: self.state.own_id(),
            session_id: self.state.session_id(),
            node_state: format!("{:?}", self.state.node_state()),
            current_seq: self.state.current_seq(),
            last_cycle_us: self.last_cycle_us,
            sync_valid: self.state.sync_valid(),
            sync_epsilon_ns: self.state.sync_epsilon_ns(),
            cycles_since_last_sync: self.cycles_since_last_sync,
            peers: self
                .state
                .peers()
                .iter()
                .map(|p| PeerStatus {
                    id: p.id,
                    health: format!("{:?}", p.health),
                    consecutive_faults: p.consecutive_faults,
                    consecutive_healthy_cycles: p.consecutive_healthy_cycles,
                })
                .collect(),
            injection: InjectionSnapshot {
                drop_next_n_results: diag.injection.drop_next_n_results,
                drop_next_n_acks: diag.injection.drop_next_n_acks,
            },
            pending_input: diag.pending.has_input(),
            pending_injection_update: diag.pending.has_injection_update(),
        }
    }

    // -------------------------------------------------------------
    // State-Handler
    // -------------------------------------------------------------

    fn handle_startup(&mut self) -> StateEvent {
        StateEvent::SelfTestOK
    }

    fn handle_isolation(&mut self) -> StateEvent {
        loop {
            warn!("isolation state entered, waiting for manual intervention");
            sleep(Duration::from_secs(1));
            self.poll_diagnostic();
        }
    }

    fn handle_init_sync(&mut self) -> StateEvent {
        info!(
            expected = self.state.participants().nominal_participants,
            "Init entered — waiting for full peer set"
        );
        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.init_sync_timeout;

        let outcome = self.collect_phase(
            "init_sync",
            deadline,
            Duration::from_millis(10),
            |this| {
                // InitSync hat noch keine relevante Beobachtungs-Mask.
                if let Err(e) = this.transport.send_state(node_state, PeerMask::EMPTY) {
                    error!(error = ?e, "send_state failed in init sync");
                    return Err(());
                }
                Ok(())
            },
            |this| this.discovery_complete(),
            |this, frame| {
                info!(peer_id = frame.node_id(), "init sync frame received");
                let _ = this.state.on_peer_discovered(frame.node_id());
            },
        );

        match outcome {
            PhaseOutcome::Complete => {
                if let Err(e) = self.state.finalize_discovery() {
                    error!(error = ?e, "finalize_discovery failed");
                    return StateEvent::SelfTestErr;
                }
                self.state.start_new_cycle(self.next_cycle_tick());
                StateEvent::InitialSyncOk
            }
            PhaseOutcome::Timeout => {
                warn!(
                    peers_found = self.state.peers().len(),
                    expected = self.state.participants().nominal_participants,
                    "discovery window elapsed without full peer set"
                );
                StateEvent::InitialSyncTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_peer_sync(&mut self) -> StateEvent {
        debug!(
            cycles_since_last_sync = self.cycles_since_last_sync,
            "handle_peer_sync entered"
        );

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
            error!("peer_sync entered without active peers");
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
                    clock_count = clocks.len(),
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
                let with_samples = peer_sync.finalize().len();
                warn!(
                    peers_with_samples = with_samples,
                    peers_total = peer_ids.len(),
                    target_samples_per_peer =
                        crate::framework::peer_sync::SAMPLES_PER_PEER,
                    "peer sync deadline exceeded"
                );
                return StateEvent::PeerSyncTimeout;
            }

            if Instant::now() >= next_request {
                let any_needs_sync =
                    peer_ids.iter().any(|&id| !peer_sync.has_pending(id));
                if any_needs_sync {
                    match self.transport.send_time_sync_req(node_state) {
                        Ok((_seq, t1)) => {
                            for &peer_id in &peer_ids {
                                if !peer_sync.has_pending(peer_id) {
                                    peer_sync.record_outgoing_request(peer_id, t1);
                                }
                            }
                            next_request =
                                Instant::now() + self.timing.peer_sync_request_interval;
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
                        if let Err(e) =
                            self.transport.send_time_sync_resp(node_state, t1, t2_local)
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
                },
                _ => {}
            }
        }
    }

    fn handle_cycle_sync(&mut self) -> StateEvent {
        debug!("handle_cycle_sync entered");

        // Distributed Observation zuruecksetzen: eigene Sicht + alle
        // gemeldeten Peer-Masken der letzten Runde.
        self.state.reset_cycle_sync_evidence();

        let node_state = self.state.node_state();
        let expected_mask = self.state.expected_sync_mask();
        let deadline = Instant::now() + self.timing.cycle_sync_timeout;

        let outcome = self.collect_phase(
            "cycle_sync",
            deadline,
            Duration::from_millis(1),
            |this| {
                // Bei jedem Resend die AKTUELLE own_seen_mask piggybacken.
                // Die entwickelt sich waehrend der Phase mit — Peers sehen
                // beim naechsten Resend eine aktuellere Sicht.
                let mask = this.state.own_seen_mask();
                if let Err(e) = this.transport.send_state(node_state, mask) {
                    error!(error = ?e, "send_state failed in cycle sync");
                    return Err(());
                }
                Ok(())
            },
            |this| this.state.own_seen_mask().as_u8() == expected_mask,
            |this, frame| {
                let peer_id = frame.node_id();
                match frame.payload() {
                    Payload::State { seen_mask } => {
                        if let Some(idx) = this.state.peer_index(peer_id) {
                            this.state.set_own_seen_bit(idx);
                            let _ = this.state.record_peer_seen_mask(peer_id, seen_mask);
                            debug!(
                                peer_id,
                                seen_mask = seen_mask.as_u8(),
                                "peer reached CycleSync"
                            );
                        }
                    }
                    _ => {
                        debug!(peer_id, "non-state frame during cycle sync, dropped");
                    }
                }
            },
        );

        // Cross-Observed-Evidenz auswerten — sowohl auf Complete- als
        // auch Timeout-Pfad. Faultet Peers nur bei Mehrheitsbeschluss.
        self.state.aggregate_cycle_sync_evidence();

        match outcome {
            PhaseOutcome::Complete => {
                self.state.start_new_cycle(self.next_cycle_tick());
                StateEvent::CycleSyncOk
            }
            PhaseOutcome::Timeout => {
                warn!(
                    got_mask = self.state.own_seen_mask().as_u8(),
                    expected_mask,
                    "cycle sync deadline exceeded"
                );
                StateEvent::CycleSyncTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_read_inputs(&mut self) -> StateEvent {
        self.apply_pending_diagnostic();

        self.wait_for_next_cycle_tick();

        let now = Instant::now();
        if let Some(prev) = self.last_cycle_start {
            let elapsed = now.duration_since(prev);
            self.last_cycle_us = Some(elapsed.as_micros());
            info!(cycle_us = elapsed.as_micros(), "cycle duration");
        }
        self.last_cycle_start = Some(now);

        debug!("reading inputs and computing payload");
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

    fn handle_share_result(&mut self) -> StateEvent {
        let suppress_send = if let Some(diag) = self.diagnostic.as_mut() {
            diag.should_drop_result()
        } else {
            false
        };
        if suppress_send {
            warn!("injection: suppressing send_result this cycle");
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
            |this, frame| this.ingest_frame(frame),
        );

        match outcome {
            PhaseOutcome::Complete => StateEvent::ResultShared,
            PhaseOutcome::Timeout => {
                // Ohne Peer-Acks (die kommen erst in SendACK) faellt
                // aggregate_result_evidence auf unilateral zurueck —
                // aequivalent zum alten fault_peers_missing_result.
                self.state.aggregate_result_evidence();
                StateEvent::ShareResultTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_send_ack(&mut self) -> StateEvent {
        let suppress_send = if let Some(diag) = self.diagnostic.as_mut() {
            diag.should_drop_ack()
        } else {
            false
        };
        if suppress_send {
            warn!("injection: suppressing send_ack this cycle");
        }

        let mask = self.received_mask();
        let candidate = self.pick_publisher_candidate();
        let node_state = self.state.node_state();
        debug!(mask = mask.as_u8(), candidate, "sending ack");

        let deadline = Instant::now() + self.timing.ack_timeout;

        let outcome = self.collect_phase(
            "send_ack",
            deadline,
            Duration::from_millis(1),
            |this| {
                if suppress_send {
                    return Ok(());
                }
                if let Err(e) = this.transport.send_ack(node_state, mask, candidate) {
                    error!(error = ?e, "send_ack failed");
                }
                Ok(())
            },
            |this| this.all_peer_acks_in(),
            |this, frame| this.ingest_frame(frame),
        );

        match outcome {
            PhaseOutcome::Complete => {
                // Alle Acks eingesammelt → volle Cross-Observed-Evidenz.
                self.state.aggregate_result_evidence();
                StateEvent::AckReceived
            }
            PhaseOutcome::Timeout => {
                // Partial Acks: aggregate mit dem was wir haben.
                self.state.aggregate_result_evidence();
                // MissedAck bleibt unilateral (siehe Doc unten).
                self.fault_peers_missing_ack_unilateral();
                StateEvent::AckTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_publish(&mut self) -> StateEvent {
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
                        error!(
                            own_id = self.state.own_id(),
                            "own value dissented from consensus, self-isolating to failsafe"
                        );
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
                        let own_id = self.state.own_id();
                        if publisher == own_id {
                            warn!(
                                publisher,
                                "Consensus with dissenters — publishing before reconfig"
                            );
                            self.sink.publish(&decision);
                        }

                        for peer_id in dissenter_ids {
                            warn!(peer_id, "peer value diverged from consensus");
                            let _ = self
                                .state
                                .record_peer_fault(peer_id, FaultKind::ValueDivergence);
                        }
                        return StateEvent::DissenterDetected;
                    }
                }

                let publisher = self.pick_publisher_candidate();
                let own_id = self.state.own_id();
                if publisher == own_id {
                    warn!(publisher, "Consensus reached — publishing as designated publisher");
                    self.sink.publish(&decision);
                } else {
                    debug!(
                        publisher,
                        own_id, "Consensus reached — peer is publisher"
                    );
                }

                // Cross-observed credit: nur Peers, die die Mehrheit als
                // liefernd gesehen hat, bekommen einen Healthy-Cycle.
                // Ersetzt den frueheren unilateralen credit_peers_delivered.
                self.state.credit_majority_delivered_peers();

                self.cycles_since_last_sync =
                    self.cycles_since_last_sync.saturating_add(1);

                if self.cycles_since_last_sync >= self.timing.resync_interval_cycles {
                    info!(
                        cycles = self.cycles_since_last_sync,
                        interval = self.timing.resync_interval_cycles,
                        "resync interval reached, triggering time sync refresh"
                    );
                    StateEvent::ResyncDue
                } else {
                    StateEvent::ResultPublished
                }
            }
            VotingOutcome::Disagreement => {
                warn!("vote resulted in disagreement (no majority)");
                StateEvent::StateDiverged
            }
            VotingOutcome::InsufficientQuorum => {
                error!(
                    "Number of peer results {}",
                    self.state.cycle().peer_results.len()
                );
                error!("insufficient quorum during publish");
                StateEvent::Fault
            }
        }
    }

   fn handle_error_management(&mut self) -> StateEvent {
    debug!("handle_error_management entered");

    // === Voting-Phase: Exclusion-Proposals austauschen ===
    //
    // Kernprinzip: kein Node wird durch einseitige Entscheidung
    // ausgeschlossen. Jeder Node sendet seine lokale Empfehlung
    // (proposed_exclusions basierend auf Fault-Countern), sammelt
    // die Empfehlungen der anderen ein, und wendet nur die durch
    // Mehrheit bestaetigten Uebergaenge an.
    self.state.reset_exclusion_proposals();

    let own_proposal = self.state.proposed_exclusions();
    let node_state = self.state.node_state();
    let deadline =
        Instant::now() + self.timing.error_management_vote_timeout;

    debug!(
        own_proposal = own_proposal.as_u8(),
        "starting exclusion vote"
    );

    let outcome = self.collect_phase(
        "exclusion_vote",
        deadline,
        Duration::from_millis(1),
        |this| {
            if let Err(e) = this
                .transport
                .send_exclusion_proposal(node_state, own_proposal)
            {
                error!(error = ?e, "send_exclusion_proposal failed");
                return Err(());
            }
            Ok(())
        },
        // Done wenn alle gesunden (nicht-angeklagten) Peers geantwortet
        // haben. Silence eines Peers, den WIR selbst ausschliessen
        // wollen, wird nicht erwartet — das ist bestaetigende Evidenz.
        |this| this.state.healthy_peers_missing_vote().is_empty(),
        |this, frame| this.ingest_frame(frame),
    );

    match outcome {
        PhaseOutcome::Complete => {
            let confirmed = self.state.aggregate_exclusion_votes();
            let transitions = self.state.apply_confirmed_exclusions(confirmed);
            if transitions > 0 {
                info!(
                    transitions,
                    confirmed = confirmed.as_u8(),
                    active_peers = self.state.active_peer_count(),
                    "vote-confirmed health transitions applied"
                );
            }

            let next_deadline = self.next_cycle_tick();
            self.state.start_new_cycle(next_deadline);

            if !self.state.quorum_available() {
                error!(
                    active_peers = self.state.active_peer_count(),
                    min_participants = self.state.participants().min_participants,
                    "quorum lost after vote, transitioning to failsafe"
                );
                return StateEvent::TooFewNodes;
            }

            // Degraded 2oo2 + keine Peer-Evidence in dieser Runde:
            // kein dritter Node mehr zum Cross-Check. "Ich bin blind"
            // und "der andere ist tot" sind aequivalent — beides fuehrt
            // zu Failsafe. Wird hier gepruefft (nicht in den Phase-
            // Handlern), damit die gesamte Failsafe-Logik an einem Ort
            // bleibt.
            if self.state.in_fail_safe_mode()
                && !self.any_peer_evidence_this_cycle()
            {
                error!(
                    active_peers = self.state.active_peer_count(),
                    required_agreement = self.state.required_agreement(),
                    "degraded mode with no peer evidence this cycle, failsafe"
                );
                return StateEvent::TooFewNodes;
            }

            // Kein Puffer mehr fuer weitere Ausfaelle — nur Warnung,
            // solange noch Kontakt zum letzten Peer besteht.
            if self.state.in_fail_safe_mode() {
                warn!(
                    active_peers = self.state.active_peer_count(),
                    required_agreement = self.state.required_agreement(),
                    "operating in degraded mode without fault tolerance buffer"
                );
            }

            StateEvent::StateOk
        }
        PhaseOutcome::Timeout => {
            // Regel 2 (b): mindestens ein Peer, den wir NICHT ausschliessen
            // wollen (= fuer uns gesund) hat nicht gevoted. Wenn die
            // gesunden Nodes untereinander die Kommunikation verlieren,
            // ist das System nicht mehr verlaesslich → Failsafe.
            let missing = self.state.healthy_peers_missing_vote();
            error!(
                missing_peers = ?missing.as_slice(),
                "exclusion vote timed out — healthy peer(s) silent, failsafe"
            );
            StateEvent::StateTimeout
        }
        PhaseOutcome::Fault => StateEvent::Fault,
    }
}


    fn any_peer_evidence_this_cycle(&self) -> bool {
    let c = self.state.cycle();
    c.peer_results.iter().any(|r| r.is_some())
        || c.peer_acks.iter().any(|a| a.is_some())
        || !self.state.own_seen_mask().is_empty()
}

    fn enter_failsafe(&mut self) {
        error!(
            own_id = self.state.own_id(),
            "entering failsafe — emergency brake"
        );
    }

    // -------------------------------------------------------------
    // Fehler-Buchhaltung (verbleibender unilateraler Pfad)
    // -------------------------------------------------------------

    /// MissedAck ist der einzige Fault-Kanal, der unilateral bleibt.
    /// Cross-Observation ueber Acks selbst ist nicht moeglich: kein Peer
    /// bezeugt in seinem Ack, wessen ACK er bekommen hat — das waere ein
    /// weiterer Round-Trip, den wir nicht bezahlen wollen.
    ///
    /// Der Voting-Layer im ErrorManagement gleicht das aus: selbst ein
    /// unilateral akkumulierter Counter fuehrt nur nach Mehrheitsbeschluss
    /// zum tatsaechlichen Ausschluss.
    ///
    /// **Self-Diagnose-Guard**: Wenn KEIN Peer-Ack angekommen ist, koennte
    /// unser eigener Inbound das Problem sein — dann keine Attribution.
    /// Nur wenn wir mindestens ein Ack gesehen haben, faulten wir die
    /// spezifisch fehlenden.
    fn fault_peers_missing_ack_unilateral(&mut self) {
        let any_ack_received = self
            .state
            .cycle()
            .peer_acks
            .iter()
            .any(|a| a.is_some());
        if !any_ack_received {
            warn!(
                "fault_peers_missing_ack_unilateral: no acks received \
                 (own inbound may be broken), skipping fault attribution"
            );
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
            let _ = self.state.record_peer_fault(peer_id, FaultKind::MissedAck);
        }
    }

    // -------------------------------------------------------------
    // Hilfsroutinen
    // -------------------------------------------------------------

    fn discovery_complete(&self) -> bool {
        let found = self.state.peers().len() as u8 + 1;
        found == self.state.participants().nominal_participants
    }

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
                    let overrun = now - deadline;
                    warn!(
                        overrun_us = overrun.as_micros(),
                        "cycle overrun, no sleep"
                    );
                }
                self.next_cycle_deadline = Some(deadline + self.timing.cycle_duration);
            }
        }
    }

    fn frame_age_if_stale(&self, frame: &UdpFrame<V::Payload>) -> Option<u64> {
        if !self.state.sync_valid() {
            return None;
        }
        let peer_id = frame.node_id();
        let local_send = self.state.peer_ts_to_local(peer_id, frame.timestamp())?;
        let now = now_monotonic_ns();
        let age = now.saturating_sub(local_send);
        let threshold_ns = self.timing.stale_threshold.as_nanos() as u64;
        if age > threshold_ns {
            Some(age)
        } else {
            None
        }
    }

    fn ingest_frame(&mut self, frame: UdpFrame<V::Payload>) {
        let peer_id = frame.node_id();

        // Nach abgeschlossener Discovery ist das Peer-Set gefroren.
        // Frames von unbekannten Peers werden abgewiesen — verhindert
        // dass sich fremde Nodes ins System einschleichen koennen.
        if self.state.discovery_locked() && self.state.peer_index(peer_id).is_none() {
            warn!(peer_id, "frame from unknown peer after discovery lock — dropped");
            return;
        }

        // Lost-Peers werden komplett ignoriert.
        if let Some(idx) = self.state.peer_index(peer_id) {
            if self.state.peers()[idx].health == PeerHealth::Lost {
                debug!(peer_id, "frame from lost peer dropped");
                return;
            }
        }

        if let Some(age) = self.frame_age_if_stale(&frame) {
            let threshold = self.timing.stale_threshold.as_nanos() as u64;
            debug!(
                peer_id,
                age_ns = age,
                threshold_ns = threshold,
                "stale frame dropped"
            );
            // Bewusst kein record_peer_fault: Stale-Frames sind ambigu.
            return;
        }

        let payload = frame.payload();
        match payload {
            Payload::Result(value) => {
                debug!(peer_id, "matched Result arm");
                if let Err(e) = self.state.record_peer_result(peer_id, value) {
                    warn!(peer_id, error = ?e, "could not record peer result");
                }
            }
            Payload::Ack {
                received_from,
                publisher_candidate,
            } => {
                debug!(peer_id, "matched Ack arm");
                let ack = AckInfo {
                    received_from: received_from.as_u8(),
                    publisher_candidate,
                };
                if let Err(e) = self.state.record_peer_ack(peer_id, ack) {
                    warn!(peer_id, error = ?e, "could not record peer ack");
                }
            }
            Payload::ExclusionProposal { propose_exclude } => {
                debug!(
                    peer_id,
                    propose_exclude = propose_exclude.as_u8(),
                    "matched ExclusionProposal arm"
                );
                if let Err(e) = self
                    .state
                    .record_peer_exclusion_proposal(peer_id, propose_exclude)
                {
                    warn!(peer_id, error = ?e, "could not record exclusion proposal");
                }
            }
            Payload::State { .. } => {
                // State ausserhalb von CycleSync ist Rauschen — CycleSync
                // hat einen eigenen Ingest-Closure, der State frisst.
                debug!(peer_id, "matched State arm (outside cycle sync, dropped)");
            }
            Payload::TimeSyncReq { .. } | Payload::TimeSyncResp { .. } => {
                debug!(peer_id, "unexpected sync frame outside PeerSync, dropped");
            }
        }
    }

    /// True wenn alle NICHT-Lost Peers ein Result geliefert haben.
    /// Lost-Peers werden ignoriert — sie senden nichts, ihre Slots
    /// blieben ohne diesen Filter dauerhaft None und die Phase liefe
    /// stets in Timeout.
    fn all_peer_results_in(&self) -> bool {
        for (idx, peer) in self.state.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if self.state.cycle().peer_results[idx].is_none() {
                return false;
            }
        }
        true
    }

    /// True wenn alle NICHT-Lost Peers ein Ack geliefert haben.
    /// Filter-Logik analog zu `all_peer_results_in`.
    fn all_peer_acks_in(&self) -> bool {
        for (idx, peer) in self.state.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if self.state.cycle().peer_acks[idx].is_none() {
                return false;
            }
        }
        true
    }

    /// Reale Bitmask der in dieser Runde lokal empfangenen Peer-Results.
    /// Wird in den Ack-Frame gepackt, damit die anderen Nodes darueber
    /// aggregieren koennen (siehe aggregate_result_evidence).
    fn received_mask(&self) -> PeerMask {
        let mut mask = PeerMask::EMPTY;
        for (idx, slot) in self.state.cycle().peer_results.iter().enumerate() {
            if slot.is_some() {
                mask.set(idx);
            }
        }
        mask
    }

    fn pick_publisher_candidate(&self) -> u8 {
        self.state.lowest_alive_id()
    }

    fn next_cycle_tick(&self) -> u64 {
        0
    }

    fn collect_phase<Snd, Done, Ing>(
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
                    error!(phase, "send failed hard, aborting phase");
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
                RecvOutcome::SeqGap {
                    peer_id,
                    gap,
                    frame,
                } => {
                    warn!(peer_id, gap, "seq gap detected, advancing cursor");
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
                            "rejoin attempt from lost peer ignored"
                        );
                    } else {
                        warn!(
                            peer_id,
                            previous_session, new_session, "peer rebooted mid-phase"
                        );
                        self.transport.accept(&frame);
                        ingest(self, frame);
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