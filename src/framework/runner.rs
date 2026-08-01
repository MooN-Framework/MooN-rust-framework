use log::Level::Warn;
use serde::Deserialize;
use std::cell::Cell;
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
    pub stale_threshold: Duration,
    pub resync_interval_cycles: u32,
}

pub struct Runner<C, V, S, const N: usize>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
{
    state: RunState<V, N>,
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

impl<C, V, S, const N: usize> Runner<C, V, S, N>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    // Fuer die Diagnose-Schnittstelle: Input muss aus JSON deserialisierbar sein.
    C::Input: for<'de> Deserialize<'de>,
{
    pub fn new(
        state: RunState<V, N>,
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

    /// Verarbeitet eingehende Diagnose-Telegramme. Kommandos die
    /// Zustand aendern werden in `diagnostic.pending` gestagt und beim
    /// naechsten `handle_read_inputs` wirksam. GetStatus wird sofort
    /// mit vollem Zustand beantwortet, weil der Runner Zugriff auf
    /// RunState braucht.
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
                    // Sollte nicht passieren — alle anderen Kommandos
                    // werden in diag.try_recv() bereits gestagt.
                    debug!("unexpected immediate command returned from diagnostic");
                }
            }
        }

        self.diagnostic = Some(diag);
    }

    /// Wendet gestagete Diagnose-Aenderungen zu Beginn eines neuen Zyklus
    /// an. Wird von `handle_read_inputs` VOR der Berechnung aufgerufen,
    /// damit alle Nodes die Aenderung im selben logischen Zyklus sehen.
    fn apply_pending_diagnostic(&mut self) {
        let Some(diag) = self.diagnostic.as_mut() else {
            return;
        };

        // Input-Update: opaque JSON gegen C::Input deserialisieren.
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

        // Injection-Updates: Zaehler in aktiven State uebertragen.
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
        info!("Init entered");
        let node_state = self.state.node_state();
        let deadline = Instant::now() + self.timing.init_sync_timeout;

        let outcome = self.collect_phase(
            "init_sync",
            deadline,
            Duration::from_millis(10),
            |this| {
                if let Err(e) = this.transport.send_state(node_state) {
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
                if self.state.finalize_discovery().is_err() {
                    error!("finalize_discovery failed");
                    return StateEvent::SelfTestErr;
                }
                self.state.start_new_cycle(self.next_cycle_tick());
                StateEvent::InitialSyncOk
            }
            PhaseOutcome::Timeout => {
                warn!(
                    peers_found = self.state.peers().len(),
                    "discovery window elapsed without enough peers"
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
                for &peer_id in &peer_ids {
                    if !peer_sync.has_pending(peer_id) {
                        match self.transport.send_time_sync_req(node_state) {
                            Ok((_seq, t1)) => {
                                peer_sync.record_outgoing_request(peer_id, t1);
                                next_request =
                                    Instant::now() + self.timing.peer_sync_request_interval;
                                break;
                            }
                            Err(e) => {
                                error!(error = ?e, "send_time_sync_req failed");
                                return StateEvent::Fault;
                            }
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

        let node_state = self.state.node_state();
        let expected_mask = self.state.expected_sync_mask();
        let peers_synced = Cell::new(0u8);
        let deadline = Instant::now() + self.timing.cycle_sync_timeout;

        let outcome = self.collect_phase(
            "cycle_sync",
            deadline,
            self.timing.cycle_sync_timeout,
            |this| {
                if let Err(e) = this.transport.send_state(node_state) {
                    error!(error = ?e, "send_state failed in cycle sync");
                    return Err(());
                }
                Ok(())
            },
            |_this| peers_synced.get() == expected_mask,
            |this, frame| {
                let peer_id = frame.node_id();
                match frame.payload() {
                    Payload::State => {
                        if let Some(idx) = this.state.peer_index(peer_id) {
                            peers_synced.set(peers_synced.get() | (1 << idx));
                            debug!(peer_id, "peer reached CycleSync");
                        }
                    }
                    _ => {
                        debug!(peer_id, "non-state frame during cycle sync, dropped");
                    }
                }
            },
        );

        match outcome {
            PhaseOutcome::Complete => {
                self.state.start_new_cycle(self.next_cycle_tick());
                StateEvent::CycleSyncOk
            }
            PhaseOutcome::Timeout => {
                let synced = peers_synced.get();
                warn!(
                    got_mask = synced,
                    expected_mask, "cycle sync deadline exceeded"
                );
                self.fault_peers_missing_cycle_sync(synced);
                StateEvent::CycleSyncTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_read_inputs(&mut self) -> StateEvent {
        // Gestagete Diagnose-Aenderungen zum Zyklusstart anwenden.
        // Alle Nodes sind wegen CycleSync-Barrier hier zum selben
        // logischen Zeitpunkt — Aenderungen wirken damit synchron.
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
        // Injection: statt wirklich zu senden, wird das Senden fuer diesen
        // Zyklus unterdrueckt. collect_phase laeuft aber normal, damit wir
        // eingehende Frames weiter verarbeiten und im Zyklustakt bleiben.
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
            Duration::from_millis(10),
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
                self.fault_peers_missing_result();
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
            Duration::from_millis(10),
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
            PhaseOutcome::Complete => StateEvent::AckReceived,
            PhaseOutcome::Timeout => {
                self.fault_peers_missing_ack();
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

                self.credit_peers_delivered();

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

        let transitions = self.state.apply_health_transitions();
        if transitions > 0 {
            info!(
                transitions,
                active_peers = self.state.active_peer_count(),
                "health transitions applied"
            );
        }

        let next_deadline = self.next_cycle_tick();
        self.state.start_new_cycle(next_deadline);

        // Erst pruefen: haben wir noch das Minimum an aktiven Nodes?
        if !self.state.quorum_available() {
            error!(
                active_peers = self.state.active_peer_count(),
                required = self.state.voter().required_participants(),
                "quorum lost, transitioning to failsafe"
            );
            return StateEvent::TooFewNodes;
        }

        // Dann pruefen: haben wir noch Puffer? Regel:
        //   required_agreement = max(floor(N_aktiv/2)+1, required_participants)
        //   in_fail_safe_mode: active_total <= required_agreement
        //
        // Wenn ja: kein Puffer mehr, jeder Fault ist fatal (KooK-Regel:
        // 2oo2, degradiertes 2oo3, 6oo8 mit 2 Ausfaellen, etc.).
        if self.state.in_fail_safe_mode() {
            error!(
                active_peers = self.state.active_peer_count(),
                required_agreement = self.state.required_agreement(),
                required_participants = self.state.voter().required_participants(),
                "fail-safe mode: no tolerance buffer left, fault triggers failsafe"
            );
            return StateEvent::TooFewNodes;
        }

        StateEvent::StateOk
    }

    fn enter_failsafe(&mut self) {
        error!(
            own_id = self.state.own_id(),
            "entering failsafe — emergency brake"
        );
    }

    // -------------------------------------------------------------
    // Fehler-Buchhaltung
    // -------------------------------------------------------------

    fn fault_peers_missing_result(&mut self) {
        let missing_ids: Vec<u8> = self
            .state
            .peers()
            .iter()
            .enumerate()
            .filter_map(|(idx, p)| {
                if self.state.cycle().peer_results[idx].is_none() {
                    Some(p.id)
                } else {
                    None
                }
            })
            .collect();
        for peer_id in missing_ids {
            let _ = self
                .state
                .record_peer_fault(peer_id, FaultKind::MissedShareResult);
        }
    }

    fn fault_peers_missing_ack(&mut self) {
        let missing_ids: Vec<u8> = self
            .state
            .peers()
            .iter()
            .enumerate()
            .filter_map(|(idx, p)| {
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

    fn fault_peers_missing_cycle_sync(&mut self, synced_mask: u8) {
        let missing_ids: Vec<u8> = self
            .state
            .peers()
            .iter()
            .enumerate()
            .filter_map(|(idx, p)| {
                if (synced_mask & (1 << idx)) == 0 {
                    Some(p.id)
                } else {
                    None
                }
            })
            .collect();
        for peer_id in missing_ids {
            let _ = self
                .state
                .record_peer_fault(peer_id, FaultKind::MissedCycleSync);
        }
    }

    fn credit_peers_delivered(&mut self) {
        let delivered_ids: Vec<u8> = self
            .state
            .peers()
            .iter()
            .enumerate()
            .filter_map(|(idx, p)| {
                if self.state.cycle().peer_results[idx].is_some() {
                    Some(p.id)
                } else {
                    None
                }
            })
            .collect();
        for peer_id in delivered_ids {
            let _ = self.state.record_peer_healthy_cycle(peer_id);
        }
    }

    // -------------------------------------------------------------
    // Hilfsroutinen
    // -------------------------------------------------------------

    fn discovery_complete(&self) -> bool {
        (self.state.peers().len() as u8 + 1) >= self.state.voter().required_participants()
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

        if let Some(idx) = self.state.peer_index(peer_id) {
            if self.state.peers()[idx].health == PeerHealth::Lost {
                debug!(peer_id, "frame from lost peer dropped");
                return;
            }
        }

        if let Some(age) = self.frame_age_if_stale(&frame) {
            let threshold = self.timing.stale_threshold.as_nanos() as u64;
            warn!(
                peer_id,
                age_ns = age,
                threshold_ns = threshold,
                "stale frame dropped"
            );
            let _ = self.state.record_peer_fault(peer_id, FaultKind::StaleFrame);
            return;
        }

        let payload = frame.payload();
        match payload {
            Payload::Result(value) => {
                warn!(peer_id, "matched Result arm");
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
            Payload::State => {
                debug!(peer_id, "matched State arm");
            }
            Payload::TimeSyncReq { .. } | Payload::TimeSyncResp { .. } => {
                debug!(peer_id, "unexpected sync frame outside PeerSync, dropped");
            }
        }
    }

    fn all_peer_results_in(&self) -> bool {
        self.state.cycle().peer_results.iter().all(|r| r.is_some())
    }

    fn all_peer_acks_in(&self) -> bool {
        self.state.cycle().peer_acks.iter().all(|a| a.is_some())
    }

    fn received_mask(&self) -> PeerMask {
        PeerMask::from_u8(0)
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