use log::Level::Warn;
use std::cell::Cell;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::framework::peer_sync::{PeerSync, SyncFields, extract_sync_fields};
use crate::framework::run_state::{AckInfo, RunState};
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
    pub init_sync_timeout: Duration,          // z.B. 5s
    pub peer_sync_timeout: Duration,          // z.B. 500ms
    pub peer_sync_request_interval: Duration, // z.B. 2ms zwischen Sync-Requests
    pub cycle_sync_timeout: Duration,         // z.B. 20ms
    pub share_timeout: Duration,
    pub ack_timeout: Duration,

    /// Maximal zulaessiges Alter eines empfangenen Frames, bevor er als
    /// stale verworfen wird. Wird erst nach abgeschlossener Zeitsynchronisation
    /// ausgewertet.
    pub stale_threshold: Duration,

    /// Anzahl erfolgreicher Zyklen, nach denen periodisch resynchronisiert wird.
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

    /// Zeitpunkt, an dem der letzte Zyklus begonnen hat. Fuer Zyklusdauer-
    /// Messung (Diagnose).
    last_cycle_start: Option<Instant>,

    /// Absolute Deadline fuer den naechsten Zyklusstart. Wird ausschliesslich
    /// in `wait_for_next_cycle_tick` fortgeschrieben (Deadline + cycle_duration),
    /// damit Zyklen nicht driften: ueberzogene Zyklen fressen sich in die
    /// Reserve des naechsten Zyklus, gleichen aber nicht die absolute
    /// Ausrichtung an der Startzeit auf.
    next_cycle_deadline: Option<Instant>,

    /// Zaehler fuer die periodische Resync-Ausloesung. Wird nach jedem
    /// erfolgreichen Publish erhoeht, nach jedem erfolgreichen PeerSync
    /// auf 0 zurueckgesetzt.
    cycles_since_last_sync: u32,
}

impl<C, V, S, const N: usize> Runner<C, V, S, N>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
{
    pub fn new(
        state: RunState<V, N>,
        transport: UdpTransport<V::Payload>,
        computation: C,
        input: C::Input,
        sink: S,
        timing: CycleTiming,
    ) -> Self {
        info!(own_id = state.own_id(), "Runner constructed");
        Self {
            state,
            transport,
            computation,
            input,
            sink,
            timing,
            last_cycle_start: None,
            next_cycle_deadline: None,
            cycles_since_last_sync: 0,
        }
    }

    /// Vom Diagnose-Pfad aufzurufen, wenn per UDP neue Eingaben eintreffen.
    pub fn set_input(&mut self, input: C::Input) {
        debug!("input updated via diagnostic path");
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

            let next = current.next(event);
            info!(from = ?current, event = ?event, to = ?next, "state transition");
            self.state.set_node_state(next);
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

    /// Zeitsynchronisation zu allen entdeckten Peers nach Cristian.
    fn handle_peer_sync(&mut self) -> StateEvent {
        debug!(
            cycles_since_last_sync = self.cycles_since_last_sync,
            "handle_peer_sync entered"
        );

        if self.state.sync_valid() {
            self.state.invalidate_sync();
        }

        let peer_ids: Vec<u8> = self.state.peers().iter().map(|p| p.id).collect();
        if peer_ids.is_empty() {
            error!("peer_sync entered without discovered peers");
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

                // Nach dem Resync die Cycle-Deadline zuruecksetzen — sonst wuerden
                // wir versuchen, die verpasste Zeit im naechsten Zyklus aufzuholen,
                // was die Zyklusdauer verzerrt.
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
                warn!(
                    got_mask = peers_synced.get(),
                    expected_mask, "cycle sync deadline exceeded"
                );
                StateEvent::CycleSyncTimeout
            }
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_read_inputs(&mut self) -> StateEvent {
        // Auf naechsten Zyklus-Tick warten. Muss vor allem anderen passieren,
        // damit die gemessene Zyklusdauer die tatsaechliche Zykluszeit
        // widerspiegelt, nicht die Rechendauer eines einzelnen Durchlaufs.
        self.wait_for_next_cycle_tick();

        let now = Instant::now();
        if let Some(prev) = self.last_cycle_start {
            info!(
                cycle_us = now.duration_since(prev).as_micros(),
                "cycle duration"
            );
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
            PhaseOutcome::Timeout => StateEvent::ShareResultTimeout,
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_send_ack(&mut self) -> StateEvent {
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
            PhaseOutcome::Timeout => StateEvent::AckTimeout,
            PhaseOutcome::Fault => StateEvent::Fault,
        }
    }

    fn handle_publish(&mut self) -> StateEvent {
        match self.state.run_vote() {
            VotingOutcome::Consensus(decision) => {
                warn!("Consensus reached, publishing decision");
                self.sink.publish(&decision);

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
                warn!("vote resulted in disagreement");
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
        let next_deadline = self.next_cycle_tick();
        debug!(next_deadline, "starting next cycle");
        self.state.start_new_cycle(next_deadline);

        if self.any_peer_diverged() {
            warn!("peer divergence detected during state management");
            StateEvent::StateDiverged
        } else {
            StateEvent::StateOk
        }
    }

    fn enter_failsafe(&mut self) {
        error!(
            own_id = self.state.own_id(),
            "entering failsafe — emergency brake"
        );
    }

    // -------------------------------------------------------------
    // Hilfsroutinen
    // -------------------------------------------------------------

    fn self_test(&mut self) -> Result<(), ()> {
        Ok(())
    }

    fn discovery_complete(&self) -> bool {
        (self.state.peers().len() as u8 + 1) >= self.state.voter().required_participants()
    }

    /// Wartet bis zum naechsten geplanten Zyklusstart. Nutzt absolute
    /// Deadlines, damit sich Ueberziehungen nicht ueber viele Zyklen
    /// akkumulieren: die Deadline schreibt sich immer um genau `cycle_duration`
    /// weiter, unabhaengig davon, wie lange der letzte Zyklus wirklich
    /// gedauert hat.
    ///
    /// Beim ersten Aufruf gibt es noch keine Deadline — dann wird sie ab
    /// jetzt gesetzt und kein Sleep durchgefuehrt.
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

    /// Prueft, ob ein Frame als stale zu verwerfen ist.
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

        if let Some(age) = self.frame_age_if_stale(&frame) {
            let threshold = self.timing.stale_threshold.as_nanos() as u64;
            warn!(
                peer_id,
                age_ns = age,
                threshold_ns = threshold,
                "stale frame dropped"
            );
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
        self.state.own_id()
    }

    fn next_cycle_tick(&self) -> u64 {
        0
    }

    fn any_peer_diverged(&self) -> bool {
        false
    }

    fn probation_passed(&self) -> bool {
        true
    }

    /// Generischer „periodisch senden + empfangen bis Bedingung erfuellt" Loop.
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
                    warn!(
                        peer_id,
                        previous_session, new_session, "peer rebooted mid-phase"
                    );
                    self.transport.accept(&frame);
                    ingest(self, frame);
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