use std::thread::sleep;
use std::time::{Duration, Instant};

use log::Level::Warn;
use tracing::{debug, error, info, warn};

use crate::framework::run_state::{AckInfo, RunState};
use crate::framework::state_machine::{NodeState, StateEvent};
use crate::framework::traits::{Computation, DecisionSink, Voter, VotingOutcome};
use crate::framework::types::PeerMask;
use crate::framework::udp_frame::{Payload, UdpFrame};
use crate::framework::udp_transport::{RecvOutcome, UdpTransport};

pub struct CycleTiming {
    pub cycle_duration: Duration,
    pub discovery_window: Duration, // z.B. 5s
    pub beacon_interval: Duration,  // z.B. 100ms
    pub share_timeout: Duration,
    pub ack_timeout: Duration,
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
                NodeState::Sync => self.handle_sync(),
                NodeState::ReadInputs => self.handle_read_inputs(),
                NodeState::ShareResult => self.handle_share_result(),
                NodeState::SendACK => self.handle_send_ack(),
                NodeState::PublishResult => self.handle_publish(),
                NodeState::StateManagement => self.handle_state_management(),
                NodeState::Probation => self.handle_probation(),
                NodeState::Failsafe => {
                    self.enter_failsafe();
                    return;
                }
            };

            let next = current.next(event);
            debug!(from = ?current, event = ?event, to = ?next, "state transition");
            self.state.set_node_state(next);
        }
    }

    // -------------------------------------------------------------
    // State-Handler
    // -------------------------------------------------------------

    fn handle_startup(&mut self) -> StateEvent {
        info!("handle_startup entered");
        match self.self_test() {
            Ok(()) => {
                info!("self_test returned Ok");
                StateEvent::SelfTestOK
            }
            Err(_) => {
                error!("self_test returned Err");
                StateEvent::SelfTestErr
            }
        }
    }

    fn handle_sync(&mut self) -> StateEvent {
        info!(
            discovery_window_ms = self.timing.discovery_window.as_millis() as u64,
            beacon_interval_ms = self.timing.beacon_interval.as_millis() as u64,
            "handle_sync entered"
        );
        let overall_deadline = Instant::now() + self.timing.discovery_window;
        let mut next_beacon = Instant::now();

        loop {
            // Beacon senden, wenn faellig.
            if Instant::now() >= next_beacon {
                match self.transport.send_state(NodeState::Sync) {
                    Ok(seq) => debug!(seq, "sync beacon sent"),
                    Err(e) => {
                        error!(error = ?e, "send_state failed in sync");
                        return StateEvent::Fault;
                    }
                }
                next_beacon = Instant::now() + self.timing.beacon_interval;
            }

            match self.transport.try_recv() {
                RecvOutcome::Valid(frame) => {
                    info!(peer_id = frame.node_id(), "sync frame received");
                    let _ = self.state.on_peer_discovered(frame.node_id());
                    if self.discovery_complete() {
                        info!("discovery threshold reached, finalizing");
                        if self.state.finalize_discovery().is_err() {
                            error!("finalize_discovery failed");
                            return StateEvent::SelfTestErr;
                        }
                        self.state.start_new_cycle(self.next_cycle_tick());
                        return StateEvent::CycleSyncOk;
                    }
                }
                RecvOutcome::Timeout => {
                    if Instant::now() >= overall_deadline {
                        warn!(
                            peers_found = self.state.peers().len(),
                            "discovery window elapsed without enough peers"
                        );
                        return StateEvent::CycleSyncTimeout;
                    }
                    // sonst weiter — naechster Beacon oder Recv-Zyklus
                }
                other => {
                    debug!(outcome = ?other, "ignored non-Valid outcome during sync");
                }
            }
        }
    }

    fn handle_read_inputs(&mut self) -> StateEvent {
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

        loop {
            if let Err(e) = self.transport.send_result(self.state.node_state(), own) {
                error!(error = ?e, "Failed to sent result msg failed");
            }

            if self.all_peer_results_in() {
                warn!("All peer results in.");
                return StateEvent::ResultShared;
            } else {
                warn!("Still not all in.");
            }

            if Instant::now() > deadline {
                warn!("Share result timeout.");
                return StateEvent::ShareResultTimeout;
            }

            match self.transport.try_recv() {
                RecvOutcome::Valid(frame) => {
                    warn!(
                        peer_id = frame.node_id(),
                        "frame received during share_result"
                    );
                    self.ingest_frame(frame);
                }
                RecvOutcome::Timeout => {
                    std::thread::sleep(Duration::from_micros(200));
                }
                other => {
                    warn!(outcome = ?other, "ignored during share_result");
                }
            }
            sleep(Duration::from_secs(1));
        }
    }

    fn handle_send_ack(&mut self) -> StateEvent {
        let mask = self.received_mask();
        let candidate = self.pick_publisher_candidate();
        debug!(mask = mask.as_u8(), candidate, "sending ack");

        let deadline = Instant::now() + self.timing.ack_timeout;

        loop {
            if let Err(e) = self
                .transport
                .send_ack(self.state.node_state(), mask, candidate)
            {
                error!(error = ?e, "send_ack failed");
            }

            if self.all_peer_acks_in() {
                warn!("All peer acks in.");
                return StateEvent::AckReceived;
            }

            if Instant::now() > deadline {
                return StateEvent::AckTimeout;
            }

            match self.transport.try_recv() {
                RecvOutcome::Valid(frame) => {
                    debug!(peer_id = frame.node_id(), "ack frame received");
                    self.ingest_frame(frame);
                }
                RecvOutcome::Timeout => {
                    std::thread::sleep(Duration::from_micros(200));
                }
                other => {
                    debug!(outcome = ?other, "ignored during send_ack");
                }
            }
            sleep(Duration::from_secs(1));
        }
    }

    fn handle_publish(&mut self) -> StateEvent {
        match self.state.run_vote() {
            VotingOutcome::Consensus(decision) => {
                warn!("Consensus reached, publishing decision");
                self.sink.publish(&decision);
                StateEvent::ResultPublished
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

    fn handle_state_management(&mut self) -> StateEvent {
        sleep(Duration::from_hours(1));
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

    fn handle_probation(&mut self) -> StateEvent {
        if self.probation_passed() {
            info!("probation passed");
            StateEvent::ProbationPassed
        } else {
            warn!("probation failed");
            StateEvent::ProbationFailed
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

    fn ingest_frame(&mut self, frame: UdpFrame<V::Payload>) {
        let peer_id = frame.node_id();
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
}
