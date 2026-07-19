use std::time::{Duration, Instant};

use crate::framework::run_state::RunState;
use crate::framework::state_machine::{NodeState, StateEvent};
use crate::framework::traits::{Computation, DecisionSink, Voter, VotingOutcome};
use crate::framework::types::PeerMask;
use crate::framework::udp_frame::UdpFrame;
use crate::framework::udp_transport::{RecvOutcome, UdpTransport};

pub struct CycleTiming {
    pub cycle_duration: Duration,
    pub sync_timeout: Duration,
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
        self.input = input;
    }

    /// Blockierende Hauptschleife. Kehrt zurueck, sobald der Node in
    /// Failsafe geht.
    pub fn run(&mut self) {
        loop {
            let event = match self.state.node_state() {
                NodeState::Startup           => self.handle_startup(),
                NodeState::Sync              => self.handle_sync(),
                NodeState::ReadInputs        => self.handle_read_inputs(),
                NodeState::CalculateCritical => self.handle_calculate(),
                NodeState::ShareResult       => self.handle_share_result(),
                NodeState::SendACK           => self.handle_send_ack(),
                NodeState::PublishResult     => self.handle_publish(),
                NodeState::StateManagement   => self.handle_state_management(),
                NodeState::Probation         => self.handle_probation(),
                NodeState::Failsafe          => {
                    self.enter_failsafe();
                    return;
                }
            };

            let next = self.state.node_state().next(event);
            self.state.set_node_state(next);
        }
    }

    // -------------------------------------------------------------
    // State-Handler
    // -------------------------------------------------------------

    fn handle_startup(&mut self) -> StateEvent {
        match self.self_test() {
            Ok(()) => StateEvent::SelfTestOK,
            Err(_) => StateEvent::SelfTestErr,
        }
    }

    fn handle_sync(&mut self) -> StateEvent {
        let deadline = Instant::now() + self.timing.sync_timeout;
        loop {
            match self.transport.recv_before(deadline) {
                RecvOutcome::Valid(frame) => {
                    let _ = self.state.on_peer_discovered(frame.node_id());
                    if self.discovery_complete() {
                        if self.state.finalize_discovery().is_err() {
                            return StateEvent::SelfTestErr;
                        }
                        self.state.start_new_cycle(self.next_cycle_tick());
                        return StateEvent::CycleSyncOk;
                    }
                }
                RecvOutcome::Timeout => return StateEvent::CycleSyncTimeout,
                _ => {}
            }
        }
    }

    fn handle_read_inputs(&mut self) -> StateEvent {
        match self.computation.compute(self.input) {
            Ok(payload) => {
                self.state.record_own_result(payload);
                StateEvent::InputsRead
            }
            Err(_) => StateEvent::Fault,
        }
    }

    fn handle_calculate(&mut self) -> StateEvent {
        // Berechnung ist bereits in handle_read_inputs erfolgt.
        // Dieser State bleibt als expliziter Punkt in der State-Machine,
        // falls spaeter zusaetzliche Konsistenzchecks eingeschoben werden.
        StateEvent::CalculationDone
    }

    fn handle_share_result(&mut self) -> StateEvent {
        let own = match self.state.cycle().own_result {
            Some(r) => r,
            None => return StateEvent::Fault,
        };
        if self
            .transport
            .send_result(self.state.node_state(), own)
            .is_err()
        {
            return StateEvent::Fault;
        }

        let deadline = Instant::now() + self.timing.share_timeout;
        while !self.all_peer_results_in() {
            match self.transport.recv_before(deadline) {
                RecvOutcome::Valid(frame) => self.ingest_frame(frame),
                RecvOutcome::Timeout => break,
                _ => {}
            }
        }
        StateEvent::ResultShared
    }

    fn handle_send_ack(&mut self) -> StateEvent {
        let mask = self.received_mask();
        let candidate = self.pick_publisher_candidate();
        if self
            .transport
            .send_ack(self.state.node_state(), mask, candidate)
            .is_err()
        {
            return StateEvent::Fault;
        }

        let deadline = Instant::now() + self.timing.ack_timeout;
        while !self.all_peer_acks_in() {
            match self.transport.recv_before(deadline) {
                RecvOutcome::Valid(frame) => self.ingest_frame(frame),
                RecvOutcome::Timeout => break,
                _ => {}
            }
        }
        StateEvent::AckReceived
    }

    fn handle_publish(&mut self) -> StateEvent {
        match self.state.run_vote() {
            VotingOutcome::Consensus(decision) => {
                self.sink.publish(&decision);
                StateEvent::ResultPublished
            }
            VotingOutcome::Disagreement => StateEvent::StateDiverged,
            VotingOutcome::InsufficientQuorum => StateEvent::Fault,
        }
    }

    fn handle_state_management(&mut self) -> StateEvent {
        let next_deadline = self.next_cycle_tick();
        self.state.start_new_cycle(next_deadline);

        if self.any_peer_diverged() {
            StateEvent::StateDiverged
        } else {
            StateEvent::StateOk
        }
    }

    fn handle_probation(&mut self) -> StateEvent {
        if self.probation_passed() {
            StateEvent::ProbationPassed
        } else {
            StateEvent::ProbationFailed
        }
    }

    fn enter_failsafe(&mut self) {
        // Diagnose persistieren, Aktuatoren sicher setzen,
        // ausgehende Kommunikation stoppen. Terminal.
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

    fn ingest_frame(&mut self, _frame: UdpFrame<V::Payload>) {
        // Frame in state.cycle einsortieren (Result vs. Ack), Peer-Cursor
        // aktualisieren, Health nachfuehren.
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