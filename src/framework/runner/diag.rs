use crate::framework::diagnostic::{
    Command, Diagnostic, InjectionSnapshot, OutgoingTelegram, PeerStatus, StatusResponse,
    TransitionRecord,
};
use crate::framework::traits::{
    ApplicationStateProvider, Computation, DecisionSink, InputSource, SelfTest, Voter,
};
use serde::Deserialize;
use tracing::{error, info, warn};

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
    /// Drain diagnostic telegrams. GetStatus is answered directly; other
    /// commands are staged inside `Diagnostic`.
    pub(super) fn poll_diagnostic(&mut self) {
        // The diagnostic poll sits in the run loop after every phase
        // handler, so when it lands between ReadInputs and ShareInputs
        // its cost comes straight out of that phase's budget. Report it
        // when it gets expensive enough to matter, so a phase timeout
        // with a large `entry_offset_us` can be attributed instead of
        // guessed at. The threshold is a quarter of a send interval.
        let poll_started = std::time::Instant::now();
        let poll_budget = self.timing.send_interval / 4;
        let Some(mut diag) = self.diagnostic.take() else {
            return;
        };
        while let Some(cmd) = diag.try_recv() {
            if let Command::GetStatus = cmd {
                let status = self.build_status_response(&diag);

                diag.send(&OutgoingTelegram::Status {
                    source_node_id: diag.node_id(),
                    data: status,
                });
            }
        }
        self.diagnostic = Some(diag);

        let poll_elapsed = poll_started.elapsed();
        if poll_elapsed > poll_budget {
            warn!(
                poll_us = poll_elapsed.as_micros(),
                budget_us = poll_budget.as_micros(),
                "diagnostic poll consumed a noticeable share of the cycle"
            );
        }
    }

    /// Apply staged diagnostic input and injection updates at cycle start.
    /// Order matters:
    /// 1. Shutdown check — fires irrevocably before any further work.
    /// 2. Apply pending injection updates (drop counters, mute, delay).
    /// 3. Tick per-cycle effects (mute, cycle_delay) → store on the runner
    ///    so send paths and wait_for_next_cycle_tick can read them.
    /// 4. Consume staged input.
    pub(super) fn apply_pending_diagnostic(&mut self) {
        let Some(diag) = self.diagnostic.as_mut() else {
            return;
        };

        if diag.take_pending_shutdown() {
            error!(
                node_id = self.state.own_id(),
                "InjectShutdown fired at cycle boundary, terminating process"
            );
            std::process::exit(0);
        }

        diag.apply_pending_injection();

        // One-shot flags picked up from the staging area; the diag
        // struct only carries the *request* — the actual behaviour
        // sits on the runner because the injection points are inside
        // handle_read_inputs and handle_share_inputs.
        if core::mem::take(&mut diag.pending.inject_input_provider_fail) {
            self.inject_input_provider_fail_next = true;
        }
        if core::mem::take(&mut diag.pending.inject_computation_fail) {
            self.inject_computation_fail_next = true;
        }

        let (mute_active, delay_ms) = diag.tick_cycle_effects();
        self.mute_active = mute_active;
        self.pending_cycle_delay_ms = delay_ms;
        if mute_active {
            warn!(node_id = self.state.own_id(), "muted this cycle");
        }
        if delay_ms > 0 {
            warn!(delay_ms, "extra sleep this cycle for skew test");
        }

        if let Some(json) = diag.take_pending_input() {
            match serde_json::from_value::<C::Input>(json) {
                Ok(new_input) => {
                    info!("staged input applied");
                    self.input_source.set(new_input);
                }
                Err(e) => {
                    warn!(error = ?e, "staged input deserialise failed, keeping previous");
                }
            }
        }
    }

    pub(super) fn build_status_response(&self, diag: &Diagnostic) -> StatusResponse {
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
                    consecutive_faults: 0,
                    consecutive_healthy_cycles: 0,
                })
                .collect(),
            injection: InjectionSnapshot {
                drop_next_n_inputs: diag.injection.drop_next_n_inputs,
                drop_next_n_results: diag.injection.drop_next_n_results,
                drop_next_n_acks: diag.injection.drop_next_n_acks,
                drop_next_n_cyclesync: diag.injection.drop_next_n_cyclesync,
                drop_next_n_crc: diag.injection.drop_next_n_crc,
                drop_next_n_votes: diag.injection.drop_next_n_votes,
                fake_crc_remaining: diag.injection.fake_crc_remaining,
                divergent_publisher_remaining: diag.injection.divergent_publisher_remaining,
                mute_cycles_remaining: diag.injection.mute_cycles_remaining,
                cycle_delay_ms: diag.injection.cycle_delay_ms,
                cycle_delay_remaining: diag.injection.cycle_delay_remaining,
                corrupt_result_remaining: diag.injection.corrupt_result_remaining,
                drop_from_peers_mask: diag.injection.drop_from_peers_mask,
                fake_phase_header_remaining: diag.injection.fake_phase_header_remaining,
                fake_phase_header_value: diag.injection.fake_phase_header_value,
            },
            pending_input: diag.pending.has_input(),
            pending_injection_update: diag.pending.has_injection_update(),
            last_failsafe_reason: self.pending_failsafe_reason.map(|r| r as u8),
            recent_transitions: self
                .recent_transitions
                .iter()
                .map(|(from, event, to)| TransitionRecord {
                    from: format!("{:?}", from),
                    event: format!("{:?}", event),
                    to: format!("{:?}", to),
                })
                .collect(),
        }
    }
}
