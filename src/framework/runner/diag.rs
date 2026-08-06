use crate::framework::diagnostic::{
    Command, Diagnostic, InjectionSnapshot, OutgoingTelegram, PeerStatus, StatusResponse,
};
use crate::framework::traits::{Computation, DecisionSink, Voter};
use serde::Deserialize;
use tracing::{info, warn};

impl<C, V, S> super::Runner<C, V, S>
where
    C: Computation,
    V: Voter<Payload = C::Payload>,
    S: DecisionSink<Decision = V::Decision>,
    C::Input: for<'de> Deserialize<'de>,
{
    /// Drain incoming diagnostic telegrams. GetStatus is answered directly;
    /// state-changing commands stage themselves inside `Diagnostic`.
    pub(super) fn poll_diagnostic(&mut self) {
        let Some(mut diag) = self.diagnostic.take() else {
            return;
        };
        while let Some(cmd) = diag.try_recv() {
            match cmd {
                Command::GetStatus => {
                    let status = self.build_status_response(&diag);
                    diag.send(&OutgoingTelegram::Status {
                        source_node_id: diag.node_id(),
                        data: status,
                    });
                }
                _ => {}
            }
        }
        self.diagnostic = Some(diag);
    }

    /// Apply staged diagnostic input and injection updates at cycle start.
    pub(super) fn apply_pending_diagnostic(&mut self) {
        let Some(diag) = self.diagnostic.as_mut() else {
            return;
        };
        if let Some(json) = diag.take_pending_input() {
            match serde_json::from_value::<C::Input>(json) {
                Ok(new_input) => {
                    info!("staged input applied");
                    self.input = new_input;
                }
                Err(e) => {
                    warn!(error = ?e, "staged input deserialise failed, keeping previous");
                }
            }
        }
        diag.apply_pending_injection();
    }

    /// Build a full status snapshot for a GetStatus reply.
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
                drop_next_n_results: diag.injection.drop_next_n_results,
                drop_next_n_acks: diag.injection.drop_next_n_acks,
            },
            pending_input: diag.pending.has_input(),
            pending_injection_update: diag.pending.has_injection_update(),
        }
    }
}
