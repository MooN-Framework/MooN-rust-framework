//! Sink for the brake voting decision. Implements the domain-specific
//! safety gate: if the voted `BrakeResult` reports that the required
//! braking distance meets or exceeds the available distance
//! (`emergency_brake == true`), the whole system must go failsafe.

use crate::brake::braking_curve::BrakeResult;
use crate::framework::traits::{DecisionSink, SinkVerdict};
use tracing::{error, info, warn};

/// Recipient of the voted brake decision.
pub struct BrakeSink;

impl BrakeSink {
    /// Build a sink. Stateless: the verdict depends only on the
    /// decision handed to [`BrakeSink::evaluate`].
    pub fn new() -> Self {
        Self
    }
}

impl Default for BrakeSink {
    fn default() -> Self {
        Self::new()
    }
}

impl DecisionSink for BrakeSink {
    type Decision = BrakeResult;

    /// Domain safety gate. The voter guarantees consensus over the flags
    /// and distance; here we translate that consensus into a safe/unsafe
    /// classification for the physical process.
    ///
    /// A decision with `valid_entry == false` means the inputs were
    /// out-of-spec (NaN, negative, target > current). That is a sensor
    /// or upstream fault and must be treated as unsafe.
    ///
    /// `emergency_brake == true` means the computed braking distance
    /// meets or exceeds the available distance: the vehicle cannot stop
    /// in time under normal service brake, so emergency brake must be
    /// applied and the system must transition to fail-stop.
    fn evaluate(&self, decision: &BrakeResult) -> SinkVerdict {
        if !decision.valid_entry {
            warn!("invalid brake input reached sink, forcing failsafe");
            return SinkVerdict::Failsafe;
        }
        if decision.emergency_brake {
            warn!(
                distance = decision.total_distance,
                "braking distance exceeded, forcing failsafe"
            );
            return SinkVerdict::Failsafe;
        }
        SinkVerdict::Deliver
    }

    /// Publish the voted brake decision to the actuator/log/diagnostic
    /// wiring. Called only by the designated publisher: an isolated
    /// node is out of the ack-based publisher election and will never
    /// reach this path.
    fn publish(&mut self, decision: &BrakeResult) {
        info!(
            total_distance = decision.total_distance,
            emergency_brake = decision.emergency_brake,
            valid_entry = decision.valid_entry,
            "brake decision published"
        );
        // TODO: wire to actual actuator interface (SIL bus / GPIO).
    }

    /// This node is isolated. It is out of the ack round and therefore
    /// out of the publisher election; the remaining fabric continues to
    /// drive the actuator. Purely a local log / diagnostic hook.
    fn on_isolation(&mut self) {
        warn!("node isolated, actuator now driven by remaining fabric");
    }

    /// System-wide fail-stop. Every node runs this locally as the
    /// runner exits.
    fn on_failsafe(&mut self) {
        error!("failsafe triggered: forcing emergency brake");
        // TODO: drive local actuator hardware to safe state.
    }
}

#[cfg(test)]
mod sink_tests {
    //! The sink is the last gate before the actuator. Its verdict table
    //! is small enough to enumerate completely, which is what makes it
    //! worth pinning: a single inverted condition here would let an
    //! emergency decision through as a normal one.
    use super::*;

    fn decision(emergency: bool, valid: bool) -> BrakeResult {
        BrakeResult {
            total_distance: 42.0,
            emergency_brake: emergency,
            valid_entry: valid,
        }
    }

    #[test]
    fn a_valid_non_emergency_decision_is_delivered() {
        assert_eq!(
            BrakeSink::new().evaluate(&decision(false, true)),
            SinkVerdict::Deliver
        );
    }

    #[test]
    fn an_emergency_decision_forces_failsafe() {
        assert_eq!(
            BrakeSink::new().evaluate(&decision(true, true)),
            SinkVerdict::Failsafe
        );
    }

    #[test]
    fn an_invalid_entry_forces_failsafe_even_without_emergency() {
        // valid_entry == false means the inputs were out of spec. That
        // is a sensor fault, and a fault must not be delivered as a
        // normal brake command just because the emergency flag is
        // clear.
        assert_eq!(
            BrakeSink::new().evaluate(&decision(false, false)),
            SinkVerdict::Failsafe
        );
        assert_eq!(
            BrakeSink::new().evaluate(&decision(true, false)),
            SinkVerdict::Failsafe
        );
    }

    #[test]
    fn the_verdict_ignores_the_distance_value() {
        // Only the two flags decide. The distance is payload for the
        // actuator, not part of the gate.
        let mut far = decision(false, true);
        far.total_distance = 1e9;
        assert_eq!(BrakeSink::new().evaluate(&far), SinkVerdict::Deliver);

        let mut nan = decision(false, true);
        nan.total_distance = f64::NAN;
        assert_eq!(BrakeSink::new().evaluate(&nan), SinkVerdict::Deliver);
    }

    #[test]
    fn lifecycle_hooks_are_callable_without_a_prior_decision() {
        // The runner calls these on paths where no decision was ever
        // published, so they must not depend on sink state.
        let mut sink = BrakeSink::default();
        sink.on_isolation();
        sink.on_failsafe();
        sink.publish(&decision(false, true));
    }
}
