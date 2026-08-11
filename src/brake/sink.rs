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
