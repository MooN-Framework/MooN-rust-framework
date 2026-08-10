//! This module implements the sink trait of the framework.
//! Its used to receive the voted brake decision and publish it to the actuator/log/diagnostic wiring.

use crate::brake::braking_curve::BrakeResult;
use crate::framework::traits::DecisionSink;

/// Recipient of the voted brake decision. Placeholder for the actual
/// actuator/log/diagnostic wiring.
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

    /// Publish the voted brake decision to the actuator/log/diagnostic wiring.
    /// TODO: Make actual failsafe decision.
    fn publish(&mut self, decision: &BrakeResult) {
        let _ = decision;
    }
}
