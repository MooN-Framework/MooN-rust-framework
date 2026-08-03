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

    fn publish(&mut self, decision: &BrakeResult) {
        let _ = decision;
    }
}
