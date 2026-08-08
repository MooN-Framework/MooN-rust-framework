use crate::brake::braking_curve::{compute_braking_curve, BrakeError, BrakeInput, BrakeResult};
use crate::framework::traits::Computation;

/// Per-field tolerance on the ShareInputs divergence gate. Two peers'
/// inputs are considered to represent the same physical measurement iff
/// every field agrees within its tolerance.
#[derive(Debug, Clone, Copy)]
pub struct BrakeInputTolerance {
    pub speed_tolerance: f64,
    pub distance_tolerance: f64,
}

impl BrakeInputTolerance {
    pub const fn new(speed_tolerance: f64, distance_tolerance: f64) -> Self {
        Self {
            speed_tolerance,
            distance_tolerance,
        }
    }

    fn matches(&self, own: &BrakeInput, peer: &BrakeInput) -> bool {
        (own.current_speed - peer.current_speed).abs() <= self.speed_tolerance
            && (own.target_speed - peer.target_speed).abs() <= self.speed_tolerance
            && (own.available_distance - peer.available_distance).abs() <= self.distance_tolerance
    }
}

/// Adapter wrapping `compute_braking_curve` as a framework `Computation`.
/// `tolerance` gates the ShareInputs divergence check.
#[derive(Debug, Clone, Copy)]
pub struct BrakeComputation {
    pub tolerance: BrakeInputTolerance,
}

impl BrakeComputation {
    pub const fn new(tolerance: BrakeInputTolerance) -> Self {
        Self { tolerance }
    }
}

impl Computation for BrakeComputation {
    type Input = BrakeInput;
    type Payload = BrakeResult;
    type Error = BrakeError;

    fn compute(&mut self, input: BrakeInput) -> Result<BrakeResult, BrakeError> {
        compute_braking_curve(input)
    }

    fn inputs_agree(&self, own: &BrakeInput, peer: &BrakeInput) -> bool {
        self.tolerance.matches(own, peer)
    }
}
