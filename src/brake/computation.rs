//! This module implements the computation trait for the brake example.
//! It defines the `BrakeComputation` struct, which wraps the `compute_braking_curve` function and 
//! implements the `Computation` trait from the framework. 
//! The computation takes a `BrakeInput` and produces a `BrakeResult`, 
//! while also providing a mechanism to check if two inputs agree within specified tolerances.

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
    /// Check if two BrakeInput instances agree within the specified tolerances.
    fn matches(&self, own: &BrakeInput, peer: &BrakeInput) -> bool {
        (own.current_speed - peer.current_speed).abs() <= self.speed_tolerance
            && (own.target_speed - peer.target_speed).abs() <= self.speed_tolerance
            && (own.available_distance - peer.available_distance).abs() <= self.distance_tolerance
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BrakeComputation {
    pub tolerance: BrakeInputTolerance,
}

impl BrakeComputation {
    pub const fn new(tolerance: BrakeInputTolerance) -> Self {
        Self { tolerance }
    }
}

/// Implement the Computation trait for BrakeComputation. 
/// This allows the framework to use BrakeComputation as a computation unit that processes BrakeInput and produces BrakeResult, 
/// while also handling input agreement checks based on defined tolerances.
/// This allows to implement the framework for different use cases, as the computation can be swapped out with other implementations 
/// that adhere to the Computation trait.
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
