//! This module implements the computation trait for the brake example.
//! It defines the `BrakeComputation` struct, which wraps the `compute_braking_curve` function and
//! implements the `Computation` trait from the framework.
//! The computation takes a `BrakeInput` and produces a `BrakeResult`,
//! while also providing a mechanism to check if two inputs agree within specified tolerances.

use crate::brake::braking_curve::{compute_braking_curve, BrakeError, BrakeInput, BrakeResult};
use crate::framework::config::MAX_TOTAL_NODES;
use crate::framework::traits::Computation;
use heapless::Vec;

/// Median of `values`, with the arithmetic mean of the two middle
/// elements on an even count. Sorting uses `f64::total_cmp` so the
/// order is total and the call cannot panic on a NaN. A NaN sorts to
/// the end and can only reach the median if it is one of the middle
/// elements, in which case the result stays NaN and
/// `compute_braking_curve` rejects it with `BrakeError::NotFinite`,
/// i.e. the cycle fails safe instead of producing a curve from a
/// poisoned value.
///
/// Deterministic across nodes: the result depends only on the multiset
/// of values, not on the order they were collected in, and `(a + b) /
/// 2.0` is exactly reproducible in IEEE 754.
fn median(values: &mut [f64]) -> f64 {
    values.sort_unstable_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values[mid]
    } else {
        (values[mid - 1] + values[mid]) / 2.0
    }
}

/// Per-field tolerance on the ShareInputs divergence gate. Two peers'
/// inputs are considered to represent the same physical measurement iff
/// every field agrees within its tolerance.
#[derive(Debug, Clone, Copy)]
pub struct BrakeInputTolerance {
    /// Maximum accepted difference on `current_speed` and
    /// `target_speed`, in m/s.
    pub speed_tolerance: f64,
    /// Maximum accepted difference on `available_distance`, in metres.
    pub distance_tolerance: f64,
}

impl BrakeInputTolerance {
    /// Build a tolerance pair. Both values are absolute, not relative.
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

/// `Computation` implementation for the braking curve.
///
/// Consolidates the sensor inputs of all participating nodes field by
/// field with `median`, then runs [`compute_braking_curve`] on the
/// consolidated value, so a single drifting sensor cannot move the
/// result.
#[derive(Debug, Clone, Copy)]
pub struct BrakeComputation {
    /// Divergence gate applied to peer inputs during ShareInputs.
    pub tolerance: BrakeInputTolerance,
}

impl BrakeComputation {
    /// Build a computation with the given input divergence gate.
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

    /// Median over `current_speed` of the own input and every peer
    /// input that passed the tolerance gate.
    ///
    /// Only `current_speed` is a sensor reading. `target_speed` and
    /// `available_distance` come from the movement authority and are
    /// identical on every node by construction, so they are taken from
    /// the own input unchanged. If that assumption ever stops holding
    /// (different nodes deriving them independently), they have to be
    /// consolidated the same way, otherwise every node computes on its
    /// own variant and the divergence only shows up one phase later in
    /// ShareResult.
    fn consolidate(&self, own: &BrakeInput, peers: &[BrakeInput]) -> BrakeInput {
        let mut speeds: Vec<f64, MAX_TOTAL_NODES> = Vec::new();
        let _ = speeds.push(own.current_speed);
        for peer in peers {
            let _ = speeds.push(peer.current_speed);
        }

        BrakeInput {
            current_speed: median(&mut speeds),
            target_speed: own.target_speed,
            available_distance: own.available_distance,
        }
    }
}

#[cfg(test)]
mod consolidation_tests {
    use super::*;

    fn comp() -> BrakeComputation {
        BrakeComputation::new(BrakeInputTolerance::new(2.0, 10.0))
    }

    fn input(speed: f64) -> BrakeInput {
        BrakeInput::new(speed, 5.0, 800.0)
    }

    #[test]
    fn median_of_three_picks_middle_value() {
        // 2oo3 happy path: the outlier inside the tolerance band gets
        // discarded, the middle sensor wins.
        let out = comp().consolidate(&input(30.0), &[input(31.5), input(30.5)]);
        assert_eq!(out.current_speed, 30.5);
    }

    #[test]
    fn median_of_two_averages() {
        // One peer excluded, two nodes left: mean of the two remaining
        // readings, which stay inside the tolerance band by definition
        // of the gate that ran before consolidate.
        let out = comp().consolidate(&input(30.0), &[input(31.0)]);
        assert_eq!(out.current_speed, 30.5);
    }

    #[test]
    fn single_input_is_passed_through() {
        let out = comp().consolidate(&input(42.0), &[]);
        assert_eq!(out.current_speed, 42.0);
    }

    #[test]
    fn non_sensor_fields_come_from_own_input() {
        let own = BrakeInput::new(30.0, 5.0, 800.0);
        let out = comp().consolidate(&own, &[input(31.0), input(29.0)]);
        assert_eq!(out.target_speed, own.target_speed);
        assert_eq!(out.available_distance, own.available_distance);
    }

    #[test]
    fn result_is_independent_of_peer_order() {
        // Cross-node determinism: node A and node B collect the same
        // inputs in whatever order the frames arrived. Both must end up
        // with the exact same bits, otherwise ShareResult diverges.
        let a = comp().consolidate(&input(30.0), &[input(31.7), input(29.3)]);
        let b = comp().consolidate(&input(30.0), &[input(29.3), input(31.7)]);
        assert_eq!(a.current_speed.to_bits(), b.current_speed.to_bits());
    }

    #[test]
    fn even_count_mean_is_bit_identical_regardless_of_order() {
        let a = comp().consolidate(&input(30.1), &[input(31.3)]);
        let b = comp().consolidate(&input(31.3), &[input(30.1)]);
        assert_eq!(a.current_speed.to_bits(), b.current_speed.to_bits());
    }

    #[test]
    fn nan_in_the_middle_fails_safe_in_compute() {
        // A NaN cannot pass inputs_agree, so it never reaches consolidate
        // in the real flow. Guard the behaviour anyway: the poisoned
        // median must be rejected by compute rather than yield a curve.
        let mut c = comp();
        let out = c.consolidate(&input(f64::NAN), &[input(30.0)]);
        assert!(out.current_speed.is_nan());
        assert_eq!(c.compute(out), Err(BrakeError::NotFinite));
    }
}
