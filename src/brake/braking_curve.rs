//! This module implements the braking curve computation for a vehicle.
//! It defines the necessary data structures, constants,
//! and functions to calculate the total braking distance required to decelerate from a current speed to a target speed within
//! an available distance. The braking curve is based on a hardcoded deceleration table that specifies
//! different deceleration rates for various speed ranges.

#![deny(unsafe_code)]

use crate::framework::traits::CyclePayload;
use crate::framework::wire::{PayloadError, WireReader, WireWriter};
use serde::Deserialize;
use std::fmt;

/// Hardcoded brake buildup time in seconds. This is the time it takes for the braking system to reach full effectiveness after the brake command is issued.
pub const BRAKE_BUILDUP_TIME_S: f64 = 2.5;

/// One speed band of the deceleration table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecelerationStage {
    /// Lower speed bound of the band, inclusive, in m/s.
    pub v_min: f64,
    /// Upper speed bound of the band, exclusive, in m/s.
    pub v_max: f64,
    /// Deceleration applied inside the band, in m/s^2.
    pub a: f64,
}

/// Hardcoded deceleration table. Each stage defines a speed range and the corresponding deceleration rate. The table is ordered from lowest to highest speed.
pub const DECELERATION_STAGES: &[DecelerationStage] = &[
    DecelerationStage {
        v_min: 0.0,
        v_max: 10.0,
        a: 1.0,
    },
    DecelerationStage {
        v_min: 10.0,
        v_max: 20.0,
        a: 0.9,
    },
    DecelerationStage {
        v_min: 20.0,
        v_max: 30.0,
        a: 0.8,
    },
    DecelerationStage {
        v_min: 30.0,
        v_max: 40.0,
        a: 0.7,
    },
    DecelerationStage {
        v_min: 40.0,
        v_max: 50.0,
        a: 0.6,
    },
    DecelerationStage {
        v_min: 50.0,
        v_max: f64::INFINITY,
        a: 0.5,
    },
];

/// Inputs for one brake curve computation.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct BrakeInput {
    /// Current speed, in m/s. Must be finite and non-negative.
    pub current_speed: f64,
    /// Speed to brake down to, in m/s. Must not exceed `current_speed`.
    pub target_speed: f64,
    /// Distance available before the target point, in metres.
    pub available_distance: f64,
}

impl BrakeInput {
    /// Build an input. Validation happens in
    /// [`compute_braking_curve`], not here, so a deserialized frame and
    /// a locally built value take the same path.
    pub fn new(current_speed: f64, target_speed: f64, available_distance: f64) -> Self {
        Self {
            current_speed,
            target_speed,
            available_distance,
        }
    }
}

/// Implementation of the `CyclePayload` trait for `BrakeInput`.
/// This allows `BrakeInput` to be serialized and deserialized for network transmission.
/// The wire layout is defined as 24 bytes in little-endian format, with each field represented as a 64-bit floating-point number (f64).
///
/// Wire layout (24 bytes, little-endian):
/// ```text
///   0..8    current_speed       (f64)
///   8..16   target_speed        (f64)
///   16..24  available_distance  (f64)
/// ```
impl CyclePayload for BrakeInput {
    const WIRE_SIZE: usize = 24;

    fn to_wire(&self, w: &mut WireWriter<'_>) {
        w.push_f64(self.current_speed);
        w.push_f64(self.target_speed);
        w.push_f64(self.available_distance);
        debug_assert_eq!(w.written(), Self::WIRE_SIZE);
    }

    fn from_wire(r: &mut WireReader<'_>) -> Result<Self, PayloadError> {
        Ok(BrakeInput {
            current_speed: r.read_f64()?,
            target_speed: r.read_f64()?,
            available_distance: r.read_f64()?,
        })
    }
}

/// Result of a brake curve computation.
/// Contains the total distance required to brake to the target speed, whether an emergency brake is needed, #
/// and whether the input was valid.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BrakeResult {
    /// Distance needed to reach the target speed, reaction distance
    /// included, in metres.
    pub total_distance: f64,
    /// True when `total_distance` meets or exceeds the available
    /// distance, so the service brake is not sufficient.
    pub emergency_brake: bool,
    /// False when the inputs were out of spec and no curve was
    /// computed. The sink treats this as a fault, not as a normal
    /// decision.
    pub valid_entry: bool,
}

impl BrakeResult {
    fn new_valid(total_distance: f64, emergency_brake: bool) -> Self {
        Self {
            total_distance,
            emergency_brake,
            valid_entry: true,
        }
    }
}

/// Error types for brake curve computation.
/// These errors indicate invalid input conditions, such as negative speeds or distances,
/// or a target speed that exceeds the current speed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BrakeError {
    /// At least one input was NaN or infinite.
    NotFinite,
    /// A speed was negative.
    NegativeSpeed,
    /// The available distance was negative.
    NegativeDistance,
    /// The target speed was above the current speed, which is an
    /// acceleration request, not a braking request.
    TargetAboveCurrent,
}

/// Implement the Display trait for BrakeError to provide user-friendly error messages.
impl fmt::Display for BrakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrakeError::NotFinite => write!(f, "input contains NaN or infinity"),
            BrakeError::NegativeSpeed => write!(f, "speed must be non-negative"),
            BrakeError::NegativeDistance => write!(f, "available distance must be non-negative"),
            BrakeError::TargetAboveCurrent => {
                write!(f, "target speed must not exceed current speed")
            }
        }
    }
}

/// Implement the Error trait for BrakeError to integrate with the standard error handling in Rust.
impl std::error::Error for BrakeError {}

/// Compute total braking distance and the emergency-brake decision.
///
/// Steps: validate inputs, add reaction distance `v0 * t_brems`, sum
/// per-stage distances `(v_hi^2 - v_lo^2) / (2a)` down the deceleration
/// table, compare against `available_distance`.
pub fn compute_braking_curve(input: BrakeInput) -> Result<BrakeResult, BrakeError> {
    validate(&input)?;
    let reaction = input.current_speed * BRAKE_BUILDUP_TIME_S;
    let braking = compute_braking_distance(input.current_speed, input.target_speed);
    let total = reaction + braking;
    let emergency = total >= input.available_distance;
    Ok(BrakeResult::new_valid(total, emergency))
}

/// Validate the brake input. Returns an error if any of the following conditions are met:
fn validate(input: &BrakeInput) -> Result<(), BrakeError> {
    if !input.current_speed.is_finite()
        || !input.target_speed.is_finite()
        || !input.available_distance.is_finite()
    {
        return Err(BrakeError::NotFinite);
    }
    if input.current_speed < 0.0 || input.target_speed < 0.0 {
        return Err(BrakeError::NegativeSpeed);
    }
    if input.available_distance < 0.0 {
        return Err(BrakeError::NegativeDistance);
    }
    if input.target_speed > input.current_speed {
        return Err(BrakeError::TargetAboveCurrent);
    }
    Ok(())
}

/// Sum stagewise braking distances from `v_start` down to `v_target`.
/// Expects validated inputs (`v_start >= v_target >= 0`, both finite).
fn compute_braking_distance(v_start: f64, v_target: f64) -> f64 {
    if v_start <= v_target {
        return 0.0;
    }
    let mut distance = 0.0;
    let mut v_upper = v_start;
    for stage in DECELERATION_STAGES.iter().rev() {
        if v_upper <= stage.v_min {
            continue;
        }
        let v_lower = stage.v_min.max(v_target);
        if v_upper <= v_lower {
            continue;
        }
        distance += (v_upper * v_upper - v_lower * v_lower) / (2.0 * stage.a);
        v_upper = v_lower;
        if v_upper <= v_target {
            break;
        }
    }
    distance
}

#[cfg(test)]
mod braking_curve_tests {
    //! The braking curve is the safety function of the demo domain.
    //! Its validation gate and its stage boundaries are pinned here,
    //! including the exact comparison that decides between service
    //! brake and emergency brake.
    use super::*;

    fn curve(current: f64, target: f64, available: f64) -> Result<BrakeResult, BrakeError> {
        compute_braking_curve(BrakeInput::new(current, target, available))
    }

    #[test]
    fn reaction_distance_is_always_added() {
        // Standing still still costs the buildup time times zero speed,
        // so the distance is zero, but the call must succeed.
        let r = curve(0.0, 0.0, 100.0).expect("valid");
        assert_eq!(r.total_distance, 0.0);
        assert!(r.valid_entry);
        assert!(!r.emergency_brake);
    }

    #[test]
    fn braking_inside_one_stage_matches_the_closed_form() {
        // 5 m/s lies entirely in the 0..10 stage with a = 1.0.
        let r = curve(5.0, 0.0, 1000.0).expect("valid");
        let expected = 5.0 * BRAKE_BUILDUP_TIME_S + (5.0 * 5.0) / (2.0 * 1.0);
        assert!((r.total_distance - expected).abs() < 1e-9);
    }

    #[test]
    fn no_speed_reduction_means_reaction_distance_only() {
        let r = curve(3.0, 3.0, 100.0).expect("valid");
        assert!((r.total_distance - 3.0 * BRAKE_BUILDUP_TIME_S).abs() < 1e-9);
    }

    #[test]
    fn braking_across_stages_sums_each_band_with_its_own_rate() {
        // 15 m/s spans the 10..20 band (a = 0.9) down to 10, then the
        // 0..10 band (a = 1.0) down to standstill.
        let r = curve(15.0, 0.0, 10_000.0).expect("valid");
        let upper = (15.0f64 * 15.0 - 10.0 * 10.0) / (2.0 * 0.9);
        let lower = (10.0f64 * 10.0) / (2.0 * 1.0);
        let expected = 15.0 * BRAKE_BUILDUP_TIME_S + upper + lower;
        assert!((r.total_distance - expected).abs() < 1e-9);
    }

    #[test]
    fn a_speed_exactly_on_a_stage_boundary_uses_the_lower_band() {
        // v_max is exclusive: at exactly 10 m/s the 10..20 band
        // contributes nothing, otherwise the distance would be counted
        // twice at every boundary.
        let r = curve(10.0, 0.0, 10_000.0).expect("valid");
        let expected = 10.0 * BRAKE_BUILDUP_TIME_S + (10.0f64 * 10.0) / (2.0 * 1.0);
        assert!((r.total_distance - expected).abs() < 1e-9);
    }

    #[test]
    fn partial_deceleration_stops_at_the_target_speed() {
        let r = curve(15.0, 12.0, 10_000.0).expect("valid");
        let expected = 15.0 * BRAKE_BUILDUP_TIME_S + (15.0f64 * 15.0 - 12.0 * 12.0) / (2.0 * 0.9);
        assert!((r.total_distance - expected).abs() < 1e-9);
    }

    #[test]
    fn the_open_ended_top_stage_is_reachable() {
        // The last band runs to infinity, so a high speed must not fall
        // off the end of the table and silently return a short distance.
        let r = curve(80.0, 0.0, 100_000.0).expect("valid");
        assert!(r.total_distance > 80.0 * BRAKE_BUILDUP_TIME_S);
        assert!(r.total_distance.is_finite());
    }

    #[test]
    fn emergency_triggers_when_the_distance_is_exactly_used_up() {
        // The comparison is `>=`: needing exactly the available
        // distance leaves no margin and must already be an emergency.
        let needed = curve(5.0, 0.0, 1e9).expect("valid").total_distance;
        assert!(curve(5.0, 0.0, needed).expect("valid").emergency_brake);
        assert!(
            !curve(5.0, 0.0, needed + 1e-6)
                .expect("valid")
                .emergency_brake
        );
    }

    #[test]
    fn invalid_inputs_are_rejected_by_category() {
        assert_eq!(curve(f64::NAN, 0.0, 10.0), Err(BrakeError::NotFinite));
        assert_eq!(curve(1.0, f64::NAN, 10.0), Err(BrakeError::NotFinite));
        assert_eq!(curve(1.0, 0.0, f64::NAN), Err(BrakeError::NotFinite));
        assert_eq!(curve(f64::INFINITY, 0.0, 10.0), Err(BrakeError::NotFinite));
        assert_eq!(curve(-1.0, 0.0, 10.0), Err(BrakeError::NegativeSpeed));
        assert_eq!(curve(1.0, -1.0, 10.0), Err(BrakeError::NegativeSpeed));
        assert_eq!(curve(1.0, 0.0, -1.0), Err(BrakeError::NegativeDistance));
        assert_eq!(curve(1.0, 2.0, 10.0), Err(BrakeError::TargetAboveCurrent));
    }

    #[test]
    fn validation_order_reports_the_coarsest_fault_first() {
        // A NaN speed combined with a negative distance is reported as
        // NotFinite, because a non-finite value makes every later
        // comparison meaningless.
        assert_eq!(curve(f64::NAN, 0.0, -1.0), Err(BrakeError::NotFinite));
    }

    #[test]
    fn the_stage_table_is_contiguous_and_ordered() {
        // A gap or an overlap in the table would silently drop or
        // double-count a speed band at runtime.
        let mut previous_max = 0.0;
        for stage in DECELERATION_STAGES {
            assert_eq!(stage.v_min, previous_max, "gap or overlap at {stage:?}");
            assert!(stage.v_max > stage.v_min);
            assert!(stage.a > 0.0, "a deceleration rate of zero divides by zero");
            previous_max = stage.v_max;
        }
        assert_eq!(previous_max, f64::INFINITY, "table must be open ended");
    }

    #[test]
    fn brake_input_round_trips_on_the_wire() {
        let input = BrakeInput::new(12.5, 3.25, 800.0);
        let mut buf = [0u8; BrakeInput::WIRE_SIZE];
        let mut w = WireWriter::new(&mut buf);
        input.to_wire(&mut w);
        assert_eq!(w.written(), BrakeInput::WIRE_SIZE);

        let mut r = WireReader::new(&buf);
        assert_eq!(BrakeInput::from_wire(&mut r), Ok(input));
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn a_truncated_brake_input_is_rejected() {
        let buf = [0u8; BrakeInput::WIRE_SIZE - 1];
        let mut r = WireReader::new(&buf);
        assert_eq!(BrakeInput::from_wire(&mut r), Err(PayloadError::TooShort));
    }
}
