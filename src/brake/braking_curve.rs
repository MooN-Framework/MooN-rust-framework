#![deny(unsafe_code)]

use serde::Deserialize;
use std::fmt;

pub const BRAKE_BUILDUP_TIME_S: f64 = 2.5;

/// One speed band of the deceleration table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecelerationStage {
    pub v_min: f64,
    pub v_max: f64,
    pub a: f64,
}

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
    pub current_speed: f64,
    pub target_speed: f64,
    pub available_distance: f64,
}

impl BrakeInput {
    pub fn new(current_speed: f64, target_speed: f64, available_distance: f64) -> Self {
        Self {
            current_speed,
            target_speed,
            available_distance,
        }
    }
}

/// Result of one brake curve computation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BrakeResult {
    pub total_distance: f64,
    pub emergency_brake: bool,
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

/// Input-validation errors. The computation itself is total on valid input.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BrakeError {
    NotFinite,
    NegativeSpeed,
    NegativeDistance,
    TargetAboveCurrent,
}

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
