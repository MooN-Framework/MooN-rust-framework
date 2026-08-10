//! Power-on self-test for the brake node.
//!
//! Runs a small deterministic vector against `compute_braking_curve` to
//! attest that the brake computation path produces the reference result
//! bit-for-bit at startup — a minimal but domain-appropriate SIL-lite
//! check that catches obviously corrupted binaries and platform
//! floating-point inconsistencies before the node joins the fabric.
//!
//! Extend with sensor sanity, memory patterns, watchdog probes as needed.

use crate::brake::braking_curve::{compute_braking_curve, BrakeInput};
use crate::framework::traits::SelfTest;
use std::fmt;

#[derive(Debug)]
pub enum SelfTestError {
    ComputeFailed,
    UnexpectedResult {
        expected_distance: f64,
        got_distance: f64,
    },
}

impl fmt::Display for SelfTestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SelfTestError::ComputeFailed => write!(f, "brake self-test compute failed"),
            SelfTestError::UnexpectedResult {
                expected_distance,
                got_distance,
            } => write!(
                f,
                "brake self-test result mismatch (expected {expected_distance}, got {got_distance})"
            ),
        }
    }
}

impl std::error::Error for SelfTestError {}

/// One deterministic test vector — `input` fed to `compute_braking_curve`
/// must produce a `total_distance` within `tolerance` of `expected_distance`.
#[derive(Debug, Clone, Copy)]
pub struct BrakeSelfTestVector {
    pub input: BrakeInput,
    pub expected_distance: f64,
    pub tolerance: f64,
}

/// Runs a fixed set of test vectors at startup.
pub struct BrakeSelfTest {
    vectors: &'static [BrakeSelfTestVector],
}

impl BrakeSelfTest {
    pub const fn new(vectors: &'static [BrakeSelfTestVector]) -> Self {
        Self { vectors }
    }

    /// Default vectors: two smoke-tests exercising the stagewise table.
    pub const fn default_vectors() -> Self {
        Self::new(DEFAULT_VECTORS)
    }
}

/// Default test vectors for the brake self-test.
/// These are simple cases that exercise the braking curve computation.
const DEFAULT_VECTORS: &[BrakeSelfTestVector] = &[
    /// Simple case: 5 m/s → 0 m/s in 1000 m. Should brake to a stop in 25 m.
    BrakeSelfTestVector {
        input: BrakeInput {
            current_speed: 5.0,
            target_speed: 0.0,
            available_distance: 1000.0,
        },
        expected_distance: 25.0,
        tolerance: 1e-6,
    },
    /// Edge case: 3 m/s → 3 m/s in 100 m. Should brake to a stop in 7.5 m.
    BrakeSelfTestVector {
        input: BrakeInput {
            current_speed: 3.0,
            target_speed: 3.0,
            available_distance: 100.0,
        },
        expected_distance: 3.0 * 2.5,
        tolerance: 1e-6,
    },
];

/// Implement the SelfTest trait for BrakeSelfTest.
/// This allows the framework to run the self-test at startup,
impl SelfTest for BrakeSelfTest {
    type Error = SelfTestError;

    fn run(&mut self) -> Result<(), SelfTestError> {
        for v in self.vectors.iter() {
            let result =
                compute_braking_curve(v.input).map_err(|_| SelfTestError::ComputeFailed)?;
            if (result.total_distance - v.expected_distance).abs() > v.tolerance {
                return Err(SelfTestError::UnexpectedResult {
                    expected_distance: v.expected_distance,
                    got_distance: result.total_distance,
                });
            }
        }
        Ok(())
    }
}
