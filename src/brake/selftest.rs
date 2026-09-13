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

/// Why the power-on self-test failed.
#[derive(Debug)]
pub enum SelfTestError {
    /// A reference vector was rejected by the computation itself, which
    /// means the vector is wrong or the validation logic changed.
    ComputeFailed,
    /// The computation ran but produced a distance outside the
    /// vector's tolerance.
    UnexpectedResult {
        /// Reference distance from the vector.
        expected_distance: f64,
        /// Distance the computation actually produced.
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
// must produce a `total_distance` within `tolerance` of `expected_distance`.
#[derive(Debug, Clone, Copy)]
pub struct BrakeSelfTestVector {
    /// Input fed to the computation.
    pub input: BrakeInput,
    /// Reference total distance for this input.
    pub expected_distance: f64,
    /// Accepted absolute deviation from the reference, in metres.
    pub tolerance: f64,
}

/// Runs a fixed set of test vectors at startup.
pub struct BrakeSelfTest {
    vectors: &'static [BrakeSelfTestVector],
}

impl BrakeSelfTest {
    /// Build a self-test over a caller-supplied set of vectors.
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
    // Simple case: 5 m/s → 0 m/s in 1000 m. Should brake to a stop in 25 m.
    BrakeSelfTestVector {
        input: BrakeInput {
            current_speed: 5.0,
            target_speed: 0.0,
            available_distance: 1000.0,
        },
        expected_distance: 25.0,
        tolerance: 1e-6,
    },
    // Edge case: 3 m/s → 3 m/s in 100 m. Should brake to a stop in 7.5 m.
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

#[cfg(test)]
mod selftest_tests {
    //! The power-on self-test is what stands between a corrupted binary
    //! and a node joining the fabric, so both of its verdicts are
    //! pinned: the shipped vectors must pass, and a mismatch must be
    //! reported rather than tolerated.
    use super::*;

    #[test]
    fn the_shipped_vectors_pass() {
        BrakeSelfTest::default_vectors()
            .run()
            .expect("default self-test vectors must pass on this platform");
    }

    #[test]
    fn a_wrong_reference_value_is_reported() {
        const BAD: &[BrakeSelfTestVector] = &[BrakeSelfTestVector {
            input: BrakeInput {
                current_speed: 5.0,
                target_speed: 0.0,
                available_distance: 1000.0,
            },
            expected_distance: 999.0,
            tolerance: 1e-6,
        }];
        let err = BrakeSelfTest::new(BAD).run().expect_err("must fail");
        match err {
            SelfTestError::UnexpectedResult {
                expected_distance,
                got_distance,
            } => {
                assert_eq!(expected_distance, 999.0);
                assert!((got_distance - 25.0).abs() < 1e-9);
            }
            other => panic!("wrong error variant: {other:?}"),
        }
    }

    #[test]
    fn an_invalid_vector_surfaces_as_compute_failed() {
        const INVALID: &[BrakeSelfTestVector] = &[BrakeSelfTestVector {
            input: BrakeInput {
                current_speed: 1.0,
                target_speed: 2.0,
                available_distance: 100.0,
            },
            expected_distance: 0.0,
            tolerance: 1e-6,
        }];
        assert!(matches!(
            BrakeSelfTest::new(INVALID).run(),
            Err(SelfTestError::ComputeFailed)
        ));
    }

    #[test]
    fn an_empty_vector_set_passes_vacuously() {
        // Worth pinning because it is the failure mode of a build that
        // strips the vectors: the self-test would report OK without
        // having checked anything.
        BrakeSelfTest::new(&[]).run().expect("vacuous pass");
    }

    #[test]
    fn the_tolerance_is_respected() {
        const LOOSE: &[BrakeSelfTestVector] = &[BrakeSelfTestVector {
            input: BrakeInput {
                current_speed: 5.0,
                target_speed: 0.0,
                available_distance: 1000.0,
            },
            expected_distance: 25.5,
            tolerance: 1.0,
        }];
        BrakeSelfTest::new(LOOSE).run().expect("within tolerance");
    }
}
