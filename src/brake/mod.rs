//! Worked example: an ETCS-style braking-curve node.
//!
//! The framework is domain-agnostic, so it needs a concrete
//! application to be useful. This module is that application, and it
//! doubles as the reference for how the traits in
//! [`crate::framework::traits`] fit together:
//!
//! - [`braking_curve`] holds the safety function and its payload types.
//! - [`computation`] wires it into `Computation`, including the
//!   divergence gate on sensor inputs.
//! - [`voter`] reduces the per-node results to one decision.
//! - [`sink`] is the gate in front of the actuator.
//! - [`selftest`] is the power-on check that runs before the node joins.

pub mod braking_curve;
pub mod computation;
pub mod selftest;
pub mod sink;
pub mod voter;
