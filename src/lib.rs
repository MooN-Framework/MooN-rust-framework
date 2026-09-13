//! Software-based fault tolerance for MooN systems.
//!
//! This crate runs the same binary on N nodes, has them compute the
//! same safety function on the same inputs every cycle, and reduces
//! their results to one decision by majority vote. A node that
//! disagrees, falls silent or drifts out of step is excluded by a vote
//! of its peers; when too few nodes are left to vote, the system goes
//! fail-stop rather than guessing.
//!
//! ```text
//!     node 0  ─┐
//!     node 1  ─┼─ UDP multicast ─→  per-cycle vote  ─→  one decision
//!     node 2  ─┘                                        (one publisher)
//! ```
//!
//! # Layout
//!
//! - [`framework`] is domain-independent: discovery, clock
//!   synchronisation, the cyclic phase schedule, cross-observation, the
//!   exclusion vote, the wire format and the diagnostic channel.
//! - [`brake`] is a worked example that plugs an ETCS-style braking
//!   curve into the framework. Read it as the reference for what an
//!   application has to supply.
//!
//! # Using it
//!
//! An application implements the six traits in
//! [`framework::traits`], builds a
//! [`framework::state::RunState`] and a
//! [`framework::transport::UdpTransport`] from its configuration, and
//! hands both to [`framework::runner::Runner`]. `Runner::run` returns
//! only when the node has reached failsafe. `src/bin/node.rs` is that
//! wiring for the brake example, in about thirty lines.
//!
//! # Limits
//!
//! [`framework::config::MAX_TOTAL_NODES`] caps a fabric at 8 nodes,
//! because every peer mask on the wire is one byte. Raising it means
//! changing the wire format.
//!
//! # Features
//!
//! `diagnostic` compiles in the multicast diagnostic channel and its
//! fault-injection hooks. Production builds leave it off, which removes
//! the injection code paths entirely rather than disabling them at
//! runtime.

#![warn(missing_docs)]

pub mod brake;
pub mod framework;
