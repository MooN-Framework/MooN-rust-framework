//! Domain-independent core of the fault-tolerance framework.
//!
//! The framework supplies everything that is identical across MooN
//! deployments: peer discovery, clock synchronisation, the cyclic phase
//! schedule, cross-observation, the exclusion vote and the wire format.
//! What it deliberately does not supply is the safety function itself.
//! An application plugs that in through the traits in [`traits`], and
//! [`runner::Runner`] drives them.
//!
//! Reading order for someone new to the code:
//!
//! 1. [`state_machine`] for the phase sequence a node walks each cycle.
//! 2. [`runner`] for the loop that drives it and the deadlines it obeys.
//! 3. [`state`] for the data every phase reads and writes.
//! 4. [`wire`] for what actually goes on the network.

pub mod config;
pub mod diagnostic;
pub mod clock_sync;
pub mod runner;
pub mod state;
pub mod state_machine;
pub mod traits;
pub mod transport;
pub mod types;
pub mod wire;
