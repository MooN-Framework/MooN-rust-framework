//! This module defines the core components of the software-based fault tolerance framework.
//! It includes configuration, diagnostics, clock synchronization, state management, and transport mechanisms.
//! The framework is designed to facilitate the development of fault-tolerant distributed systems by providing
//! abstractions for common tasks such as state replication, consensus, and error handling.
//! To function correctly, the framework requires the user to implement specific traits for computation, voting, and decision-making logic,
//! which can be customized for different use cases.

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
