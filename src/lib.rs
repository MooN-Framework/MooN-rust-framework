//! # Software-Based Fault Tolerance Framework
//!
//! This crate provides a framework for implementing software-based fault tolerance in distributed systems.
//! It includes modules for user management, error handling, and synchronization between peers.
//! This framework is designed to be flexible and extensible, allowing developers to implement custom computation, 
//! voting, and decision-making logic. The maximum number of peers is limited to 7 and the minimum number of peers is 2.
//! By that you can create MooN systems with N=3, 4, 5, 6, or 7. The framework is designed to handle various failure scenarios and ensure that the system 
//! can continue to operate correctly even in the presence of faults.
//! 
//! ## Modules
//!
//! - [`brake`] – This is a concrete example of a computation, voter, and sink implementation for a train braking system.
//! - [`framework`] – This module contains the core framework components, including the runner, state management, and transport mechanisms.

pub mod brake;
pub mod framework;