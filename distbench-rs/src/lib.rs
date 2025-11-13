//! Distributed algorithms benchmarking application.
//!
//! This application provides a framework for running and testing distributed
//! algorithms in various execution modes (offline, local, docker).
extern crate alloc;
pub mod algorithms;
pub mod config;
pub mod logging;
pub mod transport_setup;
