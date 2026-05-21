//! Library surface for the smoke test in `tests/smoke.rs`. The bin (`main.rs`)
//! and the test both pull `run_demo` through here so the test does not have
//! to compile the bin twice.

mod demo;
pub mod host_service;

pub use demo::{DemoOutcome, run_demo};
