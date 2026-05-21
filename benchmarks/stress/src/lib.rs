#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

//! tsoracle stress + chaos harness.
//!
//! See `docs/superpowers/specs/2026-05-21-stress-harness-design.md` for the
//! design. See `benchmarks/stress/README.md` for usage.

pub mod chaos;
pub mod config;
pub mod event;
pub mod git;
pub mod loadgen;
pub mod nemesis;
pub mod report;
pub mod sample;
pub mod schedule;
pub mod supervisor;
pub mod topology;
pub mod types;
pub mod violation;
