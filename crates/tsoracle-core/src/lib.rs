//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Core algorithm for tsoracle.
//!
//! No I/O, no async, no tokio. Runtime-neutral. Property-testable in microseconds.

// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod allocator;
mod bt;
mod clock;
mod epoch;
mod timestamp;

pub mod docs;

pub use allocator::{Allocator, CoreError, WindowGrant};
pub use bt::Bt;
pub use clock::{Clock, SystemClock};
pub use epoch::Epoch;
pub use timestamp::{LOGICAL_MAX, PHYSICAL_MS_MAX, Timestamp, TimestampError};

#[cfg(any(test, feature = "test-clock"))]
pub use clock::testing;
