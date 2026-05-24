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

#![doc = include_str!("../README.md")]
// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod allocator;
mod bt;
mod clock;
mod epoch;
mod peer;
mod timestamp;

pub mod docs;

pub use allocator::{Allocator, CoreError, WindowGrant};
pub use bt::Bt;
pub use clock::{Clock, SystemClock};
pub use epoch::Epoch;
pub use peer::TsoPeer;
pub use timestamp::{LOGICAL_MAX, PHYSICAL_MS_MAX, Timestamp, TimestampError};

#[cfg(any(test, feature = "test-clock"))]
pub use clock::testing;
