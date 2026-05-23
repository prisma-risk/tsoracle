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

#[macro_use]
mod failpoint;
mod fence;
mod leader_hint;
mod server;
mod service;

pub mod docs;

pub use server::{BuildError, Server, ServerBuilder, ServerError, ServingState};

#[cfg(any(test, feature = "test-fakes"))]
pub mod test_fakes;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[doc(hidden)]
pub use leader_hint::decode_leader_hint as __priv_decode_leader_hint;
