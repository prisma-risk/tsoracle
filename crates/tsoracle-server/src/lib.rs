//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

#![doc = include_str!("../README.md")]
// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod bt;
mod clock;
mod fence;
#[cfg(feature = "tracing")]
pub(crate) mod heartbeat;
mod leader_hint;
mod persist_disposition;
pub(crate) mod reporter;
mod server;
mod service;
mod serving_core;
mod signal;

pub mod docs;

pub use crate::reporter::Reporter;
pub use bt::Bt;
pub use clock::{Clock, SystemClock};
pub use server::{BuildError, Server, ServerBuilder, ServerError, ServingState, WatchGuard};
pub use signal::shutdown_signal;

#[cfg(any(test, feature = "test-fakes"))]
pub mod test_fakes;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

#[doc(hidden)]
pub use leader_hint::decode_leader_hint as __priv_decode_leader_hint;
