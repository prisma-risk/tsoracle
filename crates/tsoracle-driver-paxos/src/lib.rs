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
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod apply;
pub mod driver;
pub mod host;
pub mod log_entry;
pub mod snapshot_policy;
pub mod standalone;
pub mod state_machine;
pub mod type_config;

pub use driver::PaxosDriver;
pub use log_entry::{HighWaterCommand, HighWaterSnapshot};
pub use snapshot_policy::SnapshotPolicy;
pub use standalone::{AlreadyRunning, BuilderError, StandaloneHost, StandaloneHostBuilder};
pub use state_machine::{ApplyState, drain_decided_into, max_logged_barrier_seq, maybe_snapshot};
pub use type_config::{decode_epoch, encode_epoch};
