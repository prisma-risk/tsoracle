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
#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

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
pub use standalone::{BuilderError, StandaloneHost, StandaloneHostBuilder};
pub use state_machine::{ApplyState, drain_decided_into, maybe_snapshot};
pub use type_config::{PaxosPeer, decode_epoch, encode_epoch};
