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
/// Re-export of the cross-backend advance payload that [`HighWaterCommand::Advance`]
/// wraps, so consumers can build commands without depending on `tsoracle-consensus`.
pub use tsoracle_consensus::AdvancePayload;
pub use type_config::{decode_epoch, encode_epoch};
