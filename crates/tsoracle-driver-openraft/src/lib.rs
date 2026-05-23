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

pub mod driver;
pub mod host;
pub mod log_entry;
pub mod snapshot_store;
pub mod standalone;
pub mod state_machine;
pub mod type_config;

pub use driver::OpenraftDriver;
pub use host::OpenraftHighWaterHost;
pub use log_entry::HighWaterCommand;
#[cfg(feature = "rocksdb-snapshot-store")]
pub use snapshot_store::RocksdbSnapshotStore;
pub use snapshot_store::{InMemorySnapshotStore, SnapshotStore};
pub use standalone::StandaloneHost;
pub use state_machine::{HighWaterStateMachine, HighWaterStateMachineSnapshot};
pub use type_config::{HighWaterApplied, OpenraftPeer, TypeConfig};
