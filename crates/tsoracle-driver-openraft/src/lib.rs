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

//! openraft-backed `ConsensusDriver` for tsoracle.
//!
//! This crate replicates the TSO high-water mark across an openraft cluster
//! and exposes the result via the [`tsoracle_consensus::ConsensusDriver`]
//! trait. The caller supplies a pre-built [`openraft::Raft`] handle (with its
//! own `RaftNetworkFactory` and `RaftLogStorage`); this crate provides the
//! `RaftTypeConfig`, the log-entry type, the [`HighWaterStateMachine`] with
//! a pluggable [`SnapshotStore`] (defaulting to in-memory, with an optional
//! `RocksdbSnapshotStore` behind the `rocksdb-snapshot-store` feature), and
//! the trait bridge.

// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]
//!
//! # Layering
//!
//! - [`log_entry`] defines the single command type replicated through the log.
//! - [`state_machine`] implements `openraft::storage::RaftStateMachine` over
//!   an in-memory `u64` counter with bincode snapshots.
//! - [`type_config`] declares the `RaftTypeConfig` via
//!   `tsoracle_openraft_toolkit::declare_raft_types_ext!`.
//! - [`host`] declares the `OpenraftHighWaterHost` trait services implement
//!   to plug their consensus into the driver.
//! - [`standalone`] supplies the bundled host that owns its own raft cluster
//!   and the [`HighWaterStateMachine`].
//! - [`driver`] wraps the `Raft` handle and the state machine into the
//!   `ConsensusDriver` impl.

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
