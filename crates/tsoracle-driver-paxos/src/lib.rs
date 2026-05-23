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

//! OmniPaxos-backed `ConsensusDriver` for tsoracle.
//!
//! This crate replicates the TSO high-water mark across an OmniPaxos cluster
//! and exposes the result via the [`tsoracle_consensus::ConsensusDriver`]
//! trait. The caller supplies a pre-built host (typically [`StandaloneHost`]
//! once it lands) that owns the OmniPaxos handle, the storage, and the
//! tick task; this crate provides the log-entry type, the `Epoch ↔ Ballot`
//! encoding, the `PaxosHighWaterHost` trait, and the trait bridge.

#![cfg_attr(docsrs, feature(doc_auto_cfg))]
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod driver;
pub mod host;
pub mod log_entry;
pub mod snapshot_policy;
pub mod standalone;
pub mod state_machine;
pub mod type_config;
pub mod yieldpoint;

pub use driver::PaxosDriver;
pub use log_entry::{HighWaterCommand, HighWaterSnapshot};
pub use snapshot_policy::SnapshotPolicy;
pub use standalone::{BuilderError, StandaloneHost, StandaloneHostBuilder};
pub use state_machine::{ApplyState, drain_decided_into, maybe_snapshot};
pub use type_config::{PaxosPeer, decode_epoch, encode_epoch};
