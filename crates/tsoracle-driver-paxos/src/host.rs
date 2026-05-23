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

//! Host abstraction the OmniPaxos-backed `ConsensusDriver` builds on.
//!
//! Implementations decide where the OmniPaxos handle lives, where storage
//! is persisted, and how `current_high_water` / `submit_advance` interact
//! with the underlying paxos log. The bundled `StandaloneHost` (landing in
//! a follow-up sub-issue) owns its own OmniPaxos cluster + state machine.
//! A larger service (e.g., one that already runs OmniPaxos for other
//! state) can implement this trait directly and route TSO commands
//! through its existing log.

use std::sync::Arc;

use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use omnipaxos::storage::Storage;
use parking_lot::Mutex;
use tsoracle_consensus::ConsensusError;

use crate::log_entry::HighWaterCommand;

/// Host that knows how to read and advance the TSO high-water mark via
/// OmniPaxos.
///
/// The driver crate handles the `ConsensusDriver` trait shape and
/// leadership-event mapping; the host supplies the storage / submission
/// semantics.
#[async_trait]
pub trait PaxosHighWaterHost: Send + Sync + 'static {
    /// The storage type backing this host's OmniPaxos handle. Each host
    /// picks its own — the standalone host uses the toolkit's
    /// `RocksdbStorage`, a piggy-back host uses whatever Storage its
    /// larger OmniPaxos instance is built on.
    type Storage: Storage<HighWaterCommand> + Send + 'static;

    /// The OmniPaxos handle the driver reads leadership state from.
    fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<HighWaterCommand, Self::Storage>>>;

    /// Read the current high-water mark linearizably.
    ///
    /// Implementations append a `HighWaterCommand::Barrier`, await the
    /// apply task's notification that the cluster's `decided_idx` has
    /// advanced past the call's snapshot, and then return the in-memory
    /// high-water.
    async fn current_high_water(&self) -> Result<u64, ConsensusError>;

    /// Submit a "bump to at_least" proposal through the host's OmniPaxos
    /// log and return the new high-water value once the cluster has
    /// applied it (or a later higher value).
    ///
    /// Implementations append a `HighWaterCommand::Advance { at_least }`
    /// and wait until both (a) the cluster's `decided_idx` has advanced
    /// past the call's snapshot AND (b) the in-memory high-water is at
    /// least `at_least`.
    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError>;
}
