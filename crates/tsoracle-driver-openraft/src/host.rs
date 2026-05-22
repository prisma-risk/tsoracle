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

//! Host abstraction the openraft-backed `ConsensusDriver` builds on.
//!
//! Implementations decide where the high-water mark is stored and replicated.
//! The bundled [`StandaloneHost`](crate::standalone::StandaloneHost) owns its
//! own raft cluster + state machine. A larger service (e.g. one that already
//! runs an openraft cluster for other state) can implement this trait directly
//! and route TSO commands through its existing log.

use async_trait::async_trait;
use openraft::Raft;
use openraft::RaftTypeConfig;
use openraft::storage::RaftStateMachine;
use tsoracle_consensus::ConsensusError;

/// Host that knows how to read and advance the TSO high-water mark via openraft.
///
/// The driver crate handles `ConsensusDriver` trait shape and leadership-event
/// mapping; the host supplies the storage / submission semantics.
#[async_trait]
pub trait OpenraftHighWaterHost: Send + Sync + 'static {
    /// The host's `RaftTypeConfig`. Each host picks its own — the standalone
    /// host uses [`crate::TypeConfig`], a piggy-back host uses whatever
    /// `TypeConfig` its larger raft is built on.
    type Config: RaftTypeConfig;

    /// The host's state machine. Same indirection as `Config`.
    type StateMachine: RaftStateMachine<Self::Config>;

    /// The raft handle the driver reads leadership metrics from.
    fn raft(&self) -> &Raft<Self::Config, Self::StateMachine>;

    /// Read the current high-water mark linearizably. Implementations are
    /// expected to issue an `ensure_linearizable` barrier before reading their
    /// underlying state machine if reads should reflect all committed writes.
    async fn current_high_water(&self) -> Result<u64, ConsensusError>;

    /// Submit a "bump to at_least" proposal through the host's raft log and
    /// return the new high-water value after apply. The state machine must
    /// enforce monotonicity (`max(prev, at_least)`); the driver does not.
    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError>;
}
