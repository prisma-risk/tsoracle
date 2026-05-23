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

//! In-process OmniPaxos cluster on `MemNetwork`; chaos by partitioning the
//! leader's outbound messages.
//!
//! Mirrors `examples/paxos-embedded/src/main.rs`'s cluster bringup: a single
//! shared `MemNetwork`, per-node `RocksdbStorage` in a fresh tempdir, an
//! explicit inbox pump per node (paxos `MemNetwork::register` returns a
//! `Receiver`, not a factory), and a `tsoracle::Server` bound to a loopback
//! port. No membership-init step — OmniPaxos accepts the full `ClusterConfig`
//! at construction.
//!
//! `kill_leader` isolates the current leader on the shared `MemNetwork`'s
//! partition controller for a short window, forcing the remaining quorum to
//! elect a new leader, then restores reachability so subsequent chaos ops
//! still have a quorum to work with. `pause_leader` runs the same partition
//! shape for a caller-provided duration. `arm_failpoint`/`disarm_failpoint`
//! are feature-gated on `stress-failpoints`: enabled, they drive the
//! process-wide `fail` registry (which affects every in-process node at
//! once); disabled, they return `Skipped`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::sync::oneshot;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_paxos_toolkit::storage::RocksdbStorage;
use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;

use crate::chaos::ChaosEvent;
use crate::topology::{ChaosController, NodeId};

/// In-process OmniPaxos cluster with one `tsoracle::Server` per node.
#[allow(dead_code)]
pub struct PaxosTopology {
    pub controller: PaxosController,
    pub server_handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Owns the per-node OmniPaxos handles, the shared `MemNetwork`, and the
/// oneshot shutdown senders for each node's tsoracle server.
#[allow(dead_code)]
pub struct PaxosController {
    nodes: Vec<PaxosNode>,
    network: Arc<MemNetwork<HighWaterCommand>>,
    grace: Duration,
}

#[allow(dead_code)]
struct PaxosNode {
    node_id: NodeId,
    endpoint: String,
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, RocksdbStorage<HighWaterCommand>>>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    /// Keep the rocksdb tempdir alive for the node's lifetime.
    _storage_dir: TempDir,
}

// `ChaosController` impl. Method bodies are stubbed; subsequent commits
// replace each `unimplemented!()` with the real implementation.

#[async_trait]
impl ChaosController for PaxosController {
    async fn kill_leader(&self) -> ChaosEvent {
        unimplemented!()
    }

    async fn pause_leader(&self, _dur: Duration) -> ChaosEvent {
        unimplemented!()
    }

    async fn arm_failpoint(&self, _name: &str, _action: &str) -> ChaosEvent {
        unimplemented!()
    }

    async fn disarm_failpoint(&self, _name: &str) -> ChaosEvent {
        unimplemented!()
    }

    fn endpoints(&self) -> Vec<String> {
        unimplemented!()
    }

    fn current_leader(&self) -> Option<NodeId> {
        unimplemented!()
    }

    async fn shutdown(self: Box<Self>) {
        unimplemented!()
    }
}
