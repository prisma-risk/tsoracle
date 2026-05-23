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

use anyhow::Context;
use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use parking_lot::Mutex;
use rocksdb::{DB, Options};
use tempfile::TempDir;
use tokio::net::TcpListener;
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

#[allow(dead_code)]
fn open_paxos_storage(dir: &std::path::Path) -> anyhow::Result<RocksdbStorage<HighWaterCommand>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    // RocksdbStorage uses a single column family; DB::open always registers
    // the "default" CF, which is the one we target here.
    let db = Arc::new(DB::open(&opts, dir).context("paxos topology: open rocksdb at storage dir")?);
    RocksdbStorage::open_in(db, "default")
        .map_err(|e| anyhow::anyhow!("paxos topology: open RocksdbStorage: {e:?}"))
}

/// Pluggable backend for the spawn-time I/O dependencies that production
/// `PaxosTopology::spawn` cannot otherwise force into a failure mode.
///
/// Production code uses [`DefaultPaxosBackend`]; tests inject impls that
/// fail at a chosen step to exercise the `?` propagation paths in
/// `spawn_with`. The `id` parameter lets a test backend differentiate
/// behavior per node (fail only on node 1, succeed on the rest, etc.).
/// Production ignores it.
#[async_trait]
pub trait PaxosBackend: Send + Sync {
    /// Allocate a fresh, writable directory for node `id`'s rocksdb storage
    /// and open the store on it. The returned `TempDir` must be kept alive
    /// for the node's lifetime; dropping it deletes the directory and
    /// invalidates the storage.
    async fn prepare_node_storage(
        &self,
        id: u64,
    ) -> anyhow::Result<(TempDir, RocksdbStorage<HighWaterCommand>)>;

    /// Bind the loopback listener that node `id`'s tsoracle server will
    /// serve from. Production uses `127.0.0.1:0`.
    async fn bind_loopback(&self, id: u64) -> anyhow::Result<TcpListener>;
}

/// Production [`PaxosBackend`]: real `tempfile::TempDir`, real
/// `RocksdbStorage`, real `TcpListener::bind("127.0.0.1:0")`.
pub struct DefaultPaxosBackend;

#[async_trait]
impl PaxosBackend for DefaultPaxosBackend {
    async fn prepare_node_storage(
        &self,
        _id: u64,
    ) -> anyhow::Result<(TempDir, RocksdbStorage<HighWaterCommand>)> {
        let dir = TempDir::new().context("paxos topology: create tempdir")?;
        let storage = open_paxos_storage(dir.path())
            .with_context(|| format!("paxos topology: open storage at {:?}", dir.path()))?;
        Ok((dir, storage))
    }

    async fn bind_loopback(&self, _id: u64) -> anyhow::Result<TcpListener> {
        TcpListener::bind("127.0.0.1:0")
            .await
            .context("paxos topology: bind loopback")
    }
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
