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

//! Shared driver-paxos integration test harness.
//!
//! Boots a configurable-sized cluster of [`StandaloneHost`] instances wired
//! through a single in-process [`MemNetwork`]. Each host owns its own
//! [`PaxosRunner`] tick task; the harness additionally spawns a per-node
//! pump task that drains the node's inbox into its OmniPaxos handle via
//! `handle_incoming` so consensus actually progresses.
//!
//! Use [`build_mem_cluster`] for `MemStorage`-backed clusters (the common
//! case for liveness / fence tests). [`build_rocksdb_cluster`] exists for
//! restart / replay coverage; it allocates a `TempDir` per node and opens
//! the storage in a dedicated column family.

#![allow(dead_code)]
// Test binaries each pull this module via `#[path = ...] mod common`, and
// Rust treats every binary's view of the module independently. Helpers used
// by only a subset of test binaries trip `dead_code` from the other binaries'
// compilations; the allow keeps the warnings out without splitting the
// harness into per-test files.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omnipaxos::messages::Message;
use omnipaxos::storage::Storage;
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tsoracle_driver_paxos::{HighWaterCommand, SnapshotPolicy, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::{LeaderEventStream, MessageSink};
use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

#[cfg(feature = "rocksdb-storage")]
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
#[cfg(feature = "rocksdb-storage")]
use tsoracle_paxos_toolkit::storage::RocksdbStorage;

/// In-memory cluster topology + handles needed to drive consensus.
pub type MemCluster = Cluster<MemStorage<HighWaterCommand>>;

#[cfg(feature = "rocksdb-storage")]
pub type RocksdbCluster = Cluster<RocksdbStorage<HighWaterCommand>>;

/// Default tick interval used by harness clusters.
///
/// Two milliseconds keeps elections + replication well below the default
/// OmniPaxos election timeouts while keeping the runner's busy-loop cost
/// reasonable for a 3-node cluster.
pub const DEFAULT_TICK_INTERVAL: Duration = Duration::from_millis(2);

/// Routes outbound paxos messages through a shared [`MemNetwork`].
///
/// Each host's runner calls `MessageSink::send` for every outbound
/// message; the sink forwards it to the shared network, which consults
/// its [`PartitionController`] and pushes the payload to the destination
/// node's registered inbox.
pub struct MeshSink {
    network: Arc<MemNetwork<HighWaterCommand>>,
}

impl MeshSink {
    pub fn new(network: Arc<MemNetwork<HighWaterCommand>>) -> Self {
        Self { network }
    }
}

#[async_trait]
impl MessageSink<HighWaterCommand> for MeshSink {
    async fn send(&self, message: Message<HighWaterCommand>) {
        self.network.deliver(message).await;
    }
}

/// Per-node handle owned by the [`Cluster`].
pub struct NodeHandle<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    pub node_id: u64,
    /// The host. `Option` so we can take it for `stop()` without dropping
    /// the surrounding handle; harness teardown moves the host out, awaits
    /// `stop()`, and drops the host.
    pub host: Option<StandaloneHost<S>>,
    /// Inbox receiver registered with the shared [`MemNetwork`]. `Option`
    /// because the spawned pump task takes ownership of it.
    inbox: Option<mpsc::Receiver<Message<HighWaterCommand>>>,
    /// The leader-event stream taken off the host before start. Used by
    /// tests that build a `PaxosDriver` directly. Optional because a test
    /// can leave it on the host instead.
    pub leader_stream: Option<LeaderEventStream>,
    /// Shared OmniPaxos handle for direct inspection (`get_decided_idx`,
    /// `get_current_leader`, `append`, etc.).
    pub omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    /// Stop signal for the inbox pump task.
    pump_stop: Option<oneshot::Sender<()>>,
    pump_handle: Option<JoinHandle<()>>,
}

/// Cluster of paxos nodes wired through a shared [`MemNetwork`].
pub struct Cluster<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    pub nodes: Vec<NodeHandle<S>>,
    pub network: Arc<MemNetwork<HighWaterCommand>>,
    /// Held to keep RocksDB directories alive across the cluster's
    /// lifetime. Unused for `MemStorage` clusters.
    #[allow(dead_code)]
    tempdirs: HashMap<u64, TempDir>,
}

impl<S> Cluster<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
    <HighWaterCommand as omnipaxos::storage::Entry>::Snapshot: Send,
{
    /// Start every node's runner + inbox pump.
    ///
    /// Panics if called more than once on the same cluster (or after
    /// `stop_all`). Each node's `StandaloneHost::start` is invoked with a
    /// [`MeshSink`] pointing at the shared network. The pump task drains
    /// inbox messages and calls `handle_incoming` on the OmniPaxos handle.
    pub fn start_all(&mut self) {
        for node in &mut self.nodes {
            let host = node
                .host
                .as_mut()
                .expect("host must be present before start_all");
            let sink = Arc::new(MeshSink::new(self.network.clone()));
            host.start(sink);

            let inbox = node
                .inbox
                .take()
                .expect("inbox must be present before start_all");
            let omnipaxos = node.omnipaxos.clone();
            let (stop_tx, stop_rx) = oneshot::channel();
            node.pump_stop = Some(stop_tx);
            node.pump_handle = Some(tokio::spawn(pump_inbox(omnipaxos, inbox, stop_rx)));
        }
    }

    /// Stop every node's runner + inbox pump.
    pub async fn stop_all(&mut self) {
        for node in &mut self.nodes {
            if let Some(stop_tx) = node.pump_stop.take() {
                let _ = stop_tx.send(());
            }
            if let Some(pump) = node.pump_handle.take() {
                let _ = pump.await;
            }
            if let Some(mut host) = node.host.take() {
                host.stop().await;
            }
        }
    }

    /// Stop a single node (runner + inbox pump + host). Used by tests
    /// that need to bring one replica down without disturbing the rest.
    pub async fn stop_node(&mut self, node_id: u64) {
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .expect("node id present");
        if let Some(stop_tx) = node.pump_stop.take() {
            let _ = stop_tx.send(());
        }
        if let Some(pump) = node.pump_handle.take() {
            let _ = pump.await;
        }
        if let Some(mut host) = node.host.take() {
            host.stop().await;
        }
    }

    /// Block until `predicate(self)` returns true.
    ///
    /// Polls every millisecond. Each node's runner is spinning its own
    /// tick task and the pump tasks are draining inboxes, so this is a
    /// pure observation loop — no manual ticking needed.
    ///
    /// Panics with the predicate's text after `max_ticks` polls without
    /// success.
    pub async fn drive_until<F>(&self, mut predicate: F, max_ticks: usize)
    where
        F: FnMut(&Self) -> bool,
    {
        for _ in 0..max_ticks {
            if predicate(self) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("predicate did not become true within {max_ticks} ticks");
    }

    /// Returns the current leader's node id, or panics if none.
    pub fn leader(&self) -> u64 {
        self.nodes
            .iter()
            .find_map(|node| node.omnipaxos.lock().get_current_leader())
            .expect("no leader elected")
    }

    /// Optional variant of [`Self::leader`]; returns `None` if no node
    /// currently observes a leader.
    pub fn leader_opt(&self) -> Option<u64> {
        self.nodes
            .iter()
            .find_map(|node| node.omnipaxos.lock().get_current_leader())
    }

    /// In-memory high-water visible on the specified node.
    pub fn high_water_on(&self, node_id: u64) -> u64 {
        let node = self
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .expect("node id present");
        node.host.as_ref().expect("host present").current_value()
    }

    /// Decided index on the specified node.
    pub fn decided_idx_on(&self, node_id: u64) -> u64 {
        let node = self
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .expect("node id present");
        node.omnipaxos.lock().get_decided_idx()
    }

    /// Borrow a node by id. Panics if absent.
    pub fn node(&self, node_id: u64) -> &NodeHandle<S> {
        self.nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .expect("node id present")
    }

    /// Mutably borrow a node by id. Panics if absent.
    pub fn node_mut(&mut self, node_id: u64) -> &mut NodeHandle<S> {
        self.nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .expect("node id present")
    }
}

/// Spawn-target: drain `inbox` into `omnipaxos.handle_incoming` until the
/// stop signal fires or the inbox returns `None` (sender dropped).
async fn pump_inbox<S>(
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    mut inbox: mpsc::Receiver<Message<HighWaterCommand>>,
    mut stop_rx: oneshot::Receiver<()>,
) where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    loop {
        tokio::select! {
            biased;
            _ = &mut stop_rx => {
                return;
            }
            message = inbox.recv() => {
                match message {
                    Some(message) => {
                        omnipaxos.lock().handle_incoming(message);
                    }
                    None => return,
                }
            }
        }
    }
}

/// Build a `node_count`-node cluster backed by [`MemStorage`].
///
/// Node ids are `1..=node_count`. Each host is constructed with the
/// default tick interval and an [`SnapshotPolicy::disabled`] snapshot
/// policy. Callers can override the snapshot policy via
/// [`build_mem_cluster_with_policy`].
pub fn build_mem_cluster(node_count: usize) -> MemCluster {
    build_mem_cluster_with_policy(node_count, SnapshotPolicy::disabled())
}

pub fn build_mem_cluster_with_policy(node_count: usize, policy: SnapshotPolicy) -> MemCluster {
    assert!(
        node_count >= 3,
        "omnipaxos 0.2 rejects ClusterConfig with fewer than 3 nodes"
    );
    let network: Arc<MemNetwork<HighWaterCommand>> = Arc::new(MemNetwork::new());
    let node_ids: Vec<u64> = (1..=node_count as u64).collect();
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };

    let mut nodes = Vec::with_capacity(node_count);
    for &node_id in &node_ids {
        let server_config = ServerConfig {
            pid: node_id,
            election_tick_timeout: 5,
            resend_message_tick_timeout: 5,
            ..Default::default()
        };
        let omnipaxos_config = OmniPaxosConfig {
            cluster_config: cluster_config.clone(),
            server_config,
        };
        let storage = MemStorage::<HighWaterCommand>::new();
        let omnipaxos = omnipaxos_config
            .build(storage)
            .expect("build omnipaxos handle");
        let shared_omnipaxos = Arc::new(Mutex::new(omnipaxos));
        let inbox = network.register(node_id);

        let host = StandaloneHost::builder()
            .omnipaxos(shared_omnipaxos.clone())
            .my_node_id(node_id)
            .peers(Vec::new())
            .tick_interval(DEFAULT_TICK_INTERVAL)
            .snapshot_policy(policy)
            .build()
            .expect("build standalone host");
        let leader_stream = host.take_leader_stream();
        nodes.push(NodeHandle {
            node_id,
            host: Some(host),
            inbox: Some(inbox),
            leader_stream,
            omnipaxos: shared_omnipaxos,
            pump_stop: None,
            pump_handle: None,
        });
    }

    Cluster {
        nodes,
        network,
        tempdirs: HashMap::new(),
    }
}

#[cfg(feature = "rocksdb-storage")]
const ROCKSDB_LOG_CF: &str = "tso_paxos";

/// Build a `node_count`-node cluster backed by [`RocksdbStorage`]. Each
/// node gets its own `TempDir` whose lifetime is tied to the returned
/// cluster — drop the cluster to drop the directories.
#[cfg(feature = "rocksdb-storage")]
pub fn build_rocksdb_cluster(node_count: usize) -> RocksdbCluster {
    build_rocksdb_cluster_with_policy(node_count, SnapshotPolicy::disabled())
}

#[cfg(feature = "rocksdb-storage")]
pub fn build_rocksdb_cluster_with_policy(
    node_count: usize,
    policy: SnapshotPolicy,
) -> RocksdbCluster {
    assert!(
        node_count >= 3,
        "omnipaxos 0.2 rejects ClusterConfig with fewer than 3 nodes"
    );
    let network: Arc<MemNetwork<HighWaterCommand>> = Arc::new(MemNetwork::new());
    let node_ids: Vec<u64> = (1..=node_count as u64).collect();
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };

    let mut tempdirs = HashMap::new();
    let mut nodes = Vec::with_capacity(node_count);
    for &node_id in &node_ids {
        let dir = TempDir::new().expect("tempdir");
        let db = open_rocksdb(dir.path());
        let storage: RocksdbStorage<HighWaterCommand> =
            RocksdbStorage::open_in(db, ROCKSDB_LOG_CF).expect("open_in");
        let server_config = ServerConfig {
            pid: node_id,
            election_tick_timeout: 5,
            resend_message_tick_timeout: 5,
            ..Default::default()
        };
        let omnipaxos_config = OmniPaxosConfig {
            cluster_config: cluster_config.clone(),
            server_config,
        };
        let omnipaxos = omnipaxos_config
            .build(storage)
            .expect("build omnipaxos handle");
        let shared_omnipaxos = Arc::new(Mutex::new(omnipaxos));
        let inbox = network.register(node_id);

        let host = StandaloneHost::builder()
            .omnipaxos(shared_omnipaxos.clone())
            .my_node_id(node_id)
            .peers(Vec::new())
            .tick_interval(DEFAULT_TICK_INTERVAL)
            .snapshot_policy(policy)
            .build()
            .expect("build standalone host");
        let leader_stream = host.take_leader_stream();
        tempdirs.insert(node_id, dir);
        nodes.push(NodeHandle {
            node_id,
            host: Some(host),
            inbox: Some(inbox),
            leader_stream,
            omnipaxos: shared_omnipaxos,
            pump_stop: None,
            pump_handle: None,
        });
    }

    Cluster {
        nodes,
        network,
        tempdirs,
    }
}

#[cfg(feature = "rocksdb-storage")]
pub(crate) fn open_rocksdb(path: &std::path::Path) -> Arc<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cf = ColumnFamilyDescriptor::new(ROCKSDB_LOG_CF, Options::default());
    Arc::new(DB::open_cf_descriptors(&opts, path, vec![cf]).expect("open rocksdb"))
}

/// Re-open a RocksDB database at `path` and build a fresh
/// [`RocksdbStorage`] handle pointing at it. Used by restart tests to
/// rebuild a node after `stop_node` has dropped the previous host.
#[cfg(feature = "rocksdb-storage")]
pub fn reopen_rocksdb_storage(path: &std::path::Path) -> RocksdbStorage<HighWaterCommand> {
    let db = open_rocksdb(path);
    RocksdbStorage::open_in(db, ROCKSDB_LOG_CF).expect("open_in")
}

/// Lookup the `TempDir` path for a given node id. Returns `None` if the
/// cluster is `MemStorage`-backed.
#[cfg(feature = "rocksdb-storage")]
pub fn node_storage_path<S>(cluster: &Cluster<S>, node_id: u64) -> Option<std::path::PathBuf>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    cluster
        .tempdirs
        .get(&node_id)
        .map(|dir| dir.path().to_path_buf())
}

/// Convenience predicate: every node's `decided_idx` is at least `target`.
pub fn all_decided_at_least<S>(target: u64) -> impl FnMut(&Cluster<S>) -> bool
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    move |cluster: &Cluster<S>| {
        cluster
            .nodes
            .iter()
            .all(|node| node.omnipaxos.lock().get_decided_idx() >= target)
    }
}

/// Convenience predicate: at least one node currently reports a leader.
pub fn some_leader_elected<S>() -> impl FnMut(&Cluster<S>) -> bool
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    |cluster: &Cluster<S>| {
        cluster
            .nodes
            .iter()
            .any(|node| node.omnipaxos.lock().get_current_leader().is_some())
    }
}
