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
    ///
    /// Wrapped in `Option` so [`Cluster::stop_node`] can release the
    /// final harness-held reference, which lets RocksDB drop the
    /// directory lock before [`Cluster::rebuild_rocksdb_node`] reopens
    /// it. Use [`NodeHandle::omnipaxos`] to get a cloned handle.
    omnipaxos: Option<Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>>,
    /// Stop signal for the inbox pump task.
    pump_stop: Option<oneshot::Sender<()>>,
    pump_handle: Option<JoinHandle<()>>,
}

impl<S> NodeHandle<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    /// Cloned `Arc` to this node's OmniPaxos handle. Panics if the node
    /// has been stopped via [`Cluster::stop_node`] and not yet rebuilt.
    pub fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<HighWaterCommand, S>>> {
        self.omnipaxos
            .as_ref()
            .expect("node is running (call rebuild + start_node after stop_node)")
            .clone()
    }
}

/// Cluster of paxos nodes wired through a shared [`MemNetwork`].
pub struct Cluster<S>
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    pub nodes: Vec<NodeHandle<S>>,
    pub network: Arc<MemNetwork<HighWaterCommand>>,
    /// Captured at construction so restart helpers can rebuild a node's
    /// OmniPaxos with the same topology hint.
    pub cluster_config: ClusterConfig,
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
        let node_ids: Vec<u64> = self.nodes.iter().map(|node| node.node_id).collect();
        for node_id in node_ids {
            self.start_node(node_id);
        }
    }

    /// Start a subset of nodes: every node whose id is in `subset`.
    /// Used by the joining-node test to leave one node intentionally
    /// idle (no runner, no pump → no OmniPaxos progress).
    pub fn start_only(&mut self, subset: &[u64]) {
        for node_id in subset {
            self.start_node(*node_id);
        }
    }

    /// Start the runner + pump for a single node. Idempotent guard:
    /// panics if the node is already running.
    pub fn start_node(&mut self, node_id: u64) {
        let sink = Arc::new(MeshSink::new(self.network.clone()));
        let omnipaxos = self.node(node_id).omnipaxos();
        let slot = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .expect("node id present");
        assert!(
            slot.pump_handle.is_none(),
            "node {node_id} is already running",
        );
        let host = slot.host.as_mut().expect("host present");
        host.start(sink);
        let inbox = slot.inbox.take().expect("inbox present");
        let (stop_tx, stop_rx) = oneshot::channel();
        slot.pump_stop = Some(stop_tx);
        slot.pump_handle = Some(tokio::spawn(pump_inbox(omnipaxos, inbox, stop_rx)));
    }

    /// Stop every node's runner + inbox pump.
    pub async fn stop_all(&mut self) {
        eprintln!("[diag] stop_all begin");
        for node in &mut self.nodes {
            let node_id = node.node_id;
            if let Some(stop_tx) = node.pump_stop.take() {
                let _ = stop_tx.send(());
            }
            if let Some(pump) = node.pump_handle.take() {
                eprintln!("[diag] stop_all: node {node_id} pump_handle await");
                if tokio::time::timeout(Duration::from_secs(30), pump)
                    .await
                    .is_err()
                {
                    panic!("[diag] stop_all: node {node_id} pump_handle TIMED OUT after 30s");
                }
                eprintln!("[diag] stop_all: node {node_id} pump_handle done");
            }
            if let Some(mut host) = node.host.take() {
                eprintln!("[diag] stop_all: node {node_id} host.stop() begin");
                if tokio::time::timeout(Duration::from_secs(30), host.stop())
                    .await
                    .is_err()
                {
                    panic!("[diag] stop_all: node {node_id} host.stop() TIMED OUT after 30s");
                }
                eprintln!("[diag] stop_all: node {node_id} host.stop() done");
            }
        }
        eprintln!("[diag] stop_all done");
    }

    /// Stop a single node (runner + inbox pump + host) and drop the
    /// final harness-held `Arc` to its OmniPaxos handle.
    ///
    /// Releasing the last `Arc` reference is what lets RocksDB-backed
    /// storage drop its directory lock, so that
    /// [`Cluster::rebuild_rocksdb_node`] can reopen the same path.
    pub async fn stop_node(&mut self, node_id: u64) {
        eprintln!("[diag] stop_node({node_id}) begin");
        let node = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .expect("node id present");
        if let Some(stop_tx) = node.pump_stop.take() {
            let _ = stop_tx.send(());
        }
        if let Some(pump) = node.pump_handle.take() {
            eprintln!("[diag] stop_node({node_id}): pump_handle await");
            if tokio::time::timeout(Duration::from_secs(30), pump)
                .await
                .is_err()
            {
                panic!("[diag] stop_node({node_id}): pump_handle TIMED OUT after 30s");
            }
        }
        if let Some(mut host) = node.host.take() {
            eprintln!("[diag] stop_node({node_id}): host.stop() begin");
            if tokio::time::timeout(Duration::from_secs(30), host.stop())
                .await
                .is_err()
            {
                panic!("[diag] stop_node({node_id}): host.stop() TIMED OUT after 30s");
            }
            eprintln!("[diag] stop_node({node_id}): host.stop() done");
        }
        // Drop the harness-held Arc last; the host has already released
        // its own clone via `host.stop().await + drop`. Once this take()
        // runs, the underlying OmniPaxos + Storage are deallocated.
        node.omnipaxos.take();
        eprintln!("[diag] stop_node({node_id}) done");
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
        let started = std::time::Instant::now();
        eprintln!("[diag] drive_until begin max_ticks={max_ticks}");
        for tick in 0..max_ticks {
            if predicate(self) {
                eprintln!(
                    "[diag] drive_until ok after {tick} ticks ({:?} wall)",
                    started.elapsed()
                );
                return;
            }
            if tick > 0 && tick.is_multiple_of(500) {
                eprintln!(
                    "[diag] drive_until still polling tick={tick} elapsed={:?} \
                     leader={:?} decided_per_node={:?}",
                    started.elapsed(),
                    self.leader_opt(),
                    self.nodes
                        .iter()
                        .map(|node| (
                            node.node_id,
                            node.omnipaxos
                                .as_ref()
                                .map(|handle| handle.lock().get_decided_idx())
                        ))
                        .collect::<Vec<_>>(),
                );
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!(
            "[diag] predicate did not become true within {max_ticks} ticks (elapsed {:?} wall)",
            started.elapsed()
        );
    }

    /// Returns the current leader's node id, or panics if none.
    pub fn leader(&self) -> u64 {
        self.nodes
            .iter()
            .filter_map(|node| node.omnipaxos.as_ref())
            .find_map(|handle| handle.lock().get_current_leader())
            .expect("no leader elected")
    }

    /// Optional variant of [`Self::leader`]; returns `None` if no node
    /// currently observes a leader.
    pub fn leader_opt(&self) -> Option<u64> {
        self.nodes
            .iter()
            .filter_map(|node| node.omnipaxos.as_ref())
            .find_map(|handle| handle.lock().get_current_leader())
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
        self.node(node_id).omnipaxos().lock().get_decided_idx()
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
                        let lock_start = std::time::Instant::now();
                        omnipaxos.lock().handle_incoming(message);
                        let elapsed = lock_start.elapsed();
                        if elapsed > Duration::from_millis(100) {
                            eprintln!(
                                "[diag] pump_inbox: handle_incoming took {elapsed:?}"
                            );
                        }
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
            omnipaxos: Some(shared_omnipaxos),
            pump_stop: None,
            pump_handle: None,
        });
    }

    Cluster {
        nodes,
        network,
        cluster_config,
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
            omnipaxos: Some(shared_omnipaxos),
            pump_stop: None,
            pump_handle: None,
        });
    }

    Cluster {
        nodes,
        network,
        cluster_config,
        tempdirs,
    }
}

#[cfg(feature = "rocksdb-storage")]
impl Cluster<RocksdbStorage<HighWaterCommand>> {
    /// Re-open the storage at this node's `TempDir` and rebuild its
    /// host. Used after [`Cluster::stop_node`] to simulate a clean
    /// restart with the same on-disk state.
    ///
    /// The runner + pump are NOT started by this call; the caller drives
    /// `start_after_restart` separately so they can inspect the recovered
    /// state first.
    pub fn rebuild_rocksdb_node(&mut self, node_id: u64) {
        let path = self
            .tempdirs
            .get(&node_id)
            .map(|dir| dir.path().to_path_buf())
            .expect("node has a tempdir backing");
        let storage = reopen_rocksdb_storage(&path);
        let server_config = ServerConfig {
            pid: node_id,
            election_tick_timeout: 5,
            resend_message_tick_timeout: 5,
            ..Default::default()
        };
        let omnipaxos_config = OmniPaxosConfig {
            cluster_config: self.cluster_config.clone(),
            server_config,
        };
        let omnipaxos = omnipaxos_config
            .build(storage)
            .expect("rebuild omnipaxos from recovered storage");
        let shared_omnipaxos = Arc::new(Mutex::new(omnipaxos));
        let inbox = self.network.register(node_id);
        let host = StandaloneHost::builder()
            .omnipaxos(shared_omnipaxos.clone())
            .my_node_id(node_id)
            .peers(Vec::new())
            .tick_interval(DEFAULT_TICK_INTERVAL)
            .snapshot_policy(SnapshotPolicy::disabled())
            .build()
            .expect("build standalone host after restart");
        let leader_stream = host.take_leader_stream();

        let slot = self
            .nodes
            .iter_mut()
            .find(|node| node.node_id == node_id)
            .expect("node id present");
        slot.host = Some(host);
        slot.inbox = Some(inbox);
        slot.leader_stream = leader_stream;
        slot.omnipaxos = Some(shared_omnipaxos);
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

/// Convenience predicate: every running node's `decided_idx` is at least
/// `target`. Stopped nodes (those whose OmniPaxos handle has been
/// released by [`Cluster::stop_node`]) are skipped, since they have no
/// observable state.
pub fn all_decided_at_least<S>(target: u64) -> impl FnMut(&Cluster<S>) -> bool
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    move |cluster: &Cluster<S>| {
        cluster
            .nodes
            .iter()
            .filter_map(|node| node.omnipaxos.as_ref())
            .all(|handle| handle.lock().get_decided_idx() >= target)
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
            .filter_map(|node| node.omnipaxos.as_ref())
            .any(|handle| handle.lock().get_current_leader().is_some())
    }
}
