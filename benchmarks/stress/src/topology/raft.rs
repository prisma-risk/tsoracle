//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

//! In-process openraft cluster on `MemNetwork`; chaos by partitioning the
//! leader's outbound messages.
//!
//! Mirrors the `examples/openraft-piggyback` `build_cluster` wiring: a single
//! shared `MemNetwork` registry, per-node `RocksdbLogStore` in a fresh
//! tempdir, a `HighWaterStateMachine`, and a `tsoracle::Server` bound to a
//! loopback port. Cluster membership is initialized on node 1 once every
//! node's `Raft` handle is registered.
//!
//! `kill_leader` isolates the current leader on the shared `MemNetwork`'s
//! partition controller for a short window, forcing the remaining quorum to
//! elect a new leader, then heals the partition so subsequent chaos ops still
//! have a quorum to work with. `pause_leader` runs the same partition shape
//! for a caller-provided duration, leaving leadership intact when the window
//! is shorter than `election_timeout_min`. `arm_failpoint`/`disarm_failpoint`
//! are feature-gated on `stress-failpoints`: enabled, they drive the
//! process-wide `fail` registry (which affects every in-process node at once);
//! disabled, they return `Skipped`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::{Config, Raft, SnapshotPolicy};
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep};
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, OpenraftPeer, StandaloneHost, TypeConfig,
};
use tsoracle_openraft_toolkit::test_fakes::{MemNetwork, PartitionController};
use tsoracle_openraft_toolkit::{Flat, RocksdbLogStore};
use tsoracle_server::Server;

use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome};
use crate::topology::{ChaosController, NodeId, timed_event};

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

/// In-process openraft cluster with one `tsoracle::Server` per node.
pub struct RaftTopology {
    pub controller: RaftController,
    pub server_handles: Vec<tokio::task::JoinHandle<()>>,
}

/// Owns the per-node raft handles, the shared `MemNetwork`, and the oneshot
/// shutdown senders for each node's tsoracle server.
pub struct RaftController {
    nodes: Vec<RaftNode>,
    network: Arc<MemNetwork<TypeConfig>>,
    grace: Duration,
}

struct RaftNode {
    node_id: NodeId,
    endpoint: String,
    raft: Raft<TypeConfig, HighWaterStateMachine>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    /// Keep the rocksdb tempdir alive for the node's lifetime.
    _log_dir: TempDir,
}

fn open_log_store(dir: &std::path::Path) -> anyhow::Result<RocksdbLogStore<TypeConfig, Flat>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![
        ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
        ColumnFamilyDescriptor::new(META_CF, Options::default()),
    ];
    let driver = Arc::new(DB::open_cf_descriptors(&opts, dir, cfs)?);
    Ok(RocksdbLogStore::open(driver, LOG_CF, META_CF, Flat)?)
}

/// Pluggable backend for the spawn-time I/O dependencies that production
/// `RaftTopology::spawn` cannot otherwise force into a failure mode.
///
/// Production code uses [`DefaultRaftBackend`]; tests inject impls that fail
/// at a chosen step to exercise the `?` propagation paths in `spawn_with`.
/// The `id` parameter lets a test backend differentiate behavior per node
/// (fail only on node 1, succeed on the rest, etc.). Production ignores it.
#[async_trait]
pub trait RaftBackend: Send + Sync {
    /// Allocate a fresh, writable directory for node `id`'s rocksdb log
    /// store and open the store on it. The returned `TempDir` must be kept
    /// alive for the node's lifetime; dropping it deletes the directory and
    /// invalidates the log store.
    async fn prepare_node_storage(
        &self,
        id: u64,
    ) -> anyhow::Result<(TempDir, RocksdbLogStore<TypeConfig, Flat>)>;

    /// Bind the loopback listener that node `id`'s tsoracle server will
    /// serve from. Production uses `127.0.0.1:0`.
    async fn bind_loopback(&self, id: u64) -> anyhow::Result<TcpListener>;
}

/// Production [`RaftBackend`]: real `tempfile::TempDir`, real
/// `RocksdbLogStore`, real `TcpListener::bind("127.0.0.1:0")`.
pub struct DefaultRaftBackend;

#[async_trait]
impl RaftBackend for DefaultRaftBackend {
    async fn prepare_node_storage(
        &self,
        _id: u64,
    ) -> anyhow::Result<(TempDir, RocksdbLogStore<TypeConfig, Flat>)> {
        let dir = TempDir::new().context("raft topology: create tempdir")?;
        let store = open_log_store(dir.path())
            .with_context(|| format!("raft topology: open log store at {:?}", dir.path()))?;
        Ok((dir, store))
    }

    async fn bind_loopback(&self, _id: u64) -> anyhow::Result<TcpListener> {
        TcpListener::bind("127.0.0.1:0")
            .await
            .context("raft topology: bind loopback")
    }
}

fn raft_config() -> anyhow::Result<Arc<Config>> {
    Ok(Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            // HighWaterStateMachine is in-memory only — leaving snapshots on
            // would let openraft purge logs the SM cannot rebuild from.
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()?,
    ))
}

impl RaftTopology {
    /// Boot an N-node in-process cluster, each node running its own
    /// `tsoracle::Server` bound to a fresh loopback port. Returns once
    /// membership has been initialized and a leader has been observed.
    ///
    /// Uses [`DefaultRaftBackend`] for the spawn-time I/O steps. Tests that
    /// need to exercise the failure paths call [`Self::spawn_with`] with a
    /// fake backend.
    pub async fn spawn(node_count: usize, grace: Duration) -> anyhow::Result<Self> {
        Self::spawn_with(&DefaultRaftBackend, node_count, grace).await
    }

    /// Like [`Self::spawn`] but with a caller-supplied [`RaftBackend`] for
    /// the spawn-time I/O. Useful for tests that want to inject failures
    /// at the storage-preparation or listener-binding steps.
    pub async fn spawn_with(
        backend: &dyn RaftBackend,
        node_count: usize,
        grace: Duration,
    ) -> anyhow::Result<Self> {
        if node_count == 0 {
            bail!("raft topology requires at least one node");
        }

        let network = MemNetwork::<TypeConfig>::new();
        let config = raft_config()?;

        let mut nodes: Vec<RaftNode> = Vec::with_capacity(node_count);
        let mut server_handles: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(node_count);

        for raw_id in 1..=node_count {
            let node_id_u64 = raw_id as u64;
            let (log_dir, log_store) = backend.prepare_node_storage(node_id_u64).await?;
            let state_machine = HighWaterStateMachine::new();
            let state_machine_for_host = state_machine.clone();

            let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
                node_id_u64,
                config.clone(),
                network.factory_for(node_id_u64),
                log_store,
                state_machine,
            )
            .await
            .with_context(|| format!("raft topology: Raft::new for node {node_id_u64}"))?;
            network.register(node_id_u64, raft.clone());

            let host = StandaloneHost::new(raft.clone(), state_machine_for_host);
            let driver = OpenraftDriver::new(host);
            let server = Server::builder()
                .consensus_driver(driver)
                .build()
                .map_err(|e| anyhow::anyhow!("raft topology: server build: {e:?}"))?;

            let listener = backend.bind_loopback(node_id_u64).await?;
            let addr = listener.local_addr()?;
            let endpoint = format!("http://{addr}");
            let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
            let endpoint_for_log = endpoint.clone();
            let handle = tokio::spawn(async move {
                let shutdown = async move {
                    let _ = shutdown_rx.await;
                };
                if let Err(e) = server.serve_with_listener(listener, shutdown).await {
                    tracing::error!(error = ?e, endpoint = %endpoint_for_log, "tsoracle server died");
                }
            });

            nodes.push(RaftNode {
                node_id: NodeId(raw_id as u32),
                endpoint,
                raft,
                shutdown_tx: Mutex::new(Some(shutdown_tx)),
                _log_dir: log_dir,
            });
            server_handles.push(handle);
        }

        // Initialize membership on node 1 once every node is registered.
        let mut membership: BTreeMap<u64, OpenraftPeer> = BTreeMap::new();
        for node in &nodes {
            let id_u64 = u64::from(node.node_id.0);
            membership.insert(
                id_u64,
                OpenraftPeer {
                    addr: format!("mem-node-{id_u64}"),
                    service_endpoint: String::new(),
                },
            );
        }
        nodes[0]
            .raft
            .initialize(membership)
            .await
            .context("raft topology: initialize membership")?;

        wait_for_leader(&nodes, Duration::from_secs(2)).await?;

        Ok(RaftTopology {
            controller: RaftController {
                nodes,
                network,
                grace,
            },
            server_handles,
        })
    }
}

async fn wait_for_leader(nodes: &[RaftNode], timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            let snapshots: Vec<_> = nodes
                .iter()
                .map(|n| {
                    let metrics = n.raft.metrics().borrow_watched().clone();
                    format!(
                        "node {} state={:?} term={:?} leader={:?}",
                        n.node_id.0, metrics.state, metrics.current_term, metrics.current_leader
                    )
                })
                .collect();
            bail!(
                "raft topology: no leader within {timeout:?}; snapshots:\n  {}",
                snapshots.join("\n  ")
            );
        }
        for node in nodes {
            let metrics = node.raft.metrics().borrow_watched().clone();
            if metrics.current_leader.is_some() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(25)).await;
    }
}

/// RAII guard that heals a node-level partition on drop. Used to make
/// `kill_leader` / `pause_leader` cancel-safe: if the harness's outer
/// `select!` (e.g. the `--duration` timer) drops the chaos future while
/// it is parked at the mid-window `sleep`, the guard's `Drop` still fires
/// and restores reachability — without this, the cluster would remain
/// partitioned for the rest of the run.
struct HealOnDrop {
    partitions: Arc<PartitionController<u64>>,
    node: u64,
}

impl Drop for HealOnDrop {
    fn drop(&mut self) {
        self.partitions.heal(self.node);
    }
}

#[async_trait]
impl ChaosController for RaftController {
    async fn kill_leader(&self) -> ChaosEvent {
        // The openraft `u64` NodeId of the leader is the key the shared
        // `PartitionController` uses to gate edges. `current_leader()` on the
        // trait returns the stress `NodeId(u32)` (narrowed from the same
        // value), so we read metrics directly here to keep the openraft id
        // in its native width and avoid a u32 -> u64 round-trip.
        let leader_raft_id: Option<u64> = self
            .nodes
            .iter()
            .find_map(|n| n.raft.metrics().borrow_watched().current_leader);
        let Some(leader_raft_id) = leader_raft_id else {
            return timed_event(ChaosKind::LeaderKill, self.grace, || async {
                ChaosOutcome::Skipped {
                    reason: "no current leader".into(),
                }
            })
            .await;
        };
        let partitions = self.network.partitions();
        timed_event(ChaosKind::LeaderKill, self.grace, move || async move {
            partitions.isolate(leader_raft_id);
            // Heal-on-drop guarantees reachability is restored even if the
            // outer future is cancelled at the `sleep` below — see
            // `HealOnDrop`'s doc.
            let _guard = HealOnDrop {
                partitions,
                node: leader_raft_id,
            };
            // Election timeout is 300-600ms; openraft can also need a few
            // heartbeat-interval ticks (100ms each) before followers escalate
            // to a candidate after losing the leader. 1500ms keeps the chaos
            // window short while reliably producing a re-election in CI; the
            // 750ms baseline from the sketch was tight enough to flake.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            // `_guard` drops here on the happy path too, performing the heal.
            ChaosOutcome::Applied
        })
        .await
    }

    async fn pause_leader(&self, dur: Duration) -> ChaosEvent {
        // Same shape as `kill_leader` — see its comment for why we read the
        // openraft `u64` NodeId directly from metrics rather than going
        // through `current_leader()`'s narrowed `NodeId(u32)`.
        let leader_raft_id: Option<u64> = self
            .nodes
            .iter()
            .find_map(|n| n.raft.metrics().borrow_watched().current_leader);
        let Some(leader_raft_id) = leader_raft_id else {
            return timed_event(ChaosKind::LeaderPause, self.grace, || async {
                ChaosOutcome::Skipped {
                    reason: "no current leader".into(),
                }
            })
            .await;
        };
        let partitions = self.network.partitions();
        timed_event(ChaosKind::LeaderPause, self.grace, move || async move {
            partitions.isolate(leader_raft_id);
            // Cancel-safety guard; see `kill_leader` for the rationale.
            let _guard = HealOnDrop {
                partitions,
                node: leader_raft_id,
            };
            tokio::time::sleep(dur).await;
            ChaosOutcome::Applied
        })
        .await
    }

    async fn arm_failpoint(&self, name: &str, action: &str) -> ChaosEvent {
        let kind = ChaosKind::FailpointArm { name: name.into() };
        #[cfg(feature = "stress-failpoints")]
        {
            let name = name.to_string();
            let action = action.to_string();
            return timed_event(kind, self.grace, move || async move {
                match fail::cfg(name.as_str(), action.as_str()) {
                    Ok(()) => ChaosOutcome::Applied,
                    Err(e) => ChaosOutcome::Failed {
                        reason: format!("fail::cfg: {e}"),
                    },
                }
            })
            .await;
        }
        #[cfg(not(feature = "stress-failpoints"))]
        {
            let _ = (name, action);
            timed_event(kind, self.grace, || async {
                ChaosOutcome::Skipped {
                    reason: "stress-failpoints feature off; failpoints not linked".into(),
                }
            })
            .await
        }
    }

    async fn disarm_failpoint(&self, name: &str) -> ChaosEvent {
        let kind = ChaosKind::FailpointDisarm { name: name.into() };
        #[cfg(feature = "stress-failpoints")]
        {
            let name = name.to_string();
            return timed_event(kind, self.grace, move || async move {
                fail::remove(name.as_str());
                ChaosOutcome::Applied
            })
            .await;
        }
        #[cfg(not(feature = "stress-failpoints"))]
        {
            let _ = name;
            timed_event(kind, self.grace, || async {
                ChaosOutcome::Skipped {
                    reason: "stress-failpoints feature off".into(),
                }
            })
            .await
        }
    }

    fn endpoints(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.endpoint.clone()).collect()
    }

    fn current_leader(&self) -> Option<NodeId> {
        for node in &self.nodes {
            let metrics = node.raft.metrics().borrow_watched().clone();
            if let Some(leader_id) = metrics.current_leader {
                return Some(NodeId(leader_id as u32));
            }
        }
        None
    }

    async fn shutdown(self: Box<Self>) {
        for node in &self.nodes {
            if let Some(tx) = node.shutdown_tx.lock().take() {
                let _ = tx.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_3_nodes_reports_endpoints_and_leader() {
        let topology = RaftTopology::spawn(3, Duration::from_millis(50))
            .await
            .expect("spawn 3-node raft topology");
        let endpoints = topology.controller.endpoints();
        assert_eq!(
            endpoints.len(),
            3,
            "expected 3 endpoints, got {endpoints:?}"
        );
        assert!(
            topology.controller.current_leader().is_some(),
            "expected a leader after spawn"
        );
        Box::new(topology.controller).shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kill_leader_triggers_reelection() {
        let topology = RaftTopology::spawn(3, Duration::from_millis(750))
            .await
            .expect("spawn 3-node raft topology");
        let original_leader = topology
            .controller
            .current_leader()
            .expect("leader at boot");

        let event = topology.controller.kill_leader().await;
        assert!(
            event.outcome.is_applied(),
            "kill_leader expected Applied, got {:?}",
            event.outcome
        );

        // Poll for a different leader; election timeout is 300-600ms. The
        // wall-clock cap is generous to tolerate sanitizer or emulation
        // slowdown — the loop exits as soon as a new leader is observed, so
        // fast machines pay nothing extra.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let mut new_leader = None;
        while std::time::Instant::now() < deadline {
            if let Some(candidate) = topology.controller.current_leader() {
                if candidate != original_leader {
                    new_leader = Some(candidate);
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let new_leader = match new_leader {
            Some(id) => id,
            None => {
                let snapshots: Vec<String> = topology
                    .controller
                    .nodes
                    .iter()
                    .map(|n| {
                        let metrics = n.raft.metrics().borrow_watched().clone();
                        format!(
                            "node {} state={:?} term={:?} leader={:?}",
                            n.node_id.0,
                            metrics.state,
                            metrics.current_term,
                            metrics.current_leader
                        )
                    })
                    .collect();
                panic!(
                    "re-election should have produced a different leader (was {:?}); snapshots:\n  {}",
                    original_leader,
                    snapshots.join("\n  ")
                );
            }
        };
        assert_ne!(original_leader, new_leader);

        Box::new(topology.controller).shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn kill_leader_heals_on_cancel() {
        // The outer `--duration` timer in the stress harness wins the
        // top-level `select!` and drops the in-flight chaos future. Before
        // the `HealOnDrop` guard, that cancellation parked at the
        // mid-window `sleep` and `heal` never ran, stranding the cluster
        // partitioned for the rest of the run. The guard's `Drop` impl is
        // what makes this test pass: the partition is healed even though
        // the chaos future never reached its end-of-window heal site.
        let topology = RaftTopology::spawn(3, Duration::from_millis(750))
            .await
            .expect("spawn 3-node raft topology");
        let leader_raft_id: u64 = topology
            .controller
            .nodes
            .iter()
            .find_map(|n| n.raft.metrics().borrow_watched().current_leader)
            .expect("a leader at boot");
        let partitions = topology.controller.network.partitions();
        // The mid-window sleep in `kill_leader` is 1500ms; cancelling at
        // 50ms reliably lands us inside it on any reasonable machine.
        match tokio::time::timeout(Duration::from_millis(50), topology.controller.kill_leader())
            .await
        {
            Err(_elapsed) => {} // expected — future was dropped mid-sleep
            Ok(event) => panic!(
                "kill_leader should not have finished within 50ms; got {:?}",
                event.outcome,
            ),
        }
        // `is_reachable(x, x)` returns true iff `x` is NOT isolated, so this
        // probe directly reports the node-level partition state we care
        // about without needing to pick a peer id.
        assert!(
            partitions.is_reachable(leader_raft_id, leader_raft_id),
            "partition for node {leader_raft_id} must be healed after the \
             chaos future is dropped; the node is still in the isolated set",
        );
        Box::new(topology.controller).shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pause_leader_returns_applied() {
        let topology = RaftTopology::spawn(3, Duration::from_millis(750))
            .await
            .expect("spawn 3-node raft topology");
        let event = topology
            .controller
            .pause_leader(Duration::from_millis(200))
            .await;
        assert!(
            event.outcome.is_applied(),
            "pause_leader expected Applied, got {:?}",
            event.outcome
        );
        Box::new(topology.controller).shutdown().await;
    }

    /// Backend that delegates to `DefaultRaftBackend` for every step except
    /// the one named in `fail_step`, where it returns an injected error
    /// when the call is for node `fail_at_node`.
    struct FailingBackend {
        fail_step: SpawnStep,
        fail_at_node: u64,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SpawnStep {
        PrepareStorage,
        BindLoopback,
    }

    #[async_trait]
    impl RaftBackend for FailingBackend {
        async fn prepare_node_storage(
            &self,
            id: u64,
        ) -> anyhow::Result<(TempDir, RocksdbLogStore<TypeConfig, Flat>)> {
            if self.fail_step == SpawnStep::PrepareStorage && id == self.fail_at_node {
                bail!("injected: prepare_node_storage failed for node {id}");
            }
            DefaultRaftBackend.prepare_node_storage(id).await
        }

        async fn bind_loopback(&self, id: u64) -> anyhow::Result<TcpListener> {
            if self.fail_step == SpawnStep::BindLoopback && id == self.fail_at_node {
                bail!("injected: bind_loopback failed for node {id}");
            }
            DefaultRaftBackend.bind_loopback(id).await
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_propagates_storage_failure() {
        // Inject a `prepare_node_storage` failure for the first node. The
        // resulting `?` propagation surfaces through `spawn_with` as a
        // descriptive `anyhow::Error`.
        let backend = FailingBackend {
            fail_step: SpawnStep::PrepareStorage,
            fail_at_node: 1,
        };
        match RaftTopology::spawn_with(&backend, 3, Duration::from_millis(50)).await {
            Err(err) => {
                let msg = format!("{err:#}");
                assert!(
                    msg.contains("prepare_node_storage failed"),
                    "expected storage-failure message, got: {msg}",
                );
            }
            Ok(_) => panic!("spawn should propagate the injected storage failure"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_propagates_bind_failure() {
        // Inject a `bind_loopback` failure for the second node. The first
        // node's storage and listener succeed; the second's bind fails.
        let backend = FailingBackend {
            fail_step: SpawnStep::BindLoopback,
            fail_at_node: 2,
        };
        match RaftTopology::spawn_with(&backend, 3, Duration::from_millis(50)).await {
            Err(err) => {
                let msg = format!("{err:#}");
                assert!(
                    msg.contains("bind_loopback failed"),
                    "expected bind-failure message, got: {msg}",
                );
            }
            Ok(_) => panic!("spawn should propagate the injected bind failure"),
        }
    }

    /// Build an empty `RaftController` for tests that need to exercise the
    /// "no nodes" / "no current leader" code paths (which the production
    /// `spawn` never produces because spawn waits for a leader to emerge).
    fn empty_controller() -> RaftController {
        RaftController {
            nodes: Vec::new(),
            network: MemNetwork::<TypeConfig>::new(),
            grace: Duration::from_millis(50),
        }
    }

    #[test]
    fn current_leader_returns_none_when_no_nodes() {
        let controller = empty_controller();
        assert!(controller.current_leader().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kill_leader_skipped_when_no_nodes() {
        let controller = empty_controller();
        let ev = controller.kill_leader().await;
        match ev.outcome {
            ChaosOutcome::Skipped { ref reason } => {
                assert!(reason.contains("no current leader"), "got reason: {reason}");
            }
            other => panic!("expected Skipped, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pause_leader_skipped_when_no_nodes() {
        let controller = empty_controller();
        let ev = controller.pause_leader(Duration::from_millis(50)).await;
        match ev.outcome {
            ChaosOutcome::Skipped { ref reason } => {
                assert!(reason.contains("no current leader"), "got reason: {reason}");
            }
            other => panic!("expected Skipped, got {other:?}"),
        }
    }

    #[cfg(feature = "stress-failpoints")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn arm_disarm_failpoint_round_trip() {
        let topology = RaftTopology::spawn(3, Duration::from_millis(50))
            .await
            .expect("spawn 3-node raft topology");
        let ev_arm = topology
            .controller
            .arm_failpoint("stress-raft-test::fp_unused", "off")
            .await;
        assert!(ev_arm.outcome.is_applied(), "arm: {:?}", ev_arm.outcome);
        let ev_disarm = topology
            .controller
            .disarm_failpoint("stress-raft-test::fp_unused")
            .await;
        assert!(
            ev_disarm.outcome.is_applied(),
            "disarm: {:?}",
            ev_disarm.outcome
        );
        Box::new(topology.controller).shutdown().await;
    }

    #[cfg(not(feature = "stress-failpoints"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn failpoints_off_returns_skipped() {
        let topology = RaftTopology::spawn(3, Duration::from_millis(50))
            .await
            .expect("spawn 3-node raft topology");
        let ev = topology.controller.arm_failpoint("any", "panic").await;
        match ev.outcome {
            ChaosOutcome::Skipped { .. } => {}
            other => panic!("expected Skipped, got {other:?}"),
        }
        Box::new(topology.controller).shutdown().await;
    }

    #[tokio::test]
    async fn spawn_zero_nodes_rejected() {
        match RaftTopology::spawn(0, Duration::from_millis(50)).await {
            Err(err) => assert!(
                format!("{err:#}").contains("at least one node"),
                "unexpected error: {err:#}",
            ),
            Ok(_) => panic!("spawn(0) should reject, got Ok"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wait_for_leader_times_out() {
        let topology = RaftTopology::spawn(3, Duration::from_millis(50))
            .await
            .expect("spawn 3-node raft topology");
        // Re-invoke `wait_for_leader` with a zero deadline. The deadline check
        // runs before the metrics poll on the first iteration, so the timeout
        // branch always fires regardless of whether the cluster has a leader.
        // This exercises the diagnostic snapshot path that real users only see
        // when a cluster genuinely fails to elect.
        let result = wait_for_leader(&topology.controller.nodes, Duration::ZERO).await;
        match result {
            Err(err) => {
                let msg = format!("{err:#}");
                assert!(
                    msg.contains("no leader within") && msg.contains("snapshots:"),
                    "unexpected error: {msg}",
                );
            }
            Ok(()) => panic!("wait_for_leader with zero timeout should always bail"),
        }
        Box::new(topology.controller).shutdown().await;
    }

    #[test]
    fn open_log_store_errors_on_bad_path() {
        // Place a regular file in the ancestor chain so directory creation
        // beneath it fails with ENOTDIR for any uid (including root inside
        // a container, where DAC permission checks are bypassed).
        let tmp = tempfile::tempdir().expect("create tempdir");
        let blocker = tmp.path().join("blocker");
        std::fs::write(&blocker, b"").expect("create blocker file");
        let bad = blocker.join("stress-raft-test").join("log");
        match open_log_store(&bad) {
            Err(_) => {}
            Ok(_) => panic!("open_log_store should fail when an ancestor is not a directory"),
        }
    }

    #[cfg(feature = "stress-failpoints")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn arm_failpoint_returns_failed_on_invalid_action() {
        let topology = RaftTopology::spawn(3, Duration::from_millis(50))
            .await
            .expect("spawn 3-node raft topology");
        // The `fail` crate's action parser rejects gibberish; `arm_failpoint`
        // surfaces that as `ChaosOutcome::Failed`.
        let ev = topology
            .controller
            .arm_failpoint("stress-raft-test::fp_invalid", "not-a-real-action")
            .await;
        match ev.outcome {
            ChaosOutcome::Failed { ref reason } => {
                assert!(
                    reason.contains("fail::cfg"),
                    "expected fail::cfg in reason, got: {reason}",
                );
            }
            other => panic!("expected Failed, got {other:?}"),
        }
        Box::new(topology.controller).shutdown().await;
    }
}
