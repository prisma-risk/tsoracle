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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use omnipaxos::messages::Message;
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep};
use tsoracle_driver_paxos::{HighWaterCommand, PaxosDriver, SnapshotPolicy, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::{MessageSink, PeerEndpoint, TsoPeer};
use tsoracle_paxos_toolkit::storage::RocksdbStorage;
use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;
use tsoracle_paxos_toolkit::test_fakes::partition::PartitionController;
use tsoracle_server::Server;

use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome};
use crate::topology::{ChaosController, NodeId, timed_event};

#[allow(dead_code)]
const TICK_INTERVAL: Duration = Duration::from_millis(20);
const ELECTION_TICK_TIMEOUT: u64 = 5;
const RESEND_MESSAGE_TICK_TIMEOUT: u64 = 5;

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

const PAXOS_CF: &str = "tso_paxos";

#[allow(dead_code)]
fn open_paxos_storage(dir: &std::path::Path) -> anyhow::Result<RocksdbStorage<HighWaterCommand>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![ColumnFamilyDescriptor::new(PAXOS_CF, Options::default())];
    let db = Arc::new(
        DB::open_cf_descriptors(&opts, dir, cfs)
            .context("paxos topology: open rocksdb at storage dir")?,
    );
    RocksdbStorage::open_in(db, PAXOS_CF)
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

/// Build an `OmniPaxosConfig` for node `node_id` in the given `cluster`.
/// Election timing matches `examples/paxos-embedded` — tick interval 20 ms,
/// election timeout 5 ticks (~100 ms). This is shorter than the raft
/// topology's 300–600 ms election timeout, but well inside `grace_paxos`'s
/// 1000 ms default so the fence-freshness gate still has headroom.
#[allow(dead_code)]
fn paxos_config(node_id: u64, cluster: &ClusterConfig) -> OmniPaxosConfig {
    OmniPaxosConfig {
        cluster_config: cluster.clone(),
        server_config: ServerConfig {
            pid: node_id,
            election_tick_timeout: ELECTION_TICK_TIMEOUT,
            resend_message_tick_timeout: RESEND_MESSAGE_TICK_TIMEOUT,
            ..Default::default()
        },
    }
}

/// Build the toolkit peer list for node `my_id`: every *other* node paired with
/// its service endpoint.
///
/// The endpoint must be a **scheme-less** `host:port`. `StandaloneHost` surfaces
/// it verbatim as `LeaderState::Follower::leader_endpoint`, which the server's
/// `run_leader_watch` guard requires to be scheme-less (a `http(s)://` hint is
/// silently dropped by a TLS client's redirect path, so a follower would point
/// clients at an unreachable leader). A `SocketAddr`'s `Display` is exactly that
/// shape; the in-process `MeshSink` routes by node id, so this string is only
/// ever used as a leader hint, never dialed. See
/// `build_toolkit_peers_yields_scheme_less_endpoints`.
#[allow(dead_code)]
#[allow(clippy::expect_used)] // SocketAddr's Display is provably scheme-less host:port.
fn build_toolkit_peers(tso_endpoints: &[(u64, SocketAddr)], my_id: u64) -> Vec<TsoPeer> {
    tso_endpoints
        .iter()
        .filter(|(peer_id, _)| *peer_id != my_id)
        .map(|(peer_id, peer_addr)| TsoPeer {
            node_id: *peer_id,
            endpoint: PeerEndpoint::try_from(peer_addr.to_string())
                .expect("SocketAddr's Display is scheme-less host:port"),
        })
        .collect()
}

/// `MessageSink` that routes outbound paxos messages back through the
/// shared `MemNetwork`. Lifted from `examples/paxos-embedded`.
#[allow(dead_code)]
struct MeshSink {
    network: Arc<MemNetwork<HighWaterCommand>>,
}

#[async_trait]
impl MessageSink<HighWaterCommand> for MeshSink {
    async fn send(&self, message: Message<HighWaterCommand>) {
        self.network.deliver(message).await;
    }
}

/// Drains the per-node `MemNetwork` inbox into the node's OmniPaxos handle.
/// Lifted from `examples/paxos-embedded`. One pump task per node; lives for
/// the lifetime of the topology.
#[allow(dead_code)]
async fn run_inbox_pump(
    omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, RocksdbStorage<HighWaterCommand>>>>,
    mut inbox: mpsc::Receiver<Message<HighWaterCommand>>,
) {
    while let Some(message) = inbox.recv().await {
        omnipaxos.lock().handle_incoming(message);
    }
}

impl PaxosTopology {
    /// Boot an N-node in-process cluster, each node running its own
    /// `tsoracle::Server` bound to a fresh loopback port. Returns once
    /// a leader has been observed.
    ///
    /// Uses [`DefaultPaxosBackend`] for the spawn-time I/O steps. Tests
    /// that need to exercise the failure paths call [`Self::spawn_with`]
    /// with a fake backend.
    pub async fn spawn(node_count: usize, grace: Duration) -> anyhow::Result<Self> {
        Self::spawn_with(&DefaultPaxosBackend, node_count, grace).await
    }

    /// Like [`Self::spawn`] but with a caller-supplied [`PaxosBackend`]
    /// for the spawn-time I/O. Useful for tests that want to inject
    /// failures at the storage-preparation or listener-binding steps.
    pub async fn spawn_with(
        backend: &dyn PaxosBackend,
        node_count: usize,
        grace: Duration,
    ) -> anyhow::Result<Self> {
        if node_count == 0 {
            bail!("paxos topology requires at least one node");
        }

        let network: Arc<MemNetwork<HighWaterCommand>> = Arc::new(MemNetwork::new());
        let node_ids: Vec<u64> = (1..=node_count as u64).collect();
        let cluster = ClusterConfig {
            configuration_id: 1,
            nodes: node_ids.clone(),
            flexible_quorum: None,
        };

        // Allocate every node's loopback listener first so we know every
        // node's address before constructing per-node `StandaloneHost`s
        // (each `StandaloneHost` needs its peers' addresses for the
        // `LeaderHint` follower-redirect trailer).
        let mut listeners: Vec<(u64, TcpListener, SocketAddr)> = Vec::with_capacity(node_count);
        for &id in &node_ids {
            let listener = backend.bind_loopback(id).await?;
            let addr = listener.local_addr()?;
            listeners.push((id, listener, addr));
        }

        let tso_endpoints: Vec<(u64, SocketAddr)> =
            listeners.iter().map(|(id, _, addr)| (*id, *addr)).collect();

        let mut nodes: Vec<PaxosNode> = Vec::with_capacity(node_count);
        let mut server_handles: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(node_count);

        for (id, listener, addr) in listeners {
            // ---- Storage ----
            let (storage_dir, storage) = backend.prepare_node_storage(id).await?;

            // ---- OmniPaxos handle ----
            let omnipaxos_config = paxos_config(id, &cluster);
            let omnipaxos = Arc::new(Mutex::new(omnipaxos_config.build(storage).with_context(
                || format!("paxos topology: build OmniPaxos handle for node {id}"),
            )?));

            // ---- Inbox pump ----
            let inbox: mpsc::Receiver<Message<HighWaterCommand>> = network.register(id);
            let inbox_omnipaxos = omnipaxos.clone();
            tokio::spawn(async move {
                run_inbox_pump(inbox_omnipaxos, inbox).await;
            });

            // ---- StandaloneHost ----
            let toolkit_peers = build_toolkit_peers(&tso_endpoints, id);
            let mut host = StandaloneHost::builder()
                .omnipaxos(omnipaxos.clone())
                .my_node_id(id)
                .peers(toolkit_peers)
                .tick_interval(TICK_INTERVAL)
                .snapshot_policy(SnapshotPolicy::disabled())
                .build()
                .with_context(|| format!("paxos topology: build StandaloneHost for node {id}"))?;
            let leader_subscriber = host.take_leader_subscriber().ok_or_else(|| {
                anyhow::anyhow!("paxos topology: leader subscriber for node {id} already taken")
            })?;

            let sink = Arc::new(MeshSink {
                network: network.clone(),
            });
            host.start(sink)
                .context("paxos topology: host not already running")?;

            // ---- Driver + tsoracle server ----
            let driver = Arc::new(PaxosDriver::new(host, leader_subscriber));
            let server = Server::builder()
                .consensus_driver(driver)
                .build()
                .map_err(|e| anyhow::anyhow!("paxos topology: server build: {e:?}"))?;

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

            nodes.push(PaxosNode {
                node_id: NodeId(id as u32),
                endpoint,
                omnipaxos,
                shutdown_tx: Mutex::new(Some(shutdown_tx)),
                _storage_dir: storage_dir,
            });
            server_handles.push(handle);
        }

        wait_for_leader(&nodes, Duration::from_secs(5)).await?;

        Ok(PaxosTopology {
            controller: PaxosController {
                nodes,
                network,
                grace,
            },
            server_handles,
        })
    }
}

async fn wait_for_leader(nodes: &[PaxosNode], timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if Instant::now() >= deadline {
            let snapshots: Vec<String> = nodes
                .iter()
                .map(|n| {
                    let leader = n.omnipaxos.lock().get_current_leader();
                    format!("node {} sees leader={:?}", n.node_id.0, leader)
                })
                .collect();
            bail!(
                "paxos topology: no leader within {timeout:?}; snapshots:\n  {}",
                snapshots.join("\n  ")
            );
        }
        for node in nodes {
            if node.omnipaxos.lock().get_current_leader().is_some() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// RAII guard that restores reachability for a single node on drop. Used to
/// make `kill_leader` / `pause_leader` cancel-safe: if the harness's outer
/// `select!` (e.g. the `--duration` timer) drops the chaos future while it
/// is parked at the mid-window `sleep`, the guard's `Drop` still fires and
/// restores reachability — without this, the cluster would remain
/// partitioned for the rest of the run.
#[allow(dead_code)]
struct HealOnDrop {
    partition: Arc<PartitionController>,
    node: u64,
}

impl Drop for HealOnDrop {
    fn drop(&mut self) {
        self.partition.restore(self.node);
    }
}

// `ChaosController` impl. Method bodies are stubbed; subsequent commits
// replace each `unimplemented!()` with the real implementation.

#[async_trait]
impl ChaosController for PaxosController {
    async fn kill_leader(&self) -> ChaosEvent {
        // Find the OmniPaxos pid (u64) of the current leader by polling each
        // node's handle. The pid is what `PartitionController` uses to gate
        // edges, so we keep it in its native u64 width rather than going
        // through `current_leader()`'s narrowed `NodeId(u32)`.
        let leader_pid: Option<u64> = self
            .nodes
            .iter()
            .find_map(|n| n.omnipaxos.lock().get_current_leader());
        let Some(leader_pid) = leader_pid else {
            return timed_event(ChaosKind::LeaderKill, self.grace, || async {
                ChaosOutcome::Skipped {
                    reason: "no current leader".into(),
                }
            })
            .await;
        };
        let partition = self.network.partition();
        timed_event(ChaosKind::LeaderKill, self.grace, move || async move {
            partition.isolate(leader_pid);
            // Heal-on-drop guarantees reachability is restored even if the
            // outer future is cancelled at the `sleep` below.
            let _guard = HealOnDrop {
                partition,
                node: leader_pid,
            };
            // OmniPaxos BLE runs on a 20 ms tick with `election_tick_timeout = 5`
            // (~100 ms for followers to escalate after losing the leader).
            // 1500 ms keeps the chaos window short while reliably producing a
            // re-election; matches raft.rs's window.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            ChaosOutcome::Applied
        })
        .await
    }

    async fn pause_leader(&self, dur: Duration) -> ChaosEvent {
        // Same leader-discovery shape as `kill_leader` — see its comment.
        let leader_pid: Option<u64> = self
            .nodes
            .iter()
            .find_map(|n| n.omnipaxos.lock().get_current_leader());
        let Some(leader_pid) = leader_pid else {
            return timed_event(ChaosKind::LeaderPause, self.grace, || async {
                ChaosOutcome::Skipped {
                    reason: "no current leader".into(),
                }
            })
            .await;
        };
        let partition = self.network.partition();
        timed_event(ChaosKind::LeaderPause, self.grace, move || async move {
            partition.isolate(leader_pid);
            let _guard = HealOnDrop {
                partition,
                node: leader_pid,
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
            if let Some(leader_pid) = node.omnipaxos.lock().get_current_leader() {
                // OmniPaxos pids are assigned 1..=node_count by spawn_with;
                // the cast is safe for any sane cluster size.
                return Some(NodeId(leader_pid as u32));
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
    use std::time::Instant as StdInstant;

    // The scheme-less invariant the original form of this test guarded is now
    // enforced at the type level by `PeerEndpoint::try_from`: a scheme-bearing
    // endpoint would panic inside `build_toolkit_peers`'s `.expect(...)`
    // before this assertion runs. The test is kept as a smoke-check that
    // construction succeeds and the self-filter still holds.
    #[test]
    fn build_toolkit_peers_yields_scheme_less_endpoints() {
        let tso_endpoints: Vec<(u64, SocketAddr)> = vec![
            (1, "127.0.0.1:5001".parse().unwrap()),
            (2, "127.0.0.1:5002".parse().unwrap()),
            (3, "127.0.0.1:5003".parse().unwrap()),
        ];

        let peers = build_toolkit_peers(&tso_endpoints, 2);

        assert_eq!(peers.len(), 2, "a node must not list itself as a peer");
        assert!(
            peers.iter().all(|peer| peer.node_id != 2),
            "node 2 must be filtered from its own peer list, got {peers:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_3_nodes_reports_endpoints_and_leader() {
        let topology = PaxosTopology::spawn(3, Duration::from_millis(50))
            .await
            .expect("spawn 3-node paxos topology");
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
        let topology = PaxosTopology::spawn(3, Duration::from_millis(1000))
            .await
            .expect("spawn 3-node paxos topology");
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

        // Poll for a different leader. OmniPaxos's BLE election runs on a
        // 20 ms tick, so a re-election typically completes within a few
        // hundred ms after the partition window. The 30 s wall-clock cap
        // tolerates CI slowdown.
        let deadline = StdInstant::now() + Duration::from_secs(30);
        let mut new_leader = None;
        while StdInstant::now() < deadline {
            if let Some(candidate) = topology.controller.current_leader()
                && candidate != original_leader
            {
                new_leader = Some(candidate);
                break;
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
                        let leader = n.omnipaxos.lock().get_current_leader();
                        format!("node {} sees leader={:?}", n.node_id.0, leader)
                    })
                    .collect();
                panic!(
                    "re-election should have produced a different leader (was {:?}); \
                     snapshots:\n  {}",
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
        // the `HealOnDrop` guard, that cancellation parked at the mid-window
        // `sleep` and `restore` never ran, stranding the cluster partitioned
        // for the rest of the run. The guard's `Drop` impl is what makes
        // this test pass: the partition is restored even though the chaos
        // future never reached its end-of-window point.
        let topology = PaxosTopology::spawn(3, Duration::from_millis(1000))
            .await
            .expect("spawn 3-node paxos topology");
        let leader_pid: u64 = topology
            .controller
            .nodes
            .iter()
            .find_map(|n| n.omnipaxos.lock().get_current_leader())
            .expect("a leader at boot");
        let partition = topology.controller.network.partition();
        // kill_leader's mid-window sleep is 1500ms; cancelling at 50ms
        // reliably parks the future inside it on any sane machine.
        match tokio::time::timeout(Duration::from_millis(50), topology.controller.kill_leader())
            .await
        {
            Err(_elapsed) => {} // expected: future was dropped mid-sleep
            Ok(event) => panic!(
                "kill_leader should not have finished within 50ms; got {:?}",
                event.outcome,
            ),
        }
        // After the cancel, the partition guard must have restored
        // reachability — i.e., the leader pid is no longer in the isolated
        // set. `is_blocked(x, x)` returns true iff x is isolated, so the
        // probe below asserts the un-isolated state.
        assert!(
            !partition.is_blocked(leader_pid, leader_pid),
            "partition for node {leader_pid} must be restored after the \
             chaos future is dropped; the node is still in the isolated set"
        );
        Box::new(topology.controller).shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pause_leader_returns_applied() {
        let topology = PaxosTopology::spawn(3, Duration::from_millis(1000))
            .await
            .expect("spawn 3-node paxos topology");
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
    impl PaxosBackend for FailingBackend {
        async fn prepare_node_storage(
            &self,
            id: u64,
        ) -> anyhow::Result<(TempDir, RocksdbStorage<HighWaterCommand>)> {
            if self.fail_step == SpawnStep::PrepareStorage && id == self.fail_at_node {
                bail!("injected: prepare_node_storage failed for node {id}");
            }
            DefaultPaxosBackend.prepare_node_storage(id).await
        }

        async fn bind_loopback(&self, id: u64) -> anyhow::Result<TcpListener> {
            if self.fail_step == SpawnStep::BindLoopback && id == self.fail_at_node {
                bail!("injected: bind_loopback failed for node {id}");
            }
            DefaultPaxosBackend.bind_loopback(id).await
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_propagates_storage_failure() {
        let backend = FailingBackend {
            fail_step: SpawnStep::PrepareStorage,
            fail_at_node: 1,
        };
        match PaxosTopology::spawn_with(&backend, 3, Duration::from_millis(50)).await {
            Err(err) => {
                let msg = format!("{err:#}");
                assert!(
                    msg.contains("prepare_node_storage failed"),
                    "expected storage-failure message, got: {msg}"
                );
            }
            Ok(_) => panic!("spawn should propagate the injected storage failure"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_propagates_bind_failure() {
        let backend = FailingBackend {
            fail_step: SpawnStep::BindLoopback,
            fail_at_node: 2,
        };
        match PaxosTopology::spawn_with(&backend, 3, Duration::from_millis(50)).await {
            Err(err) => {
                let msg = format!("{err:#}");
                assert!(
                    msg.contains("bind_loopback failed"),
                    "expected bind-failure message, got: {msg}"
                );
            }
            Ok(_) => panic!("spawn should propagate the injected bind failure"),
        }
    }

    // Runs under tokio virtual time (`start_paused`). The whole topology — the
    // in-process `MemNetwork`, the per-node runner `interval` ticks, election,
    // and `wait_for_leader`'s poll `sleep` — advances in simulated time, and so
    // does the completion guard below. The tonic servers serve over real
    // loopback but have no peer connections (peers talk via `MemNetwork`), so a
    // healthy graceful drain finishes the instant its shutdown oneshot fires.
    // `start_paused` implies the current-thread runtime; the storage and paxos
    // work is short and cooperative, so a single worker is sufficient.
    #[tokio::test(start_paused = true)]
    async fn shutdown_completes_server_tasks() {
        let topology = PaxosTopology::spawn(3, Duration::from_millis(1000))
            .await
            .expect("spawn 3-node paxos topology");
        let server_handles = topology.server_handles;
        // Fire the per-node shutdown signals.
        Box::new(topology.controller).shutdown().await;
        // Liveness guard, not a latency assertion. Each server task must drain
        // and complete once its shutdown oneshot fires; a `shutdown` that failed
        // to trigger the drain path would leave the task running forever. Under
        // virtual time the timeout only elapses when the runtime goes idle with
        // the handle unfinished — i.e. a genuine hang — so it can never trip on
        // CI scheduling starvation (the old wall-clock bound's flake), and the
        // bound is simulated time that costs no wall clock. Any finite value
        // distinguishes "drained" from "hung".
        for (idx, handle) in server_handles.into_iter().enumerate() {
            match tokio::time::timeout(Duration::from_secs(30), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => panic!("server task {idx} did not exit cleanly: {join_err:?}"),
                Err(_elapsed) => {
                    panic!("server task {idx} did not complete after shutdown (drain hung?)")
                }
            }
        }
    }

    #[cfg(not(feature = "stress-failpoints"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn arm_failpoint_returns_skipped_without_feature() {
        let topology = PaxosTopology::spawn(3, Duration::from_millis(1000))
            .await
            .expect("spawn 3-node paxos topology");
        let event = topology
            .controller
            .arm_failpoint("paxos-coverage-test", "panic")
            .await;
        match &event.outcome {
            ChaosOutcome::Skipped { reason } => {
                assert!(
                    reason.contains("stress-failpoints"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("expected Skipped, got {other:?}"),
        }
        let event = topology
            .controller
            .disarm_failpoint("paxos-coverage-test")
            .await;
        match &event.outcome {
            ChaosOutcome::Skipped { reason } => {
                assert!(
                    reason.contains("stress-failpoints"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("expected Skipped, got {other:?}"),
        }
        Box::new(topology.controller).shutdown().await;
    }

    #[cfg(feature = "stress-failpoints")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn arm_then_disarm_failpoint_round_trip() {
        let topology = PaxosTopology::spawn(3, Duration::from_millis(1000))
            .await
            .expect("spawn 3-node paxos topology");
        // Use a unique name so concurrent test runs don't race on a shared
        // failpoint registry slot.
        let name = "paxos-stress-coverage-arm-disarm";
        let armed = topology.controller.arm_failpoint(name, "off").await;
        assert!(
            armed.outcome.is_applied(),
            "arm_failpoint expected Applied, got {:?}",
            armed.outcome
        );
        let disarmed = topology.controller.disarm_failpoint(name).await;
        assert!(
            disarmed.outcome.is_applied(),
            "disarm_failpoint expected Applied, got {:?}",
            disarmed.outcome
        );
        Box::new(topology.controller).shutdown().await;
    }
}
