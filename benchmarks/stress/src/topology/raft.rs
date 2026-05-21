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
//! is shorter than `election_timeout_min`. The failpoint primitives are
//! stubbed to return `Skipped` until follow-up PRs land them.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use openraft::async_runtime::watch::WatchReceiver;
use openraft::{Config, Raft, SnapshotPolicy};
use openraft_toolkit::test_fakes::MemNetwork;
use openraft_toolkit::{Flat, RocksdbLogStore};
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Instant, sleep};
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, OpenraftPeer, StandaloneHost, TypeConfig,
};
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
    pub async fn spawn(node_count: usize, grace: Duration) -> anyhow::Result<Self> {
        if node_count == 0 {
            bail!("raft topology requires at least one node");
        }

        let network = MemNetwork::<TypeConfig>::new();
        let config = raft_config()?;

        let mut nodes: Vec<RaftNode> = Vec::with_capacity(node_count);
        let mut server_handles: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(node_count);

        for raw_id in 1..=node_count {
            let node_id_u64 = raw_id as u64;
            let log_dir = TempDir::new().context("raft topology: create tempdir")?;
            let log_store = open_log_store(log_dir.path())
                .with_context(|| format!("raft topology: open log store for node {node_id_u64}"))?;
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

            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .context("raft topology: bind loopback")?;
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
                },
            );
        }
        nodes[0]
            .raft
            .initialize(membership)
            .await
            .context("raft topology: initialize membership")?;

        wait_for_leader(&nodes).await?;

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

async fn wait_for_leader(nodes: &[RaftNode]) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        for node in nodes {
            let metrics = node.raft.metrics().borrow_watched().clone();
            if metrics.current_leader.is_some() {
                return Ok(());
            }
        }
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
                "raft topology: no leader within 2s; snapshots:\n  {}",
                snapshots.join("\n  ")
            );
        }
        sleep(Duration::from_millis(25)).await;
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
            // Election timeout is 300-600ms; openraft can also need a few
            // heartbeat-interval ticks (100ms each) before followers escalate
            // to a candidate after losing the leader. 1500ms keeps the chaos
            // window short while reliably producing a re-election in CI; the
            // 750ms baseline from the sketch was tight enough to flake.
            tokio::time::sleep(Duration::from_millis(1500)).await;
            partitions.heal(leader_raft_id);
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
            tokio::time::sleep(dur).await;
            partitions.heal(leader_raft_id);
            ChaosOutcome::Applied
        })
        .await
    }

    async fn arm_failpoint(&self, name: &str, _action: &str) -> ChaosEvent {
        let kind = ChaosKind::FailpointArm { name: name.into() };
        timed_event(kind, self.grace, || async {
            ChaosOutcome::Skipped {
                reason: "arm_failpoint not yet implemented for raft topology".into(),
            }
        })
        .await
    }

    async fn disarm_failpoint(&self, name: &str) -> ChaosEvent {
        let kind = ChaosKind::FailpointDisarm { name: name.into() };
        timed_event(kind, self.grace, || async {
            ChaosOutcome::Skipped {
                reason: "disarm_failpoint not yet implemented for raft topology".into(),
            }
        })
        .await
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

        // Poll for a different leader; election timeout is 300-600ms, so up
        // to 2s of polling is comfortably above the worst case while still
        // failing fast if no re-election occurs.
        let mut new_leader = None;
        for _ in 0..40 {
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
}
