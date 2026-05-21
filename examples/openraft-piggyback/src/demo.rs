//! In-process 3-node piggyback demo.
//!
//! Boots three openraft nodes connected via `openraft_toolkit::MemNetwork`,
//! each running a tsoracle::Server bound to a unique loopback port. Drives a
//! scripted sequence: KV writes via the host service, GetTs via a
//! tsoracle-client, then a failover, asserting both halves of the freshness
//! invariant survive.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use openraft::async_runtime::watch::WatchReceiver;
use openraft::{Config, Raft, SnapshotPolicy};
use openraft_toolkit::test_fakes::MemNetwork;
use openraft_toolkit::{Flat, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tokio::time::{Instant, sleep};
use tracing::info;
use tsoracle_client::Client as TsoClient;
use tsoracle_core::Timestamp;
use tsoracle_driver_openraft::OpenraftDriver;
use tsoracle_server::Server as TsoServer;

use crate::host_service::{
    HostCommand, HostPeer, HostStateMachine, HostTypeConfig, KvOp, PiggybackHost,
};

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

/// Per-node state held by the demo across the script.
struct Node {
    id: u64,
    raft: Raft<HostTypeConfig, HostStateMachine>,
    sm: HostStateMachine,
    tso_port: u16,
    /// `Some(_)` until we shut it down (for the failover step).
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// Holds the rocksdb dir open for the node's lifetime.
    _log_dir: TempDir,
}

/// Per-demo result, returned so the smoke test in `tests/smoke.rs` can assert
/// on it without re-running the demo internals.
pub struct DemoOutcome {
    pub pre_failover_last_ts: Timestamp,
    pub pre_failover_high_water: u64,
    pub post_failover_first_ts: Timestamp,
    pub post_failover_high_water: u64,
    pub kv_after_writes: BTreeMap<String, Vec<u8>>,
    pub high_water_unchanged_across_getts: bool,
}

fn open_log_store(dir: &std::path::Path) -> anyhow::Result<RocksdbLogStore<HostTypeConfig, Flat>> {
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
            // See README "Snapshots disabled" — the host SM is in-memory only.
            snapshot_policy: SnapshotPolicy::Never,
            ..Default::default()
        }
        .validate()?,
    ))
}

async fn build_cluster() -> anyhow::Result<(Vec<Node>, Arc<MemNetwork<HostTypeConfig>>)> {
    let network = MemNetwork::<HostTypeConfig>::new();
    let config = raft_config()?;
    let mut nodes: Vec<Node> = Vec::new();

    for id in [1u64, 2, 3] {
        let log_dir = TempDir::new()?;
        let log_store = open_log_store(log_dir.path())?;
        let sm = HostStateMachine::new();
        let sm_for_host = sm.clone();
        let raft = Raft::<HostTypeConfig, HostStateMachine>::new(
            id,
            config.clone(),
            network.factory_for(id),
            log_store,
            sm,
        )
        .await?;
        network.register(id, raft.clone());

        let host = PiggybackHost::new(raft.clone(), sm_for_host.clone());
        let driver = OpenraftDriver::new(host);
        let server = TsoServer::builder().consensus_driver(driver).build()?;

        // Bind on port 0 so the OS assigns a free port, avoiding conflicts when
        // the smoke test runs alongside other services or in parallel CI jobs.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let tso_port = listener.local_addr()?.port();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx.await;
            };
            if let Err(e) = server.serve_with_listener(listener, shutdown).await {
                tracing::error!(error = ?e, "tsoracle server died on port {tso_port}");
            }
        });

        nodes.push(Node {
            id,
            raft,
            sm: sm_for_host,
            tso_port,
            shutdown: Some(shutdown_tx),
            _log_dir: log_dir,
        });
    }

    // Initialize membership on node 1.
    let mut membership = BTreeMap::new();
    for node in &nodes {
        membership.insert(
            node.id,
            HostPeer {
                addr: format!("mem-node-{}", node.id),
            },
        );
    }
    nodes[0].raft.initialize(membership).await?;

    Ok((nodes, network))
}

async fn wait_for_leader(nodes: &[Node]) -> anyhow::Result<usize> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for (idx, node) in nodes.iter().enumerate() {
            if let Some(leader_id) = node.raft.current_leader().await {
                if leader_id == node.id {
                    return Ok(idx);
                }
            }
        }
        if Instant::now() >= deadline {
            // Take a metrics snapshot for the error to aid diagnosis.
            let snapshots: Vec<_> = nodes
                .iter()
                .map(|n| {
                    let metrics = n.raft.metrics().borrow_watched().clone();
                    format!(
                        "node {} state={:?} term={:?}",
                        n.id, metrics.state, metrics.current_term
                    )
                })
                .collect();
            bail!(
                "no leader within 5s; snapshots:\n  {}",
                snapshots.join("\n  ")
            );
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Build a tsoracle client that knows about every node's loopback port —
/// `Client` rotates across endpoints and honors `LeaderHint` trailers so a
/// follower-bound request finds the leader.
async fn build_client(nodes: &[Node]) -> anyhow::Result<TsoClient> {
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|n| format!("http://127.0.0.1:{}", n.tso_port))
        .collect();
    Ok(TsoClient::connect(endpoints).await?)
}

/// Demo helper: return the leader's current high-water by reading its SM
/// directly. This is NOT the production read path — production code goes
/// through `ConsensusDriver::load_high_water`, which `PiggybackHost`
/// implements with an `ensure_linearizable` barrier. The shortcut is safe
/// here because we already confirmed leadership via `current_leader()` and
/// the demo runs single-threaded against committed in-process state.
async fn high_water_of_leader(nodes: &[Node]) -> u64 {
    for node in nodes {
        if let Some(leader_id) = node.raft.current_leader().await {
            if leader_id == node.id {
                return node.sm.high_water().await;
            }
        }
    }
    0
}

pub async fn run_demo() -> anyhow::Result<DemoOutcome> {
    let (mut nodes, _network) = build_cluster().await?;
    let leader_idx = wait_for_leader(&nodes).await?;

    // Give the tsoracle-server fence a moment to fire on the new leader. The
    // fence path runs once leadership is observed via the leadership_events
    // stream and persists `serving_floor + failover_advance`. We poll on the
    // leader's SM until it advances above 0 or we hit a short deadline.
    let fence_deadline = Instant::now() + Duration::from_secs(3);
    while nodes[leader_idx].sm.high_water().await == 0 && Instant::now() < fence_deadline {
        sleep(Duration::from_millis(25)).await;
    }
    let initial_high_water = nodes[leader_idx].sm.high_water().await;
    info!(
        leader = nodes[leader_idx].id,
        high_water = initial_high_water,
        "post-fence high-water (driver persisted serving_floor + failover_advance)"
    );
    println!("\n--- Leader elected, fence ran ---");
    println!("leader: node {}", nodes[leader_idx].id);
    println!("post-fence high-water: {initial_high_water}");

    // ---- Section 1: host KV writes go through the same raft as TSO ----
    println!("\n--- Host KV writes (ride the same raft) ---");
    let leader_raft = nodes[leader_idx].raft.clone();
    for (key, value) in [("alpha", b"first".to_vec()), ("beta", b"second".to_vec())] {
        let resp = leader_raft
            .client_write(HostCommand::Kv(KvOp::Put {
                key: key.into(),
                value: value.clone(),
            }))
            .await
            .map_err(|e| anyhow!("kv put: {e}"))?;
        // `kv` may be Some(prior_value) on overwrites; we only assert that the
        // TSO field stays None on a Kv apply (the half-isolation invariant).
        assert!(
            resp.data.tso.is_none(),
            "Kv apply must leave HostApplied.tso = None (got {:?})",
            resp.data.tso,
        );
        println!("  put {key} = {} bytes (tso untouched)", value.len());
    }
    let kv_after_writes = nodes[leader_idx].sm.kv_dump();
    let high_water_after_kv = nodes[leader_idx].sm.high_water().await;
    println!(
        "host SM after KV writes: kv keys={:?} high_water={high_water_after_kv}",
        kv_after_writes.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        high_water_after_kv, initial_high_water,
        "KV writes must not mutate the TSO field"
    );

    // ---- Section 2: GetTs is allocator-served; doesn't advance high-water ----
    println!("\n--- TSO bursts (allocator-served, no consensus per call) ---");
    let client = build_client(&nodes).await?;
    let high_water_before_burst = nodes[leader_idx].sm.high_water().await;
    // The tsoracle server transitions to Serving just after persist_high_water
    // returns, which is slightly after our SM high-water poll detected > 0.
    // Retry the first GetTs until it succeeds (or a 3s deadline elapses) to
    // bridge that small window.
    let first_ts = {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match client.get_ts().await {
                Ok(ts) => break ts,
                Err(e) if Instant::now() < deadline => {
                    tracing::warn!(error = ?e, "get_ts retrying");
                    sleep(Duration::from_millis(25)).await;
                }
                Err(e) => return Err(anyhow!("get_ts failed after 3s retries: {e}")),
            }
        }
    };
    println!("  GetTs #1: {first_ts:?}");
    let mut last_ts: Option<Timestamp> = Some(first_ts);
    for i in 2..=5u32 {
        let ts = client.get_ts().await?;
        if let Some(prev) = last_ts {
            assert!(ts > prev, "timestamps must be strictly monotonic");
        }
        last_ts = Some(ts);
        println!("  GetTs #{i}: {ts:?}");
    }
    let high_water_after_burst = high_water_of_leader(&nodes).await;
    let high_water_unchanged_across_getts = high_water_after_burst == high_water_before_burst;
    println!(
        "high-water before burst: {high_water_before_burst}; after burst: {high_water_after_burst} (unchanged = {high_water_unchanged_across_getts})"
    );
    let pre_failover_last_ts = last_ts.context("at least one timestamp was issued")?;
    let pre_failover_high_water = high_water_after_burst;

    // ---- Section 3: failover preserves monotonicity ----
    println!("\n--- Failover: shut down leader, observe new leader fence ---");
    // Shut down the leader's raft so a survivor elects.
    let old_leader_id = nodes[leader_idx].id;
    nodes[leader_idx].raft.shutdown().await.ok();
    // Also stop its tsoracle server so the client doesn't keep trying it.
    if let Some(tx) = nodes[leader_idx].shutdown.take() {
        let _ = tx.send(());
    }
    println!("  shut down node {old_leader_id}; waiting for new leader...");

    let survivors: Vec<&Node> = nodes
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != leader_idx)
        .map(|(_, n)| n)
        .collect();
    let new_leader_deadline = Instant::now() + Duration::from_secs(10);
    let new_leader_id = loop {
        let mut found_id: Option<u64> = None;
        for survivor in &survivors {
            if let Some(leader_id) = survivor.raft.current_leader().await {
                if leader_id == survivor.id {
                    found_id = Some(survivor.id);
                    break;
                }
            }
        }
        if let Some(leader_id) = found_id {
            break Ok(leader_id);
        }
        if Instant::now() >= new_leader_deadline {
            break Err(anyhow!("no new leader within 10s"));
        }
        sleep(Duration::from_millis(50)).await;
    }?;
    println!("  new leader: node {new_leader_id}");

    // Wait for the new leader's fence to fire.
    let mut post_failover_high_water = 0u64;
    let fence_deadline = Instant::now() + Duration::from_secs(5);
    while post_failover_high_water <= pre_failover_high_water && Instant::now() < fence_deadline {
        for survivor in &survivors {
            if survivor.id == new_leader_id {
                post_failover_high_water = survivor.sm.high_water().await;
            }
        }
        if post_failover_high_water <= pre_failover_high_water {
            sleep(Duration::from_millis(25)).await;
        }
    }
    println!("  post-failover high-water: {post_failover_high_water}");
    assert!(
        post_failover_high_water > pre_failover_high_water,
        "post-failover high-water ({post_failover_high_water}) must exceed pre-failover ({pre_failover_high_water})"
    );

    // Issue one more GetTs through the client; should land on the new leader.
    // The new leader's tsoracle server transitions to Serving shortly after the
    // SM high-water advance we polled above; retry with backoff to bridge that
    // small window.
    let post_failover_first_ts = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client.get_ts().await {
                Ok(ts) => break ts,
                Err(e) if Instant::now() < deadline => {
                    tracing::warn!(error = ?e, "get_ts retrying");
                    sleep(Duration::from_millis(25)).await;
                }
                Err(e) => return Err(anyhow!("get_ts failed after 5s retries post-failover: {e}")),
            }
        }
    };
    println!("  first post-failover GetTs: {post_failover_first_ts:?}");
    assert!(
        post_failover_first_ts > pre_failover_last_ts,
        "post-failover timestamp ({post_failover_first_ts:?}) must exceed last pre-failover timestamp ({pre_failover_last_ts:?})"
    );

    // ---- Clean shutdown of remaining nodes ----
    for node in nodes.iter_mut() {
        // Only the leader we already shut down has `shutdown == None`.
        node.raft.shutdown().await.ok();
        if let Some(tx) = node.shutdown.take() {
            let _ = tx.send(());
        }
    }

    Ok(DemoOutcome {
        pre_failover_last_ts,
        pre_failover_high_water,
        post_failover_first_ts,
        post_failover_high_water,
        kv_after_writes,
        high_water_unchanged_across_getts,
    })
}
