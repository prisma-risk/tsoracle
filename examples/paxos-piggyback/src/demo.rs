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

//! In-process 3-node piggyback demo.
//!
//! Boots three OmniPaxos nodes connected via `MemNetwork`, each running a
//! tsoracle::Server bound to a unique loopback port. Drives a scripted
//! sequence: KV writes (appended via the leader's OmniPaxos handle), GetTs
//! via a tsoracle-client, then a failover — asserting both halves of the
//! freshness invariant survive.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use omnipaxos::messages::Message;
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tracing::info;
use tsoracle_client::Client as TsoClient;
use tsoracle_core::Timestamp;
use tsoracle_driver_paxos::PaxosDriver;
use tsoracle_paxos_toolkit::lifecycle::{MessageSink, PaxosRunner};
use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;
use tsoracle_server::{Server as TsoServer, ServingState};

use crate::host_service::{HostState, KvOp, MyAppCommand, PiggybackHost, drain_decided_into};

/// Tick interval for every node's runner. 20 ms with the toolkit defaults
/// keeps elections in the 100–200 ms range.
const TICK_INTERVAL: Duration = Duration::from_millis(20);

/// Per-node election + resend tick budget. Same values the test harness
/// uses; ample for an in-process cluster wired through MemNetwork.
const ELECTION_TICK_TIMEOUT: u64 = 5;
const RESEND_MESSAGE_TICK_TIMEOUT: u64 = 5;

/// Per-demo result, returned so the smoke test in `tests/smoke.rs` can
/// assert on it without re-running the demo internals.
pub struct DemoOutcome {
    pub pre_failover_last_ts: Timestamp,
    pub pre_failover_high_water: u64,
    pub post_failover_first_ts: Timestamp,
    pub post_failover_high_water: u64,
    pub kv_after_writes: BTreeMap<String, Vec<u8>>,
    pub high_water_unchanged_across_getts: bool,
}

// ---------------------------------------------------------------------------
// Mesh sink — outbound messages flow through the shared MemNetwork.
// ---------------------------------------------------------------------------

struct MeshSink {
    network: Arc<MemNetwork<MyAppCommand>>,
}

#[async_trait]
impl MessageSink<MyAppCommand> for MeshSink {
    async fn send(&self, message: Message<MyAppCommand>) {
        self.network.deliver(message).await;
    }
}

// ---------------------------------------------------------------------------
// Per-node state held by the demo across the script.
// ---------------------------------------------------------------------------

struct Node {
    id: u64,
    omnipaxos: Arc<Mutex<OmniPaxos<MyAppCommand, MemStorage<MyAppCommand>>>>,
    state: HostState,
    runner: Option<PaxosRunner<MyAppCommand, MemStorage<MyAppCommand>>>,
    pump_shutdowns: Vec<oneshot::Sender<()>>,
    pump_handles: Vec<JoinHandle<()>>,
    tso_port: u16,
    serving_state_rx: tokio::sync::watch::Receiver<ServingState>,
    tso_shutdown: Option<oneshot::Sender<()>>,
    tso_handle: Option<JoinHandle<()>>,
}

impl Node {
    async fn shutdown(&mut self) {
        if let Some(tx) = self.tso_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.tso_handle.take() {
            let _ = handle.await;
        }
        for tx in self.pump_shutdowns.drain(..) {
            let _ = tx.send(());
        }
        for handle in self.pump_handles.drain(..) {
            let _ = handle.await;
        }
        if let Some(mut runner) = self.runner.take() {
            runner.stop().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster bring-up.
// ---------------------------------------------------------------------------

async fn build_cluster() -> anyhow::Result<(Vec<Node>, Arc<MemNetwork<MyAppCommand>>, ClusterConfig)>
{
    let network: Arc<MemNetwork<MyAppCommand>> = Arc::new(MemNetwork::new());
    let node_ids: Vec<u64> = vec![1, 2, 3];
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };

    let mut nodes: Vec<Node> = Vec::new();
    for id in node_ids {
        nodes.push(build_node(id, &network, &cluster_config).await?);
    }
    Ok((nodes, network, cluster_config))
}

async fn build_node(
    id: u64,
    network: &Arc<MemNetwork<MyAppCommand>>,
    cluster_config: &ClusterConfig,
) -> anyhow::Result<Node> {
    // ---- OmniPaxos handle ----
    let server_config = ServerConfig {
        pid: id,
        election_tick_timeout: ELECTION_TICK_TIMEOUT,
        resend_message_tick_timeout: RESEND_MESSAGE_TICK_TIMEOUT,
        ..Default::default()
    };
    let omnipaxos_config = OmniPaxosConfig {
        cluster_config: cluster_config.clone(),
        server_config,
    };
    let omnipaxos = Arc::new(Mutex::new(
        omnipaxos_config
            .build(MemStorage::<MyAppCommand>::new())
            .context("build OmniPaxos handle")?,
    ));
    let state = HostState::new();

    // ---- Runner + leader stream ----
    let mut runner = PaxosRunner::new(omnipaxos.clone(), id, vec![], TICK_INTERVAL);
    let leader_stream = runner
        .take_leader_stream()
        .context("leader stream is fresh")?;
    let runner_apply_notify = runner.apply_notify();

    // ---- Pumps: inbox + apply ----
    let mut pump_shutdowns: Vec<oneshot::Sender<()>> = Vec::new();
    let mut pump_handles: Vec<JoinHandle<()>> = Vec::new();

    let inbox: mpsc::Receiver<Message<MyAppCommand>> = network.register(id);
    let inbox_omnipaxos = omnipaxos.clone();
    let (inbox_stop_tx, inbox_stop_rx) = oneshot::channel::<()>();
    let inbox_handle = tokio::spawn(async move {
        run_inbox_pump(inbox_omnipaxos, inbox, inbox_stop_rx).await;
    });
    pump_shutdowns.push(inbox_stop_tx);
    pump_handles.push(inbox_handle);

    let apply_omnipaxos = omnipaxos.clone();
    let apply_state = state.clone();
    let (apply_stop_tx, mut apply_stop_rx) = oneshot::channel::<()>();
    let apply_handle = tokio::spawn(async move {
        let mut cursor: u64 = 0;
        loop {
            tokio::select! {
                _ = runner_apply_notify.notified() => {
                    drain_decided_into(&apply_omnipaxos, &mut cursor, &apply_state);
                }
                _ = &mut apply_stop_rx => {
                    break;
                }
            }
        }
    });
    pump_shutdowns.push(apply_stop_tx);
    pump_handles.push(apply_handle);

    // ---- Start runner with mesh sink ----
    let sink = Arc::new(MeshSink {
        network: network.clone(),
    });
    runner.start(sink);

    // ---- Driver + tsoracle server ----
    let host = PiggybackHost::new(omnipaxos.clone(), state.clone(), id);
    let driver = Arc::new(PaxosDriver::new(host, leader_stream));
    let server = TsoServer::builder().consensus_driver(driver).build()?;
    let serving_state_rx = server.subscribe();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let tso_port = listener.local_addr()?.port();
    let (tso_shutdown_tx, tso_shutdown_rx) = oneshot::channel::<()>();
    let tso_handle = tokio::spawn(async move {
        let shutdown = async move {
            let _ = tso_shutdown_rx.await;
        };
        if let Err(err) = server.serve_with_listener(listener, shutdown).await {
            tracing::error!(error = ?err, port = tso_port, "tsoracle server died");
        }
    });

    Ok(Node {
        id,
        omnipaxos,
        state,
        runner: Some(runner),
        pump_shutdowns,
        pump_handles,
        tso_port,
        serving_state_rx,
        tso_shutdown: Some(tso_shutdown_tx),
        tso_handle: Some(tso_handle),
    })
}

/// Spawn-target: drain `inbox` into `omnipaxos.handle_incoming` until the
/// shutdown signal fires (the sender stays in `MemNetwork::senders` for the
/// process lifetime, so `inbox.recv()` does not naturally return `None`).
async fn run_inbox_pump(
    omnipaxos: Arc<Mutex<OmniPaxos<MyAppCommand, MemStorage<MyAppCommand>>>>,
    mut inbox: mpsc::Receiver<Message<MyAppCommand>>,
    mut stop_rx: oneshot::Receiver<()>,
) {
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

// ---------------------------------------------------------------------------
// Demo helpers.
// ---------------------------------------------------------------------------

async fn wait_for_leader(nodes: &[Node]) -> anyhow::Result<usize> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for (idx, node) in nodes.iter().enumerate() {
            if let Some(leader_id) = node.omnipaxos.lock().get_current_leader() {
                if leader_id == node.id {
                    return Ok(idx);
                }
            }
        }
        if Instant::now() >= deadline {
            bail!("no leader within 5s");
        }
        sleep(Duration::from_millis(25)).await;
    }
}

async fn build_client(nodes: &[Node]) -> anyhow::Result<TsoClient> {
    let endpoints: Vec<String> = nodes
        .iter()
        .map(|node| format!("http://127.0.0.1:{}", node.tso_port))
        .collect();
    Ok(TsoClient::connect(endpoints).await?)
}

/// Append `cmd` on the leader's OmniPaxos and wait for it to decide.
/// Equivalent to openraft-piggyback's `leader_raft.client_write`, but
/// OmniPaxos's `append` is fire-and-forget so we poll `decided_idx`.
async fn append_on_leader_and_wait(node: &Node, cmd: MyAppCommand) -> anyhow::Result<()> {
    let snapshot_decided = node.omnipaxos.lock().get_decided_idx();
    node.omnipaxos
        .lock()
        .append(cmd)
        .map_err(|err| anyhow!("append on leader: {err:?}"))?;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if node.omnipaxos.lock().get_decided_idx() > snapshot_decided {
            return Ok(());
        }
        sleep(Duration::from_millis(5)).await;
    }
    bail!("append did not decide within 3s")
}

async fn wait_until_serving(
    rx: &mut tokio::sync::watch::Receiver<ServingState>,
) -> anyhow::Result<()> {
    let fence_timeout = Duration::from_secs(10);
    timeout(fence_timeout, async {
        loop {
            if matches!(*rx.borrow_and_update(), ServingState::Serving) {
                return Ok::<(), anyhow::Error>(());
            }
            rx.changed()
                .await
                .context("serving-state stream closed before reaching Serving")?;
        }
    })
    .await
    .with_context(|| format!("server did not reach Serving within {fence_timeout:?}"))??;
    Ok(())
}

// ---------------------------------------------------------------------------
// The demo script.
// ---------------------------------------------------------------------------

pub async fn run_demo() -> anyhow::Result<DemoOutcome> {
    let (mut nodes, _network, _cluster_config) = build_cluster().await?;
    let leader_idx = wait_for_leader(&nodes).await?;
    info!(leader = nodes[leader_idx].id, "leader elected");

    // Wait for the leader's tsoracle server to finish the fence + transition
    // to Serving. The fence flips serving-state to Serving only after the
    // driver's persist_high_water has committed and applied — so once we
    // see Serving, the leader's state.high_water reflects the new epoch's
    // floor.
    let mut leader_serving_rx = nodes[leader_idx].serving_state_rx.clone();
    wait_until_serving(&mut leader_serving_rx).await?;

    let initial_high_water = nodes[leader_idx].state.high_water();
    info!(
        leader = nodes[leader_idx].id,
        high_water = initial_high_water,
        "post-fence high-water (driver persisted serving_floor + failover_advance)"
    );
    println!("\n--- Leader elected, fence ran ---");
    println!("leader: node {}", nodes[leader_idx].id);
    println!("post-fence high-water: {initial_high_water}");

    // ---- Section 1: host KV writes ride the same paxos log ----
    println!("\n--- Host KV writes (ride the same paxos log) ---");
    let leader_node = &nodes[leader_idx];
    for (key, value) in [("alpha", b"first".to_vec()), ("beta", b"second".to_vec())] {
        append_on_leader_and_wait(
            leader_node,
            MyAppCommand::Kv(KvOp::Put {
                key: key.into(),
                value: value.clone(),
            }),
        )
        .await?;
        println!("  put {key} = {} bytes (tso untouched)", value.len());
    }

    // Wait for every node's apply pump to absorb the writes — KV writes
    // happen on the leader's handle but every follower's pump also drains
    // them. We poll until all nodes' kv maps contain both keys.
    let kv_observed_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let all_have_keys = nodes.iter().all(|n| {
            let map = n.state.kv_dump();
            map.contains_key("alpha") && map.contains_key("beta")
        });
        if all_have_keys {
            break;
        }
        if Instant::now() >= kv_observed_deadline {
            for n in &nodes {
                let decided = n.omnipaxos.lock().get_decided_idx();
                let map = n.state.kv_dump();
                eprintln!(
                    "  node {}: decided_idx={decided}, kv_keys={:?}, high_water={}",
                    n.id,
                    map.keys().collect::<Vec<_>>(),
                    n.state.high_water()
                );
            }
            bail!("KV writes did not propagate to all nodes within 3s");
        }
        sleep(Duration::from_millis(10)).await;
    }
    let kv_after_writes = nodes[leader_idx].state.kv_dump();
    let high_water_after_kv = nodes[leader_idx].state.high_water();
    println!(
        "host state after KV writes: kv keys={:?} high_water={high_water_after_kv}",
        kv_after_writes.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        high_water_after_kv, initial_high_water,
        "KV writes must not mutate the TSO field"
    );

    // ---- Section 2: GetTs is allocator-served; doesn't advance high-water ----
    println!("\n--- TSO bursts (allocator-served, no consensus per call) ---");
    let client = build_client(&nodes).await?;
    let high_water_before_burst = nodes[leader_idx].state.high_water();
    let first_ts = {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match client.get_ts().await {
                Ok(ts) => break ts,
                Err(err) if Instant::now() < deadline => {
                    tracing::warn!(error = ?err, "get_ts retrying");
                    sleep(Duration::from_millis(25)).await;
                }
                Err(err) => return Err(anyhow!("get_ts failed after 3s retries: {err}")),
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
    let high_water_after_burst = nodes[leader_idx].state.high_water();
    let high_water_unchanged_across_getts = high_water_after_burst == high_water_before_burst;
    println!(
        "high-water before burst: {high_water_before_burst}; after burst: {high_water_after_burst} (unchanged = {high_water_unchanged_across_getts})"
    );
    let pre_failover_last_ts = last_ts.context("at least one timestamp was issued")?;
    let pre_failover_high_water = high_water_after_burst;

    // ---- Section 3: failover preserves monotonicity ----
    println!("\n--- Failover: shut down leader, observe new leader fence ---");
    let old_leader_id = nodes[leader_idx].id;
    nodes[leader_idx].shutdown().await;
    println!("  shut down node {old_leader_id}; waiting for new leader...");

    let mut new_leader_idx: Option<usize> = None;
    let new_leader_deadline = Instant::now() + Duration::from_secs(15);
    let mut last_progress_log = Instant::now();
    while Instant::now() < new_leader_deadline {
        for (idx, node) in nodes.iter().enumerate() {
            if idx == leader_idx {
                continue;
            }
            if let Some(leader_id) = node.omnipaxos.lock().get_current_leader() {
                if leader_id == node.id {
                    new_leader_idx = Some(idx);
                    break;
                }
            }
        }
        if new_leader_idx.is_some() {
            break;
        }
        if last_progress_log.elapsed() > Duration::from_secs(2) {
            for (idx, node) in nodes.iter().enumerate() {
                if idx == leader_idx {
                    continue;
                }
                let leader = node.omnipaxos.lock().get_current_leader();
                let decided = node.omnipaxos.lock().get_decided_idx();
                println!(
                    "  ...polling: node {} sees leader={leader:?}, decided_idx={decided}",
                    node.id
                );
            }
            last_progress_log = Instant::now();
        }
        sleep(Duration::from_millis(25)).await;
    }
    let new_leader_idx = new_leader_idx.context("no new leader within 15s")?;
    let new_leader_id = nodes[new_leader_idx].id;
    println!("  new leader: node {new_leader_id}");

    // Wait for the new leader's fence to publish Serving.
    let mut new_serving_rx = nodes[new_leader_idx].serving_state_rx.clone();
    wait_until_serving(&mut new_serving_rx).await?;

    let post_failover_high_water = nodes[new_leader_idx].state.high_water();
    println!("  post-failover high-water: {post_failover_high_water}");
    assert!(
        post_failover_high_water > pre_failover_high_water,
        "post-failover high-water ({post_failover_high_water}) must exceed pre-failover ({pre_failover_high_water})"
    );

    // The dead leader's endpoint is still in the client's rotation; allow
    // retries while it falls back to a survivor.
    let post_failover_first_ts = {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match client.get_ts().await {
                Ok(ts) => break ts,
                Err(err) if Instant::now() < deadline => {
                    tracing::warn!(error = ?err, "post-failover get_ts retrying");
                    sleep(Duration::from_millis(25)).await;
                }
                Err(err) => {
                    return Err(anyhow!(
                        "get_ts failed after 5s retries post-failover: {err}"
                    ));
                }
            }
        }
    };
    println!("  first post-failover GetTs: {post_failover_first_ts:?}");
    assert!(
        post_failover_first_ts > pre_failover_last_ts,
        "post-failover timestamp ({post_failover_first_ts:?}) must exceed last pre-failover timestamp ({pre_failover_last_ts:?})"
    );

    // ---- Clean shutdown of remaining nodes ----
    for (idx, node) in nodes.iter_mut().enumerate() {
        if idx == leader_idx {
            continue; // already shut down for the failover step
        }
        node.shutdown().await;
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
