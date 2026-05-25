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

//! Embed a 3-node OmniPaxos cluster + tsoracle-server in your own binary.
//!
//! OmniPaxos has no single-node mode (BLE election requires majority of the
//! configured nodes), so "embedded" here means "all three paxos nodes
//! running inside one process, wired through the toolkit's MemNetwork".
//! Each node binds tsoracle's gRPC API on its own loopback port; the
//! tsoracle-client rotates across them.
//!
//! Run: `cargo run -p example-paxos-embedded`
//! Then talk to it on http://127.0.0.1:50591 / 50592 / 50593. Ctrl-C exits.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use omnipaxos::messages::Message;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tsoracle_driver_paxos::{HighWaterCommand, PaxosDriver, SnapshotPolicy, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::{MessageSink, TsoPeer};
use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;
use tsoracle_server::Server as TsoServer;

/// Tick interval for every node's runner. With election_tick_timeout=5 this
/// keeps leader election in the 100–200 ms range.
const TICK_INTERVAL: Duration = Duration::from_millis(20);
const ELECTION_TICK_TIMEOUT: u64 = 5;
const RESEND_MESSAGE_TICK_TIMEOUT: u64 = 5;

const NODE_COUNT: usize = 3;
const BASE_PORT: u16 = 50591;

struct MeshSink {
    network: Arc<MemNetwork<HighWaterCommand>>,
}

#[async_trait]
impl MessageSink<HighWaterCommand> for MeshSink {
    async fn send(&self, message: Message<HighWaterCommand>) {
        self.network.deliver(message).await;
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let network: Arc<MemNetwork<HighWaterCommand>> = Arc::new(MemNetwork::new());
    let node_ids: Vec<u64> = (1..=NODE_COUNT as u64).collect();
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids.clone(),
        flexible_quorum: None,
    };

    // Build a `TsoPeer` list of TSO endpoints so each node's StandaloneHost can
    // populate `LeaderHint` follower-redirect with the leader's address.
    let tso_endpoints: Vec<(u64, SocketAddr)> = node_ids
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            (
                *id,
                format!("127.0.0.1:{}", BASE_PORT + idx as u16)
                    .parse()
                    .unwrap(),
            )
        })
        .collect();

    let mut tso_shutdowns: Vec<oneshot::Sender<()>> = Vec::new();

    for &id in &node_ids {
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
                .build(MemStorage::<HighWaterCommand>::new())
                .context("build OmniPaxos handle")?,
        ));

        // ---- Inbox pump (drains MemNetwork into handle_incoming) ----
        let inbox: mpsc::Receiver<Message<HighWaterCommand>> = network.register(id);
        let inbox_omnipaxos = omnipaxos.clone();
        tokio::spawn(async move {
            run_inbox_pump(inbox_omnipaxos, inbox).await;
        });

        // ---- StandaloneHost ----
        let toolkit_peers: Vec<TsoPeer> = tso_endpoints
            .iter()
            .filter(|(peer_id, _)| *peer_id != id)
            .map(|(peer_id, addr)| TsoPeer {
                node_id: *peer_id,
                endpoint: format!("http://{addr}"),
            })
            .collect();
        let mut host = StandaloneHost::builder()
            .omnipaxos(omnipaxos)
            .my_node_id(id)
            .peers(toolkit_peers)
            .tick_interval(TICK_INTERVAL)
            .snapshot_policy(SnapshotPolicy::disabled())
            .build()
            .context("build StandaloneHost")?;
        let leader_subscriber = host
            .take_leader_subscriber()
            .context("leader subscriber is fresh")?;

        let sink = Arc::new(MeshSink {
            network: network.clone(),
        });
        host.start(sink).context("host not already running")?;

        // ---- Driver + tsoracle server ----
        let driver = Arc::new(PaxosDriver::new(host, leader_subscriber));
        let server = TsoServer::builder().consensus_driver(driver).build()?;

        let (tso_shutdown_tx, tso_shutdown_rx) = oneshot::channel::<()>();
        tso_shutdowns.push(tso_shutdown_tx);
        let addr = tso_endpoints[(id - 1) as usize].1;
        tokio::spawn(async move {
            let shutdown = async move {
                let _ = tso_shutdown_rx.await;
            };
            if let Err(err) = server.serve_with_shutdown(addr, shutdown).await {
                tracing::error!(error = ?err, node = id, "tsoracle server died");
            }
        });
    }

    println!("Embedded tsoracle/paxos cluster running:");
    for (id, addr) in &tso_endpoints {
        println!("  node {id}: http://{addr}");
    }
    println!("Press Ctrl-C to shut down.");

    // Wait for Ctrl-C.
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("ctrl-c received, draining");

    for tx in tso_shutdowns {
        let _ = tx.send(());
    }
    // Give server tasks a beat to drain before the runtime tears down.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}

async fn run_inbox_pump(
    omnipaxos: Arc<Mutex<omnipaxos::OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>>,
    mut inbox: mpsc::Receiver<Message<HighWaterCommand>>,
) {
    while let Some(message) = inbox.recv().await {
        omnipaxos.lock().handle_incoming(message);
    }
}
