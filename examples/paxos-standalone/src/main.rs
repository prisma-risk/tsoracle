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

//! Multi-process 3-node tsoracle cluster backed by `tsoracle-driver-paxos`.
//!
//! The integration body is just three calls (`StandaloneHost::builder`,
//! `host.start(sink)`, `PaxosDriver::new`); everything else is transport
//! plumbing (`src/network.rs`), RocksDB setup, and config parsing.
//!
//! Unlike the openraft variant, OmniPaxos does not need a `--bootstrap` step:
//! cluster membership is carried in the `ClusterConfig` every node builds
//! from `--peers`, and leader election runs as soon as quorum is reachable.

mod network;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tsoracle_driver_paxos::{HighWaterCommand, PaxosDriver, SnapshotPolicy, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::TsoPeer;
use tsoracle_paxos_toolkit::storage::RocksdbStorage;
use tsoracle_server::Server as TsoServer;

use crate::network::{PeerSink, server as peer_server};

/// RocksDB column family that holds the paxos log + meta (promise,
/// accepted_round, decided_idx, compacted_idx, snapshot, stopsign).
const PAXOS_CF: &str = "tso_paxos";

/// Tick interval for the OmniPaxos runner. 20 ms is well below the default
/// election timeouts and matches the test harness's choice; it also keeps the
/// per-runner CPU cost negligible for a 3-node cluster.
const TICK_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Parser, Debug)]
#[command(name = "paxos-standalone")]
struct Cli {
    /// This node's numeric id (the OmniPaxos pid). Must be unique across the cluster.
    #[arg(long)]
    node_id: u64,

    /// Address on which to listen for paxos peer RPCs (e.g. 127.0.0.1:53001).
    ///
    /// Defaults to `127.0.0.1:0` (loopback, OS-assigned port). The peer
    /// transport is unauthenticated plaintext, so binding off-loopback must be
    /// an explicit, deliberate override — never the default.
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,

    /// Address on which to serve the tsoracle gRPC API (e.g. 127.0.0.1:50581).
    ///
    /// Defaults to `127.0.0.1:0` (loopback, OS-assigned port) for the same
    /// reason as `--listen`: the client API is also unauthenticated plaintext.
    #[arg(long, default_value = "127.0.0.1:0")]
    tso_listen: SocketAddr,

    /// Comma-separated `id=host:port` pairs for paxos peer addresses.
    /// Example: `1=127.0.0.1:53001,2=127.0.0.1:53002,3=127.0.0.1:53003`
    #[arg(long)]
    peers: String,

    /// Comma-separated `id=host:port` pairs for tsoracle service addresses
    /// used to populate `LeaderHint` follower-redirect.
    /// Example: `1=127.0.0.1:50581,2=127.0.0.1:50582,3=127.0.0.1:50583`
    #[arg(long)]
    tso_peers: String,

    /// Directory where the paxos log + meta are persisted.
    #[arg(long)]
    data_dir: PathBuf,
}

fn parse_peer_map(input: &str) -> anyhow::Result<HashMap<u64, String>> {
    let mut out = HashMap::new();
    for pair in input.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (id, addr) = pair
            .split_once('=')
            .with_context(|| format!("bad peer entry {pair:?}, expected id=host:port"))?;
        out.insert(id.trim().parse::<u64>()?, addr.trim().to_string());
    }
    Ok(out)
}

fn open_rocksdb(dir: &std::path::Path) -> anyhow::Result<Arc<DB>> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![ColumnFamilyDescriptor::new(PAXOS_CF, Options::default())];
    Ok(Arc::new(DB::open_cf_descriptors(&opts, dir, cfs)?))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    let paxos_addrs = parse_peer_map(&cli.peers)?;
    let tso_addrs = parse_peer_map(&cli.tso_peers)?;

    // ---- Storage ----
    std::fs::create_dir_all(&cli.data_dir)?;
    let db = open_rocksdb(&cli.data_dir)?;
    let storage = RocksdbStorage::<HighWaterCommand>::open_in(db, PAXOS_CF)?;

    // ---- OmniPaxos handle ----
    // ClusterConfig.nodes is the full pid set; ordering doesn't matter to
    // OmniPaxos itself, but sorted output keeps logs readable.
    let mut node_ids: Vec<u64> = paxos_addrs.keys().copied().collect();
    node_ids.sort_unstable();
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids,
        flexible_quorum: None,
    };
    let server_config = ServerConfig {
        pid: cli.node_id,
        ..Default::default()
    };
    let omnipaxos_config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    let omnipaxos = Arc::new(Mutex::new(
        omnipaxos_config
            .build(storage)
            .context("build OmniPaxos handle")?,
    ));

    // ---- Peer-transport server (paxos-internal RPCs) ----
    // Inbound messages flow straight into `OmniPaxos::handle_incoming` via
    // the shared handle. The server is spawned before `host.start()` so any
    // probe messages from peers that come up first are accepted, even though
    // the local runner is not yet ticking.
    let peer_service = peer_server(omnipaxos.clone());
    let listen = cli.listen;
    tokio::spawn(async move {
        if let Err(err) = tonic::transport::Server::builder()
            .add_service(peer_service)
            .serve(listen)
            .await
        {
            tracing::error!(error = ?err, "paxos peer server died");
        }
    });

    // ---- StandaloneHost ----
    // `peers` carries tsoracle service addresses (not paxos peer RPC); the
    // toolkit's `LeadershipState::from_omnipaxos` consults this list when a
    // peer is elected so `LeaderState::Follower::leader_endpoint` points at
    // the leader's tsoracle gRPC address.
    let toolkit_peers: Vec<TsoPeer> = tso_addrs
        .iter()
        .filter(|(id, _)| **id != cli.node_id)
        .map(|(id, endpoint)| TsoPeer {
            node_id: *id,
            endpoint: format!("http://{endpoint}"),
        })
        .collect();
    let mut host = StandaloneHost::builder()
        .omnipaxos(omnipaxos)
        .my_node_id(cli.node_id)
        .peers(toolkit_peers)
        .tick_interval(TICK_INTERVAL)
        .snapshot_policy(SnapshotPolicy::disabled())
        .build()
        .context("build StandaloneHost")?;

    let leader_subscriber = host
        .take_leader_subscriber()
        .context("leader subscriber available on a fresh host")?;

    // ---- Start runner + apply task ----
    let sink = Arc::new(PeerSink::new(paxos_addrs));
    host.start(sink).context("host not already running")?;

    // ---- ConsensusDriver ----
    let driver = Arc::new(PaxosDriver::new(host, leader_subscriber));

    // ---- Tsoracle gRPC server ----
    let tso = TsoServer::builder().consensus_driver(driver).build()?;
    println!(
        "tsoracle paxos node {} on http://{}",
        cli.node_id, cli.tso_listen
    );
    tso.serve_with_shutdown(cli.tso_listen, tsoracle_server::shutdown_signal())
        .await?;
    Ok(())
}
