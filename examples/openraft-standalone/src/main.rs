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

//! Multi-process 3-node tsoracle cluster backed by `tsoracle-driver-openraft`.
//!
//! The integration body is just two bindings (`StandaloneHost::new`,
//! `OpenraftDriver::new`); everything else is transport plumbing
//! (`src/network.rs`) and config parsing.
//!
//! `--bootstrap` flag goes on exactly one node at first cluster init.

mod network;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use openraft::{Config, Raft};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, OpenraftPeer, RocksdbSnapshotStore, SnapshotStore,
    StandaloneHost, TypeConfig,
};
use tsoracle_openraft_toolkit::{Flat, RocksdbLogStore};
use tsoracle_server::Server as TsoServer;

use crate::network::{MAX_PEER_MESSAGE_BYTES, PeerFactory, server as peer_server};

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";
const SNAP_CF: &str = "raft_snapshot";

/// Cap on concurrent HTTP/2 streams the peer server will service at once.
/// Bounds a connection/stream flood; generous for a small cluster's peer fan-in.
const MAX_CONCURRENT_STREAMS: u32 = 256;

/// Explicit HTTP/2 max frame size for the peer server. Defense-in-depth behind
/// the per-message decode cap (`network::MAX_PEER_MESSAGE_BYTES`): it bounds an
/// individual transport frame, not a whole reassembled message.
const MAX_FRAME_SIZE: u32 = 64 * 1024;

#[derive(Parser, Debug)]
#[command(name = "openraft-standalone")]
struct Cli {
    /// This node's numeric ID. Must be unique across the cluster.
    #[arg(long)]
    id: u64,

    /// Address on which to listen for raft peer RPCs (e.g. 127.0.0.1:51001).
    ///
    /// The raft peer transport is UNAUTHENTICATED by design: any client that
    /// can reach this socket can drive replication (append-entries/vote) and
    /// stream snapshots into this node. The handler bounds per-message and
    /// total snapshot memory (see `network.rs`) so a reachable peer cannot OOM
    /// the process, but it performs no peer-identity check. Bind it only where
    /// the network is trusted. Operators should do one of:
    ///   (a) bind loopback or a private subnet reachable only by cluster peers;
    ///   (b) wrap the transport in mTLS with a client-cert allowlist (see the
    ///       `tls-mtls` example);
    ///   (c) front it with an authorizing proxy.
    #[arg(long)]
    raft_addr: SocketAddr,

    /// Address on which to serve the tsoracle gRPC API (e.g. 127.0.0.1:50561).
    #[arg(long)]
    tso_addr: SocketAddr,

    /// Comma-separated `id=host:port` pairs for raft peer addresses.
    /// Example: `1=127.0.0.1:51001,2=127.0.0.1:51002,3=127.0.0.1:51003`
    #[arg(long)]
    peers: String,

    /// Comma-separated `id=host:port` pairs for tsoracle service addresses.
    /// Example: `1=127.0.0.1:50561,2=127.0.0.1:50562,3=127.0.0.1:50563`
    #[arg(long)]
    tso_peers: String,

    /// Directory where raft log and state-machine data are persisted.
    #[arg(long)]
    raft_dir: PathBuf,

    /// Pass this flag on exactly one node to initialize the cluster.
    /// After the cluster is formed, restarts must omit this flag.
    #[arg(long)]
    bootstrap: bool,
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
    let cfs = vec![
        ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
        ColumnFamilyDescriptor::new(META_CF, Options::default()),
        ColumnFamilyDescriptor::new(SNAP_CF, Options::default()),
    ];
    Ok(Arc::new(DB::open_cf_descriptors(&opts, dir, cfs)?))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    let raft_addrs = parse_peer_map(&cli.peers)?;
    let tso_addrs = parse_peer_map(&cli.tso_peers)?;

    // ---- Storage + state machine ----
    // One rocksdb instance covers the raft log (`raft_log` / `raft_meta` CFs)
    // and the state-machine snapshot (`raft_snapshot` CF), so a single
    // `set_sync(true)` write fsyncs both halves together.
    std::fs::create_dir_all(&cli.raft_dir)?;
    let db = open_rocksdb(&cli.raft_dir)?;
    let log_store = RocksdbLogStore::open(db.clone(), LOG_CF, META_CF, Flat)?;
    let snapshot_store: Arc<dyn SnapshotStore> = Arc::new(RocksdbSnapshotStore::open(db, SNAP_CF)?);
    let state_machine = HighWaterStateMachine::with_store(snapshot_store)
        .context("rehydrate state machine from persisted snapshot")?;
    let state_machine_for_host = state_machine.clone();

    // ---- Openraft config ----
    // Default snapshot policy is fine now that the state machine writes
    // through to a durable snapshot store: snapshots survive restart, so
    // openraft is free to purge the log prefix each one covers.
    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 1_000,
            election_timeout_max: 2_000,
            ..Default::default()
        }
        .validate()?,
    );

    // ---- Raft ----
    let network = PeerFactory::new(raft_addrs.clone());
    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        cli.id,
        config,
        network,
        log_store,
        state_machine,
    )
    .await?;

    // ---- Peer-transport server (raft-internal RPCs) ----
    // The transport is unauthenticated (see `Cli::raft_addr`); these caps bound
    // the memory a reachable peer can force this node to allocate. The decode
    // cap must stay >= one snapshot chunk (`network::MAX_PEER_MESSAGE_BYTES` is
    // derived from `SNAPSHOT_CHUNK_SIZE` to guarantee that); the total snapshot
    // reassembly bound lives in the snapshot handler itself.
    let peer_service = peer_server(raft.clone())
        .max_decoding_message_size(MAX_PEER_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_PEER_MESSAGE_BYTES);
    let raft_addr = cli.raft_addr;
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
            .max_frame_size(MAX_FRAME_SIZE)
            .add_service(peer_service)
            .serve(raft_addr)
            .await
        {
            tracing::error!(error = ?e, "raft peer server died");
        }
    });

    // ---- Optional one-time cluster initialization ----
    if cli.bootstrap {
        let mut nodes: BTreeMap<u64, OpenraftPeer> = BTreeMap::new();
        for (id, addr) in raft_addrs.iter() {
            let service_endpoint = tso_addrs.get(id).cloned().unwrap_or_default();
            nodes.insert(
                *id,
                OpenraftPeer {
                    addr: addr.clone(),
                    service_endpoint,
                },
            );
        }
        if let Err(e) = raft.initialize(nodes).await {
            // "already initialized" on a re-run with --bootstrap still set is
            // operator-friendly: log + continue.
            tracing::warn!(
                error = ?e,
                "initialize() returned an error (expected if already initialized)"
            );
        }
    }

    // ---- Driver: StandaloneHost -> OpenraftDriver ----
    // Follower redirects resolve from the leader's membership node
    // (OpenraftPeer.service_endpoint), so no static peer map is passed here.
    let host = StandaloneHost::new(raft.clone(), state_machine_for_host);
    let driver = OpenraftDriver::new(host);

    // ---- Tsoracle gRPC server ----
    let tso = TsoServer::builder().consensus_driver(driver).build()?;
    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("ctrl-c received, shutting down");
    };
    println!(
        "tsoracle openraft node {} on http://{}",
        cli.id, cli.tso_addr
    );
    tso.serve_with_shutdown(cli.tso_addr, shutdown).await?;
    Ok(())
}
