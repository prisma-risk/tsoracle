//! Multi-process 3-node tsoracle cluster backed by `tsoracle-driver-openraft`.
//!
//! The integration body is just three bindings (`StandaloneHost::new`,
//! `OpenraftDriver::new`, `StandaloneRouter::new`); everything else is
//! transport plumbing (`src/network.rs`) and config parsing.
//!
//! `--bootstrap` flag goes on exactly one node at first cluster init.

mod network;
mod router;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use openraft::{Config, Raft, SnapshotPolicy};
use openraft_toolkit::{Flat, RocksdbLogStore};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, OpenraftPeer, StandaloneHost, TypeConfig,
};
use tsoracle_server::Server as TsoServer;

use crate::network::{PeerFactory, server as peer_server};
use crate::router::StandaloneRouter;

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

#[derive(Parser, Debug)]
#[command(name = "openraft-standalone")]
struct Cli {
    /// This node's numeric ID. Must be unique across the cluster.
    #[arg(long)]
    id: u64,

    /// Address on which to listen for raft peer RPCs (e.g. 127.0.0.1:51001).
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

fn open_rocksdb_log_store(
    dir: &std::path::Path,
) -> anyhow::Result<RocksdbLogStore<TypeConfig, Flat>> {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    let raft_addrs = parse_peer_map(&cli.peers)?;
    let tso_addrs = Arc::new(parse_peer_map(&cli.tso_peers)?);

    // ---- Storage + state machine ----
    std::fs::create_dir_all(&cli.raft_dir)?;
    let log_store = open_rocksdb_log_store(&cli.raft_dir)?;
    let state_machine = HighWaterStateMachine::new();
    let state_machine_for_host = state_machine.clone();

    // ---- Openraft config ----
    // SnapshotPolicy::Never is intentional: the driver crate's
    // HighWaterStateMachine keeps state and snapshots in memory only, so the
    // default snapshot+purge policy could let openraft purge logs the SM
    // cannot rebuild from on restart. See README "Production caveats."
    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 1_000,
            election_timeout_max: 2_000,
            snapshot_policy: SnapshotPolicy::Never,
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
    let peer_service = peer_server(raft.clone());
    let raft_addr = cli.raft_addr;
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
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
            nodes.insert(*id, OpenraftPeer { addr: addr.clone() });
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

    // ---- Driver: StandaloneHost -> OpenraftDriver -> StandaloneRouter ----
    let host = StandaloneHost::new(raft.clone(), state_machine_for_host);
    let inner_driver = OpenraftDriver::new(host);
    let driver = StandaloneRouter::new(inner_driver, raft.clone(), tso_addrs);

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
