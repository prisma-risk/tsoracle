mod driver;
mod leader_watch;
mod network;
mod store;
mod types;

use crate::driver::OpenraftDriver;
use crate::network::{PeerFactory, server as peer_server};
use crate::store::{FileStateMachine, FileStore};
use crate::types::{Node, NodeId, TypeConfig};
use clap::Parser;
use openraft::{BasicNode, Config};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tsoracle_server::Server as TsoServer;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "openraft-cluster")]
struct Cli {
    /// This node's numeric ID (must be unique across the cluster).
    #[arg(long)]
    id: NodeId,

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_peer_map(s: &str) -> anyhow::Result<HashMap<NodeId, String>> {
    let mut out = HashMap::new();
    for pair in s.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (id, addr) = pair
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("bad peer entry {pair:?}, expected id=host:port"))?;
        out.insert(id.trim().parse::<NodeId>()?, addr.trim().to_string());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();

    let raft_addrs = parse_peer_map(&cli.peers)?;
    let tso_addrs = Arc::new(parse_peer_map(&cli.tso_peers)?);

    // Open (or restore) the file-backed log store and state machine.
    let (log_store, state_machine) = FileStore::open(cli.raft_dir.clone()).await?;

    // Capture the read-side handle BEFORE moving `state_machine` into Raft::new.
    let state_handle = state_machine.state.clone();

    // Build and validate the raft configuration.
    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 1_000,
            election_timeout_max: 2_000,
            ..Default::default()
        }
        .validate()?,
    );

    // Construct the network factory.
    let network = PeerFactory::new(raft_addrs.clone());

    // Start the raft instance.
    let raft = openraft::Raft::<TypeConfig, FileStateMachine>::new(
        cli.id,
        config,
        network,
        log_store,
        state_machine,
    )
    .await?;

    // -----------------------------------------------------------------------
    // Peer-transport gRPC server (raft-internal RPCs).
    // -----------------------------------------------------------------------
    let peer_svc = peer_server(raft.clone());
    let raft_addr = cli.raft_addr;
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(peer_svc)
            .serve(raft_addr)
            .await
        {
            tracing::error!(error = ?e, "raft peer server died");
        }
    });

    // -----------------------------------------------------------------------
    // Leader-watch task — translates openraft metrics → LeaderState stream.
    // -----------------------------------------------------------------------
    let leader_rx = leader_watch::spawn(raft.clone(), tso_addrs.clone());

    // -----------------------------------------------------------------------
    // Optional one-time cluster initialization.
    // -----------------------------------------------------------------------
    if cli.bootstrap {
        let mut nodes: BTreeMap<NodeId, Node> = BTreeMap::new();
        for (id, addr) in raft_addrs.iter() {
            nodes.insert(*id, BasicNode::new(addr));
        }
        if let Err(e) = raft.initialize(nodes).await {
            tracing::warn!(
                error = ?e,
                "initialize() returned an error (often expected if already initialized)"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Build the tsoracle server.
    // -----------------------------------------------------------------------
    let driver = Arc::new(OpenraftDriver {
        raft,
        state: state_handle,
        leader_events: leader_rx,
    });

    let tso = TsoServer::builder().consensus_driver(driver).build()?;

    // -----------------------------------------------------------------------
    // Graceful shutdown on Ctrl-C.
    // -----------------------------------------------------------------------
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
