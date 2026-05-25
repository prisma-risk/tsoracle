//! 3-node OmniPaxos tsoracle cluster — thin demonstration of `tsoracle-standalone`.

use std::time::Duration;

use anyhow::{Error, Result};
use clap::Parser;
use tsoracle_server::Server;
use tsoracle_standalone::{DriverConfig, PaxosConfig, build, parse_peer_map};

#[derive(Parser, Debug)]
#[command(name = "paxos-standalone")]
struct Cli {
    #[arg(long)]
    node_id: u64,
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: std::net::SocketAddr,
    #[arg(long, default_value = "127.0.0.1:0")]
    tso_listen: std::net::SocketAddr,
    #[arg(long)]
    peers: String,
    #[arg(long)]
    tso_peers: String,
    #[arg(long)]
    data_dir: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    let cfg = DriverConfig::Paxos(PaxosConfig {
        node_id: cli.node_id,
        peer_listen: cli.listen,
        peers: parse_peer_map(&cli.peers).map_err(Error::msg)?,
        tso_peers: parse_peer_map(&cli.tso_peers).map_err(Error::msg)?,
        data_dir: cli.data_dir,
        tick_interval: Duration::from_millis(20),
    });
    let node = build(cfg).await?;
    let server = Server::builder()
        .consensus_driver(node.driver.clone())
        .build()?;
    println!(
        "tsoracle paxos node {} on http://{}",
        cli.node_id, cli.tso_listen
    );
    server
        .serve_with_shutdown(cli.tso_listen, tsoracle_server::shutdown_signal())
        .await?;
    node.shutdown().await;
    Ok(())
}
