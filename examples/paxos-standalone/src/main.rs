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

//! 3-node OmniPaxos tsoracle cluster — thin demonstration of `tsoracle-standalone`.

use std::time::Duration;

use anyhow::{Error, Result};
use clap::Parser;
use tsoracle_server::Server;
use tsoracle_standalone::{DriverConfig, PaxosConfig, PeerTlsConfig, build, parse_peer_map};

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
    #[arg(long)]
    peer_tls_cert: Option<std::path::PathBuf>,
    #[arg(long)]
    peer_tls_key: Option<std::path::PathBuf>,
    #[arg(long)]
    peer_tls_ca: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    let peer_tls = match (cli.peer_tls_cert, cli.peer_tls_key, cli.peer_tls_ca) {
        (None, None, None) => None,
        (Some(cert), Some(key), Some(ca)) => Some(PeerTlsConfig { cert, key, ca }),
        _ => {
            anyhow::bail!("--peer-tls-cert, --peer-tls-key, --peer-tls-ca must all be set together")
        }
    };
    let cfg = DriverConfig::Paxos(PaxosConfig {
        node_id: cli.node_id,
        peer_listen: cli.listen,
        peers: parse_peer_map(&cli.peers).map_err(Error::msg)?,
        tso_peers: parse_peer_map(&cli.tso_peers).map_err(Error::msg)?,
        data_dir: cli.data_dir,
        tick_interval: Duration::from_millis(20),
        peer_tls,
    });
    let mut node = build(cfg).await?;
    let drain = node.take_drain();
    let server = Server::builder()
        .consensus_driver(node.driver.clone())
        .build()?;
    println!(
        "tsoracle paxos node {} on http://{}",
        cli.node_id, cli.tso_listen
    );
    let shutdown = async move {
        tsoracle_server::shutdown_signal().await;
        if let Some(drain) = drain {
            drain.await;
        }
    };
    server.serve_with_shutdown(cli.tso_listen, shutdown).await?;
    node.shutdown().await;
    Ok(())
}
