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

//! 3-node openraft tsoracle cluster — thin demonstration of `tsoracle-standalone`.
//!
//! Run one process per node. Pass `--bootstrap` + `--members` on exactly one
//! node at first cluster init.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::Parser;
use tsoracle_server::Server;
use tsoracle_standalone::{
    DriverConfig, MemberAddr, OpenraftConfig, PeerTlsConfig, RaftTuning, build,
};

#[derive(Parser, Debug)]
#[command(name = "openraft-standalone")]
struct Cli {
    #[arg(long)]
    id: u64,
    #[arg(long)]
    raft_addr: std::net::SocketAddr,
    #[arg(long)]
    tso_addr: std::net::SocketAddr,
    #[arg(long)]
    raft_dir: std::path::PathBuf,
    #[arg(long)]
    bootstrap: bool,
    /// id=raft_host:port/service_host:port/admin_host:port,... (only with --bootstrap)
    #[arg(long)]
    members: Option<String>,
    #[arg(long)]
    peer_tls_cert: Option<std::path::PathBuf>,
    #[arg(long)]
    peer_tls_key: Option<std::path::PathBuf>,
    #[arg(long)]
    peer_tls_ca: Option<std::path::PathBuf>,
}

fn parse_members(input: &str) -> Result<BTreeMap<u64, MemberAddr>> {
    let mut out = BTreeMap::new();
    for entry in input.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addrs) = entry
            .split_once('=')
            .context("expected id=raft_addr/service_endpoint/admin_endpoint")?;
        let mut parts = addrs.split('/');
        let raft_addr = parts.next().filter(|s| !s.is_empty());
        let service_endpoint = parts.next();
        let admin_endpoint = parts.next();
        let (Some(raft_addr), Some(service_endpoint), Some(admin_endpoint)) =
            (raft_addr, service_endpoint, admin_endpoint)
        else {
            anyhow::bail!(
                "bad member {entry:?}, expected raft_addr/service_endpoint/admin_endpoint"
            );
        };
        out.insert(
            id.trim().parse()?,
            MemberAddr {
                raft_addr: raft_addr.trim().to_string(),
                service_endpoint: service_endpoint.trim().to_string(),
                admin_endpoint: admin_endpoint.trim().to_string(),
            },
        );
    }
    Ok(out)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    let members = cli.members.as_deref().map(parse_members).transpose()?;
    let peer_tls = match (cli.peer_tls_cert, cli.peer_tls_key, cli.peer_tls_ca) {
        (None, None, None) => None,
        (Some(cert), Some(key), Some(ca)) => Some(PeerTlsConfig { cert, key, ca }),
        _ => {
            anyhow::bail!("--peer-tls-cert, --peer-tls-key, --peer-tls-ca must all be set together")
        }
    };
    let cfg = DriverConfig::Openraft(OpenraftConfig {
        id: cli.id,
        raft_addr: cli.raft_addr,
        raft_dir: cli.raft_dir,
        bootstrap: cli.bootstrap,
        initial_membership: members,
        tuning: RaftTuning::default(),
        peer_tls,
        admin_listen: None,
        admin_tls: None,
        allow_insecure_peer: false,
    });
    let mut node = build(cfg).await?;
    let drain = node.take_drain();
    let server = Server::builder()
        .consensus_driver(node.driver.clone())
        .build()?;
    println!(
        "tsoracle openraft node {} on http://{}",
        cli.id, cli.tso_addr
    );
    let shutdown = async move {
        tsoracle_server::shutdown_signal().await;
        if let Some(drain) = drain {
            drain.await;
        }
    };
    server.serve_with_shutdown(cli.tso_addr, shutdown).await?;
    node.shutdown().await;
    Ok(())
}
