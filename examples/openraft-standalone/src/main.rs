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

//! 3-node openraft tsoracle cluster — thin demonstration of `tsoracle-standalone`.
//!
//! Run one process per node. Pass `--bootstrap` + `--members` on exactly one
//! node at first cluster init.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::Parser;
use tsoracle_server::Server;
use tsoracle_standalone::{DriverConfig, MemberAddr, OpenraftConfig, RaftTuning, build};

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
    /// id=raft_host:port/service_host:port,... (only with --bootstrap)
    #[arg(long)]
    members: Option<String>,
}

fn parse_members(input: &str) -> Result<BTreeMap<u64, MemberAddr>> {
    let mut out = BTreeMap::new();
    for entry in input.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, addrs) = entry.split_once('=').context("expected id=raft/service")?;
        let (raft_addr, service_endpoint) =
            addrs.split_once('/').context("expected raft/service")?;
        out.insert(
            id.trim().parse()?,
            MemberAddr {
                raft_addr: raft_addr.trim().to_string(),
                service_endpoint: service_endpoint.trim().to_string(),
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
    let cfg = DriverConfig::Openraft(OpenraftConfig {
        id: cli.id,
        raft_addr: cli.raft_addr,
        raft_dir: cli.raft_dir,
        bootstrap: cli.bootstrap,
        initial_membership: members,
        tuning: RaftTuning::default(),
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
