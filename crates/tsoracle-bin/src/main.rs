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

//! The `tsoracle` CLI: `serve file|openraft|paxos` and `init`.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

mod cli;

#[cfg(feature = "openraft")]
use std::collections::BTreeMap;

use anyhow::{Context, Result};
use clap::Parser;
use cli::{Cli, Cmd, CommonServeArgs, ServeCmd};
use tracing_subscriber::EnvFilter;
use tsoracle_server::Server;
use tsoracle_standalone::{DriverConfig, Standalone};

/// Drivers compiled into this build, for the friendly "not included" message.
#[cfg(any(
    not(feature = "file"),
    not(feature = "openraft"),
    not(feature = "paxos")
))]
fn available_drivers() -> &'static [&'static str] {
    &[
        #[cfg(feature = "file")]
        "file",
        #[cfg(feature = "openraft")]
        "openraft",
        #[cfg(feature = "paxos")]
        "paxos",
    ]
}

#[cfg(any(
    not(feature = "file"),
    not(feature = "openraft"),
    not(feature = "paxos")
))]
fn not_compiled_in(driver: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "this build does not include the {driver} driver; rebuild with `--features {driver}`. \
         available drivers: {}",
        available_drivers().join(", ")
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Some(Cmd::Init(args)) => run_init(args.state_dir, args.seed_physical_ms),
        Some(Cmd::Serve(serve)) => dispatch_serve(serve).await,
        // Bare `tsoracle` defaults to `serve file` when file is compiled in.
        None => {
            #[cfg(feature = "file")]
            {
                let file = cli.serve_file;
                let cfg = DriverConfig::File(tsoracle_standalone::FileConfig {
                    state_dir: file.state_dir,
                });
                return run_serve(file.common, cfg).await;
            }
            #[cfg(not(feature = "file"))]
            {
                anyhow::bail!(
                    "no subcommand given and this build excludes the file driver; \
                     specify `serve <driver>`. available drivers: {}",
                    available_drivers().join(", ")
                );
            }
        }
    }
}

async fn dispatch_serve(serve: ServeCmd) -> Result<()> {
    match serve {
        ServeCmd::File(args) => {
            #[cfg(feature = "file")]
            {
                let cfg = DriverConfig::File(tsoracle_standalone::FileConfig {
                    state_dir: args.state_dir,
                });
                run_serve(args.common, cfg).await
            }
            #[cfg(not(feature = "file"))]
            {
                let _ = args;
                Err(not_compiled_in("file"))
            }
        }
        ServeCmd::Openraft(args) => {
            #[cfg(feature = "openraft")]
            {
                let members = match args.members {
                    Some(s) => Some(parse_members(&s)?),
                    None => None,
                };
                let cfg = DriverConfig::Openraft(tsoracle_standalone::OpenraftConfig {
                    id: args.id,
                    raft_addr: args.raft_addr,
                    raft_dir: args.raft_dir,
                    bootstrap: args.bootstrap,
                    initial_membership: members,
                    tuning: tsoracle_standalone::RaftTuning {
                        heartbeat_ms: args.heartbeat_ms,
                        election_min_ms: args.election_min_ms,
                        election_max_ms: args.election_max_ms,
                    },
                });
                run_serve(args.common, cfg).await
            }
            #[cfg(not(feature = "openraft"))]
            {
                let _ = args;
                Err(not_compiled_in("openraft"))
            }
        }
        ServeCmd::Paxos(args) => {
            #[cfg(feature = "paxos")]
            {
                let cfg = DriverConfig::Paxos(tsoracle_standalone::PaxosConfig {
                    node_id: args.node_id,
                    peer_listen: args.peer_listen,
                    peers: tsoracle_standalone::parse_peer_map(&args.peers)
                        .map_err(anyhow::Error::msg)?,
                    tso_peers: tsoracle_standalone::parse_peer_map(&args.tso_peers)
                        .map_err(anyhow::Error::msg)?,
                    data_dir: args.data_dir,
                    tick_interval: args.tick_interval,
                });
                run_serve(args.common, cfg).await
            }
            #[cfg(not(feature = "paxos"))]
            {
                let _ = args;
                Err(not_compiled_in("paxos"))
            }
        }
    }
}

#[cfg(feature = "file")]
fn run_init(state_dir: std::path::PathBuf, seed_physical_ms: u64) -> Result<()> {
    tsoracle_standalone::init_file_seeded(&state_dir, seed_physical_ms)
        .with_context(|| format!("init state_dir={}", state_dir.display()))?;
    println!(
        "Initialized {} at seed physical_ms={seed_physical_ms}",
        state_dir.display()
    );
    Ok(())
}

#[cfg(not(feature = "file"))]
fn run_init(_state_dir: std::path::PathBuf, _seed_physical_ms: u64) -> Result<()> {
    Err(not_compiled_in("file"))
}

async fn run_serve(common: CommonServeArgs, cfg: DriverConfig) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&common.log).unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let node: Standalone = tsoracle_standalone::build(cfg)
        .await
        .context("driver bootstrap")?;
    let server = Server::builder()
        .consensus_driver(node.driver.clone())
        .window_ahead(common.window_ahead)
        .failover_advance(common.failover_advance)
        .build()
        .context("server build")?;

    let listener = tokio::net::TcpListener::bind(common.listen)
        .await
        .with_context(|| format!("bind {}", common.listen))?;
    let local_addr = listener.local_addr().context("listener.local_addr()")?;
    // Plain-stdout contract: supervisors parse this to discover the OS-picked port.
    println!("serving on {local_addr}");
    tracing::info!(addr = %local_addr, "tsoracle serving");

    let result = server
        .serve_with_listener(listener, tsoracle_server::shutdown_signal())
        .await
        .context("serve");
    node.shutdown().await;
    result
}

#[cfg(feature = "openraft")]
fn parse_members(input: &str) -> Result<BTreeMap<u64, tsoracle_standalone::MemberAddr>> {
    let mut out = BTreeMap::new();
    for entry in input.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (id, addrs) = entry.split_once('=').with_context(|| {
            format!("bad member {entry:?}, expected id=raft_addr/service_endpoint")
        })?;
        let (raft_addr, service_endpoint) = addrs.split_once('/').with_context(|| {
            format!("bad member {entry:?}, expected raft_addr/service_endpoint")
        })?;
        out.insert(
            id.trim()
                .parse()
                .with_context(|| format!("bad member id in {entry:?}"))?,
            tsoracle_standalone::MemberAddr {
                raft_addr: raft_addr.trim().to_string(),
                service_endpoint: service_endpoint.trim().to_string(),
            },
        );
    }
    Ok(out)
}
