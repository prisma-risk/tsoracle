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

//! The `tsoracle` CLI.
//!
//! Two subcommands:
//! - `serve` (the default) — start a single-node tsoracle backed by the
//!   file driver, listening on gRPC. State is fsync-durable.
//! - `init` — initialize a state directory at a seeded high-water for
//!   one-shot migration off a prior sequence or oracle. Refuses to
//!   overwrite an existing state file.
//!
//! To embed the server inside an existing binary instead of running this
//! CLI, use [`tsoracle_server::Server`] directly; see the
//! `examples/embedded-server` crate for a worked example.

// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the bin's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;
use tsoracle_driver_file::FileDriver;
use tsoracle_server::Server;

#[derive(Parser, Debug)]
#[command(name = "tsoracle", version, about = "Standalone timestamp oracle")]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,

    // Default subcommand fields (mirrors `serve`).
    #[command(flatten)]
    serve: ServeArgs,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the timestamp oracle server (default).
    Serve(ServeArgs),
    /// Initialize a fresh state directory at a seeded high-water (one-shot migration setup).
    Init(InitArgs),
}

#[derive(Parser, Debug, Clone)]
struct ServeArgs {
    /// gRPC listen address.
    #[arg(long, default_value = "127.0.0.1:50551")]
    listen: SocketAddr,
    /// Where to persist window state.
    #[arg(long, default_value = "./tsoracle-data")]
    state_dir: PathBuf,
    /// How far ahead to allocate windows.
    #[arg(long, value_parser = parse_duration, default_value = "3s")]
    window_ahead: Duration,
    /// Advance on leadership gain.
    #[arg(long, value_parser = parse_duration, default_value = "1s")]
    failover_advance: Duration,
    /// Log level.
    #[arg(long, default_value = "info")]
    log: String,
}

#[derive(Parser, Debug)]
struct InitArgs {
    /// Where to write the seeded state.
    #[arg(long, default_value = "./tsoracle-data")]
    state_dir: PathBuf,
    /// Seed high-water in milliseconds since Unix epoch.
    #[arg(long)]
    seed_physical_ms: u64,
}

fn parse_duration(input: &str) -> std::result::Result<Duration, String> {
    humantime::parse_duration(input).map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let cmd = cli.cmd.unwrap_or(Cmd::Serve(cli.serve));
    match cmd {
        Cmd::Init(args) => run_init(args),
        Cmd::Serve(args) => run_serve(args).await,
    }
}

fn run_init(args: InitArgs) -> Result<()> {
    FileDriver::init_seeded(&args.state_dir, args.seed_physical_ms)
        .with_context(|| format!("init state_dir={}", args.state_dir.display()))?;
    println!(
        "Initialized {} at seed physical_ms={}",
        args.state_dir.display(),
        args.seed_physical_ms
    );
    Ok(())
}

async fn run_serve(args: ServeArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&args.log).unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let driver = FileDriver::open_or_init(&args.state_dir)
        .with_context(|| format!("open state_dir={}", args.state_dir.display()))?;

    let server = Server::builder()
        .consensus_driver(driver)
        .window_ahead(args.window_ahead)
        .failover_advance(args.failover_advance)
        .build()
        .context("server build")?;

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind {}", args.listen))?;
    let local_addr = listener.local_addr().context("listener.local_addr()")?;

    // Plain-stdout contract: process supervisors (e.g. the stress harness)
    // parse this line to discover the OS-picked port when --listen uses :0.
    // Keep the prefix stable; downstream code does `strip_prefix("serving on ")`.
    println!("serving on {local_addr}");

    tracing::info!(
        addr = %local_addr,
        state_dir = %args.state_dir.display(),
        "tsoracle serving"
    );

    server
        .serve_with_listener(listener, shutdown_signal())
        .await
        .context("serve")?;
    Ok(())
}

/// Resolves when the process receives an OS request to terminate.
///
/// Container orchestrators (Kubernetes, `docker stop`) and systemd stop
/// processes with SIGTERM, while an interactive Ctrl-C sends SIGINT. Both must
/// drive tonic's graceful drain; otherwise the default SIGTERM disposition
/// kills the process mid-flight and the supervisor SIGKILLs it after the grace
/// period (#245). On non-unix targets only Ctrl-C is available.
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(error) => {
            // Installing the SIGTERM handler only fails on resource exhaustion
            // or an invalid signal — neither expected here. Degrade to Ctrl-C
            // rather than refuse to serve an otherwise-healthy node.
            tracing::warn!(%error, "could not install SIGTERM handler; only Ctrl-C will trigger shutdown");
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!(signal = "SIGINT", "shutdown signal received");
            return;
        }
    };

    let signal_name = tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = sigterm.recv() => "SIGTERM",
    };
    tracing::info!(signal = signal_name, "shutdown signal received");
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!(signal = "SIGINT", "shutdown signal received");
}
