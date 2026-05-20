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

fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
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

    tracing::info!(
        listen = %args.listen,
        state_dir = %args.state_dir.display(),
        "tsoracle starting"
    );

    server
        .serve_with_shutdown(args.listen, async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutdown signal received");
        })
        .await
        .context("serve")?;
    Ok(())
}
