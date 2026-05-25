use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "tsoracle", version, about = "Standalone timestamp oracle")]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Option<Cmd>,
    /// Default subcommand fields (mirrors `serve file`).
    #[command(flatten)]
    pub serve_file: FileArgs,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Run the timestamp oracle server with a selected driver.
    #[command(subcommand)]
    Serve(ServeCmd),
    /// Initialize a fresh file-driver state directory at a seeded high-water.
    Init(InitArgs),
}

#[derive(Subcommand, Debug)]
pub enum ServeCmd {
    /// Single-node, fsync-durable file driver (default).
    File(FileArgs),
    /// HA via openraft (3+ node cluster).
    Openraft(OpenraftArgs),
    /// HA via OmniPaxos (3+ node cluster).
    Paxos(PaxosArgs),
}

/// Knobs shared by every serve subcommand; feed the bin's `Server`, not `DriverConfig`.
#[derive(Parser, Debug, Clone)]
pub struct CommonServeArgs {
    /// Client-facing gRPC listen address.
    #[arg(long, default_value = "127.0.0.1:50551")]
    pub listen: SocketAddr,
    /// How far ahead to allocate windows.
    #[arg(long, value_parser = parse_duration, default_value = "3s")]
    pub window_ahead: Duration,
    /// Advance on leadership gain.
    #[arg(long, value_parser = parse_duration, default_value = "1s")]
    pub failover_advance: Duration,
    /// Log level.
    #[arg(long, default_value = "info")]
    pub log: String,
}

#[derive(Parser, Debug, Clone)]
pub struct FileArgs {
    #[command(flatten)]
    pub common: CommonServeArgs,
    /// Where to persist window state.
    #[arg(long, default_value = "./tsoracle-data")]
    pub state_dir: PathBuf,
}

#[derive(Parser, Debug, Clone)]
pub struct OpenraftArgs {
    #[command(flatten)]
    pub common: CommonServeArgs,
    /// This node's numeric raft id (unique across the cluster).
    #[arg(long)]
    pub id: u64,
    /// Address to listen for raft peer RPCs. UNAUTHENTICATED plaintext — bind only on a trusted network.
    #[arg(long)]
    pub raft_addr: SocketAddr,
    /// Directory for raft log + state-machine data.
    #[arg(long)]
    pub raft_dir: PathBuf,
    /// Initialize the cluster on exactly one node, first boot only.
    #[arg(long)]
    pub bootstrap: bool,
    /// Initial membership, ONLY with --bootstrap: `id=raft_host:port/service_host:port,...`.
    #[arg(long)]
    pub members: Option<String>,
    #[arg(long, default_value = "250")]
    pub heartbeat_ms: u64,
    #[arg(long, default_value = "1000")]
    pub election_min_ms: u64,
    #[arg(long, default_value = "2000")]
    pub election_max_ms: u64,
}

#[derive(Parser, Debug, Clone)]
pub struct PaxosArgs {
    #[command(flatten)]
    pub common: CommonServeArgs,
    /// This node's OmniPaxos pid (unique across the cluster).
    #[arg(long)]
    pub node_id: u64,
    /// Address to listen for paxos peer RPCs. UNAUTHENTICATED plaintext — bind only on a trusted network.
    #[arg(long)]
    pub peer_listen: SocketAddr,
    /// Comma-separated `id=host:port` paxos peer addresses (required every start).
    #[arg(long)]
    pub peers: String,
    /// Comma-separated `id=host:port` tsoracle service addresses for LeaderHint redirect.
    #[arg(long)]
    pub tso_peers: String,
    /// Directory for the paxos log + meta.
    #[arg(long)]
    pub data_dir: PathBuf,
    #[arg(long, value_parser = parse_duration, default_value = "20ms")]
    pub tick_interval: Duration,
}

#[derive(Parser, Debug)]
pub struct InitArgs {
    #[arg(long, default_value = "./tsoracle-data")]
    pub state_dir: PathBuf,
    #[arg(long)]
    pub seed_physical_ms: u64,
}

pub fn parse_duration(input: &str) -> Result<Duration, String> {
    humantime::parse_duration(input).map_err(|e| e.to_string())
}
