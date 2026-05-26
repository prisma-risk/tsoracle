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
    Serve(Box<ServeCmd>),
    /// Initialize a fresh file-driver state directory at a seeded high-water.
    Init(InitArgs),
    /// Administer cluster membership over the admin gRPC port.
    #[cfg(feature = "openraft")]
    #[command(subcommand)]
    Admin(AdminCmd),
}

#[cfg(feature = "openraft")]
#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// List current members.
    Members(AdminEndpointArgs),
    /// Add a non-voting learner.
    AddLearner(AddLearnerArgs),
    /// Promote a learner to voter.
    Promote(AdminIdArgs),
    /// Remove a node.
    Remove(AdminIdArgs),
    /// Initiate a format-version activation. Runs the all-members capability
    /// gate then proposes the bump via raft. Exit codes:
    /// 0=success, 2=gate-rejected (MEMBERS_BELOW_TARGET),
    /// 3=NOT_LEADER (this node is a follower),
    /// 4=local-range-rejected (TARGET_OUT_OF_RANGE — receiving node's
    /// MAX_READABLE_VERSION < target), 1=other failure.
    ActivateFormat(ActivateFormatArgs),
}

#[cfg(feature = "openraft")]
#[derive(Parser, Debug, Clone)]
pub struct AdminClientTlsArgs {
    /// PEM client certificate to present to the admin gRPC server (admin mTLS).
    #[arg(long)]
    pub client_tls_cert: Option<std::path::PathBuf>,
    /// PEM private key for `--client-tls-cert`.
    #[arg(long)]
    pub client_tls_key: Option<std::path::PathBuf>,
    /// PEM CA to verify the admin server's certificate.
    #[arg(long)]
    pub client_tls_ca: Option<std::path::PathBuf>,
}

#[cfg(feature = "openraft")]
#[derive(Parser, Debug)]
pub struct AdminEndpointArgs {
    /// Any node's admin endpoint, e.g. `https://127.0.0.1:50561`.
    #[arg(long)]
    pub endpoint: String,
    #[command(flatten)]
    pub tls: AdminClientTlsArgs,
}

#[cfg(feature = "openraft")]
#[derive(Parser, Debug)]
pub struct AdminIdArgs {
    #[arg(long)]
    pub endpoint: String,
    #[arg(long)]
    pub id: u64,
    #[command(flatten)]
    pub tls: AdminClientTlsArgs,
}

#[cfg(feature = "openraft")]
#[derive(Parser, Debug)]
pub struct AddLearnerArgs {
    #[arg(long)]
    pub endpoint: String,
    #[arg(long)]
    pub id: u64,
    #[arg(long)]
    pub raft_addr: String,
    #[arg(long)]
    pub service_endpoint: String,
    #[arg(long)]
    pub admin_endpoint: String,
    #[command(flatten)]
    pub tls: AdminClientTlsArgs,
}

#[cfg(feature = "openraft")]
#[derive(Parser, Debug)]
pub struct ActivateFormatArgs {
    /// Admin endpoint of any cluster member (the CLI does NOT follow
    /// leader-redirect for activation — see `dispatch_admin`).
    #[arg(long)]
    pub endpoint: String,
    /// Target format version. Must be within the local binary's readable
    /// range and supported by every cluster member.
    #[arg(long)]
    pub target: u8,
    #[command(flatten)]
    pub tls: AdminClientTlsArgs,
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
    /// PEM server certificate chain for the client gRPC API (enables TLS).
    #[arg(long)]
    pub tls_cert: Option<std::path::PathBuf>,
    /// PEM private key for `--tls-cert`.
    #[arg(long)]
    pub tls_key: Option<std::path::PathBuf>,
    /// PEM CA to verify CLIENT certificates (enables client mTLS on the API).
    #[arg(long)]
    pub tls_client_ca: Option<std::path::PathBuf>,
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
    /// Initial membership, ONLY with --bootstrap: `id=raft_host:port/service_host:port/admin_host:port,...`.
    #[arg(long)]
    pub members: Option<String>,
    #[arg(long, default_value = "250")]
    pub heartbeat_ms: u64,
    #[arg(long, default_value = "1000")]
    pub election_min_ms: u64,
    #[arg(long, default_value = "2000")]
    pub election_max_ms: u64,
    /// Bind address for the membership-admin gRPC server. UNAUTHENTICATED plaintext on loopback; non-loopback bind requires --admin-tls-{cert,key,ca}. Omit to serve no admin surface.
    #[arg(long)]
    pub admin_listen: Option<SocketAddr>,
    /// PEM server certificate for the admin gRPC server (enables admin mTLS; needs all three).
    #[arg(long)]
    pub admin_tls_cert: Option<std::path::PathBuf>,
    /// PEM private key for `--admin-tls-cert`.
    #[arg(long)]
    pub admin_tls_key: Option<std::path::PathBuf>,
    /// PEM CA (operator-dedicated, NOT the peer CA) to verify connecting admin clients.
    #[arg(long)]
    pub admin_tls_ca: Option<std::path::PathBuf>,
    /// PEM node certificate for the peer transport (enables peer mTLS; needs all three).
    #[arg(long)]
    pub peer_tls_cert: Option<std::path::PathBuf>,
    /// PEM private key for `--peer-tls-cert`.
    #[arg(long)]
    pub peer_tls_key: Option<std::path::PathBuf>,
    /// PEM CA (cluster-dedicated) to verify connecting peers.
    #[arg(long)]
    pub peer_tls_ca: Option<std::path::PathBuf>,
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
    /// PEM node certificate for the peer transport (enables peer mTLS; needs all three).
    #[arg(long)]
    pub peer_tls_cert: Option<std::path::PathBuf>,
    /// PEM private key for `--peer-tls-cert`.
    #[arg(long)]
    pub peer_tls_key: Option<std::path::PathBuf>,
    /// PEM CA (cluster-dedicated) to verify connecting peers.
    #[arg(long)]
    pub peer_tls_ca: Option<std::path::PathBuf>,
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
