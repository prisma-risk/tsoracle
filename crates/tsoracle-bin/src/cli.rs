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
    /// List current members. With `--capabilities`, also gather and show each
    /// member's format-version capabilities.
    Members(MembersArgs),
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
    /// Report every member's format-version capabilities (min/max readable and
    /// active write version). Read-only; answerable by any node. Use before
    /// `activate-format` to confirm the cluster is ready for a target version.
    Capabilities(CapabilitiesArgs),
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
pub struct MembersArgs {
    /// Any node's admin endpoint, e.g. `https://127.0.0.1:50561`.
    #[arg(long)]
    pub endpoint: String,
    /// Also gather and display each member's format-version capabilities.
    #[arg(long)]
    pub capabilities: bool,
    #[command(flatten)]
    pub tls: AdminClientTlsArgs,
}

#[cfg(feature = "openraft")]
#[derive(Parser, Debug)]
pub struct CapabilitiesArgs {
    /// Any node's admin endpoint, e.g. `https://127.0.0.1:50561`.
    #[arg(long)]
    pub endpoint: String,
    /// Emit the report as JSON instead of the aligned table.
    #[arg(long)]
    pub json: bool,
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
    /// Admin endpoint of any cluster member. `NOT_LEADER` responses for
    /// activation carry an empty `leader_admin_endpoint` (the underlying
    /// `FormatActivationError::NotLeader` is a unit variant), so the
    /// existing `with_redirect` short-circuits and the CLI exits 3 rather
    /// than auto-redirecting. Re-issue against a known leader.
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
    /// Interval between proof-of-life heartbeat log lines. Default 10s.
    /// Pass `0s` to disable.
    #[arg(long, value_parser = parse_duration, default_value = "10s")]
    pub heartbeat_interval: Duration,
    /// Log level.
    #[arg(long, default_value = "info")]
    pub log: String,
    /// Prometheus /metrics listen address.
    #[cfg(feature = "metrics")]
    #[arg(long, default_value = "127.0.0.1:9551")]
    pub metrics_listen: SocketAddr,
    /// Disable the Prometheus metrics exporter. Accepted in every build so
    /// scripts and test harnesses can pass it unconditionally; it is a no-op
    /// when the binary is compiled without the `metrics` feature.
    #[arg(long)]
    pub no_metrics: bool,
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
    /// Address to listen for raft peer RPCs. Plaintext on loopback; routable bind requires --peer-tls-{cert,key,ca} or --allow-insecure-peer.
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
    /// Opt out of the peer-listener secure-by-default guard. Allows
    /// routable bind without --peer-tls-*. Matches the helm chart's
    /// tls.allowInsecurePeer. Intended for single-host dev or
    /// service-mesh-terminated mTLS only.
    #[arg(long)]
    pub allow_insecure_peer: bool,
}

#[derive(Parser, Debug, Clone)]
pub struct PaxosArgs {
    #[command(flatten)]
    pub common: CommonServeArgs,
    /// This node's OmniPaxos pid (unique across the cluster).
    #[arg(long)]
    pub node_id: u64,
    /// Address to listen for paxos peer RPCs. Plaintext on loopback; routable bind requires --peer-tls-{cert,key,ca} or --allow-insecure-peer.
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
    /// Opt out of the peer-listener secure-by-default guard. Allows
    /// routable bind without --peer-tls-*. Matches the helm chart's
    /// tls.allowInsecurePeer. Intended for single-host dev or
    /// service-mesh-terminated mTLS only.
    #[arg(long)]
    pub allow_insecure_peer: bool,
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

#[cfg(all(test, feature = "openraft"))]
mod admin_capabilities_parse_tests {
    use super::{AdminCmd, Cli, Cmd};
    use clap::Parser;

    #[test]
    fn capabilities_parses_endpoint() {
        let cli = Cli::try_parse_from([
            "tsoracle",
            "admin",
            "capabilities",
            "--endpoint",
            "http://127.0.0.1:51002",
        ])
        .unwrap();
        match cli.cmd {
            Some(Cmd::Admin(AdminCmd::Capabilities(args))) => {
                assert_eq!(args.endpoint, "http://127.0.0.1:51002");
                assert!(!args.json);
            }
            other => panic!("expected Capabilities, got {other:?}"),
        }
    }

    #[test]
    fn capabilities_json_flag_sets_true() {
        let cli = Cli::try_parse_from([
            "tsoracle",
            "admin",
            "capabilities",
            "--endpoint",
            "http://x",
            "--json",
        ])
        .unwrap();
        match cli.cmd {
            Some(Cmd::Admin(AdminCmd::Capabilities(args))) => assert!(args.json),
            other => panic!("expected Capabilities, got {other:?}"),
        }
    }

    #[test]
    fn members_capabilities_flag_sets_true() {
        let cli = Cli::try_parse_from([
            "tsoracle",
            "admin",
            "members",
            "--endpoint",
            "http://x",
            "--capabilities",
        ])
        .unwrap();
        match cli.cmd {
            Some(Cmd::Admin(AdminCmd::Members(args))) => {
                assert_eq!(args.endpoint, "http://x");
                assert!(args.capabilities);
            }
            other => panic!("expected Members, got {other:?}"),
        }
    }
}

#[cfg(all(test, feature = "metrics"))]
mod metrics_args_tests {
    use super::{Cli, Cmd, ServeCmd};
    use clap::Parser;
    use std::net::SocketAddr;

    #[test]
    fn serve_metrics_args_default_to_loopback_exporter() {
        let cli = Cli::try_parse_from([
            "tsoracle",
            "serve",
            "file",
            "--state-dir",
            "/tmp/tsoracle-data",
        ])
        .unwrap();
        let Some(Cmd::Serve(serve)) = cli.cmd else {
            panic!("expected serve command");
        };
        let ServeCmd::File(args) = *serve else {
            panic!("expected file args");
        };
        assert_eq!(
            args.common.metrics_listen,
            SocketAddr::from(([127, 0, 0, 1], 9551))
        );
        assert!(!args.common.no_metrics);
    }

    #[test]
    fn serve_metrics_args_allow_override_and_disable() {
        let cli = Cli::try_parse_from([
            "tsoracle",
            "serve",
            "file",
            "--metrics-listen",
            "0.0.0.0:9551",
            "--no-metrics",
            "--state-dir",
            "/tmp/tsoracle-data",
        ])
        .unwrap();
        let Some(Cmd::Serve(serve)) = cli.cmd else {
            panic!("expected serve command");
        };
        let ServeCmd::File(args) = *serve else {
            panic!("expected file args");
        };
        assert_eq!(
            args.common.metrics_listen,
            SocketAddr::from(([0, 0, 0, 0], 9551))
        );
        assert!(args.common.no_metrics);
    }
}

#[cfg(all(test, not(feature = "metrics")))]
mod no_metrics_flag_without_feature_tests {
    use super::{Cli, Cmd, ServeCmd};
    use clap::Parser;

    // The stress process harness (and other scripts) pass `--no-metrics`
    // unconditionally; it must parse even in builds compiled without the
    // `metrics` feature, where it is a documented no-op.
    #[test]
    fn serve_accepts_no_metrics_without_feature() {
        let cli = Cli::try_parse_from([
            "tsoracle",
            "serve",
            "file",
            "--no-metrics",
            "--state-dir",
            "/tmp/tsoracle-data",
        ])
        .unwrap();
        let Some(Cmd::Serve(serve)) = cli.cmd else {
            panic!("expected serve command");
        };
        let ServeCmd::File(args) = *serve else {
            panic!("expected file args");
        };
        assert!(args.common.no_metrics);
    }
}
