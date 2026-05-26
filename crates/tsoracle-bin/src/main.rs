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
        Some(Cmd::Serve(serve)) => dispatch_serve(*serve).await,
        #[cfg(feature = "openraft")]
        Some(Cmd::Admin(cmd)) => dispatch_admin(cmd).await,
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
                    peer_tls: peer_tls_config(
                        args.peer_tls_cert,
                        args.peer_tls_key,
                        args.peer_tls_ca,
                    )?,
                    admin_listen: args.admin_listen,
                    admin_tls: admin_tls_config(
                        args.admin_tls_cert,
                        args.admin_tls_key,
                        args.admin_tls_ca,
                    )?,
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
                    peer_tls: peer_tls_config(
                        args.peer_tls_cert,
                        args.peer_tls_key,
                        args.peer_tls_ca,
                    )?,
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

/// Assemble the client-API server TLS config from flags (server-auth + optional
/// client mTLS). `Ok(None)` when no TLS flags are given (plaintext). Eagerly
/// validated: `from_pem` is lazy and the server applies tls only inside serve()
/// (after bind + "serving on"), so we dry-run the acceptor build here.
fn client_tls_config(
    common: &CommonServeArgs,
) -> anyhow::Result<Option<tonic::transport::ServerTlsConfig>> {
    match (&common.tls_cert, &common.tls_key) {
        (None, None) => {
            if common.tls_client_ca.is_some() {
                anyhow::bail!("--tls-client-ca requires --tls-cert and --tls-key");
            }
            Ok(None)
        }
        (Some(cert), Some(key)) => {
            let cert_pem =
                std::fs::read(cert).with_context(|| format!("read {}", cert.display()))?;
            let key_pem = std::fs::read(key).with_context(|| format!("read {}", key.display()))?;
            let mut tls = tonic::transport::ServerTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(&cert_pem, &key_pem));
            if let Some(ca) = &common.tls_client_ca {
                let ca_pem = std::fs::read(ca).with_context(|| format!("read {}", ca.display()))?;
                tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(&ca_pem));
            }
            tonic::transport::Server::builder()
                .tls_config(tls.clone())
                .context("invalid client-API TLS configuration")?;
            Ok(Some(tls))
        }
        _ => anyhow::bail!("--tls-cert and --tls-key must be provided together"),
    }
}

/// Assemble the peer `PeerTlsConfig` from the all-or-nothing flag trio.
#[cfg(any(feature = "openraft", feature = "paxos"))]
fn peer_tls_config(
    cert: Option<std::path::PathBuf>,
    key: Option<std::path::PathBuf>,
    ca: Option<std::path::PathBuf>,
) -> anyhow::Result<Option<tsoracle_standalone::PeerTlsConfig>> {
    match (cert, key, ca) {
        (None, None, None) => Ok(None),
        (Some(cert), Some(key), Some(ca)) => {
            Ok(Some(tsoracle_standalone::PeerTlsConfig { cert, key, ca }))
        }
        _ => anyhow::bail!(
            "--peer-tls-cert, --peer-tls-key, and --peer-tls-ca must all be set together"
        ),
    }
}

/// Assemble the admin `AdminTlsConfig` from the all-or-nothing flag trio.
#[cfg(feature = "openraft")]
fn admin_tls_config(
    cert: Option<std::path::PathBuf>,
    key: Option<std::path::PathBuf>,
    ca: Option<std::path::PathBuf>,
) -> anyhow::Result<Option<tsoracle_standalone::AdminTlsConfig>> {
    match (cert, key, ca) {
        (None, None, None) => Ok(None),
        (Some(cert), Some(key), Some(ca)) => {
            Ok(Some(tsoracle_standalone::AdminTlsConfig { cert, key, ca }))
        }
        _ => anyhow::bail!(
            "--admin-tls-cert, --admin-tls-key, and --admin-tls-ca must all be set together"
        ),
    }
}

/// Build a `ClientTlsConfig` for `tsoracle admin` from the all-or-nothing flag trio.
#[cfg(feature = "openraft")]
fn admin_client_tls(
    args: &cli::AdminClientTlsArgs,
) -> anyhow::Result<Option<tonic::transport::ClientTlsConfig>> {
    match (
        args.client_tls_cert.as_ref(),
        args.client_tls_key.as_ref(),
        args.client_tls_ca.as_ref(),
    ) {
        (None, None, None) => Ok(None),
        (Some(cert), Some(key), Some(ca)) => {
            let cert_pem =
                std::fs::read(cert).with_context(|| format!("read {}", cert.display()))?;
            let key_pem = std::fs::read(key).with_context(|| format!("read {}", key.display()))?;
            let ca_pem = std::fs::read(ca).with_context(|| format!("read {}", ca.display()))?;
            Ok(Some(
                tonic::transport::ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(&ca_pem))
                    .identity(tonic::transport::Identity::from_pem(&cert_pem, &key_pem)),
            ))
        }
        _ => anyhow::bail!(
            "--client-tls-cert, --client-tls-key, and --client-tls-ca must all be set together"
        ),
    }
}

/// Dial an admin endpoint, optionally with mTLS material.
#[cfg(feature = "openraft")]
async fn admin_connect(
    endpoint: &str,
    tls: Option<&tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<tonic::transport::Channel> {
    let builder = tonic::transport::Channel::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid endpoint {endpoint}"))?;
    let builder = match tls {
        Some(t) => builder
            .tls_config(t.clone())
            .with_context(|| "apply admin client TLS")?,
        None => builder,
    };
    builder
        .connect()
        .await
        .with_context(|| format!("connect {endpoint}"))
}

async fn run_serve(common: CommonServeArgs, cfg: DriverConfig) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&common.log).unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let mut node: Standalone = tsoracle_standalone::build(cfg)
        .await
        .context("driver bootstrap")?;
    let drain = node.take_drain();
    let tls = client_tls_config(&common)?;
    let mut builder = Server::builder()
        .consensus_driver(node.driver.clone())
        .window_ahead(common.window_ahead)
        .failover_advance(common.failover_advance);
    if let Some(tls) = tls {
        builder = builder.tls_config(tls);
    }
    let server = builder.build().context("server build")?;

    let listener = tokio::net::TcpListener::bind(common.listen)
        .await
        .with_context(|| format!("bind {}", common.listen))?;
    let local_addr = listener.local_addr().context("listener.local_addr()")?;
    // Plain-stdout contract: supervisors parse this to discover the OS-picked port.
    println!("serving on {local_addr}");
    tracing::info!(addr = %local_addr, "tsoracle serving");

    // On drain, run the driver's pre-shutdown step (openraft: leadership
    // handoff) BEFORE the gRPC server stops accepting, then let serve drain.
    let shutdown = async move {
        tsoracle_server::shutdown_signal().await;
        if let Some(drain) = drain {
            drain.await;
        }
    };
    let result = server
        .serve_with_listener(listener, shutdown)
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
            format!("bad member {entry:?}, expected id=raft_addr/service_endpoint/admin_endpoint")
        })?;
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
            id.trim()
                .parse()
                .with_context(|| format!("bad member id in {entry:?}"))?,
            tsoracle_standalone::MemberAddr {
                raft_addr: raft_addr.trim().to_string(),
                service_endpoint: service_endpoint.trim().to_string(),
                admin_endpoint: admin_endpoint.trim().to_string(),
            },
        );
    }
    Ok(out)
}

#[cfg(feature = "openraft")]
async fn dispatch_admin(cmd: cli::AdminCmd) -> Result<()> {
    use cli::AdminCmd;
    use tsoracle_standalone::admin_proto::membership_admin_client::MembershipAdminClient;
    use tsoracle_standalone::admin_proto::{
        ActivateFormatRequest, AddLearnerRequest, AdminErrorKind, ChangeResponse,
        ListMembersRequest, MemberRole, PromoteRequest, RemoveNodeRequest,
    };

    // Re-run `op` against the leader if the first call says NOT_LEADER. The
    // leader-hint's scheme is reconstructed from the originally dialed
    // endpoint so a `https://` operator session doesn't get downgraded to
    // plaintext on redirect (and vice versa).
    async fn with_redirect<MakeCall>(
        endpoint: String,
        tls: Option<&tonic::transport::ClientTlsConfig>,
        op: MakeCall,
    ) -> Result<ChangeResponse>
    where
        MakeCall: Fn(
            MembershipAdminClient<tonic::transport::Channel>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<ChangeResponse, tonic::Status>> + Send>,
        >,
    {
        let channel = admin_connect(&endpoint, tls).await?;
        let resp = op(MembershipAdminClient::new(channel))
            .await
            .context("admin rpc")?;
        if resp.error == AdminErrorKind::NotLeader as i32 && !resp.leader_admin_endpoint.is_empty()
        {
            let scheme = if endpoint.starts_with("https://") {
                "https"
            } else {
                "http"
            };
            let leader = format!("{scheme}://{}", resp.leader_admin_endpoint);
            let channel = admin_connect(&leader, tls).await?;
            return op(MembershipAdminClient::new(channel))
                .await
                .context("admin rpc (leader)");
        }
        Ok(resp)
    }

    fn report(resp: ChangeResponse) -> Result<()> {
        if resp.ok {
            println!("ok");
            Ok(())
        } else {
            let kind = AdminErrorKind::try_from(resp.error)
                .map(|kind| format!("{kind:?}"))
                .unwrap_or_else(|_| resp.error.to_string());
            anyhow::bail!("admin error ({kind}): {}", resp.message)
        }
    }

    /// Activation-specific reporter: success / gate-rejection / NOT_LEADER /
    /// local-range-rejection each get their own exit code per the issue #484
    /// contract. Anything else exits 1 via the bubbled `anyhow::Error`.
    fn report_activation(resp: ChangeResponse) -> Result<()> {
        if resp.ok {
            println!("ok");
            return Ok(());
        }
        let kind = AdminErrorKind::try_from(resp.error)
            .map(|k| format!("{k:?}"))
            .unwrap_or_else(|_| resp.error.to_string());
        match AdminErrorKind::try_from(resp.error) {
            Ok(AdminErrorKind::MembersBelowTarget) => {
                eprintln!("activate-format: gate rejected: {}", resp.message);
                std::process::exit(2);
            }
            Ok(AdminErrorKind::NotLeader) => {
                eprintln!("activate-format: not the leader");
                std::process::exit(3);
            }
            Ok(AdminErrorKind::TargetOutOfRange) => {
                eprintln!(
                    "activate-format: target outside local readable range: {}",
                    resp.message
                );
                std::process::exit(4);
            }
            _ => anyhow::bail!("activate-format error ({kind}): {}", resp.message),
        }
    }

    match cmd {
        AdminCmd::Members(args) => {
            let tls = admin_client_tls(&args.tls)?;
            let channel = admin_connect(&args.endpoint, tls.as_ref()).await?;
            let mut client = MembershipAdminClient::new(channel);
            let view = client
                .list_members(ListMembersRequest {})
                .await
                .context("list_members")?
                .into_inner();
            let leader_str: String = if view.has_leader {
                view.leader.to_string()
            } else {
                "none".into()
            };
            println!("leader: {leader_str}");
            for member in view.members {
                let role = MemberRole::try_from(member.role)
                    .map(|role| format!("{role:?}"))
                    .unwrap_or_else(|_| member.role.to_string());
                println!(
                    "  id={} role={role} raft={} service={} admin={}",
                    member.id, member.raft_addr, member.service_endpoint, member.admin_endpoint
                );
            }
            Ok(())
        }
        AdminCmd::AddLearner(args) => {
            let tls = admin_client_tls(&args.tls)?;
            let endpoint = args.endpoint.clone();
            let id = args.id;
            let raft_addr = args.raft_addr.clone();
            let service_endpoint = args.service_endpoint.clone();
            let admin_endpoint = args.admin_endpoint.clone();
            report(
                with_redirect(endpoint, tls.as_ref(), move |mut client| {
                    let request = AddLearnerRequest {
                        id,
                        raft_addr: raft_addr.clone(),
                        service_endpoint: service_endpoint.clone(),
                        admin_endpoint: admin_endpoint.clone(),
                    };
                    Box::pin(
                        async move { client.add_learner(request).await.map(|r| r.into_inner()) },
                    )
                })
                .await?,
            )
        }
        AdminCmd::Promote(args) => {
            let tls = admin_client_tls(&args.tls)?;
            let endpoint = args.endpoint.clone();
            let id = args.id;
            report(
                with_redirect(endpoint, tls.as_ref(), move |mut client| {
                    Box::pin(async move {
                        client
                            .promote(PromoteRequest { id })
                            .await
                            .map(|r| r.into_inner())
                    })
                })
                .await?,
            )
        }
        AdminCmd::Remove(args) => {
            let tls = admin_client_tls(&args.tls)?;
            let endpoint = args.endpoint.clone();
            let id = args.id;
            report(
                with_redirect(endpoint, tls.as_ref(), move |mut client| {
                    Box::pin(async move {
                        client
                            .remove_node(RemoveNodeRequest { id })
                            .await
                            .map(|r| r.into_inner())
                    })
                })
                .await?,
            )
        }
        AdminCmd::ActivateFormat(args) => {
            let tls = admin_client_tls(&args.tls)?;
            let endpoint = args.endpoint.clone();
            let target = args.target;
            report_activation(
                with_redirect(endpoint, tls.as_ref(), move |mut client| {
                    Box::pin(async move {
                        client
                            .activate_format(ActivateFormatRequest {
                                target: target as u32,
                            })
                            .await
                            .map(|r| r.into_inner())
                    })
                })
                .await?,
            )
        }
    }
}

#[cfg(all(test, feature = "openraft"))]
mod parse_members_tests {
    use super::parse_members;

    #[test]
    fn parses_three_address_members() {
        let map = parse_members("1=10.0.0.1:9/10.0.0.1:8/10.0.0.1:7").unwrap();
        let member = map.get(&1).expect("id 1 present");
        assert_eq!(member.raft_addr, "10.0.0.1:9");
        assert_eq!(member.service_endpoint, "10.0.0.1:8");
        assert_eq!(member.admin_endpoint, "10.0.0.1:7");
    }

    #[test]
    fn rejects_member_missing_admin_endpoint() {
        let err = parse_members("1=10.0.0.1:9/10.0.0.1:8").unwrap_err();
        assert!(
            err.to_string()
                .contains("raft_addr/service_endpoint/admin_endpoint"),
            "got: {err}"
        );
    }
}

#[cfg(all(test, feature = "openraft"))]
mod admin_tls_config_tests {
    use super::admin_tls_config;
    use std::path::PathBuf;

    #[test]
    fn none_returns_none() {
        let result = admin_tls_config(None, None, None).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn full_trio_returns_some() {
        let cfg = admin_tls_config(
            Some(PathBuf::from("/x/c")),
            Some(PathBuf::from("/x/k")),
            Some(PathBuf::from("/x/a")),
        )
        .unwrap()
        .expect("Some");
        assert_eq!(cfg.cert, PathBuf::from("/x/c"));
        assert_eq!(cfg.key, PathBuf::from("/x/k"));
        assert_eq!(cfg.ca, PathBuf::from("/x/a"));
    }

    #[test]
    fn partial_trio_errors() {
        for (c, k, a) in [
            (Some(PathBuf::from("c")), None, None),
            (None, Some(PathBuf::from("k")), None),
            (None, None, Some(PathBuf::from("a"))),
            (Some(PathBuf::from("c")), Some(PathBuf::from("k")), None),
            (Some(PathBuf::from("c")), None, Some(PathBuf::from("a"))),
            (None, Some(PathBuf::from("k")), Some(PathBuf::from("a"))),
        ] {
            let err = admin_tls_config(c, k, a).unwrap_err();
            assert!(
                err.to_string().contains("must all be set together"),
                "got: {err}"
            );
        }
    }
}

#[cfg(all(test, feature = "openraft"))]
mod admin_client_tls_tests {
    use super::admin_client_tls;
    use crate::cli::AdminClientTlsArgs;
    use std::path::PathBuf;

    fn args(cert: Option<&str>, key: Option<&str>, ca: Option<&str>) -> AdminClientTlsArgs {
        AdminClientTlsArgs {
            client_tls_cert: cert.map(PathBuf::from),
            client_tls_key: key.map(PathBuf::from),
            client_tls_ca: ca.map(PathBuf::from),
        }
    }

    #[test]
    fn none_returns_none() {
        let result = admin_client_tls(&args(None, None, None)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn partial_trio_errors() {
        let err = admin_client_tls(&args(Some("c"), None, None)).unwrap_err();
        assert!(
            err.to_string().contains("must all be set together"),
            "got: {err}"
        );
    }

    // No "full trio returns Some" test here — that would require real PEM files
    // on disk because admin_client_tls reads + parses them. The integration test
    // in openraft_membership.rs already proves a real cert produces a working
    // channel.
}
