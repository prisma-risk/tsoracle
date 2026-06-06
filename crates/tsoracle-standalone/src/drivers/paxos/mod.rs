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

mod network;

use std::sync::Arc;

use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tokio_stream::wrappers::TcpListenerStream;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_driver_paxos::{HighWaterCommand, PaxosDriver, SnapshotPolicy, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::{PeerEndpoint, TsoPeer};
use tsoracle_paxos_toolkit::storage::RocksdbStorage;

use crate::config::PaxosConfig;
use crate::error::StandaloneError;
use crate::{FatalSignal, Standalone, TransportHandle};

use network::{PeerSink, server as peer_server};

const PAXOS_CF: &str = "tso_paxos";

fn open_rocksdb(dir: &std::path::Path) -> Result<Arc<DB>, StandaloneError> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![ColumnFamilyDescriptor::new(PAXOS_CF, Options::default())];
    DB::open_cf_descriptors(&opts, dir, cfs)
        .map(Arc::new)
        .map_err(|source| StandaloneError::Storage {
            path: dir.to_path_buf(),
            source: Box::new(source),
        })
}

pub(crate) async fn build_paxos(cfg: PaxosConfig) -> Result<Standalone, StandaloneError> {
    build_paxos_inner(cfg, None).await
}

/// Test-only entry point: build with a pre-bound `TcpListener` for the paxos
/// peer port, bypassing the internal `TcpListener::bind`. Lets a test hold an
/// OS-assigned ephemeral port continuously from `lease_port()` through `build`,
/// eliminating the close/rebind race that periodically surfaces as `EADDRINUSE`
/// under parallel-test load. Sibling of `build_openraft_with_listeners` in the
/// openraft driver.
#[cfg(any(test, feature = "test-support"))]
pub async fn build_paxos_with_listeners(
    cfg: PaxosConfig,
    peer_listener: tokio::net::TcpListener,
) -> Result<Standalone, StandaloneError> {
    build_paxos_inner(cfg, Some(peer_listener)).await
}

async fn build_paxos_inner(
    cfg: PaxosConfig,
    peer_listener: Option<tokio::net::TcpListener>,
) -> Result<Standalone, StandaloneError> {
    // Mirror the openraft peer secure-by-default guard (drivers/openraft/mod.rs):
    // dry-build peer TLS first so a bad PEM surfaces as Tls (operator's
    // explicit intent) before the routable guard fires.
    let peer_tls = match &cfg.peer_tls {
        Some(p) => Some(crate::peer_tls::build_peer_tls(p)?),
        None => None,
    };

    if peer_tls.is_none() && !cfg.peer_listen.ip().is_loopback() {
        if !cfg.allow_insecure_peer {
            return Err(StandaloneError::PeerInsecureRoutable {
                addr: cfg.peer_listen,
            });
        }
        tracing::warn!(
            addr = %cfg.peer_listen,
            "peer listener bound without TLS via --allow-insecure-peer; \
             relying on out-of-band transport security"
        );
    }

    // Validate self identity (spec: Lifecycle): a node absent from its own
    // ClusterConfig.nodes can never be elected.
    if !cfg.peers.contains_key(&cfg.node_id) {
        return Err(StandaloneError::Config(format!(
            "peers map must contain this node's id {}",
            cfg.node_id
        )));
    }

    std::fs::create_dir_all(&cfg.data_dir).map_err(|source| StandaloneError::Storage {
        path: cfg.data_dir.clone(),
        source: Box::new(source),
    })?;
    let db = open_rocksdb(&cfg.data_dir)?;
    let storage = RocksdbStorage::<HighWaterCommand>::open_in(db, PAXOS_CF).map_err(|e| {
        StandaloneError::Storage {
            path: cfg.data_dir.clone(),
            source: Box::new(e),
        }
    })?;

    let mut node_ids: Vec<u64> = cfg.peers.keys().copied().collect();
    node_ids.sort_unstable();
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: node_ids,
        flexible_quorum: None,
    };
    let server_config = ServerConfig {
        pid: cfg.node_id,
        ..Default::default()
    };
    let omnipaxos = Arc::new(Mutex::new(
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
        .build(storage)
        .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?,
    ));

    // Bind the peer listener BEFORE spawning. Tests can hand in a `TcpListener`
    // they've held since `lease_port()` to skip the close/rebind race window.
    let listener = match peer_listener {
        Some(l) => l,
        None => tokio::net::TcpListener::bind(cfg.peer_listen)
            .await
            .map_err(|source| StandaloneError::PeerBind {
                addr: cfg.peer_listen,
                source,
            })?,
    };
    let peer_service = peer_server(omnipaxos.clone());
    let mut builder = tonic::transport::Server::builder();
    if let Some(material) = &peer_tls {
        builder = builder
            .tls_config(material.server.clone())
            .map_err(|source| StandaloneError::Tls {
                path: cfg
                    .peer_tls
                    .as_ref()
                    .map(|p| p.cert.clone())
                    .unwrap_or_default(),
                source: Box::new(source),
            })?;
    }
    let router = builder.add_service(peer_service);
    // Fail-fast signal for the peer server: an unexpected exit must take the
    // whole node down instead of leaving the consensus driver unreachable.
    let fatal = FatalSignal::new();
    let transport =
        TransportHandle::spawn_supervised("paxos peer server", fatal.clone(), move |shutdown| {
            router.serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
        });

    let admin_view = crate::admin::MembershipView {
        members: cfg
            .peers
            .iter()
            .map(|(id, addr)| crate::admin::MemberEntry {
                id: *id,
                role: crate::admin::MemberRole::Voter,
                raft_addr: addr.clone(),
                service_endpoint: cfg.tso_peers.get(id).cloned().unwrap_or_default(),
                admin_endpoint: String::new(),
            })
            .collect(),
        leader: None,
    };

    let toolkit_peers: Vec<TsoPeer> = cfg
        .tso_peers
        .iter()
        .filter(|(id, _)| **id != cfg.node_id)
        .map(|(id, endpoint)| {
            PeerEndpoint::try_from(endpoint.clone())
                .map(|endpoint| TsoPeer {
                    node_id: *id,
                    endpoint,
                })
                .map_err(|e| StandaloneError::Config(format!("peer {id}: {e}")))
        })
        .collect::<Result<_, _>>()?;
    let mut host = StandaloneHost::builder()
        .omnipaxos(omnipaxos)
        .my_node_id(cfg.node_id)
        .peers(toolkit_peers)
        .tick_interval(cfg.tick_interval)
        .snapshot_policy(SnapshotPolicy::disabled())
        .build()
        .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;
    let leader_subscriber = host
        .take_leader_subscriber()
        .ok_or_else(|| StandaloneError::Bootstrap("leader subscriber unavailable".into()))?;

    let sink = Arc::new(PeerSink::new(
        cfg.peers.into_iter().collect(),
        peer_tls.as_ref().map(|m| m.client.clone()),
    ));
    host.start(sink)
        .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;

    let driver = Arc::new(PaxosDriver::new(host, leader_subscriber));
    Ok(Standalone {
        driver: driver as Arc<dyn ConsensusDriver>,
        transport,
        drain: None,
        admin: std::sync::Arc::new(crate::admin::UnsupportedAdmin::new(admin_view)),
        admin_transport: crate::TransportHandle::noop(),
        admin_listen_addr: None,
        fatal,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    #[tokio::test]
    async fn build_paxos_rejects_node_absent_from_peers() {
        let mut peers = BTreeMap::new();
        peers.insert(2u64, "127.0.0.1:1".to_string());
        let cfg = PaxosConfig {
            node_id: 1,
            peer_listen: "127.0.0.1:0".parse().unwrap(),
            peers,
            tso_peers: BTreeMap::new(),
            data_dir: std::path::PathBuf::from("/this/path/must/not/be/touched"),
            tick_interval: Duration::from_millis(20),
            peer_tls: None,
            allow_insecure_peer: false,
        };
        match build_paxos(cfg).await {
            Err(StandaloneError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected Config error, got Ok"),
        }
    }
}
