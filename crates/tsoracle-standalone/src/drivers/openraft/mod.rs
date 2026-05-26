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

mod handoff;
mod network;

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::{Config, Raft};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_driver_openraft::{
    HighWaterStateMachine, OpenraftDriver, OpenraftLogCodec, OpenraftPeer, RocksdbSnapshotStore,
    SnapshotStore, StandaloneHost, TypeConfig,
};
use tsoracle_openraft_toolkit::{
    ActiveWriteVersion, Flat, RocksdbLogStore, recover_active_write_version,
};

use crate::config::OpenraftConfig;
use crate::error::StandaloneError;
use crate::{Standalone, TransportHandle};

use network::{MAX_PEER_MESSAGE_BYTES, PeerFactory, WriteVersionSource, server as peer_server};

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";
const SNAP_CF: &str = "raft_snapshot";
const MAX_CONCURRENT_STREAMS: u32 = 256;
const MAX_FRAME_SIZE: u32 = 64 * 1024;

fn open_rocksdb(dir: &std::path::Path) -> Result<Arc<DB>, StandaloneError> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cfs = vec![
        ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
        ColumnFamilyDescriptor::new(META_CF, Options::default()),
        ColumnFamilyDescriptor::new(SNAP_CF, Options::default()),
    ];
    DB::open_cf_descriptors(&opts, dir, cfs)
        .map(Arc::new)
        .map_err(|source| StandaloneError::Storage {
            path: dir.to_path_buf(),
            source: Box::new(source),
        })
}

pub(crate) async fn build_openraft(cfg: OpenraftConfig) -> Result<Standalone, StandaloneError> {
    // Validate membership/self identity (spec: Lifecycle).
    match (cfg.bootstrap, &cfg.initial_membership) {
        (true, Some(members)) if !members.contains_key(&cfg.id) => {
            return Err(StandaloneError::Config(format!(
                "initial membership must contain this node's id {}",
                cfg.id
            )));
        }
        (true, None) => {
            return Err(StandaloneError::Config(
                "--bootstrap requires initial membership".into(),
            ));
        }
        (false, Some(_)) => {
            return Err(StandaloneError::Config(
                "initial membership is only valid with --bootstrap".into(),
            ));
        }
        _ => {}
    }

    // Cross-trio guard: reject the trivial operator error of pointing
    // --peer-tls-ca and --admin-tls-ca at the same CA file. tonic's
    // client_ca_root accepts ANY cert signed by the configured CA, so a
    // shared CA means a compromised peer key can escalate to membership
    // control via the admin port (and vice versa). We check canonicalized
    // paths AND file bytes; we cannot detect "two distinct CAs where one
    // chains to the other" — that remains an operator deployment concern
    // documented in the AdminTlsConfig doc comment.
    if let (Some(peer), Some(admin)) = (cfg.peer_tls.as_ref(), cfg.admin_tls.as_ref()) {
        let same_path = std::fs::canonicalize(&peer.ca)
            .ok()
            .zip(std::fs::canonicalize(&admin.ca).ok())
            .map(|(a, b)| a == b)
            .unwrap_or(false);
        let same_bytes = !same_path
            && std::fs::read(&peer.ca)
                .ok()
                .zip(std::fs::read(&admin.ca).ok())
                .map(|(a, b)| a == b)
                .unwrap_or(false);
        if same_path || same_bytes {
            return Err(StandaloneError::Config(format!(
                "--peer-tls-ca ({}) and --admin-tls-ca ({}) resolve to the same CA; \
                 admin authentication must use a CA distinct from the peer CA so a \
                 compromised peer cert cannot change cluster membership",
                peer.ca.display(),
                admin.ca.display(),
            )));
        }
    }
    // Build (and eagerly dry-run validate) admin TLS material first, so a
    // bad PEM surfaces as Tls — the operator's explicit intent — and takes
    // precedence over the secure-by-default routable guard below.
    let admin_tls_material: Option<crate::admin_tls::AdminTlsMaterial> = match &cfg.admin_tls {
        Some(t) => Some(crate::admin_tls::build_admin_tls(t)?),
        None => None,
    };
    // Secure-by-default: routable bind requires admin TLS. Loopback
    // (127.0.0.0/8, ::1) is allowed plaintext for local-dev and same-pod
    // sidecar callers; everything else must present TLS. Run BEFORE opening
    // storage / binding any socket so a misconfigured deployment fails fast
    // without leaving a half-initialized data dir.
    if let Some(listen) = cfg.admin_listen
        && admin_tls_material.is_none()
        && !listen.ip().is_loopback()
    {
        return Err(StandaloneError::AdminInsecureRoutable { addr: listen });
    }

    std::fs::create_dir_all(&cfg.raft_dir).map_err(|source| StandaloneError::Storage {
        path: cfg.raft_dir.clone(),
        source: Box::new(source),
    })?;
    let db = open_rocksdb(&cfg.raft_dir)?;
    let log_store: RocksdbLogStore<TypeConfig, Flat, OpenraftLogCodec> =
        RocksdbLogStore::open(db.clone(), LOG_CF, META_CF, Flat).map_err(|e| {
            StandaloneError::Storage {
                path: cfg.raft_dir.clone(),
                source: Box::new(e),
            }
        })?;
    let snapshot_store: Arc<dyn SnapshotStore> =
        Arc::new(RocksdbSnapshotStore::open(db, SNAP_CF).map_err(|e| {
            StandaloneError::Storage {
                path: cfg.raft_dir.clone(),
                source: Box::new(e),
            }
        })?);

    // Seed the single, process-shared active-write-version cell from
    // fsync-durable evidence. NO meta key is read: durability of any
    // activation is the raft log (deterministically re-applied on restart)
    // plus the snapshot's leading frame byte; the cell is just the runtime
    // holder. Recovery takes the max of the baseline, the persisted snapshot's
    // leading version byte, and the highest version byte among durable log
    // records — never below what this node actually wrote.
    let snapshot_leading_byte = snapshot_store
        .load()
        .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?
        .as_deref()
        .and_then(|bytes| bytes.first().copied());
    let highest_log_record_byte = log_store
        .highest_log_record_version()
        .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;
    let active_write_version = ActiveWriteVersion::new(
        recover_active_write_version(snapshot_leading_byte, highest_log_record_byte)
            .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?,
    );

    // Thread the SAME cell into both writers: the log store stamps appended
    // records, the state machine stamps snapshots, both reading this one cell.
    let log_store = log_store.with_active_write_version(active_write_version.clone());
    let state_machine =
        HighWaterStateMachine::with_store_and_active_version(snapshot_store, active_write_version)
            .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;
    let state_machine_for_host = state_machine.clone();

    // Peer transport reads the active write version per-RPC through this
    // closure so a runtime activation flip takes effect on the next message
    // without rebuilding the transport.
    let version_source: WriteVersionSource = {
        let state_machine = state_machine.clone();
        Arc::new(move || state_machine.active_write_version())
    };

    let config = Arc::new(
        Config {
            heartbeat_interval: cfg.tuning.heartbeat_ms,
            election_timeout_min: cfg.tuning.election_min_ms,
            election_timeout_max: cfg.tuning.election_max_ms,
            ..Default::default()
        }
        .validate()
        .map_err(|e| StandaloneError::Config(e.to_string()))?,
    );

    let peer_tls = match &cfg.peer_tls {
        Some(p) => Some(crate::peer_tls::build_peer_tls(p)?),
        None => None,
    };

    let network = PeerFactory::new(
        peer_tls.as_ref().map(|m| m.client.clone()),
        version_source.clone(),
    );
    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        cfg.id,
        config,
        network,
        log_store,
        state_machine,
    )
    .await
    .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;

    // Bind the peer listener BEFORE spawning, so bind failures surface here.
    let listener = tokio::net::TcpListener::bind(cfg.raft_addr)
        .await
        .map_err(|source| StandaloneError::PeerBind {
            addr: cfg.raft_addr,
            source,
        })?;
    let peer_service = peer_server(raft.clone(), version_source)
        .max_decoding_message_size(MAX_PEER_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_PEER_MESSAGE_BYTES);
    let mut builder = tonic::transport::Server::builder()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_frame_size(MAX_FRAME_SIZE);
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
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let shutdown = async {
            let _ = cancel_rx.await;
        };
        if let Err(e) = router
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await
        {
            tracing::error!(error = ?e, "raft peer server died");
        }
    });

    // Bootstrap once (after the peer server is listening).
    if cfg.bootstrap {
        if let Some(members) = cfg.initial_membership {
            let nodes: BTreeMap<u64, OpenraftPeer> = members
                .into_iter()
                .map(|(id, m)| {
                    (
                        id,
                        OpenraftPeer {
                            addr: m.raft_addr,
                            service_endpoint: m.service_endpoint,
                            admin_endpoint: m.admin_endpoint,
                        },
                    )
                })
                .collect();
            if let Err(e) = raft.initialize(nodes).await {
                tracing::warn!(error = ?e, "initialize() returned an error (expected if already initialized)");
            }
        }
    }

    let raft_for_drain = raft.clone();
    let raft_for_admin = raft.clone();
    let my_id = cfg.id;

    let host = StandaloneHost::new(raft, state_machine_for_host);
    // OpenraftDriver::new returns Arc<Self> — do NOT wrap again.
    let driver = OpenraftDriver::new(host);

    let admin: std::sync::Arc<dyn crate::admin::MembershipAdmin> = std::sync::Arc::new(
        crate::admin::openraft::OpenraftMembershipAdmin::new(raft_for_admin),
    );

    let (admin_transport, admin_listen_addr) = match cfg.admin_listen {
        Some(listen) => {
            let (cancel, join, bound) =
                crate::admin::service::serve_admin(admin.clone(), listen, admin_tls_material)
                    .await
                    .map_err(|source| StandaloneError::AdminBind {
                        addr: listen,
                        source,
                    })?;
            (TransportHandle::new(cancel, join), Some(bound))
        }
        None => (TransportHandle::noop(), None),
    };

    Ok(Standalone {
        driver: driver as Arc<dyn ConsensusDriver>,
        transport: TransportHandle::new(cancel_tx, join),
        drain: Some(Box::pin(async move {
            handoff::graceful_leader_handoff(&raft_for_drain, my_id).await
        })),
        admin,
        admin_transport,
        admin_listen_addr,
    })
}
