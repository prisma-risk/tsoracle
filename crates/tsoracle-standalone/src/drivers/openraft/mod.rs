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
    HighWaterStateMachine, OpenraftDriver, OpenraftPeer, RocksdbSnapshotStore, SnapshotStore,
    StandaloneHost, TypeConfig,
};
use tsoracle_openraft_toolkit::{Flat, RocksdbLogStore};

use crate::config::OpenraftConfig;
use crate::error::StandaloneError;
use crate::{Standalone, TransportHandle};

use network::{MAX_PEER_MESSAGE_BYTES, PeerFactory, server as peer_server};

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

    std::fs::create_dir_all(&cfg.raft_dir).map_err(|source| StandaloneError::Storage {
        path: cfg.raft_dir.clone(),
        source: Box::new(source),
    })?;
    let db = open_rocksdb(&cfg.raft_dir)?;
    let log_store = RocksdbLogStore::open(db.clone(), LOG_CF, META_CF, Flat).map_err(|e| {
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
    let state_machine = HighWaterStateMachine::with_store(snapshot_store)
        .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;
    let state_machine_for_host = state_machine.clone();

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

    let network = PeerFactory::new(peer_tls.as_ref().map(|m| m.client.clone()));
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
    let peer_service = peer_server(raft.clone())
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
    let my_id = cfg.id;

    let host = StandaloneHost::new(raft, state_machine_for_host);
    // OpenraftDriver::new returns Arc<Self> — do NOT wrap again.
    let driver = OpenraftDriver::new(host);

    Ok(Standalone {
        driver: driver as Arc<dyn ConsensusDriver>,
        transport: TransportHandle::new(cancel_tx, join),
        drain: Some(Box::pin(async move {
            handoff::graceful_leader_handoff(&raft_for_drain, my_id).await
        })),
    })
}
