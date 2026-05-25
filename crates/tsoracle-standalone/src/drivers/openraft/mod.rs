mod network;

use std::collections::BTreeMap;
use std::sync::Arc;

use openraft::async_runtime::watch::WatchReceiver;
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

    let network = PeerFactory::new();
    let raft = Raft::<TypeConfig, HighWaterStateMachine>::new(
        cfg.id,
        config,
        network,
        log_store,
        state_machine,
    )
    .await
    .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;

    // Non-bootstrap restart: membership is recovered from persisted raft state
    // (#408). Refuse to come up isolated against an empty/uninitialized store.
    if !cfg.bootstrap {
        // `membership_config` is `Arc<StoredMembership<..>>`; `.nodes()` yields
        // `(&NodeId, &Node)` (mirrors the toolkit's `lifecycle/leader.rs`).
        let recovered = raft.metrics().borrow_watched().membership_config.clone();
        let known_self = recovered.nodes().any(|(id, _)| *id == cfg.id);
        if !known_self {
            return Err(StandaloneError::Config(format!(
                "node {} started without --bootstrap but persisted state has no \
                 membership including it; bootstrap the cluster first",
                cfg.id
            )));
        }
    }

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
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let shutdown = async {
            let _ = cancel_rx.await;
        };
        if let Err(e) = tonic::transport::Server::builder()
            .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
            .max_frame_size(MAX_FRAME_SIZE)
            .add_service(peer_service)
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

    let host = StandaloneHost::new(raft, state_machine_for_host);
    // OpenraftDriver::new returns Arc<Self> — do NOT wrap again.
    let driver = OpenraftDriver::new(host);

    Ok(Standalone {
        driver: driver as Arc<dyn ConsensusDriver>,
        transport: TransportHandle::new(cancel_tx, join),
    })
}
