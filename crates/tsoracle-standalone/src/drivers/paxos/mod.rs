mod network;

use std::sync::Arc;

use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_driver_paxos::{HighWaterCommand, PaxosDriver, SnapshotPolicy, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::TsoPeer;
use tsoracle_paxos_toolkit::storage::RocksdbStorage;

use crate::config::PaxosConfig;
use crate::error::StandaloneError;
use crate::{Standalone, TransportHandle};

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

    // Bind the peer listener BEFORE spawning.
    let listener = tokio::net::TcpListener::bind(cfg.peer_listen)
        .await
        .map_err(|source| StandaloneError::PeerBind {
            addr: cfg.peer_listen,
            source,
        })?;
    let peer_service = peer_server(omnipaxos.clone());
    let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
    let join = tokio::spawn(async move {
        let shutdown = async {
            let _ = cancel_rx.await;
        };
        if let Err(err) = tonic::transport::Server::builder()
            .add_service(peer_service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown)
            .await
        {
            tracing::error!(error = ?err, "paxos peer server died");
        }
    });

    let toolkit_peers: Vec<TsoPeer> = cfg
        .tso_peers
        .iter()
        .filter(|(id, _)| **id != cfg.node_id)
        .map(|(id, endpoint)| TsoPeer {
            node_id: *id,
            endpoint: format!("http://{endpoint}"),
        })
        .collect();
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

    let sink = Arc::new(PeerSink::new(cfg.peers.into_iter().collect()));
    host.start(sink)
        .map_err(|e| StandaloneError::Bootstrap(Box::new(e)))?;

    let driver = Arc::new(PaxosDriver::new(host, leader_subscriber));
    Ok(Standalone {
        driver: driver as Arc<dyn ConsensusDriver>,
        transport: TransportHandle::new(cancel_tx, join),
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
        };
        match build_paxos(cfg).await {
            Err(StandaloneError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected Config error, got Ok"),
        }
    }
}
