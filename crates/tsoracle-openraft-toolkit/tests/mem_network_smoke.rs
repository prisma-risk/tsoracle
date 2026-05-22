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

//! Smoke test for `tsoracle_openraft_toolkit::test_fakes::MemNetwork`.
//!
//! Stands up a 3-voter cluster with the in-memory network, asserts leader
//! election within 10 seconds, writes one log entry through the leader, and
//! asserts all three nodes' state machines see the apply within 5 seconds.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use openraft::storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::{Config, EntryPayload, OptionalSend, Raft, StoredMembership};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tsoracle_openraft_toolkit::declare_raft_types_ext;
use tsoracle_openraft_toolkit::test_fakes::MemNetwork;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmokeNode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmokeCmd(pub u64);

impl std::fmt::Display for SmokeCmd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SmokeCmd({})", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SmokeApplied(pub u64);

declare_raft_types_ext! {
    pub SmokeConfig:
        Node            = SmokeNode,
        AppData         = SmokeCmd,
        AppDataResponse = SmokeApplied,
        SnapshotData    = std::io::Cursor<Vec<u8>>,
}

type LogId = LogIdOf<SmokeConfig>;
type SnapMeta = SnapshotMetaOf<SmokeConfig>;
type SnapOf = SnapshotOf<SmokeConfig>;
type StoredMem = StoredMembershipOf<SmokeConfig>;

#[derive(Clone)]
pub struct SmokeStateMachine {
    core: Arc<Mutex<SmokeCore>>,
}

struct SmokeCore {
    value: u64,
    last_applied: Option<LogId>,
    last_membership: StoredMem,
}

impl SmokeStateMachine {
    pub fn new() -> Self {
        Self {
            core: Arc::new(Mutex::new(SmokeCore {
                value: 0,
                last_applied: None,
                last_membership: StoredMembership::default(),
            })),
        }
    }

    pub fn value(&self) -> u64 {
        self.core.lock().value
    }
}

impl Default for SmokeStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftSnapshotBuilder<SmokeConfig> for SmokeStateMachine {
    async fn build_snapshot(&mut self) -> Result<SnapOf, io::Error> {
        let core = self.core.lock();
        let meta = SnapMeta {
            last_log_id: core.last_applied,
            last_membership: core.last_membership.clone(),
            snapshot_id: format!("smoke-{}", core.last_applied.map(|l| l.index).unwrap_or(0)),
        };
        let bytes = postcard::to_stdvec(&core.value)
            .map_err(|e| io::Error::other(format!("smoke build_snapshot: {e}")))?;
        Ok(Snapshot {
            meta,
            snapshot: std::io::Cursor::new(bytes),
        })
    }
}

impl RaftStateMachine<SmokeConfig> for SmokeStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMem), io::Error> {
        let core = self.core.lock();
        Ok((core.last_applied, core.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<SmokeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        use futures::StreamExt;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let log_id = entry.log_id;
            let applied = match &entry.payload {
                EntryPayload::Blank => SmokeApplied(self.core.lock().value),
                EntryPayload::Normal(SmokeCmd(v)) => {
                    let mut core = self.core.lock();
                    core.value = *v;
                    core.last_applied = Some(log_id);
                    SmokeApplied(core.value)
                }
                EntryPayload::Membership(m) => {
                    let mut core = self.core.lock();
                    core.last_membership = StoredMembership::new(Some(log_id), m.clone());
                    core.last_applied = Some(log_id);
                    SmokeApplied(core.value)
                }
            };
            self.core.lock().last_applied = Some(log_id);
            if let Some(r) = responder {
                r.send(applied);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<std::io::Cursor<Vec<u8>>, io::Error> {
        Ok(std::io::Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapMeta,
        snapshot: std::io::Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let value: u64 = postcard::from_bytes(&snapshot.into_inner())
            .map_err(|e| io::Error::other(format!("smoke install_snapshot: {e}")))?;
        self.core.lock().value = value;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapOf>, io::Error> {
        Ok(None)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_mem_network_elects_and_replicates() {
    use rocksdb::{ColumnFamilyDescriptor, DB, Options};
    use tempfile::TempDir;
    use tsoracle_openraft_toolkit::{Flat, RocksdbLogStore};

    let net = MemNetwork::<SmokeConfig>::new();
    let cfg = Arc::new(
        Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let mut nodes: Vec<(
        u64,
        Raft<SmokeConfig, SmokeStateMachine>,
        SmokeStateMachine,
        TempDir,
    )> = Vec::new();

    for id in [1u64, 2, 3] {
        let dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![
            ColumnFamilyDescriptor::new("raft_log", Options::default()),
            ColumnFamilyDescriptor::new("raft_meta", Options::default()),
        ];
        let db = Arc::new(DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap());
        let log: RocksdbLogStore<SmokeConfig, Flat> =
            RocksdbLogStore::open(db, "raft_log", "raft_meta", Flat).unwrap();
        let sm = SmokeStateMachine::new();
        let raft = Raft::new(id, cfg.clone(), net.factory_for(id), log, sm.clone())
            .await
            .expect("Raft::new");
        net.register(id, raft.clone());
        nodes.push((id, raft, sm, dir));
    }

    let mut mem = BTreeMap::new();
    for id in [1u64, 2, 3] {
        mem.insert(id, SmokeNode);
    }
    nodes[0].1.initialize(mem).await.expect("initialize");

    // Wait for a leader.
    let leader_id = timeout(Duration::from_secs(10), async {
        loop {
            for (id, raft, _, _) in nodes.iter() {
                if let Some(l) = raft.current_leader().await {
                    if l == *id {
                        return *id;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("a leader elected within 10s");

    let leader_idx = nodes
        .iter()
        .position(|(id, _, _, _)| *id == leader_id)
        .expect("leader is one of our nodes");

    nodes[leader_idx]
        .1
        .client_write(SmokeCmd(42))
        .await
        .expect("client_write");

    timeout(Duration::from_secs(5), async {
        loop {
            if nodes.iter().all(|(_, _, sm, _)| sm.value() == 42) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("all nodes converged to value=42 within 5s");
}
