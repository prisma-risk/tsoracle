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

//! Shared harness for the toolkit's integration tests. Pulled into each
//! test binary via `#[path = "common/mod.rs"] mod common;`.
//!
//! Helpers that touch `MemNetwork` / `MemStorage` compile only with the
//! `test-fakes` feature; helpers that touch RocksDB compile only with the
//! `rocksdb-storage` feature. The common shared types (`TestCommand`,
//! `TestSnapshot`, `TEST_CF`) are always available so any combination of
//! the two features produces a usable harness for the test binaries that
//! enable them.

#![allow(dead_code, unused_imports)] // not every test uses every helper

use omnipaxos::storage::{Entry, Snapshot};

pub const TEST_CF: &str = "tso_paxos_test";

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TestCommand(pub u64);

impl Entry for TestCommand {
    type Snapshot = TestSnapshot;
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TestSnapshot(pub u64);

impl Snapshot<TestCommand> for TestSnapshot {
    fn create(entries: &[TestCommand]) -> Self {
        Self(entries.iter().map(|cmd| cmd.0).max().unwrap_or(0))
    }
    fn merge(&mut self, other: Self) {
        self.0 = self.0.max(other.0);
    }
    fn use_snapshots() -> bool {
        true
    }
}

#[cfg(feature = "rocksdb-storage")]
mod rocksdb_helpers {
    use std::sync::Arc;

    use rocksdb::{ColumnFamilyDescriptor, DB, Options};
    use tempfile::TempDir;
    use tsoracle_paxos_toolkit::storage::RocksdbStorage;

    use super::TestCommand;

    pub fn open_rocksdb_in_tempdir(cf_name: &str) -> (TempDir, Arc<DB>) {
        let dir = TempDir::new().expect("tempdir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cf = ColumnFamilyDescriptor::new(cf_name, Options::default());
        let database =
            Arc::new(DB::open_cf_descriptors(&opts, dir.path(), vec![cf]).expect("open db"));
        (dir, database)
    }

    pub fn open_rocksdb_storage(dir: &TempDir, cf_name: &str) -> RocksdbStorage<TestCommand> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cf = ColumnFamilyDescriptor::new(cf_name, Options::default());
        let database =
            Arc::new(DB::open_cf_descriptors(&opts, dir.path(), vec![cf]).expect("open db"));
        RocksdbStorage::open_in(database, cf_name).expect("open_in")
    }
}

#[cfg(feature = "rocksdb-storage")]
pub use rocksdb_helpers::{open_rocksdb_in_tempdir, open_rocksdb_storage};

#[cfg(feature = "test-fakes")]
mod mem_helpers {
    use std::sync::Arc;

    use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
    use parking_lot::Mutex;
    use tokio::sync::mpsc;

    use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;
    use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

    use super::TestCommand;

    pub struct MemCluster {
        pub nodes: Vec<MemNode>,
        pub network: Arc<MemNetwork<TestCommand>>,
    }

    pub struct MemNode {
        pub node_id: u64,
        pub omnipaxos: Arc<Mutex<OmniPaxos<TestCommand, MemStorage<TestCommand>>>>,
        pub inbox: mpsc::Receiver<omnipaxos::messages::Message<TestCommand>>,
    }

    pub fn build_mem_cluster(node_count: usize) -> MemCluster {
        assert!(node_count >= 1, "cluster size must be at least 1");
        let network: Arc<MemNetwork<TestCommand>> = Arc::new(MemNetwork::new());
        let node_ids: Vec<u64> = (1..=node_count as u64).collect();
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: node_ids.clone(),
            flexible_quorum: None,
        };

        let mut nodes = Vec::with_capacity(node_count);
        for &node_id in &node_ids {
            let server_config = ServerConfig {
                pid: node_id,
                election_tick_timeout: 5,
                resend_message_tick_timeout: 5,
                ..Default::default()
            };
            let storage = MemStorage::<TestCommand>::new();
            let op_config = OmniPaxosConfig {
                cluster_config: cluster_config.clone(),
                server_config,
            };
            let omnipaxos = op_config.build(storage).expect("build omnipaxos");
            let inbox = network.register(node_id);
            nodes.push(MemNode {
                node_id,
                omnipaxos: Arc::new(Mutex::new(omnipaxos)),
                inbox,
            });
        }
        MemCluster { nodes, network }
    }

    pub async fn drive_cluster_until<F>(
        cluster: &mut MemCluster,
        mut predicate: F,
        max_ticks: usize,
    ) where
        F: FnMut(&MemCluster) -> bool,
    {
        for _ in 0..max_ticks {
            // Tick every node and drain outbound messages with the guard dropped
            // before the network deliver call (which is async).
            for node in &cluster.nodes {
                let outgoing = {
                    let mut op = node.omnipaxos.lock();
                    op.tick();
                    op.outgoing_messages()
                };
                for message in outgoing {
                    cluster.network.deliver(message).await;
                }
            }
            // Drain inboxes back into each OmniPaxos handle.
            for node in &mut cluster.nodes {
                while let Ok(message) = node.inbox.try_recv() {
                    node.omnipaxos.lock().handle_incoming(message);
                }
            }
            if predicate(cluster) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!("predicate did not become true within {max_ticks} ticks");
    }
}

#[cfg(feature = "test-fakes")]
pub use mem_helpers::{MemCluster, MemNode, build_mem_cluster, drive_cluster_until};
