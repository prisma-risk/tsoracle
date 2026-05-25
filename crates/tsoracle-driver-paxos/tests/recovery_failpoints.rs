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

//! Recovery behaviour under a lost non-synced storage write.
//!
//! `set_decided_idx` is a `batch_async` (non-synced) write, while log appends
//! are `batch_sync` (fsynced). A crash can therefore recover a `decided_idx`
//! below a `Barrier` that is still durably in the log. The barrier-nonce
//! recovery seed must survive that: it is scanned from the durable log
//! contents, not from the recovered `decided_idx`.

#![cfg(all(feature = "failpoints", feature = "rocksdb-storage"))]

use std::sync::Arc;

use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::storage::Storage as _;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;
use tsoracle_driver_paxos::{
    AdvancePayload, ApplyState, HighWaterCommand, drain_decided_into, max_logged_barrier_seq,
};
use tsoracle_paxos_toolkit::storage::RocksdbStorage;

const CF: &str = "tso_paxos_recovery";

/// Failpoints live in the process-global `fail` registry, so serialize the
/// tests in this binary; `parking_lot::Mutex` is non-poisoning so a failing
/// test can't cascade into its siblings.
static FAILPOINT_TEST_SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn open_db(dir: &TempDir) -> Arc<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let cf = ColumnFamilyDescriptor::new(CF, Options::default());
    Arc::new(DB::open_cf_descriptors(&opts, dir.path(), vec![cf]).expect("open db"))
}

/// A barrier fsynced into the log, then a lost `set_decided_idx` bump, must
/// still be recoverable as the barrier-nonce seed — proving the seed is derived
/// from the durable log and not from the non-synced `decided_idx`.
///
/// The assertions contrast the two candidate seed sources against the same
/// recovered store: the decided-only fold (the previous seed) under-counts to
/// 0, while the accepted-suffix scan recovers the barrier's true sequence.
#[test]
fn lost_decided_idx_write_leaves_barrier_recoverable_from_durable_log() {
    let _serial = FAILPOINT_TEST_SERIAL.lock();
    const MY_NODE: u64 = 1;
    const DURABLE_BARRIER_SEQ: u64 = 9;

    let dir = TempDir::new().unwrap();

    // Prior lifetime: durably log an Advance and this node's Barrier, record
    // the Advance as decided, then lose the decided-index bump that would have
    // recorded the Barrier's decision — exactly a truncated non-synced tail.
    {
        let mut storage: RocksdbStorage<HighWaterCommand> =
            RocksdbStorage::open_in(open_db(&dir), CF).expect("open_in");
        storage
            .set_promise(Ballot::with(1, 1, 0, MY_NODE))
            .expect("set promise");
        storage
            .append_entries(vec![
                HighWaterCommand::Advance(AdvancePayload { at_least: 100 }),
                HighWaterCommand::Barrier {
                    node: MY_NODE,
                    seq: DURABLE_BARRIER_SEQ,
                },
            ])
            .expect("append durable log");
        // The Advance's decision persists (index 0 decided).
        storage.set_decided_idx(1).expect("persist decided idx 1");
        // The Barrier's decision bump is the write the crash drops.
        tsoracle_failpoint::fail::cfg("paxos_toolkit::storage::async_write", "return").unwrap();
        let lost = storage.set_decided_idx(2);
        tsoracle_failpoint::fail::remove("paxos_toolkit::storage::async_write");
        assert!(
            lost.is_err(),
            "the failpoint must drop the decided-index bump"
        );
    }

    // Restart: reopen the durable store and rebuild OmniPaxos over it.
    let storage: RocksdbStorage<HighWaterCommand> =
        RocksdbStorage::open_in(open_db(&dir), CF).expect("reopen");
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        flexible_quorum: None,
    };
    let server_config = ServerConfig {
        pid: MY_NODE,
        ..Default::default()
    };
    let omnipaxos = OmniPaxosConfig {
        cluster_config,
        server_config,
    }
    .build(storage)
    .expect("build over recovered storage");
    let handle = Arc::new(Mutex::new(omnipaxos));

    // The lost write left `decided_idx` stale-low: the barrier sits past it.
    assert_eq!(
        handle.lock().get_decided_idx(),
        1,
        "the dropped bump must leave decided_idx recovered below the barrier",
    );

    // The decided-only fold — the previous seed source — cannot see the
    // barrier, so it would have under-counted the nonce ceiling to 0.
    let state = ApplyState::new();
    let mut cursor = 0u64;
    drain_decided_into(&handle, &mut cursor, &state);
    assert_eq!(
        state.applied_barrier_seq(MY_NODE),
        0,
        "the decided fold misses the barrier stranded in the lost suffix",
    );

    // The durable-log scan — the seed the host now uses — recovers it, so a
    // freshly minted post-restart nonce will exceed it.
    assert_eq!(
        max_logged_barrier_seq(&handle, MY_NODE),
        DURABLE_BARRIER_SEQ,
        "the accepted-suffix scan recovers the barrier despite the lost decided_idx",
    );
}
