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

#![cfg(feature = "rocksdb-log-store")]

//! Drives openraft 0.10.0-alpha.20's bundled `Suite::test_all` against the
//! rocksdb-backed [`RocksdbLogStore`].
//!
//! The bundled suite tests `RaftLogStorage` and `RaftStateMachine` together
//! (some tests apply entries via the SM to set up scenarios for log queries),
//! so this file also includes an in-memory `StubStateMachine` that is just
//! capable enough to satisfy the suite: it tracks `last_applied` and
//! `last_membership`, applies entries by updating those fields, and round-trips
//! a `(last_applied, last_membership)` payload through `build_snapshot` /
//! `install_snapshot`. It is intentionally not a real state machine — the
//! goal is to exercise `RocksdbLogStore`, not the SM.

use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;

use futures::Stream;
use futures::StreamExt;
use openraft::RaftTypeConfig;
use openraft::StorageError;
use openraft::entry::RaftEntry;
use openraft::entry::RaftPayload;
use openraft::errors::ErrorSource;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use openraft::testing::log::StoreBuilder;
use openraft::testing::log::Suite;
use openraft::type_config::alias::LogIdOf;
use openraft::type_config::alias::SnapshotMetaOf;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::StoredMembershipOf;
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tsoracle_openraft_toolkit::{Flat, GroupPrefixed, KeySpace, RocksdbLogStore};

mod common;
use common::TestTypeConfig;

const LOG_CF: &str = "raft_log";
const META_CF: &str = "raft_meta";

/// Convert a `Display`-able value into the test config's `ErrorSource`.
fn err_src(msg: impl ToString) -> <TestTypeConfig as RaftTypeConfig>::ErrorSource {
    <TestTypeConfig as RaftTypeConfig>::ErrorSource::from_string(msg)
}

type TestLogId = LogIdOf<TestTypeConfig>;
type TestSnapshotMeta = SnapshotMetaOf<TestTypeConfig>;
type TestSnapshot = SnapshotOf<TestTypeConfig>;
type TestStoredMembership = StoredMembershipOf<TestTypeConfig>;

#[derive(Clone, Default)]
struct StubStateMachine {
    state: Arc<Mutex<StubState>>,
}

#[derive(Default)]
struct StubState {
    last_applied: Option<TestLogId>,
    last_membership: TestStoredMembership,
    snapshot_counter: u64,
    current_snapshot: Option<StoredSnapshot>,
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: TestSnapshotMeta,
    bytes: Vec<u8>,
}

/// What we serialize on `build_snapshot` and re-hydrate on `install_snapshot`.
#[derive(Default, Serialize, Deserialize)]
struct SnapshotPayload {
    last_applied: Option<TestLogId>,
    last_membership: TestStoredMembership,
}

impl RaftStateMachine<TestTypeConfig> for StubStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<TestLogId>, TestStoredMembership), io::Error> {
        let s = self.state.lock().unwrap();
        Ok((s.last_applied, s.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<TestTypeConfig>, io::Error>> + Unpin + Send,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let log_id = entry.log_id();
            let membership = entry.get_membership();
            {
                let mut s = self.state.lock().unwrap();
                s.last_applied = Some(log_id);
                if let Some(mem) = membership {
                    s.last_membership = TestStoredMembership::new(Some(log_id), mem);
                }
            }
            if let Some(r) = responder {
                r.send(common::TestAppliedState);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &TestSnapshotMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        let payload: SnapshotPayload = postcard::from_bytes(&bytes).map_err(io::Error::other)?;
        let mut s = self.state.lock().unwrap();
        s.last_applied = payload.last_applied;
        s.last_membership = payload.last_membership;
        s.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<TestSnapshot>, io::Error> {
        let s = self.state.lock().unwrap();
        let Some(ss) = s.current_snapshot.clone() else {
            return Ok(None);
        };
        Ok(Some(TestSnapshot {
            meta: ss.meta,
            snapshot: Cursor::new(ss.bytes),
        }))
    }
}

impl RaftSnapshotBuilder<TestTypeConfig> for StubStateMachine {
    async fn build_snapshot(&mut self) -> Result<TestSnapshot, io::Error> {
        let mut s = self.state.lock().unwrap();
        s.snapshot_counter += 1;
        let snapshot_id = format!("test-snapshot-{}", s.snapshot_counter);
        let payload = SnapshotPayload {
            last_applied: s.last_applied,
            last_membership: s.last_membership.clone(),
        };
        let bytes = postcard::to_stdvec(&payload).map_err(io::Error::other)?;
        let meta = TestSnapshotMeta {
            last_log_id: s.last_applied,
            last_membership: s.last_membership.clone(),
            snapshot_id,
        };
        s.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            bytes: bytes.clone(),
        });
        Ok(TestSnapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}

/// Builder for the bundled suite, generic over the [`KeySpace`] under test.
///
/// The `TempDir` is the suite's `G` guard; it lives as long as the test does
/// and removes the on-disk artifacts on drop.
struct TestStoreBuilder<K: KeySpace> {
    keys: K,
}

impl<K> StoreBuilder<TestTypeConfig, RocksdbLogStore<TestTypeConfig, K>, StubStateMachine, TempDir>
    for TestStoreBuilder<K>
where
    K: KeySpace,
{
    async fn build(
        &self,
    ) -> Result<
        (
            TempDir,
            RocksdbLogStore<TestTypeConfig, K>,
            StubStateMachine,
        ),
        StorageError<TestTypeConfig>,
    > {
        let dir =
            TempDir::new().map_err(|e| StorageError::read(err_src(format!("tempdir: {e}"))))?;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![
            ColumnFamilyDescriptor::new(LOG_CF, Options::default()),
            ColumnFamilyDescriptor::new(META_CF, Options::default()),
        ];
        let db = Arc::new(
            DB::open_cf_descriptors(&opts, dir.path(), cfs)
                .map_err(|e| StorageError::read(err_src(format!("open rocksdb: {e}"))))?,
        );

        let store =
            RocksdbLogStore::<TestTypeConfig, K>::open(db, LOG_CF, META_CF, self.keys.clone())
                .map_err(|e| StorageError::read(err_src(format!("open log store: {e}"))))?;

        Ok((dir, store, StubStateMachine::default()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rocksdb_log_store_passes_openraft_suite_flat() {
    let builder = TestStoreBuilder { keys: Flat };
    Suite::<
        TestTypeConfig,
        RocksdbLogStore<TestTypeConfig, Flat>,
        StubStateMachine,
        TestStoreBuilder<Flat>,
        TempDir,
    >::test_all(builder)
    .await
    .unwrap();
}

/// Same suite, but against [`GroupPrefixed`] with a non-trivial group id so
/// any latent assumption that log/meta keys start at byte 0 (rather than after
/// an 8-byte group prefix) fails the suite.
#[tokio::test(flavor = "multi_thread")]
async fn rocksdb_log_store_passes_openraft_suite_group_prefixed() {
    let builder = TestStoreBuilder {
        keys: GroupPrefixed::new(42),
    };
    Suite::<
        TestTypeConfig,
        RocksdbLogStore<TestTypeConfig, GroupPrefixed>,
        StubStateMachine,
        TestStoreBuilder<GroupPrefixed>,
        TempDir,
    >::test_all(builder)
    .await
    .unwrap();
}
