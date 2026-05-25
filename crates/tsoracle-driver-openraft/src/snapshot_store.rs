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

//! Persistence backend for [`HighWaterStateMachine`](crate::HighWaterStateMachine)
//! snapshots.
//!
//! The trait stores a single opaque blob — the most recently built or
//! installed snapshot. [`HighWaterStateMachine`](crate::HighWaterStateMachine)
//! consults its store on construction (rebuilding state from the persisted
//! blob, if any) and writes through to the store from `build_snapshot` and
//! `install_snapshot`. This is what lets openraft re-enable the default
//! `SnapshotPolicy::LogsSinceLast(N)` against this state machine without
//! losing data when the log prefix is purged.
//!
//! Two backends ship with this crate:
//!
//! - [`InMemorySnapshotStore`] — the default for
//!   [`HighWaterStateMachine::new`](crate::HighWaterStateMachine::new);
//!   matches the historical in-process-only behavior.
//! - [`RocksdbSnapshotStore`] (feature `rocksdb-snapshot-store`) — durable
//!   storage keyed in a caller-managed RocksDB column family. Suitable for
//!   sharing the same `Arc<DB>` already used by the log store.
//!
//! Embedders with custom durability needs implement [`SnapshotStore`]
//! themselves and hand the resulting `Arc<dyn SnapshotStore>` to
//! [`HighWaterStateMachine::with_store`](crate::HighWaterStateMachine::with_store).

use std::io;

use parking_lot::Mutex;

/// Durable storage for a single openraft snapshot blob.
///
/// Implementations must be safe to call from any thread; openraft drives
/// `build_snapshot` from a cloned `HighWaterStateMachine` handle while
/// `apply` may run on the main handle, so the store may see overlapping
/// `save`/`load` calls.
pub trait SnapshotStore: Send + Sync + 'static {
    /// Replace the persisted snapshot with `payload`. Any prior snapshot must
    /// no longer be returned by subsequent [`load`](SnapshotStore::load) calls
    /// once this returns `Ok`.
    fn save(&self, payload: &[u8]) -> io::Result<()>;

    /// Return the most recent persisted snapshot, or `None` if the store is
    /// empty. Called once at state-machine construction (to recover state
    /// across process restarts) and again whenever openraft asks the
    /// state machine for its current snapshot.
    fn load(&self) -> io::Result<Option<Vec<u8>>>;
}

/// `SnapshotStore` that keeps the most recent snapshot in process memory.
///
/// Default backend for [`HighWaterStateMachine::new`](crate::HighWaterStateMachine::new):
/// snapshots survive within a process but not across restarts. Production
/// deployments should use a durable backend such as
/// [`RocksdbSnapshotStore`] (feature `rocksdb-snapshot-store`).
#[derive(Default)]
pub struct InMemorySnapshotStore {
    bytes: Mutex<Option<Vec<u8>>>,
}

impl InMemorySnapshotStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SnapshotStore for InMemorySnapshotStore {
    fn save(&self, payload: &[u8]) -> io::Result<()> {
        *self.bytes.lock() = Some(payload.to_vec());
        Ok(())
    }

    fn load(&self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.bytes.lock().clone())
    }
}

#[cfg(feature = "rocksdb-snapshot-store")]
mod rocksdb_backed {
    use std::io;
    use std::sync::Arc;

    use rocksdb::{DB, WriteOptions};

    use super::SnapshotStore;

    /// `SnapshotStore` backed by a single key in a caller-managed RocksDB
    /// column family.
    ///
    /// The caller owns the `Arc<DB>` and is responsible for opening the column
    /// family identified by `cf` before constructing the store; using the same
    /// `Arc<DB>` as the
    /// [`tsoracle_openraft_toolkit::RocksdbLogStore`](https://docs.rs/tsoracle-openraft-toolkit/latest/tsoracle_openraft_toolkit/struct.RocksdbLogStore.html)
    /// is the intended pattern — that way one rocksdb instance covers both the
    /// raft log and the snapshot.
    ///
    /// Writes go through with `WriteOptions::set_sync(true)`, matching the
    /// log store's durability contract: when `save` returns `Ok`, the
    /// snapshot has been fsynced.
    ///
    /// `key` defaults to `b"snapshot"` via [`RocksdbSnapshotStore::open`]; the
    /// caller can pick a different key to multiplex several state machines
    /// onto a shared CF (analogous to
    /// [`tsoracle_openraft_toolkit::GroupPrefixed`](https://docs.rs/tsoracle-openraft-toolkit/latest/tsoracle_openraft_toolkit/struct.GroupPrefixed.html)
    /// for the log store).
    pub struct RocksdbSnapshotStore {
        db: Arc<DB>,
        cf: String,
        key: Vec<u8>,
    }

    impl std::fmt::Debug for RocksdbSnapshotStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RocksdbSnapshotStore")
                .field("cf", &self.cf)
                .field("key_len", &self.key.len())
                .finish()
        }
    }

    impl RocksdbSnapshotStore {
        /// Construct a store that reads/writes the single key `b"snapshot"`
        /// in column family `cf`.
        ///
        /// Returns `Err` if `cf` is not a valid column family on `db`.
        pub fn open(db: Arc<DB>, cf: impl Into<String>) -> io::Result<Self> {
            Self::open_with_key(db, cf, b"snapshot".to_vec())
        }

        /// Construct a store with an explicit key. Useful when multiplexing
        /// multiple state machines onto a shared snapshot CF.
        pub fn open_with_key(db: Arc<DB>, cf: impl Into<String>, key: Vec<u8>) -> io::Result<Self> {
            let cf = cf.into();
            if db.cf_handle(&cf).is_none() {
                return Err(io::Error::other(format!(
                    "snapshot column family `{cf}` not found on db"
                )));
            }
            Ok(Self { db, cf, key })
        }

        fn cf_handle(&self) -> io::Result<Arc<rocksdb::BoundColumnFamily<'_>>> {
            self.db.cf_handle(&self.cf).ok_or_else(|| {
                io::Error::other(format!(
                    "snapshot column family `{}` disappeared from db",
                    self.cf
                ))
            })
        }
    }

    impl SnapshotStore for RocksdbSnapshotStore {
        fn save(&self, payload: &[u8]) -> io::Result<()> {
            let cf = self.cf_handle()?;
            let mut wo = WriteOptions::default();
            wo.set_sync(true);
            self.db
                .put_cf_opt(&cf, &self.key, payload, &wo)
                .map_err(io::Error::other)
        }

        fn load(&self) -> io::Result<Option<Vec<u8>>> {
            let cf = self.cf_handle()?;
            self.db.get_cf(&cf, &self.key).map_err(io::Error::other)
        }
    }
}

#[cfg(feature = "rocksdb-snapshot-store")]
pub use rocksdb_backed::RocksdbSnapshotStore;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn in_memory_load_is_none_before_save() {
        let store = InMemorySnapshotStore::new();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn in_memory_save_then_load_round_trips() {
        let store = InMemorySnapshotStore::new();
        store.save(b"first").unwrap();
        assert_eq!(store.load().unwrap().as_deref(), Some(&b"first"[..]));
    }

    #[test]
    fn in_memory_save_overwrites_prior_value() {
        let store = InMemorySnapshotStore::new();
        store.save(b"first").unwrap();
        store.save(b"second").unwrap();
        assert_eq!(store.load().unwrap().as_deref(), Some(&b"second"[..]));
    }

    #[test]
    fn in_memory_store_is_shareable_via_arc() {
        // The state machine clones an `Arc<dyn SnapshotStore>` between its own
        // clones, so we exercise that pattern here: two handles pointing at
        // the same store observe each other's writes.
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let a = Arc::clone(&store);
        let b = Arc::clone(&store);
        a.save(b"shared").unwrap();
        assert_eq!(b.load().unwrap().as_deref(), Some(&b"shared"[..]));
    }
}

#[cfg(all(test, feature = "rocksdb-snapshot-store"))]
mod rocksdb_tests {
    use super::*;
    use std::sync::Arc;

    use rocksdb::{ColumnFamilyDescriptor, DB, Options};
    use tempfile::TempDir;

    const SNAP_CF: &str = "raft_snapshot";

    fn open_db(path: &std::path::Path) -> Arc<DB> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = vec![ColumnFamilyDescriptor::new(SNAP_CF, Options::default())];
        Arc::new(DB::open_cf_descriptors(&opts, path, cfs).unwrap())
    }

    #[test]
    fn rocksdb_save_then_load_in_same_open() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path());
        let store = RocksdbSnapshotStore::open(db, SNAP_CF).unwrap();
        store.save(b"hello").unwrap();
        assert_eq!(store.load().unwrap().as_deref(), Some(&b"hello"[..]));
    }

    #[test]
    fn rocksdb_load_is_none_before_save() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path());
        let store = RocksdbSnapshotStore::open(db, SNAP_CF).unwrap();
        assert!(store.load().unwrap().is_none());
    }

    #[test]
    fn rocksdb_save_overwrites_prior_value() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path());
        let store = RocksdbSnapshotStore::open(db, SNAP_CF).unwrap();
        store.save(b"v1").unwrap();
        store.save(b"v2").unwrap();
        assert_eq!(store.load().unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[test]
    fn rocksdb_save_survives_reopen() {
        // The acceptance criterion for snapshot persistence: a save must be
        // visible after the DB is dropped and reopened at the same path.
        let dir = TempDir::new().unwrap();
        {
            let db = open_db(dir.path());
            let store = RocksdbSnapshotStore::open(db, SNAP_CF).unwrap();
            store.save(b"durable").unwrap();
        }
        {
            let db = open_db(dir.path());
            let store = RocksdbSnapshotStore::open(db, SNAP_CF).unwrap();
            assert_eq!(store.load().unwrap().as_deref(), Some(&b"durable"[..]));
        }
    }

    #[test]
    fn rocksdb_open_errors_when_cf_missing() {
        // Open the DB without the snapshot CF; constructing the store must
        // fail rather than silently target the default CF.
        let dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, dir.path()).unwrap());
        let err = RocksdbSnapshotStore::open(db, "missing_cf").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("missing_cf"));
    }

    #[test]
    fn rocksdb_load_errors_when_cf_dropped_after_open() {
        // `cf_handle` validates the CF at `open` time but re-resolves on every
        // save/load against the live DB. If the CF is dropped from underneath
        // the store, save/load must surface a structured error (rather than
        // panicking on a missing handle) so callers can map it to a
        // recoverable `io::Error`.
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path());
        let store = RocksdbSnapshotStore::open(db.clone(), SNAP_CF).unwrap();
        store.save(b"prior").unwrap();
        db.drop_cf(SNAP_CF).unwrap();
        let err = store.load().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("disappeared"));
        assert!(msg.contains(SNAP_CF));
    }

    #[test]
    fn rocksdb_debug_includes_cf_and_key_len() {
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path());
        let store = RocksdbSnapshotStore::open_with_key(db, SNAP_CF, b"some-key".to_vec()).unwrap();
        let rendered = format!("{store:?}");
        assert!(rendered.contains(SNAP_CF));
        assert!(rendered.contains("key_len: 8"));
    }

    #[test]
    fn rocksdb_distinct_keys_do_not_collide() {
        // Two state machines sharing one CF must be able to keep separate
        // snapshots via distinct keys — the contract `open_with_key` exists
        // to support.
        let dir = TempDir::new().unwrap();
        let db = open_db(dir.path());
        let store_a =
            RocksdbSnapshotStore::open_with_key(db.clone(), SNAP_CF, b"a".to_vec()).unwrap();
        let store_b = RocksdbSnapshotStore::open_with_key(db, SNAP_CF, b"b".to_vec()).unwrap();
        store_a.save(b"snap-a").unwrap();
        store_b.save(b"snap-b").unwrap();
        assert_eq!(store_a.load().unwrap().as_deref(), Some(&b"snap-a"[..]));
        assert_eq!(store_b.load().unwrap().as_deref(), Some(&b"snap-b"[..]));
    }
}
