//! `RaftStateMachine` for the high-water counter with pluggable snapshot
//! persistence.
//!
//! State is one `u64` plus the openraft-required apply-progress metadata
//! (`last_applied`, `last_membership`). Snapshots are postcard-encoded blobs of
//! that tuple, written through to a [`SnapshotStore`] so they survive a
//! process restart. The default store is in-memory
//! ([`HighWaterStateMachine::new`]); production deployments construct via
//! [`HighWaterStateMachine::with_store`] and supply a durable backend such as
//! [`crate::snapshot_store::RocksdbSnapshotStore`].
//!
//! All state lives inside an `Arc<Mutex<Core>>` so the state machine is
//! cheaply cloneable — required because openraft's
//! `RaftStateMachine::SnapshotBuilder = Self` design hands out clones to drive
//! `build_snapshot` concurrently with `apply`. The `Arc<dyn SnapshotStore>` is
//! shared the same way, so every clone sees the same persisted snapshot.

use std::io;
use std::io::Cursor;
use std::sync::Arc;

use futures::StreamExt;
use openraft::EntryPayload;
use openraft::RaftSnapshotBuilder;
use openraft::StoredMembership;
use openraft::storage::EntryResponder;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::log_entry::HighWaterCommand;
use crate::snapshot_store::{InMemorySnapshotStore, SnapshotStore};
use crate::type_config::{HighWaterApplied, TypeConfig};

type LogId = LogIdOf<TypeConfig>;
type SnapMeta = SnapshotMetaOf<TypeConfig>;
type SnapOf = SnapshotOf<TypeConfig>;
type SnapData = Cursor<Vec<u8>>;
type StoredMem = StoredMembershipOf<TypeConfig>;

/// Snapshot payload — postcard-encoded under the hood.
///
/// Exposed at the crate root so callers building tooling around the snapshot
/// format (e.g. inspectors, migration tools) can decode it without re-deriving
/// the layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HighWaterStateMachineSnapshot {
    pub current_value: u64,
    pub last_applied: Option<LogId>,
    pub last_membership: StoredMem,
}

/// On-disk envelope written to the [`SnapshotStore`]: pairs the openraft
/// snapshot meta with the postcard-encoded [`HighWaterStateMachineSnapshot`]
/// bytes. Kept private — embedders that need to inspect persisted snapshots
/// decode the inner `data` blob as [`HighWaterStateMachineSnapshot`].
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapMeta,
    data: Vec<u8>,
}

/// Mutable core guarded by a single mutex. `parking_lot::Mutex` because the
/// apply path is hot and the state is fully re-derivable from the raft log
/// (poison semantics would just mask the originating panic).
struct Core {
    current_value: u64,
    last_applied: Option<LogId>,
    last_membership: StoredMem,
    /// Snapshot index counter, used to make snapshot ids unique even when two
    /// snapshots are produced at the same `last_applied` log id.
    snapshot_idx: u64,
    /// The most-recently built or installed snapshot, retained in memory so
    /// `get_current_snapshot` does not need to rebuild on every call.
    current_snapshot: Option<StoredSnapshot>,
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapMeta,
    data: Vec<u8>,
}

/// `RaftStateMachine` for the high-water counter, with pluggable snapshot
/// persistence.
///
/// Clone-cheap: both the `Arc<Mutex<Core>>` and the `Arc<dyn SnapshotStore>`
/// are shared by clone. Required by openraft's `get_snapshot_builder`
/// contract which uses `SnapshotBuilder = Self`.
pub struct HighWaterStateMachine {
    core: Arc<Mutex<Core>>,
    store: Arc<dyn SnapshotStore>,
}

impl Clone for HighWaterStateMachine {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            store: Arc::clone(&self.store),
        }
    }
}

impl Default for HighWaterStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl HighWaterStateMachine {
    /// Build a state machine backed by an in-memory snapshot store.
    ///
    /// Equivalent to
    /// `HighWaterStateMachine::with_store(Arc::new(InMemorySnapshotStore::new()))`.
    /// Snapshots survive within a process but not across restarts; for that
    /// use [`HighWaterStateMachine::with_store`] with a durable backend such
    /// as [`crate::snapshot_store::RocksdbSnapshotStore`].
    #[expect(
        clippy::expect_used,
        reason = "`with_store` only fails when `store.load()` returns Err; \
                  `InMemorySnapshotStore::load` is `Ok(None)` for a fresh \
                  store, so this branch is unreachable."
    )]
    pub fn new() -> Self {
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        Self::with_store(store).expect("InMemorySnapshotStore::load is infallible")
    }

    /// Build a state machine backed by `store` and rehydrated from whatever
    /// snapshot the store currently holds.
    ///
    /// If `store.load()` returns a snapshot, the state machine's
    /// `current_value`, `last_applied`, `last_membership`, and
    /// `get_current_snapshot()` all reflect that snapshot on return. This is
    /// the contract that lets openraft re-enable the default snapshot policy:
    /// after a restart the log store's `last_purged_log_id` may sit above
    /// index 0, and the state machine must already cover those purged entries
    /// or openraft will panic during recovery.
    pub fn with_store(store: Arc<dyn SnapshotStore>) -> io::Result<Self> {
        let mut core = Core {
            current_value: 0,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
            current_snapshot: None,
        };
        if let Some(bytes) = store.load()? {
            let persisted: PersistedSnapshot = postcard::from_bytes(&bytes).map_err(|e| {
                io::Error::other(format!("persisted snapshot envelope decode: {e}"))
            })?;
            let payload: HighWaterStateMachineSnapshot = postcard::from_bytes(&persisted.data)
                .map_err(|e| io::Error::other(format!("persisted snapshot payload decode: {e}")))?;
            core.current_value = payload.current_value;
            core.last_applied = payload.last_applied;
            core.last_membership = payload.last_membership;
            core.current_snapshot = Some(StoredSnapshot {
                meta: persisted.meta,
                data: persisted.data,
            });
        }
        Ok(Self {
            core: Arc::new(Mutex::new(core)),
            store,
        })
    }

    /// Read the current high-water value without going through raft.
    ///
    /// Returns the value most-recently written by `apply` or
    /// `install_snapshot`. This is a state-machine-local read; callers that
    /// need linearizability must coordinate a read barrier through `Raft`
    /// before calling.
    pub async fn current_value(&self) -> u64 {
        self.core.lock().current_value
    }

    fn snapshot_id_for(last_applied: Option<&LogId>, idx: u64) -> String {
        let log_index = last_applied.map(|l| l.index).unwrap_or(0);
        format!("{log_index}-{idx}")
    }
}

impl RaftSnapshotBuilder<TypeConfig> for HighWaterStateMachine {
    async fn build_snapshot(&mut self) -> Result<SnapOf, io::Error> {
        // Build the payload + meta under the lock, then release it before
        // calling the store: `SnapshotStore::save` may do disk I/O (rocksdb
        // sync write), and holding a `parking_lot::Mutex` across that would
        // serialize `apply` against snapshot persistence unnecessarily.
        let (snapshot_payload, meta) = {
            let mut core = self.core.lock();
            core.snapshot_idx += 1;
            let payload = HighWaterStateMachineSnapshot {
                current_value: core.current_value,
                last_applied: core.last_applied,
                last_membership: core.last_membership.clone(),
            };
            let snapshot_id = Self::snapshot_id_for(core.last_applied.as_ref(), core.snapshot_idx);
            let meta = SnapMeta {
                last_log_id: core.last_applied,
                last_membership: core.last_membership.clone(),
                snapshot_id,
            };
            let bytes = postcard::to_stdvec(&payload)
                .map_err(|e| io::Error::other(format!("snapshot payload serialize: {e}")))?;
            (bytes, meta)
        };

        let persisted = PersistedSnapshot {
            meta: meta.clone(),
            data: snapshot_payload.clone(),
        };
        let envelope = postcard::to_stdvec(&persisted)
            .map_err(|e| io::Error::other(format!("snapshot envelope serialize: {e}")))?;
        // Persist BEFORE publishing to `current_snapshot`. If the store write
        // fails openraft sees a `build_snapshot` error and `current_snapshot`
        // stays at the prior (already-persisted) value, so a follower
        // streaming via `get_current_snapshot` will not observe a snapshot we
        // could not durably write.
        self.store.save(&envelope)?;

        self.core.lock().current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: snapshot_payload.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(snapshot_payload),
        })
    }
}

impl RaftStateMachine<TypeConfig> for HighWaterStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMem), io::Error> {
        let core = self.core.lock();
        Ok((core.last_applied, core.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder_opt) = item?;
            let log_id = entry.log_id;

            let applied = match &entry.payload {
                EntryPayload::Blank => {
                    let mut core = self.core.lock();
                    core.last_applied = Some(log_id);
                    HighWaterApplied {
                        value: core.current_value,
                    }
                }
                EntryPayload::Normal(cmd) => {
                    let HighWaterCommand::Bump { target } = cmd;
                    let mut core = self.core.lock();
                    if *target > core.current_value {
                        core.current_value = *target;
                    }
                    core.last_applied = Some(log_id);
                    HighWaterApplied {
                        value: core.current_value,
                    }
                }
                EntryPayload::Membership(membership) => {
                    let mut core = self.core.lock();
                    core.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    core.last_applied = Some(log_id);
                    HighWaterApplied {
                        value: core.current_value,
                    }
                }
            };

            if let Some(responder) = responder_opt {
                responder.send(applied);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapData, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapMeta,
        snapshot: SnapData,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        let payload: HighWaterStateMachineSnapshot = postcard::from_bytes(&bytes)
            .map_err(|e| io::Error::other(format!("snapshot payload decode: {e}")))?;

        let persisted = PersistedSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        let envelope = postcard::to_stdvec(&persisted)
            .map_err(|e| io::Error::other(format!("snapshot envelope serialize: {e}")))?;
        // Persist BEFORE mutating in-memory state. If the store write fails
        // openraft retries the install and the SM stays at its prior state —
        // safer than advancing the apply progress past a snapshot we could
        // not durably record.
        self.store.save(&envelope)?;

        let mut core = self.core.lock();
        core.current_value = payload.current_value;
        core.last_applied = payload.last_applied;
        core.last_membership = payload.last_membership;
        core.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapOf>, io::Error> {
        let core = self.core.lock();
        Ok(core.current_snapshot.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Cursor::new(s.data.clone()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    use futures::stream;
    use openraft::EntryPayload;
    use openraft::entry::RaftEntry;
    use openraft::storage::EntryResponder;
    use openraft::type_config::alias::EntryOf;

    use crate::log_entry::HighWaterCommand;
    use crate::type_config::TypeConfig;

    // --- Test helpers ---

    /// Build a `LogId` for the toolkit's default leader-id layout
    /// (`LeaderId<u64, u64>`) with term=1, node_id=1, and the given index.
    fn log_id(index: u64) -> LogIdOf<TypeConfig> {
        openraft::testing::log_id::<TypeConfig>(1, 1, index)
    }

    fn entry(
        index: u64,
        payload: EntryPayload<HighWaterCommand, u64, crate::type_config::OpenraftPeer>,
    ) -> EntryResponder<TypeConfig> {
        let e: EntryOf<TypeConfig> = match payload {
            EntryPayload::Blank => EntryOf::<TypeConfig>::new_blank(log_id(index)),
            EntryPayload::Normal(d) => EntryOf::<TypeConfig>::new_normal(log_id(index), d),
            EntryPayload::Membership(m) => EntryOf::<TypeConfig>::new_membership(log_id(index), m),
        };
        (e, None)
    }

    async fn apply_one(
        sm: &mut HighWaterStateMachine,
        index: u64,
        payload: EntryPayload<HighWaterCommand, u64, crate::type_config::OpenraftPeer>,
    ) {
        sm.apply(stream::iter([Ok(entry(index, payload))]))
            .await
            .expect("apply");
    }

    // --- Tests ---

    #[tokio::test]
    async fn apply_blank_updates_only_log_id() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(&mut sm, 1, EntryPayload::Blank).await;
        assert_eq!(sm.current_value().await, 0);
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.map(|l| l.index), Some(1));
    }

    #[tokio::test]
    async fn apply_normal_advances_value() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 100 }),
        )
        .await;
        assert_eq!(sm.current_value().await, 100);
    }

    #[tokio::test]
    async fn apply_normal_holds_monotonic_under_stale_target() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 100 }),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 50 }),
        )
        .await;
        assert_eq!(sm.current_value().await, 100);
    }

    #[tokio::test]
    async fn apply_normal_equal_target_holds_value() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 100 }),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 100 }),
        )
        .await;
        assert_eq!(sm.current_value().await, 100);
    }

    #[tokio::test]
    async fn apply_membership_updates_membership_only() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 42 }),
        )
        .await;
        let mem = openraft::Membership::new_with_defaults(vec![BTreeSet::from([1u64])], [1u64]);
        apply_one(&mut sm, 2, EntryPayload::Membership(mem)).await;
        assert_eq!(sm.current_value().await, 42);
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.map(|l| l.index), Some(2));
    }

    // ---- Snapshot tests ----

    #[tokio::test]
    async fn build_snapshot_round_trips_payload() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 500 }),
        )
        .await;

        let snap = sm.build_snapshot().await.expect("build_snapshot");
        let bytes = snap.snapshot.into_inner();
        let payload: HighWaterStateMachineSnapshot =
            postcard::from_bytes(&bytes).expect("decode snapshot");
        assert_eq!(payload.current_value, 500);
        assert_eq!(payload.last_applied.map(|l| l.index), Some(1));
    }

    #[tokio::test]
    async fn build_snapshot_uses_fresh_id_each_time() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 7 }),
        )
        .await;

        let a = sm.build_snapshot().await.expect("build_snapshot a");
        let b = sm.build_snapshot().await.expect("build_snapshot b");
        assert_ne!(
            a.meta.snapshot_id, b.meta.snapshot_id,
            "two snapshots at same last_applied must have distinct ids"
        );
    }

    #[tokio::test]
    async fn install_snapshot_replaces_state() {
        let mut sm = HighWaterStateMachine::new();
        let payload = HighWaterStateMachineSnapshot {
            current_value: 999,
            last_applied: Some(log_id(5)),
            last_membership: StoredMem::default(),
        };
        let bytes = postcard::to_stdvec(&payload).expect("serialize payload");

        let meta = SnapMeta {
            last_log_id: payload.last_applied,
            last_membership: payload.last_membership.clone(),
            snapshot_id: "test-install-1".to_string(),
        };
        sm.install_snapshot(&meta, std::io::Cursor::new(bytes))
            .await
            .expect("install_snapshot");

        assert_eq!(sm.current_value().await, 999);
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.map(|l| l.index), Some(5));

        let current = sm
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot")
            .expect("snapshot present");
        assert_eq!(current.meta.snapshot_id, "test-install-1");
    }

    #[tokio::test]
    async fn get_current_snapshot_initially_none() {
        let mut sm = HighWaterStateMachine::new();
        let s = sm
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot");
        assert!(s.is_none());
    }

    // ---- SnapshotStore integration ----

    use crate::snapshot_store::{InMemorySnapshotStore, SnapshotStore};

    #[tokio::test]
    async fn build_snapshot_writes_through_store() {
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("with_store");
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Bump { target: 42 }),
        )
        .await;
        sm.build_snapshot().await.expect("build_snapshot");
        assert!(
            store.load().expect("load").is_some(),
            "store must contain a persisted snapshot after build_snapshot"
        );
    }

    #[tokio::test]
    async fn with_store_recovers_prior_snapshot_on_construction() {
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        {
            let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("first SM");
            apply_one(
                &mut sm,
                1,
                EntryPayload::Normal(HighWaterCommand::Bump { target: 99 }),
            )
            .await;
            sm.build_snapshot().await.expect("build_snapshot");
        }
        let mut sm = HighWaterStateMachine::with_store(store).expect("reopened SM");
        assert_eq!(sm.current_value().await, 99);
        // `applied_state` must report the snapshot's last_log_id after reopen —
        // without this, openraft re-applies from index 0 and panics on missing
        // log entries that the snapshot already covered.
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last.map(|l| l.index), Some(1));
        let snap = sm
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot")
            .expect("snapshot present after reopen");
        assert_eq!(snap.meta.last_log_id.map(|l| l.index), Some(1));
    }

    #[tokio::test]
    async fn default_constructor_matches_new() {
        // `Default` is the canonical "no-arg" entry point for embedders that
        // build the SM via `..Default::default()`; pinning behavior here keeps
        // the in-memory snapshot store as the unsurprising default.
        let mut sm = HighWaterStateMachine::default();
        assert_eq!(sm.current_value().await, 0);
        let snap = sm
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot");
        assert!(snap.is_none());
    }

    #[tokio::test]
    async fn begin_receiving_snapshot_returns_empty_cursor() {
        // openraft hands the returned cursor to the snapshot-receiving network
        // path; the contract is "empty, writable buffer." Anything non-empty
        // would corrupt the install on the receiving side.
        let mut sm = HighWaterStateMachine::new();
        let cursor = sm
            .begin_receiving_snapshot()
            .await
            .expect("begin_receiving_snapshot");
        assert!(cursor.into_inner().is_empty());
    }

    #[tokio::test]
    async fn with_store_errors_on_malformed_persisted_envelope() {
        // Hardens the recovery path against on-disk corruption: a snapshot
        // blob that doesn't decode as `PersistedSnapshot` must surface as a
        // structured `io::Error` from `with_store`, not a silent reset to
        // the default state (which would lose the value across restart).
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        store.save(b"not a postcard envelope").unwrap();
        let Err(err) = HighWaterStateMachine::with_store(store) else {
            panic!("with_store must reject malformed envelope");
        };
        let msg = err.to_string();
        assert!(msg.contains("envelope decode"));
    }

    #[tokio::test]
    async fn with_store_errors_on_malformed_inner_payload() {
        // Envelope decodes but the inner `HighWaterStateMachineSnapshot` does
        // not — also surfaces as `io::Error` rather than silent state reset.
        let envelope = PersistedSnapshot {
            meta: SnapMeta::default(),
            data: b"not a postcard payload".to_vec(),
        };
        let bytes = postcard::to_stdvec(&envelope).unwrap();
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        store.save(&bytes).unwrap();
        let Err(err) = HighWaterStateMachine::with_store(store) else {
            panic!("with_store must reject malformed inner payload");
        };
        let msg = err.to_string();
        assert!(msg.contains("payload decode"));
    }

    #[tokio::test]
    async fn install_snapshot_writes_through_store() {
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("with_store");
        let payload = HighWaterStateMachineSnapshot {
            current_value: 700,
            last_applied: Some(log_id(10)),
            last_membership: StoredMem::default(),
        };
        let bytes = postcard::to_stdvec(&payload).unwrap();
        let meta = SnapMeta {
            last_log_id: payload.last_applied,
            last_membership: payload.last_membership.clone(),
            snapshot_id: "install-1".into(),
        };
        sm.install_snapshot(&meta, std::io::Cursor::new(bytes))
            .await
            .expect("install_snapshot");
        // Reopen with the same store: install must have written through.
        let mut sm2 = HighWaterStateMachine::with_store(store).expect("reopened SM");
        assert_eq!(sm2.current_value().await, 700);
        let (last, _) = sm2.applied_state().await.unwrap();
        assert_eq!(last.map(|l| l.index), Some(10));
    }

    // ---- Property tests ----

    use proptest::prelude::*;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    proptest! {
        /// Monotonicity invariant: applying an arbitrary sequence of `Bump`
        /// targets leaves the state machine at `max(0, max(targets))`, and
        /// `current_value` is non-decreasing at every intermediate step.
        #[test]
        fn p1_monotonicity_under_arbitrary_bumps(
            targets in prop::collection::vec(any::<u64>(), 0..=64)
        ) {
            rt().block_on(async {
                let mut sm = HighWaterStateMachine::new();
                let mut prev = 0u64;
                let mut idx = 0u64;
                for t in &targets {
                    idx += 1;
                    apply_one(
                        &mut sm,
                        idx,
                        EntryPayload::Normal(HighWaterCommand::Bump { target: *t }),
                    )
                    .await;
                    let now = sm.current_value().await;
                    prop_assert!(now >= prev, "value went backwards: prev={prev} now={now}");
                    prev = now;
                }
                let expected = targets.iter().copied().max().unwrap_or(0);
                prop_assert_eq!(sm.current_value().await, expected);
                Ok(())
            })?;
        }

        /// Snapshot payload round-trip: build_snapshot -> install_snapshot
        /// preserves (current_value, last_applied, last_membership) across
        /// arbitrary apply sequences.
        #[test]
        fn p2_snapshot_payload_round_trip(
            bumps in prop::collection::vec(any::<u64>(), 0..=32)
        ) {
            rt().block_on(async {
                let mut sm = HighWaterStateMachine::new();
                let mut idx = 0u64;
                for t in &bumps {
                    idx += 1;
                    apply_one(
                        &mut sm,
                        idx,
                        EntryPayload::Normal(HighWaterCommand::Bump { target: *t }),
                    )
                    .await;
                }

                let snap = sm.build_snapshot().await.expect("build_snapshot");
                let meta = snap.meta.clone();
                let bytes = snap.snapshot.into_inner();

                let mut sm2 = HighWaterStateMachine::new();
                sm2.install_snapshot(&meta, std::io::Cursor::new(bytes))
                    .await
                    .expect("install_snapshot");

                prop_assert_eq!(sm2.current_value().await, sm.current_value().await);
                let (a_last, a_mem) = sm.applied_state().await.unwrap();
                let (b_last, b_mem) = sm2.applied_state().await.unwrap();
                prop_assert_eq!(a_last, b_last);
                prop_assert_eq!(a_mem, b_mem);
                Ok(())
            })?;
        }
    }
}
