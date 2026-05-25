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

// #[PerformanceCriticalPath]
//! `RaftStateMachine` for the high-water counter with pluggable snapshot
//! persistence.
//!
//! State is one `u64` plus the openraft-required apply-progress metadata
//! (`last_applied`, `last_membership`). Snapshots are encoded with the toolkit's
//! version-prefixed codec ([`tsoracle_openraft_toolkit::encode`] /
//! [`tsoracle_openraft_toolkit::decode`] at [`SCHEMA_VERSION`]) — a leading
//! version byte followed by the postcard body — written through to a
//! [`SnapshotStore`] so they survive a
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

use tsoracle_openraft_toolkit::{SCHEMA_VERSION, codec_io_error, decode, encode};

use crate::log_entry::HighWaterCommand;
use crate::snapshot_store::{InMemorySnapshotStore, SnapshotStore};
use crate::type_config::{HighWaterApplied, TypeConfig};

type LogId = LogIdOf<TypeConfig>;
type SnapMeta = SnapshotMetaOf<TypeConfig>;
type SnapOf = SnapshotOf<TypeConfig>;
type SnapData = Cursor<Vec<u8>>;
type StoredMem = StoredMembershipOf<TypeConfig>;

/// Snapshot payload. The persisted/streamed bytes are version-framed as
/// `[SCHEMA_VERSION | postcard(Self)]`, so decode them through the toolkit codec
/// ([`tsoracle_openraft_toolkit::decode`] with [`SCHEMA_VERSION`]) rather than
/// raw `postcard::from_bytes`.
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
/// snapshot meta with the version-framed payload, where `data` holds
/// `[SCHEMA_VERSION | postcard(HighWaterStateMachineSnapshot)]`. Kept private —
/// embedders that need to inspect persisted snapshots decode the inner `data`
/// blob through the toolkit codec ([`tsoracle_openraft_toolkit::decode`] with
/// [`SCHEMA_VERSION`]).
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

/// Whether a snapshot at `incoming` may replace one currently published at
/// `published` without regressing the applied log id.
///
/// Equal ids may replace (a fresh rebuild at the same `last_applied` carries
/// the same state, only a new `snapshot_id`). `None` — no prior snapshot, or a
/// pre-genesis snapshot — is the minimum, so a `None` incoming never displaces
/// a `Some` published. This is the monotone guard that closes the
/// `build_snapshot`/`install_snapshot` publish TOCTOU.
fn supersedes_published(incoming: Option<LogId>, published: Option<LogId>) -> bool {
    incoming >= published
}

/// `RaftStateMachine` for the high-water counter, with pluggable snapshot
/// persistence.
///
/// Clone-cheap: the `Arc<Mutex<Core>>`, the `Arc<dyn SnapshotStore>`, and the
/// `Arc<Mutex<()>>` persist lock are all shared by clone. Required by
/// openraft's `get_snapshot_builder` contract which uses
/// `SnapshotBuilder = Self`.
pub struct HighWaterStateMachine {
    core: Arc<Mutex<Core>>,
    store: Arc<dyn SnapshotStore>,
    /// Serializes the persist-then-publish sequence of `build_snapshot` and
    /// `install_snapshot` across all clones. openraft drives `build_snapshot`
    /// on a clone concurrently with `install_snapshot` on the main handle;
    /// without this lock a slow build could write the store and publish
    /// `current_snapshot` *after* a newer install, rolling both the durable
    /// and in-memory snapshot back to a stale `last_log_id`. Distinct from
    /// `core` precisely so the hot `apply` path never serializes against
    /// snapshot I/O — `core` is never held across `store.save`.
    persist: Arc<Mutex<()>>,
}

impl Clone for HighWaterStateMachine {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            store: Arc::clone(&self.store),
            persist: Arc::clone(&self.persist),
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
            let persisted: PersistedSnapshot = decode(SCHEMA_VERSION, &bytes)
                .map_err(|e| codec_io_error("persisted snapshot envelope decode", e))?;
            let payload: HighWaterStateMachineSnapshot = decode(SCHEMA_VERSION, &persisted.data)
                .map_err(|e| codec_io_error("persisted snapshot payload decode", e))?;
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
            persist: Arc::new(Mutex::new(())),
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

    /// Durably persist `envelope`, then publish `(meta, data)` as the current
    /// in-memory snapshot, running `on_adopt` against the core under the same
    /// publish lock — but only if `meta.last_log_id` does not regress the
    /// snapshot already published.
    ///
    /// The whole sequence is serialized across clones by `self.persist` so a
    /// slow `build_snapshot` cannot interleave its store write or its publish
    /// behind a newer `install_snapshot` (or vice-versa) and roll the durable
    /// and in-memory snapshot back to a stale `last_log_id`. The published
    /// `last_log_id` is re-read *inside* the lock, immediately before the
    /// store write, so the decision can't be invalidated by a concurrent
    /// commit. `self.core` is taken only in brief bursts and never held across
    /// `store.save`, so `apply` never serializes against snapshot I/O.
    ///
    /// Returns `Ok(true)` if the snapshot was adopted, `Ok(false)` if it was
    /// dropped as stale because a newer snapshot is already durable and
    /// published.
    fn commit_snapshot(
        &self,
        meta: SnapMeta,
        data: Vec<u8>,
        envelope: &[u8],
        on_adopt: impl FnOnce(&mut Core),
    ) -> io::Result<bool> {
        let _persist = self.persist.lock();

        let published = self
            .core
            .lock()
            .current_snapshot
            .as_ref()
            .and_then(|s| s.meta.last_log_id);
        if !supersedes_published(meta.last_log_id, published) {
            // `debug!`, not `warn!`: this file is on the per-entry apply hot
            // path (`#[PerformanceCriticalPath]`), where info-or-higher logging
            // is banned. A discarded stale publish is also a benign, expected
            // race resolution — the monotone gate doing its job — not an
            // operational fault, so debug is the right level on the merits too.
            tracing::debug!(
                incoming.last_log_id = ?meta.last_log_id,
                published.last_log_id = ?published,
                snapshot_id = %meta.snapshot_id,
                "discarding stale snapshot publish: a newer snapshot is \
                 already durable and published",
            );
            return Ok(false);
        }

        self.store.save(envelope)?;

        let mut core = self.core.lock();
        on_adopt(&mut core);
        core.current_snapshot = Some(StoredSnapshot { meta, data });
        Ok(true)
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
            let bytes = encode(SCHEMA_VERSION, &payload)
                .map_err(|e| codec_io_error("snapshot payload serialize", e))?;
            (bytes, meta)
        };

        let persisted = PersistedSnapshot {
            meta: meta.clone(),
            data: snapshot_payload.clone(),
        };
        let envelope = encode(SCHEMA_VERSION, &persisted)
            .map_err(|e| codec_io_error("snapshot envelope serialize", e))?;
        // Persist + publish through the monotone, serialized commit path. The
        // store write happens BEFORE `current_snapshot` is updated, so a
        // failed write leaves the prior (already-persisted) snapshot in place
        // and a follower streaming via `get_current_snapshot` never observes a
        // snapshot we could not durably write. A build whose `last_applied`
        // was captured before a newer install bumped the published snapshot is
        // dropped here rather than rolling the durable + in-memory snapshot
        // back to its stale `last_log_id`. We still return the snapshot we
        // built — openraft asked for one, and the bytes faithfully capture the
        // state we observed.
        self.commit_snapshot(meta.clone(), snapshot_payload.clone(), &envelope, |_| {})?;

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
                    let HighWaterCommand::Advance(advance) = cmd;
                    let mut core = self.core.lock();
                    core.current_value = advance.merge(core.current_value);
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
        let payload: HighWaterStateMachineSnapshot = decode(SCHEMA_VERSION, &bytes)
            .map_err(|e| codec_io_error("snapshot payload decode", e))?;

        // `meta.last_log_id` is read back by `get_current_snapshot` while
        // `payload.last_applied` is read back by `applied_state`. `build_snapshot`
        // derives both from the same `last_applied`, so they always agree for an
        // honest peer. Reject any disagreement before persisting or mutating: a
        // mismatch would otherwise desync those two reader methods permanently
        // and silently. Checked before `store.save` so a rejected install leaves
        // both the store and in-memory state untouched for openraft to retry.
        if meta.last_log_id != payload.last_applied {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot meta/payload disagree on last_log_id: \
                     meta.last_log_id={:?}, payload.last_applied={:?}",
                    meta.last_log_id, payload.last_applied
                ),
            ));
        }

        let persisted = PersistedSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        let envelope = encode(SCHEMA_VERSION, &persisted)
            .map_err(|e| codec_io_error("snapshot envelope serialize", e))?;
        // Persist, apply the snapshot's state, and publish atomically through
        // the monotone, serialized commit path — the same one `build_snapshot`
        // uses, so neither can clobber the other. The store write happens
        // before the in-memory state is advanced, so a failed write leaves the
        // SM at its prior state for openraft to retry. A stale install (a
        // lower `last_log_id` than the already-published snapshot) is dropped
        // as an accepted no-op: we already cover at least this snapshot, so
        // adopting it would regress both the durable snapshot and the applied
        // state.
        self.commit_snapshot(meta.clone(), bytes, &envelope, |core| {
            core.current_value = payload.current_value;
            core.last_applied = payload.last_applied;
            core.last_membership = payload.last_membership;
        })?;
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
    use tsoracle_consensus::AdvancePayload;

    // --- Test helpers ---

    /// Install a process-wide `DEBUG`-level subscriber so the `tracing::debug!`
    /// on the stale-publish reject path actually evaluates its fields (without
    /// an interested subscriber the macro short-circuits and the argument
    /// expressions are never executed). Idempotent across tests via `try_init`.
    fn enable_tracing() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_test_writer()
            .try_init();
    }

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
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 100 })),
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
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 100 })),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 50 })),
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
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 100 })),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 100 })),
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
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 42 })),
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
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 500 })),
        )
        .await;

        let snap = sm.build_snapshot().await.expect("build_snapshot");
        let bytes = snap.snapshot.into_inner();
        let payload: HighWaterStateMachineSnapshot =
            tsoracle_openraft_toolkit::decode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &bytes)
                .expect("decode snapshot");
        assert_eq!(payload.current_value, 500);
        assert_eq!(payload.last_applied.map(|l| l.index), Some(1));
    }

    #[tokio::test]
    async fn build_snapshot_uses_fresh_id_each_time() {
        let mut sm = HighWaterStateMachine::new();
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 7 })),
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
        let bytes =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &payload)
                .expect("serialize payload");

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
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 42 })),
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
                EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 99 })),
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
        let bytes =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &envelope)
                .unwrap();
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
        let bytes =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &payload)
                .unwrap();
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

    /// Encode a `(meta, framed-payload-bytes)` pair for an install at
    /// `(value, last_applied)`, with `meta.last_log_id` matching the payload so
    /// the meta/payload consistency check passes and the install reaches the
    /// publish path.
    fn install_payload(value: u64, last_applied: LogId) -> (SnapMeta, Vec<u8>) {
        let payload = HighWaterStateMachineSnapshot {
            current_value: value,
            last_applied: Some(last_applied),
            last_membership: StoredMem::default(),
        };
        let bytes =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &payload)
                .expect("serialize payload");
        let meta = SnapMeta {
            last_log_id: payload.last_applied,
            last_membership: payload.last_membership.clone(),
            snapshot_id: format!("snap-{}", last_applied.index),
        };
        (meta, bytes)
    }

    #[tokio::test]
    async fn install_snapshot_does_not_regress_published_snapshot() {
        // `build_snapshot` publishes both the durable store (:256) and the
        // in-memory `current_snapshot` (:258) AFTER releasing the core lock,
        // and openraft can drive a stale build on a clone concurrently with a
        // newer install. The publish must therefore be monotone by
        // `last_log_id`: a later-arriving, lower-indexed snapshot must not roll
        // the durable + in-memory snapshot back to a stale state (which a
        // subsequent restart would then recover, after openraft has already
        // purged its log past the newer snapshot). We exercise that invariant
        // through the install path here — install and build share the same
        // monotone publish, so a regressing install stands in for a stale build
        // that finishes its write last.
        enable_tracing();
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("with_store");

        let (newer_meta, newer_bytes) = install_payload(80, log_id(8));
        sm.install_snapshot(&newer_meta, Cursor::new(newer_bytes))
            .await
            .expect("install newer");

        // A stale snapshot (lower last_log_id) arriving afterwards is a no-op:
        // we are already at least this far, so it must neither error nor
        // regress state.
        let (older_meta, older_bytes) = install_payload(50, log_id(5));
        sm.install_snapshot(&older_meta, Cursor::new(older_bytes))
            .await
            .expect("stale install must be an accepted no-op, not an error");

        assert_eq!(
            sm.current_value().await,
            80,
            "value must not regress to the stale snapshot"
        );
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(
            last.map(|l| l.index),
            Some(8),
            "last_applied must not regress"
        );
        let current = sm
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot")
            .expect("snapshot present");
        assert_eq!(
            current.meta.last_log_id.map(|l| l.index),
            Some(8),
            "in-memory current_snapshot must not regress"
        );

        // The durable store must also still hold the newer snapshot — this is
        // the recovery-critical half: reopening must not resurrect the stale
        // state.
        let mut reopened = HighWaterStateMachine::with_store(store).expect("reopen");
        assert_eq!(
            reopened.current_value().await,
            80,
            "durable store must not have been rolled back to the stale snapshot"
        );
        let (last, _) = reopened.applied_state().await.unwrap();
        assert_eq!(last.map(|l| l.index), Some(8));
    }

    #[test]
    fn supersedes_published_is_monotone_by_last_log_id() {
        // The exact rule the publish gate enforces. Equal or greater
        // last_log_id supersedes; a lower one — or `None` against a `Some` —
        // does not. `None` is the minimum: a fresh/pre-genesis snapshot never
        // displaces an established one, but anything fills an empty slot.
        assert!(
            supersedes_published(Some(log_id(5)), None),
            "any Some fills an empty slot"
        );
        assert!(
            supersedes_published(None, None),
            "first publish into an empty slot is allowed"
        );
        assert!(
            supersedes_published(Some(log_id(8)), Some(log_id(5))),
            "higher index supersedes"
        );
        assert!(
            supersedes_published(Some(log_id(5)), Some(log_id(5))),
            "equal index may republish (same state, fresh snapshot_id)"
        );
        assert!(
            !supersedes_published(Some(log_id(5)), Some(log_id(8))),
            "lower index must not regress"
        );
        assert!(
            !supersedes_published(None, Some(log_id(1))),
            "None must not regress an established snapshot"
        );
    }

    #[tokio::test]
    async fn commit_snapshot_drops_a_stale_publish() {
        // Exercise the build-side of the TOCTOU directly at the seam
        // build_snapshot shares with install. Once a newer snapshot is durable
        // and published, a commit carrying a lower last_log_id — a build whose
        // state was captured before the newer install but whose write lands
        // last — must be dropped: it reports `Ok(false)`, runs no adopt
        // callback, and leaves both the store and current_snapshot byte-for-byte
        // untouched. (The full build path can only reach this state under a
        // real scheduling race, so we drive the shared commit seam directly.)
        enable_tracing();
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("with_store");

        let (newer_meta, newer_bytes) = install_payload(80, log_id(8));
        sm.install_snapshot(&newer_meta, Cursor::new(newer_bytes))
            .await
            .expect("install newer");
        let durable_after_newer = store
            .load()
            .expect("load")
            .expect("durable snapshot present");

        let (stale_meta, stale_data) = install_payload(50, log_id(5));
        let envelope = tsoracle_openraft_toolkit::encode(
            tsoracle_openraft_toolkit::SCHEMA_VERSION,
            &PersistedSnapshot {
                meta: stale_meta.clone(),
                data: stale_data.clone(),
            },
        )
        .expect("encode envelope");

        let adopt_ran = std::cell::Cell::new(false);
        let adopted = sm
            .commit_snapshot(stale_meta, stale_data, &envelope, |_| adopt_ran.set(true))
            .expect("commit_snapshot");

        assert!(!adopted, "stale commit must report it was not adopted");
        assert!(
            !adopt_ran.get(),
            "adopt callback must not run for a dropped commit"
        );
        assert_eq!(
            store.load().expect("load").as_deref(),
            Some(durable_after_newer.as_slice()),
            "durable snapshot must be unchanged — a stale commit must not write"
        );
        let current = sm
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot")
            .expect("snapshot present");
        assert_eq!(
            current.meta.last_log_id.map(|l| l.index),
            Some(8),
            "in-memory current_snapshot must be unchanged"
        );
    }

    #[tokio::test]
    async fn install_snapshot_rejects_meta_payload_log_id_mismatch() {
        // `meta.last_log_id` (read back by `get_current_snapshot`) and
        // `payload.last_applied` (read back by `applied_state`) must agree —
        // honest peers always build them from the same `last_applied`, so a
        // disagreement signals corruption or a peer bug. Installing it anyway
        // would silently desync the two reader methods, so the install must be
        // rejected, leave state untouched, and persist nothing.
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("with_store");

        let payload = HighWaterStateMachineSnapshot {
            current_value: 123,
            last_applied: Some(log_id(5)),
            last_membership: StoredMem::default(),
        };
        let bytes =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &payload)
                .expect("serialize payload");

        let meta = SnapMeta {
            last_log_id: Some(log_id(6)),
            last_membership: payload.last_membership.clone(),
            snapshot_id: "mismatch-1".into(),
        };

        let Err(err) = sm
            .install_snapshot(&meta, std::io::Cursor::new(bytes))
            .await
        else {
            panic!("install_snapshot must reject meta/payload last_log_id mismatch");
        };
        let msg = err.to_string();
        assert!(
            msg.contains("last_log_id"),
            "error should name the mismatched field: {msg}"
        );

        assert_eq!(sm.current_value().await, 0);
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last, None);
        assert!(
            sm.get_current_snapshot().await.unwrap().is_none(),
            "rejected install must not publish a current snapshot"
        );
        assert!(
            store.load().expect("load").is_none(),
            "rejected install must not persist an envelope"
        );
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
        /// Monotonicity invariant: applying an arbitrary sequence of `Advance`
        /// values leaves the state machine at `max(0, max(at_leasts))`, and
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
                        EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: *t })),
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
                        EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: *t })),
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

    #[test]
    fn snapshot_payload_pins_v3_layout() {
        // Hand-built v3 frame: [SCHEMA_VERSION | postcard(payload)]. The leading
        // byte advanced 2 -> 3 when OpenraftPeer gained the service_endpoint field
        // (a breaking on-disk change for membership); the postcard body is
        // unchanged. Body [7, 0, 0, 0, 0] = current_value 7, last_applied None,
        // then default StoredMembership (None log id + empty configs + empty
        // nodes). Reordering or inserting a field changes these bytes and trips
        // this test, forcing a SCHEMA_VERSION bump.
        let payload = HighWaterStateMachineSnapshot {
            current_value: 7,
            last_applied: None,
            last_membership: StoredMembership::default(),
        };
        let framed =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &payload)
                .expect("encode");
        assert_eq!(framed, vec![3, 7, 0, 0, 0, 0]);
    }

    #[test]
    fn log_entry_pins_v3_layout() {
        // The bytes RocksdbLogStore<TypeConfig> persists per entry: the v3
        // frame around a Normal entry carrying Advance(AdvancePayload { at_least: 5 })
        // at log id (term 1, node 1, index 1). Leading byte 3 is SCHEMA_VERSION;
        // body [1,1,1,1,0,5] = leader (term 1, node 1), index 1,
        // EntryPayload::Normal tag (1), Advance variant (0), at_least 5 —
        // byte-identical to the pre-v3 layout.
        let lid = openraft::testing::log_id::<TypeConfig>(1, 1, 1);
        let entry: EntryOf<TypeConfig> = EntryOf::<TypeConfig>::new_normal(
            lid,
            HighWaterCommand::Advance(AdvancePayload { at_least: 5 }),
        );
        let framed =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &entry)
                .expect("encode");
        assert_eq!(framed, vec![3, 1, 1, 1, 1, 0, 5]);
    }

    #[test]
    fn meta_vote_pins_v3_layout() {
        // The bytes RocksdbLogStore<TypeConfig> now persists in the meta column
        // for a Vote — historically a bare, unversioned postcard blob, now the
        // same [SCHEMA_VERSION | postcard] frame as the log column. This is the
        // recovery-critical field (term/leader-election safety) the framing
        // protects: a foreign version loud-rejects instead of misdecoding. Body
        // [7, 3, 1] = leader (term 7, node 3), committed flag true; leading byte
        // 3 is SCHEMA_VERSION. A layout change to Vote trips this test.
        let vote: openraft::type_config::alias::VoteOf<TypeConfig> =
            openraft::Vote::new_committed(7, 3);
        let framed =
            tsoracle_openraft_toolkit::encode(tsoracle_openraft_toolkit::SCHEMA_VERSION, &vote)
                .expect("encode");
        assert_eq!(framed, vec![3, 7, 3, 1]);
    }

    #[tokio::test]
    async fn install_snapshot_rejects_foreign_schema_version() {
        // A streamed snapshot framed with a foreign version must be rejected
        // as InvalidData before any store write or state mutation.
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("with_store");
        let meta = SnapMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "test".to_string(),
        };
        let foreign = vec![0xFF, 7, 0, 0, 0, 0];
        let err = sm
            .install_snapshot(&meta, Cursor::new(foreign))
            .await
            .expect_err("must reject foreign version");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        assert_eq!(sm.current_value().await, 0);
        let (last, _) = sm.applied_state().await.unwrap();
        assert_eq!(last, None);
        assert!(
            sm.get_current_snapshot().await.unwrap().is_none(),
            "rejected install must not publish a current snapshot"
        );
        assert!(
            store.load().expect("load").is_none(),
            "rejected install must not persist an envelope"
        );
    }
}
