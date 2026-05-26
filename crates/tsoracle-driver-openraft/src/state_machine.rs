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
//! [`tsoracle_openraft_toolkit::decode`] at the active write version) — a leading
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

use tsoracle_codec::{
    VersionedCodec, decode_framed, decode_postcard_exact, encode_framed, encode_postcard,
};
use tsoracle_openraft_toolkit::{
    ActiveWriteVersion, BASELINE_WRITE_VERSION, MAX_READABLE_VERSION, MIN_READABLE_VERSION,
    codec_io_error,
};

use crate::log_entry::{HighWaterCommand, SetFormatVersionPayload};
use crate::snapshot_store::{InMemorySnapshotStore, SnapshotStore};
use crate::type_config::{ApplyOutcome, HighWaterApplied, TypeConfig};

type LogId = LogIdOf<TypeConfig>;
type SnapMeta = SnapshotMetaOf<TypeConfig>;
type SnapOf = SnapshotOf<TypeConfig>;
type SnapData = Cursor<Vec<u8>>;
type StoredMem = StoredMembershipOf<TypeConfig>;

/// Snapshot payload. The persisted/streamed bytes are version-framed as
/// `[active write version | postcard(Self)]`, so decode them through the
/// toolkit `decode_framed` over the readable range
/// `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]` rather than raw
/// `postcard::from_bytes`.
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
/// `[active write version | postcard(HighWaterStateMachineSnapshot)]`. Kept
/// private — embedders that need to inspect persisted snapshots decode the
/// inner `data` blob through the toolkit `decode_framed` over the readable
/// range.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapMeta,
    data: Vec<u8>,
}

impl VersionedCodec for HighWaterStateMachineSnapshot {
    fn decode_version(version: u8, body: &[u8]) -> Result<Self, tsoracle_codec::CodecError> {
        match version {
            // v4 (BASELINE_WRITE_VERSION): whole-value postcard, byte-identical
            // to the pre-seam frame. Later phases add older/newer arms here as
            // the layout evolves and MIN_READABLE_VERSION/MAX_READABLE_VERSION
            // widen.
            v if v == BASELINE_WRITE_VERSION => decode_postcard_exact(body),
            other => Err(tsoracle_codec::CodecError::VersionUnsupported {
                min: MIN_READABLE_VERSION,
                max: MAX_READABLE_VERSION,
                actual: other,
            }),
        }
    }

    fn encode_version(&self, version: u8) -> Result<Vec<u8>, tsoracle_codec::CodecError> {
        match version {
            v if v == BASELINE_WRITE_VERSION => encode_postcard(self),
            other => Err(tsoracle_codec::CodecError::VersionUnsupported {
                min: MIN_READABLE_VERSION,
                max: MAX_READABLE_VERSION,
                actual: other,
            }),
        }
    }
}

impl VersionedCodec for PersistedSnapshot {
    fn decode_version(version: u8, body: &[u8]) -> Result<Self, tsoracle_codec::CodecError> {
        match version {
            v if v == BASELINE_WRITE_VERSION => decode_postcard_exact(body),
            other => Err(tsoracle_codec::CodecError::VersionUnsupported {
                min: MIN_READABLE_VERSION,
                max: MAX_READABLE_VERSION,
                actual: other,
            }),
        }
    }

    fn encode_version(&self, version: u8) -> Result<Vec<u8>, tsoracle_codec::CodecError> {
        match version {
            v if v == BASELINE_WRITE_VERSION => encode_postcard(self),
            other => Err(tsoracle_codec::CodecError::VersionUnsupported {
                min: MIN_READABLE_VERSION,
                max: MAX_READABLE_VERSION,
                actual: other,
            }),
        }
    }
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
    /// Shared active write version: the version this SM stamps onto snapshots
    /// it builds and installs. Shares the one cell with the log store (and,
    /// in a later phase, the wire sender) so all writers emit the same
    /// version. Mutated only by a successful activation apply (later phase);
    /// defaults to BASELINE.
    active_write_version: ActiveWriteVersion,
}

impl Clone for HighWaterStateMachine {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            store: Arc::clone(&self.store),
            persist: Arc::clone(&self.persist),
            active_write_version: self.active_write_version.clone(),
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
        Self::with_store_and_active_version(store, ActiveWriteVersion::default())
    }

    /// Build a state machine backed by `store`, sharing `active_write_version`
    /// with the log store (and, in a later phase, the wire sender). Bootstrap
    /// constructs and seeds the cell once, then threads the same clone here
    /// and into the log store; non-bootstrap callers use
    /// [`with_store`](Self::with_store), which supplies a fresh BASELINE cell.
    pub fn with_store_and_active_version(
        store: Arc<dyn SnapshotStore>,
        active_write_version: ActiveWriteVersion,
    ) -> io::Result<Self> {
        let mut core = Core {
            current_value: 0,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
            current_snapshot: None,
        };
        if let Some(bytes) = store.load()? {
            let persisted: PersistedSnapshot =
                decode_framed(MIN_READABLE_VERSION, MAX_READABLE_VERSION, &bytes)
                    .map_err(|e| codec_io_error("persisted snapshot envelope decode", e))?;
            let payload: HighWaterStateMachineSnapshot =
                decode_framed(MIN_READABLE_VERSION, MAX_READABLE_VERSION, &persisted.data)
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
            active_write_version,
        })
    }

    /// The version this SM currently stamps onto snapshots.
    pub fn active_write_version(&self) -> u8 {
        self.active_write_version.get()
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
            let bytes = encode_framed(self.active_write_version.get(), &payload)
                .map_err(|e| codec_io_error("snapshot payload serialize", e))?;
            (bytes, meta)
        };

        let persisted = PersistedSnapshot {
            meta: meta.clone(),
            data: snapshot_payload.clone(),
        };
        let envelope = encode_framed(self.active_write_version.get(), &persisted)
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
                        outcome: ApplyOutcome::Advanced,
                    }
                }
                EntryPayload::Normal(HighWaterCommand::Advance(advance)) => {
                    let mut core = self.core.lock();
                    core.current_value = advance.merge(core.current_value);
                    core.last_applied = Some(log_id);
                    HighWaterApplied {
                        value: core.current_value,
                        outcome: ApplyOutcome::Advanced,
                    }
                }
                EntryPayload::Normal(HighWaterCommand::SetFormatVersion(
                    SetFormatVersionPayload {
                        target,
                        gated_members,
                    },
                )) => {
                    let target = *target;
                    // Evaluate the subset against the membership committed as
                    // of this entry's log position. Apply folds the log in
                    // index order, so `core.last_membership` is exactly that
                    // membership (a `Membership` entry at a lower index has
                    // already updated it; a later one has not). Cover BOTH
                    // voters and learners: openraft replicates/snapshots
                    // learners, so an un-gated learner must force a no-op.
                    let mut core = self.core.lock();
                    let committed_members: std::collections::BTreeSet<u64> = core
                        .last_membership
                        .membership()
                        .nodes()
                        .map(|(node_id, _node)| *node_id)
                        .collect();
                    let outcome = if committed_members.is_subset(gated_members) {
                        // Successful (non-no-op) apply: set the
                        // process-shared active-write-version cell. NO meta
                        // write — durability is the raft log (deterministic
                        // replay re-runs this exact check) plus the snapshot
                        // frame byte.
                        self.active_write_version.set(target);
                        ApplyOutcome::FormatActivated { target }
                    } else {
                        // Membership grew an un-gated member between the gate
                        // and this entry's position. Leave the cell
                        // untouched; the operator re-gates and re-issues. A
                        // no-op writes nothing at `target`, so it cannot
                        // resurrect on restart.
                        ApplyOutcome::FormatActivationNoop { target }
                    };
                    core.last_applied = Some(log_id);
                    HighWaterApplied {
                        value: core.current_value,
                        outcome,
                    }
                }
                EntryPayload::Membership(membership) => {
                    let mut core = self.core.lock();
                    core.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    core.last_applied = Some(log_id);
                    HighWaterApplied {
                        value: core.current_value,
                        outcome: ApplyOutcome::Advanced,
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
        let payload: HighWaterStateMachineSnapshot =
            decode_framed(MIN_READABLE_VERSION, MAX_READABLE_VERSION, &bytes)
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
        let envelope = encode_framed(self.active_write_version.get(), &persisted)
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

    // ---- SetFormatVersion apply tests ----
    //
    // The apply path keys the flip off a successful (non-no-op) apply: the
    // subset check `committed_members ⊆ gated_members` runs against
    // `core.last_membership` evaluated at the bump's own log position (apply
    // folds the log in index order). On a hit the shared cell is set; on a
    // miss the cell is untouched and a no-op outcome is returned. Direct
    // cell observation is sufficient since `SetFormatVersionPayload`'s effect
    // is exactly "did the cell move?"; we don't need to capture the
    // ApplyOutcome through the responder channel for these checks.

    /// A membership whose voter config is `voters`, as a `Membership` payload.
    /// The concrete `Membership<NID, N>` type is inferred from the call site
    /// (e.g. `EntryPayload::Membership(...)` infers it for `TypeConfig`).
    fn voters_membership(
        voters: &[u64],
    ) -> openraft::Membership<u64, crate::type_config::OpenraftPeer> {
        openraft::Membership::new_with_defaults(
            vec![voters.iter().copied().collect::<BTreeSet<u64>>()],
            voters.to_vec(),
        )
    }

    #[tokio::test]
    async fn set_format_version_subset_match_sets_cell() {
        let mut sm = HighWaterStateMachine::new();
        // Establish membership {1, 2} at index 1.
        apply_one(
            &mut sm,
            1,
            EntryPayload::Membership(voters_membership(&[1, 2])),
        )
        .await;
        // Bump gated on a superset of the committed membership → subset holds.
        apply_one(
            &mut sm,
            2,
            EntryPayload::Normal(HighWaterCommand::SetFormatVersion(
                SetFormatVersionPayload {
                    target: 7,
                    gated_members: BTreeSet::from([1u64, 2u64, 3u64]),
                },
            )),
        )
        .await;
        assert_eq!(
            sm.active_write_version(),
            7,
            "cell set on successful subset apply"
        );
    }

    #[tokio::test]
    async fn set_format_version_membership_not_subset_is_noop() {
        let mut sm = HighWaterStateMachine::new();
        let before = sm.active_write_version();
        // Membership {1, 2, 9} at index 1; the gate only covered {1, 2}.
        apply_one(
            &mut sm,
            1,
            EntryPayload::Membership(voters_membership(&[1, 2, 9])),
        )
        .await;
        apply_one(
            &mut sm,
            2,
            EntryPayload::Normal(HighWaterCommand::SetFormatVersion(
                SetFormatVersionPayload {
                    target: 7,
                    gated_members: BTreeSet::from([1u64, 2u64]),
                },
            )),
        )
        .await;
        assert_eq!(
            sm.active_write_version(),
            before,
            "no-op must leave the shared cell untouched"
        );
    }

    #[tokio::test]
    async fn set_format_version_covers_learners_not_only_voters() {
        let mut sm = HighWaterStateMachine::new();
        let before = sm.active_write_version();
        // Voter {1}, learner {2}. `new_with_defaults(voter_groups, all_ids)`
        // treats node ids in `all_ids` but absent from `voter_groups` as
        // learners. A voter-only gate `{1}` must NOT satisfy the subset —
        // the learner is a current member and openraft replicates to it,
        // so an un-gated learner forces a no-op.
        let membership =
            openraft::Membership::new_with_defaults(vec![BTreeSet::from([1u64])], [1u64, 2u64]);
        apply_one(&mut sm, 1, EntryPayload::Membership(membership)).await;
        apply_one(
            &mut sm,
            2,
            EntryPayload::Normal(HighWaterCommand::SetFormatVersion(
                SetFormatVersionPayload {
                    target: 7,
                    gated_members: BTreeSet::from([1u64]),
                },
            )),
        )
        .await;
        assert_eq!(
            sm.active_write_version(),
            before,
            "un-gated learner must force a no-op"
        );
    }

    // ---- Replay / recovery confirmation tests ----
    //
    // These model openraft's deterministic recovery: a fresh state machine
    // (fresh BASELINE-seeded cell) re-applies the committed log in index
    // order, so the SetFormatVersion subset check re-runs per entry. A
    // no-op replays as a no-op (no record was ever written at the higher
    // version); a success re-establishes the cell.

    #[derive(Clone)]
    enum ReplayEntry {
        Membership(Vec<u64>),
        Bump { target: u8, gated: Vec<u64> },
    }

    async fn replay(sm: &mut HighWaterStateMachine, log: &[(u64, ReplayEntry)]) {
        for (index, kind) in log {
            let payload = match kind {
                ReplayEntry::Membership(voters) => {
                    EntryPayload::Membership(voters_membership(voters))
                }
                ReplayEntry::Bump { target, gated } => EntryPayload::Normal(
                    HighWaterCommand::SetFormatVersion(SetFormatVersionPayload {
                        target: *target,
                        gated_members: gated.iter().copied().collect(),
                    }),
                ),
            };
            apply_one(sm, *index, payload).await;
        }
    }

    #[tokio::test]
    async fn successful_activation_survives_replay() {
        let log = vec![
            (1u64, ReplayEntry::Membership(vec![1, 2])),
            (
                2u64,
                ReplayEntry::Bump {
                    target: 7,
                    gated: vec![1, 2, 3],
                },
            ),
        ];
        // Live apply sets the cell.
        let mut live = HighWaterStateMachine::new();
        replay(&mut live, &log).await;
        assert_eq!(live.active_write_version(), 7);

        // "Restart": a fresh SM with a fresh BASELINE-seeded cell, replaying
        // the same committed log, re-establishes target via the re-run
        // subset check. NO meta key consulted.
        let mut recovered = HighWaterStateMachine::new();
        assert_ne!(
            recovered.active_write_version(),
            7,
            "fresh cell starts at baseline"
        );
        replay(&mut recovered, &log).await;
        assert_eq!(
            recovered.active_write_version(),
            7,
            "replay re-applies the flip"
        );
    }

    #[tokio::test]
    async fn noop_bump_never_advances_across_restart() {
        let log = vec![
            // Membership has an un-gated member 9; the gate only covered {1, 2}.
            (1u64, ReplayEntry::Membership(vec![1, 2, 9])),
            (
                2u64,
                ReplayEntry::Bump {
                    target: 7,
                    gated: vec![1, 2],
                },
            ),
        ];
        let mut live = HighWaterStateMachine::new();
        let baseline = live.active_write_version();
        replay(&mut live, &log).await;
        assert_eq!(
            live.active_write_version(),
            baseline,
            "no-op leaves the cell"
        );

        // Restart: replay the same log; the no-op replays as a no-op (the
        // subset check fails identically), so the cell stays at baseline.
        // The committed entry's mere presence never advances the version.
        let mut recovered = HighWaterStateMachine::new();
        replay(&mut recovered, &log).await;
        assert_eq!(
            recovered.active_write_version(),
            baseline,
            "a no-op'd bump cannot resurrect on restart"
        );
    }

    #[tokio::test]
    async fn replay_is_deterministic() {
        let log = vec![
            (1u64, ReplayEntry::Membership(vec![1, 2])),
            (
                2u64,
                ReplayEntry::Bump {
                    target: 7,
                    gated: vec![1, 2, 3],
                },
            ),
            // A later bump gated on a set that excludes a now-added member.
            (3u64, ReplayEntry::Membership(vec![1, 2, 5])),
            (
                4u64,
                ReplayEntry::Bump {
                    target: 8,
                    gated: vec![1, 2],
                },
            ),
        ];
        let mut first = HighWaterStateMachine::new();
        replay(&mut first, &log).await;
        let mut second = HighWaterStateMachine::new();
        replay(&mut second, &log).await;
        // Index-2 bump succeeded (membership {1,2} ⊆ {1,2,3}); index-4 bump
        // no-ops (membership {1,2,5} ⊄ {1,2}), so the cell stays at 7.
        assert_eq!(first.active_write_version(), 7);
        assert_eq!(
            first.active_write_version(),
            second.active_write_version(),
            "two replays of the same committed log yield the same cell"
        );
    }

    // ---- Snapshot active-write-version stamping (format migration) ----
    //
    // These tests pin the contract that the snapshot builder reads the
    // node's CURRENT active write version (the shared cell) at build time,
    // rather than a hard-coded constant. Today that is BASELINE_WRITE_VERSION
    // since no activation has flipped the cell; the assertions are written
    // against the accessor (`sm.active_write_version()`) so they remain
    // correct after a real activation moves the cell forward and the next
    // build emits at the new version.

    #[tokio::test]
    async fn build_snapshot_stamps_active_write_version() {
        // The persisted envelope's leading version byte must equal what the
        // accessor reports — not a literal. This proves the plumbing is
        // active-version-driven and would catch a regression to a constant.
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("with_store");
        apply_one(
            &mut sm,
            1,
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 321 })),
        )
        .await;

        let active = sm.active_write_version();
        sm.build_snapshot().await.expect("build_snapshot");

        let envelope_bytes = store.load().expect("load").expect("snapshot present");
        assert_eq!(
            envelope_bytes[0], active,
            "envelope leading version byte must equal the active write version"
        );

        // The inner payload blob is the on-disk record openraft replays from
        // on snapshot install; it must be stamped at the active version too
        // so a follower receiving it can decode against its own version
        // window.
        let persisted: PersistedSnapshot = tsoracle_codec::decode_framed(
            tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
            tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
            &envelope_bytes,
        )
        .expect("decode envelope");
        assert_eq!(
            persisted.data[0], active,
            "inner snapshot payload leading version byte must equal the active write version"
        );
    }

    #[tokio::test]
    async fn old_version_snapshot_is_read_and_reemitted_at_active_version() {
        // Migration-on-next-write: a snapshot persisted at version V is
        // readable across the multi-version codec and the next
        // build_snapshot re-emits at the cluster's ACTIVE write version.
        //
        // HONESTY: MIN==MAX==BASELINE today, so there is no genuine lower
        // production version. This exercises the seam end-to-end at the
        // only version that exists; the genuine cross-version rewrite
        // (install vN, flip to vN+1, assert rebuild is vN+1) is the
        // structural extension a real vN+1 work would make by
        // parameterizing the installed version.
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());

        // Persist a snapshot via a first SM instance, then drop it.
        {
            let mut writer = HighWaterStateMachine::with_store(store.clone()).expect("writer SM");
            apply_one(
                &mut writer,
                3,
                EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 777 })),
            )
            .await;
            writer
                .build_snapshot()
                .await
                .expect("build initial snapshot");
        }

        // Reopen: the recovery path decodes across the readable range and
        // restores state.
        let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("reopened SM");
        assert_eq!(
            sm.current_value().await,
            777,
            "recovered value across reopen"
        );

        // The next build re-emits at the active write version.
        let active = sm.active_write_version();
        apply_one(
            &mut sm,
            4,
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 888 })),
        )
        .await;
        sm.build_snapshot().await.expect("rebuild snapshot");

        let rebuilt = store.load().expect("load").expect("snapshot present");
        assert_eq!(
            rebuilt[0], active,
            "rebuilt snapshot must be emitted at the active write version"
        );
        let payload: PersistedSnapshot = tsoracle_codec::decode_framed(
            tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
            tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
            &rebuilt,
        )
        .expect("decode rebuilt envelope");
        assert_eq!(payload.meta.last_log_id.map(|l| l.index), Some(4));
    }

    #[test]
    fn snapshot_codec_accepts_full_readable_range() {
        // The migration-seam invariant in isolation: the snapshot decoder
        // must accept any version in [MIN_READABLE_VERSION,
        // MAX_READABLE_VERSION] and reject anything outside it. This is
        // what makes an OLD-version on-disk snapshot readable after the
        // active version moves forward. Asserted against the codec range
        // directly so it documents the contract even while MIN==MAX today.
        let payload = HighWaterStateMachineSnapshot {
            current_value: 5,
            last_applied: Some(log_id(2)),
            last_membership: StoredMem::default(),
        };
        for version in tsoracle_openraft_toolkit::MIN_READABLE_VERSION
            ..=tsoracle_openraft_toolkit::MAX_READABLE_VERSION
        {
            let framed = tsoracle_codec::encode_framed(version, &payload).expect("encode in range");
            let decoded: HighWaterStateMachineSnapshot = tsoracle_codec::decode_framed(
                tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
                tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
                &framed,
            )
            .expect("decode in range");
            assert_eq!(decoded, payload);
        }

        // One past the readable max must be rejected loudly.
        let above = tsoracle_openraft_toolkit::MAX_READABLE_VERSION.saturating_add(1);
        let mut framed_above = vec![above];
        framed_above.extend_from_slice(&tsoracle_codec::encode_postcard(&payload).expect("body"));
        assert!(
            tsoracle_codec::decode_framed::<HighWaterStateMachineSnapshot>(
                tsoracle_openraft_toolkit::MIN_READABLE_VERSION,
                tsoracle_openraft_toolkit::MAX_READABLE_VERSION,
                &framed_above,
            )
            .is_err(),
            "a version above the readable max must be rejected"
        );
    }

    #[tokio::test]
    async fn active_write_version_survives_reopen_and_drives_writes() {
        // The durable active write version is recovered on reopen and is
        // the version stamped onto snapshot writes. On `main` the recovered
        // value is BASELINE because no activation has flipped it; this
        // test pins that (a) the accessor is stable across a reopen and
        // (b) the snapshot write uses exactly that recovered value.
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let recovered_active;
        {
            let mut sm = HighWaterStateMachine::with_store(store.clone()).expect("first SM");
            apply_one(
                &mut sm,
                1,
                EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 55 })),
            )
            .await;
            sm.build_snapshot().await.expect("build_snapshot");
            recovered_active = sm.active_write_version();
        }

        let mut reopened = HighWaterStateMachine::with_store(store.clone()).expect("reopened SM");
        assert_eq!(
            reopened.active_write_version(),
            recovered_active,
            "active write version must be stable across reopen"
        );

        apply_one(
            &mut reopened,
            2,
            EntryPayload::Normal(HighWaterCommand::Advance(AdvancePayload { at_least: 66 })),
        )
        .await;
        reopened.build_snapshot().await.expect("rebuild");
        let bytes = store.load().expect("load").expect("snapshot present");
        assert_eq!(
            bytes[0],
            reopened.active_write_version(),
            "rebuilt snapshot stamped with the recovered active write version"
        );
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
        let payload: HighWaterStateMachineSnapshot = tsoracle_openraft_toolkit::decode(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &bytes,
        )
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
        let bytes = tsoracle_openraft_toolkit::encode(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &payload,
        )
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
    async fn state_machine_defaults_to_baseline_active_write_version() {
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let sm = HighWaterStateMachine::with_store(store).expect("with_store");
        assert_eq!(
            sm.active_write_version(),
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION
        );
    }

    #[tokio::test]
    async fn state_machine_reads_the_same_shared_cell() {
        // The SM and the log store hold clones of ONE cell. A set() on the
        // cell (a later phase's activation apply will do this) is observed
        // identically by every clone.
        let cell = tsoracle_openraft_toolkit::ActiveWriteVersion::default();
        let store: Arc<dyn SnapshotStore> = Arc::new(InMemorySnapshotStore::new());
        let sm = HighWaterStateMachine::with_store_and_active_version(store, cell.clone())
            .expect("with_store_and_active_version");
        assert_eq!(
            sm.active_write_version(),
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION
        );
        cell.set(7);
        assert_eq!(sm.active_write_version(), 7);
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
        let bytes = tsoracle_openraft_toolkit::encode(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &envelope,
        )
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
        let bytes = tsoracle_openraft_toolkit::encode(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &payload,
        )
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
        let bytes = tsoracle_openraft_toolkit::encode(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &payload,
        )
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
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
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
        let bytes = tsoracle_openraft_toolkit::encode(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &payload,
        )
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
    fn snapshot_payload_versioned_codec_matches_legacy_frame() {
        use tsoracle_codec::{decode_framed, encode_framed};
        // The framed bytes through the new VersionedCodec seam must equal the
        // legacy `tsoracle_openraft_toolkit::encode(BASELINE_WRITE_VERSION, ..)` frame —
        // proving the on-disk format did not move.
        let payload = HighWaterStateMachineSnapshot {
            current_value: 7,
            last_applied: None,
            last_membership: StoredMembership::default(),
        };
        let via_seam = encode_framed(tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION, &payload)
            .expect("encode_framed");
        let legacy = tsoracle_openraft_toolkit::encode(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &payload,
        )
        .expect("legacy encode");
        assert_eq!(via_seam, legacy);
        assert_eq!(via_seam, vec![4, 7, 0, 0, 0, 0]);

        let back: HighWaterStateMachineSnapshot = decode_framed(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &via_seam,
        )
        .expect("decode_framed");
        assert_eq!(back, payload);
    }

    #[test]
    fn snapshot_payload_versioned_codec_rejects_foreign_version() {
        use tsoracle_codec::{CodecError, decode_framed};
        let framed = vec![0xFFu8, 7, 0, 0, 0, 0];
        let err = decode_framed::<HighWaterStateMachineSnapshot>(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &framed,
        )
        .expect_err("must reject");
        assert!(matches!(
            err,
            CodecError::VersionUnsupported { actual: 0xFF, .. }
        ));
    }

    #[test]
    fn snapshot_payload_pins_v4_layout() {
        use tsoracle_codec::encode_framed;
        // Hand-built v4 frame: [BASELINE_WRITE_VERSION | postcard(payload)],
        // now produced through the VersionedCodec seam that build_snapshot
        // uses. Leading byte advanced 3 -> 4 when OpenraftPeer gained the
        // admin_endpoint field (a breaking on-disk change for membership);
        // the postcard body is unchanged. Body [7, 0, 0, 0, 0] =
        // current_value 7, last_applied None, then default StoredMembership
        // (None log id + empty configs + empty nodes). Reordering or
        // inserting a field changes these bytes and trips this test, forcing
        // a deliberate version bump (a future evolution would advance
        // MAX_READABLE_VERSION + BASELINE_WRITE_VERSION through an
        // activation barrier rather than the historical stop-the-world bump).
        let payload = HighWaterStateMachineSnapshot {
            current_value: 7,
            last_applied: None,
            last_membership: StoredMembership::default(),
        };
        let framed = encode_framed(tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION, &payload)
            .expect("encode");
        assert_eq!(framed, vec![4, 7, 0, 0, 0, 0]);
    }

    #[test]
    fn log_entry_pins_v4_layout() {
        use crate::log_codec::OpenraftLogCodec;
        use tsoracle_openraft_toolkit::LogStoreCodec;
        // The bytes RocksdbLogStore<TypeConfig> persists per entry: the
        // active write version byte (prepended by the store) +
        // OpenraftLogCodec entry body — the v4 frame around a Normal entry
        // carrying Advance(AdvancePayload { at_least: 5 })
        // at log id (term 1, node 1, index 1). Body [1,1,1,1,0,5] = leader
        // (term 1, node 1), index 1, EntryPayload::Normal tag (1), Advance
        // variant (0), at_least 5 — byte-identical to the pre-seam layout.
        let lid = openraft::testing::log_id::<TypeConfig>(1, 1, 1);
        let entry: EntryOf<TypeConfig> = EntryOf::<TypeConfig>::new_normal(
            lid,
            HighWaterCommand::Advance(AdvancePayload { at_least: 5 }),
        );
        let body = <OpenraftLogCodec as LogStoreCodec<TypeConfig>>::encode_entry(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &entry,
        )
        .expect("encode entry body");
        let mut framed = vec![tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION];
        framed.extend_from_slice(&body);
        assert_eq!(framed, vec![4, 1, 1, 1, 1, 0, 5]);
    }

    #[test]
    fn meta_vote_pins_v4_layout() {
        use crate::log_codec::OpenraftLogCodec;
        use tsoracle_openraft_toolkit::LogStoreCodec;
        // The bytes RocksdbLogStore<TypeConfig> persists in the meta column for a
        // Vote: active write version byte + OpenraftLogCodec vote body. Body [7, 3, 1] =
        // leader (term 7, node 3), committed flag true. A layout change to Vote
        // trips this test. This is the recovery-critical field the framing
        // protects: a foreign version loud-rejects instead of misdecoding.
        let vote: openraft::type_config::alias::VoteOf<TypeConfig> =
            openraft::Vote::new_committed(7, 3);
        let body = <OpenraftLogCodec as LogStoreCodec<TypeConfig>>::encode_vote(
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION,
            &vote,
        )
        .expect("encode vote body");
        let mut framed = vec![tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION];
        framed.extend_from_slice(&body);
        assert_eq!(framed, vec![4, 7, 3, 1]);
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
