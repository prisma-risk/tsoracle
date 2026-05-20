//! In-memory `RaftStateMachine` for the high-water counter.
//!
//! State is one `u64` plus the openraft-required apply-progress metadata
//! (`last_applied`, `last_membership`). Snapshots are postcard-encoded blobs of
//! that tuple. All state lives inside an `Arc<Mutex<HighWaterCore>>` so the
//! state machine is cheaply cloneable — required because openraft's
//! `RaftStateMachine::SnapshotBuilder = Self` design hands out clones to drive
//! `build_snapshot` concurrently with `apply`.
//!
//! # Why in-memory only
//!
//! This is the Phase A driver: durability comes from the raft log (which the
//! caller persists via their own `RaftLogStorage`, e.g.
//! `openraft_toolkit::RocksdbLogStore`). On restart the log replays into the
//! state machine, rebuilding `current_value`. A persisted backend can ship as
//! a follow-up without changing the trait wiring.

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

/// In-memory `RaftStateMachine` implementation.
///
/// Clone-cheap: every field is behind an `Arc`, so cloning hands out another
/// handle pointing at the same `Core`. Required by openraft's
/// `get_snapshot_builder` contract which uses `SnapshotBuilder = Self`.
pub struct HighWaterStateMachine {
    core: Arc<Mutex<Core>>,
}

impl Clone for HighWaterStateMachine {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl Default for HighWaterStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl HighWaterStateMachine {
    /// Build a fresh, empty state machine.
    pub fn new() -> Self {
        Self {
            core: Arc::new(Mutex::new(Core {
                current_value: 0,
                last_applied: None,
                last_membership: StoredMembership::default(),
                snapshot_idx: 0,
                current_snapshot: None,
            })),
        }
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
        // Clone the relevant fields out from under the lock, then encode
        // outside the critical section.
        let (snapshot_payload, meta, stored) = {
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
                .map_err(|e| io::Error::other(format!("snapshot serialize: {e}")))?;
            let stored = StoredSnapshot {
                meta: meta.clone(),
                data: bytes.clone(),
            };
            core.current_snapshot = Some(stored.clone());
            (bytes, meta, stored)
        };

        // `stored` is retained for `get_current_snapshot`; the returned
        // `Snapshot` owns its own copy of the bytes.
        let _ = stored;
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
            .map_err(|e| io::Error::other(format!("snapshot deserialize: {e}")))?;

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

    fn entry(index: u64, payload: EntryPayload<HighWaterCommand, u64, crate::type_config::OpenraftPeer>) -> EntryResponder<TypeConfig> {
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
        sm.apply(stream::iter([Ok(entry(index, payload))])).await.expect("apply");
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
        let mem = openraft::Membership::new_with_defaults(
            vec![BTreeSet::from([1u64])],
            [1u64],
        );
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
}
