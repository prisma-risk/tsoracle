//! Snapshot logic. Held separately from state.rs to keep the state-machine
//! apply path readable. The `RaftStateMachine` impl in state.rs delegates
//! `install_snapshot` and `get_current_snapshot` to the free functions here.

use std::io;
use std::io::Cursor;

use openraft::RaftSnapshotBuilder;
use openraft::storage::Snapshot;
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf};

use crate::types::TypeConfig;

use super::io as disk;
use super::state::{AppliedState, FileStateMachine};

type SnapMeta = SnapshotMetaOf<TypeConfig>;
type SnapOf = SnapshotOf<TypeConfig>;
type SnapData = Cursor<Vec<u8>>;

/// Snapshot bytes + metadata held in memory after build or install.
///
/// Fields are `pub(super)` (not just the struct) so io.rs (sibling module)
/// can construct StoredSnapshot from disk-loaded bytes via the literal
/// syntax `StoredSnapshot { meta, data }`. Struct visibility alone leaves
/// fields private in Rust.
pub(super) struct StoredSnapshot {
    pub(super) meta: SnapMeta,
    pub(super) data: Vec<u8>,
}

impl Clone for StoredSnapshot {
    fn clone(&self) -> Self {
        Self {
            meta: self.meta.clone(),
            data: self.data.clone(),
        }
    }
}

/// Install a snapshot received from the leader: parse it into AppliedState,
/// persist the bytes + meta to disk, and update the in-memory state.
pub(super) async fn install(
    sm: &mut FileStateMachine,
    meta: &SnapMeta,
    snapshot: SnapData,
) -> Result<(), io::Error> {
    let data = snapshot.into_inner();
    let s: AppliedState =
        serde_json::from_slice(&data).map_err(|e| io::Error::other(e.to_string()))?;
    *sm.state.write().await = s;
    disk::save_snapshot(&sm.dir, meta, &data)?;
    *sm.current_snapshot.write().await = Some(StoredSnapshot {
        meta: meta.clone(),
        data,
    });
    Ok(())
}

/// Return the current snapshot (in memory or none).
pub(super) async fn current(sm: &mut FileStateMachine) -> Result<Option<SnapOf>, io::Error> {
    let guard = sm.current_snapshot.read().await;
    Ok(guard.as_ref().map(|s| Snapshot {
        meta: s.meta.clone(),
        snapshot: Cursor::new(s.data.clone()),
    }))
}

impl RaftSnapshotBuilder<TypeConfig> for FileStateMachine {
    async fn build_snapshot(&mut self) -> Result<SnapOf, io::Error> {
        let state = self.state.read().await.clone();
        let bytes = serde_json::to_vec(&state).map_err(|e| io::Error::other(e.to_string()))?;

        let last_applied = state.last_applied;
        let mut idx = self.snapshot_idx.write().await;
        *idx += 1;
        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied
                .as_ref()
                .map(|x| format!("{}", x.leader_id))
                .unwrap_or_else(|| "0".into()),
            last_applied.as_ref().map(|x| x.index).unwrap_or(0),
            *idx
        );

        let meta = SnapMeta {
            last_log_id: last_applied,
            last_membership: state.last_membership.clone(),
            snapshot_id,
        };

        disk::save_snapshot(&self.dir, &meta, &bytes)?;

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: bytes.clone(),
        };
        *self.current_snapshot.write().await = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}
