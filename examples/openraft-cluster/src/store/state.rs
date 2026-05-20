//! State machine. The entire RaftStateMachine trait impl lives here;
//! snapshot install/get methods delegate to free functions in snapshot.rs
//! because Rust's coherence rule forbids two impl blocks for the same
//! (Trait, Type) pair.

use std::io;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use openraft::EntryPayload;
use openraft::StoredMembership;
use openraft::storage::RaftStateMachine;
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::types::{TsoExtendResp, TypeConfig};

use super::io as disk;
use super::snapshot::{self, StoredSnapshot};

type LogId = LogIdOf<TypeConfig>;
type StoredMem = StoredMembershipOf<TypeConfig>;
type SnapMeta = SnapshotMetaOf<TypeConfig>;
type SnapOf = SnapshotOf<TypeConfig>;
type SnapData = Cursor<Vec<u8>>;

/// The persisted state of the state machine.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct AppliedState {
    /// The last log id that has been applied to the state machine.
    pub last_applied: Option<LogId>,
    /// The last stored membership.
    pub last_membership: StoredMem,
    /// The high-water mark: the maximum timestamp epoch committed.
    pub high_water: u64,
}

/// File-backed state machine and snapshot builder.
pub struct FileStateMachine {
    pub(super) dir: PathBuf,
    /// Shared applied state — also readable by the driver.
    pub state: Arc<RwLock<AppliedState>>,
    pub(super) snapshot_idx: Arc<RwLock<u64>>,
    pub(super) current_snapshot: Arc<RwLock<Option<StoredSnapshot>>>,
}

impl FileStateMachine {
    pub(super) fn from_loaded(
        dir: PathBuf,
        applied: AppliedState,
        snapshot: Option<StoredSnapshot>,
    ) -> Self {
        FileStateMachine {
            dir,
            state: Arc::new(RwLock::new(applied)),
            snapshot_idx: Arc::new(RwLock::new(0)),
            current_snapshot: Arc::new(RwLock::new(snapshot)),
        }
    }
}

impl RaftStateMachine<TypeConfig> for FileStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMem), io::Error> {
        let state = self.state.read().await;
        Ok((state.last_applied, state.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<openraft::storage::EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + Send,
    {
        let mut s = self.state.write().await;
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            s.last_applied = Some(entry.log_id);
            let resp = match &entry.payload {
                EntryPayload::Blank => TsoExtendResp {
                    persisted: s.high_water,
                },
                EntryPayload::Normal(req) => {
                    // Monotonic advance: never regress the high_water.
                    if req.at_least > s.high_water {
                        s.high_water = req.at_least;
                    }
                    TsoExtendResp {
                        persisted: s.high_water,
                    }
                }
                EntryPayload::Membership(m) => {
                    s.last_membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    TsoExtendResp {
                        persisted: s.high_water,
                    }
                }
            };
            // Persist state atomically before sending the response.
            disk::save_state(&self.dir, &s)?;
            // Send response to any waiting client (None on followers).
            if let Some(r) = responder {
                r.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        FileStateMachine {
            dir: self.dir.clone(),
            state: self.state.clone(),
            snapshot_idx: self.snapshot_idx.clone(),
            current_snapshot: self.current_snapshot.clone(),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<SnapData, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapMeta,
        snapshot: SnapData,
    ) -> Result<(), io::Error> {
        snapshot::install(self, meta, snapshot).await
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapOf>, io::Error> {
        snapshot::current(self).await
    }
}
