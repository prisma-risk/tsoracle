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

//! The "existing" host service — a tiny in-memory KV — with TSO piggybacked
//! onto the same openraft log.
//!
//! Envelope shape: `AppData = HostCommand::{Kv(KvOp), Tso(HighWaterCommand)}`.
//! Apply path enforces TSO monotonicity (`max(prev, target)`) inside the host
//! state machine, sitting next to the KV map. Snapshot includes both halves.

use std::collections::BTreeMap;
use std::io;
use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use openraft::error::{ClientWriteError, LinearizableReadError, RaftError};
use openraft::storage::{EntryResponder, RaftStateMachine, Snapshot};
use openraft::type_config::alias::{
    LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, WatchReceiverOf,
};
use openraft::{
    EntryPayload, Raft, RaftMetrics, RaftSnapshotBuilder, ReadPolicy, StoredMembership,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tsoracle_consensus::ConsensusError;
use tsoracle_driver_openraft::{AdvancePayload, HighWaterCommand, OpenraftHighWaterHost};
use tsoracle_openraft_toolkit::declare_raft_types_ext;

// ---------- Envelope shape ----------

/// Per-key operation on the host's KV store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvOp {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

/// The host's `AppData`: envelope over the host's own commands plus TSO.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostCommand {
    Kv(KvOp),
    Tso(HighWaterCommand),
}

impl std::fmt::Display for HostCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostCommand::Kv(op) => write!(f, "Kv({op:?})"),
            HostCommand::Tso(cmd) => write!(f, "Tso({cmd})"),
        }
    }
}

/// Per-entry apply response. The `Kv` arm fills only `kv` (the value previously
/// at the put key, if any); the `Tso` arm fills only `tso` (the high-water
/// after apply); Blank/Membership fill neither.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostApplied {
    pub kv: Option<Vec<u8>>,
    pub tso: Option<u64>,
}

/// Peer identity carried in membership entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostPeer {
    pub addr: String,
}

impl tsoracle_driver_openraft::ServiceEndpoint for HostPeer {
    // The piggyback KV demo never redirects tsoracle clients, so it carries
    // no client endpoint and no admin endpoint.
    fn service_endpoint(&self) -> Option<tsoracle_core::PeerEndpoint> {
        None
    }
    fn admin_endpoint(&self) -> Option<tsoracle_core::PeerEndpoint> {
        None
    }
}

declare_raft_types_ext! {
    pub HostTypeConfig:
        Node            = HostPeer,
        AppData         = HostCommand,
        AppDataResponse = HostApplied,
        SnapshotData    = Cursor<Vec<u8>>,
}

// ---------- Snapshot payload ----------

type LogId = LogIdOf<HostTypeConfig>;
type SnapMeta = SnapshotMetaOf<HostTypeConfig>;
type StoredMem = StoredMembershipOf<HostTypeConfig>;

/// On-the-wire snapshot payload — postcard-encoded. Carries BOTH the KV map
/// and the high-water (the envelope's two halves).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostSnapshot {
    pub kv: BTreeMap<String, Vec<u8>>,
    pub high_water: u64,
    pub last_applied: Option<LogId>,
    pub last_membership: StoredMem,
}

// ---------- State machine ----------

struct Core {
    kv: BTreeMap<String, Vec<u8>>,
    high_water: u64,
    last_applied: Option<LogId>,
    last_membership: StoredMem,
    snapshot_idx: u64,
    current_snapshot: Option<(SnapMeta, Vec<u8>)>,
}

pub struct HostStateMachine {
    core: Arc<Mutex<Core>>,
}

impl Clone for HostStateMachine {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl Default for HostStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl HostStateMachine {
    pub fn new() -> Self {
        Self {
            core: Arc::new(Mutex::new(Core {
                kv: BTreeMap::new(),
                high_water: 0,
                last_applied: None,
                last_membership: StoredMembership::default(),
                snapshot_idx: 0,
                current_snapshot: None,
            })),
        }
    }

    /// Read the current high-water (state-machine-local; callers requiring
    /// linearizability must coordinate a barrier via `Raft`).
    pub async fn high_water(&self) -> u64 {
        self.core.lock().high_water
    }

    /// Dump the KV map for the demo to print. Real services would expose a
    /// proper read API.
    pub fn kv_dump(&self) -> BTreeMap<String, Vec<u8>> {
        self.core.lock().kv.clone()
    }
}

impl RaftSnapshotBuilder<HostTypeConfig> for HostStateMachine {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<HostTypeConfig>, io::Error> {
        let (bytes, meta) = {
            let mut core = self.core.lock();
            core.snapshot_idx += 1;
            let payload = HostSnapshot {
                kv: core.kv.clone(),
                high_water: core.high_water,
                last_applied: core.last_applied,
                last_membership: core.last_membership.clone(),
            };
            let log_index = core.last_applied.map(|l| l.index).unwrap_or(0);
            let snapshot_id = format!("{log_index}-{}", core.snapshot_idx);
            let meta = SnapMeta {
                last_log_id: core.last_applied,
                last_membership: core.last_membership.clone(),
                snapshot_id,
            };
            let bytes = postcard::to_stdvec(&payload)
                .map_err(|e| io::Error::other(format!("snapshot serialize: {e}")))?;
            core.current_snapshot = Some((meta.clone(), bytes.clone()));
            (bytes, meta)
        };
        Ok(Snapshot {
            meta,
            snapshot: Cursor::new(bytes),
        })
    }
}

impl RaftStateMachine<HostTypeConfig> for HostStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMem), io::Error> {
        let core = self.core.lock();
        Ok((core.last_applied, core.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<HostTypeConfig>, io::Error>>
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
                    HostApplied {
                        kv: None,
                        tso: None,
                    }
                }
                EntryPayload::Normal(HostCommand::Kv(op)) => {
                    let mut core = self.core.lock();
                    let prior = match op {
                        KvOp::Put { key, value } => core.kv.insert(key.clone(), value.clone()),
                        KvOp::Delete { key } => core.kv.remove(key),
                    };
                    core.last_applied = Some(log_id);
                    HostApplied {
                        kv: prior,
                        tso: None,
                    }
                }
                EntryPayload::Normal(HostCommand::Tso(HighWaterCommand::Advance(
                    AdvancePayload { at_least },
                ))) => {
                    let mut core = self.core.lock();
                    if *at_least > core.high_water {
                        core.high_water = *at_least;
                    }
                    core.last_applied = Some(log_id);
                    HostApplied {
                        kv: None,
                        tso: Some(core.high_water),
                    }
                }
                EntryPayload::Normal(HostCommand::Tso(HighWaterCommand::SetFormatVersion(_))) => {
                    // The piggyback example does not own an active-write-version
                    // cell of its own — the format activation barrier is a no-op
                    // here. A real piggyback host that cares about the format
                    // version would forward this entry to its embedded
                    // `HighWaterStateMachine`'s apply.
                    let mut core = self.core.lock();
                    core.last_applied = Some(log_id);
                    HostApplied {
                        kv: None,
                        tso: Some(core.high_water),
                    }
                }
                EntryPayload::Normal(HostCommand::Tso(HighWaterCommand::AdvanceDense {
                    ..
                })) => {
                    // The piggyback example does not implement dense sequence
                    // state. This arm handles the exhaustiveness requirement
                    // introduced when `AdvanceDense` was added in Task 2 of the
                    // dense-openraft plan. A real piggyback host that supports
                    // dense sequences would forward this entry to a
                    // `HighWaterStateMachine` (Task 6 of that plan).
                    let mut core = self.core.lock();
                    core.last_applied = Some(log_id);
                    HostApplied {
                        kv: None,
                        tso: Some(core.high_water),
                    }
                }
                EntryPayload::Normal(HostCommand::Tso(HighWaterCommand::AdvanceDenseBatch {
                    ..
                })) => {
                    // The piggyback example does not implement batch dense sequence
                    // state. This arm satisfies exhaustiveness; a real piggyback
                    // host that supports batch advances would forward this entry to
                    // its embedded `HighWaterStateMachine`.
                    let mut core = self.core.lock();
                    core.last_applied = Some(log_id);
                    HostApplied {
                        kv: None,
                        tso: Some(core.high_water),
                    }
                }
                EntryPayload::Membership(membership) => {
                    let mut core = self.core.lock();
                    core.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    core.last_applied = Some(log_id);
                    HostApplied {
                        kv: None,
                        tso: None,
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

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapMeta,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        let payload: HostSnapshot = postcard::from_bytes(&bytes)
            .map_err(|e| io::Error::other(format!("snapshot deserialize: {e}")))?;
        let mut core = self.core.lock();
        core.kv = payload.kv;
        core.high_water = payload.high_water;
        core.last_applied = payload.last_applied;
        core.last_membership = payload.last_membership;
        core.current_snapshot = Some((meta.clone(), bytes));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<HostTypeConfig>>, io::Error> {
        let core = self.core.lock();
        Ok(core
            .current_snapshot
            .as_ref()
            .map(|(meta, bytes)| Snapshot {
                meta: meta.clone(),
                snapshot: Cursor::new(bytes.clone()),
            }))
    }
}

// ---------- OpenraftHighWaterHost impl ----------

/// The integration: 30-ish lines. Owns a clone of the host's raft handle and
/// the state machine; wraps `HighWaterCommand::Advance` in `HostCommand::Tso`
/// for `submit_advance`.
pub struct PiggybackHost {
    raft: Raft<HostTypeConfig, HostStateMachine>,
    state_machine: HostStateMachine,
}

impl PiggybackHost {
    pub fn new(raft: Raft<HostTypeConfig, HostStateMachine>, sm: HostStateMachine) -> Self {
        Self {
            raft,
            state_machine: sm,
        }
    }
}

#[async_trait]
impl OpenraftHighWaterHost for PiggybackHost {
    type Config = HostTypeConfig;

    fn metrics(&self) -> WatchReceiverOf<Self::Config, RaftMetrics<Self::Config>> {
        self.raft.metrics()
    }

    async fn current_high_water(&self) -> Result<u64, ConsensusError> {
        // Multi-node host: barrier-then-read. Mirrors StandaloneHost's error
        // mapping so this example teaches the same pattern — ForwardToLeader
        // becomes NotLeader (the server fence steps down cleanly); other
        // RaftError variants are transient.
        if let Err(e) = self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
            return match e {
                RaftError::APIError(LinearizableReadError::ForwardToLeader(_)) => {
                    Err(ConsensusError::NotLeader { observed: None })
                }
                _ => Err(ConsensusError::TransientDriver(Box::new(e))),
            };
        }
        Ok(self.state_machine.high_water().await)
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        // Envelope: wrap the driver's HighWaterCommand into HostCommand::Tso
        // so it rides the same log entries as the host's KV writes.
        let cmd = HostCommand::Tso(HighWaterCommand::Advance(AdvancePayload { at_least }));
        match self.raft.client_write(cmd).await {
            Ok(resp) => Ok(resp
                .data
                .tso
                .expect("Tso apply branch always fills HostApplied.tso")),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(ConsensusError::NotLeader { observed: None })
            }
            Err(e) => Err(ConsensusError::TransientDriver(Box::new(e))),
        }
    }

    fn active_write_version(&self) -> u8 {
        // The piggyback host does not own an `ActiveWriteVersion` cell — dense
        // sequences are not implemented here. Return BASELINE so the driver
        // gate always returns `DenseNotActivated` for this host (dense is
        // deferred for piggyback; a real dense piggyback would share a cell).
        tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION
    }

    async fn submit_advance_dense(
        &self,
        _key: &tsoracle_core::SeqKey,
        _count: u32,
    ) -> Result<u64, ConsensusError> {
        // Dense sequences are not implemented in the piggyback example.
        // The driver gate (`active_write_version < DENSE_WRITE_VERSION`)
        // prevents this from ever being called in normal operation.
        Err(ConsensusError::DenseUnsupported)
    }

    async fn submit_advance_dense_batch(
        &self,
        _entries: &[(tsoracle_core::SeqKey, u32)],
    ) -> Result<Vec<u64>, ConsensusError> {
        // Dense sequences are not implemented in the piggyback example.
        // The driver gate (`active_write_version < BATCH_WRITE_VERSION`)
        // prevents this from ever being called in normal operation.
        Err(ConsensusError::DenseUnsupported)
    }

    async fn current_dense_seq(&self, _key: &tsoracle_core::SeqKey) -> Result<u64, ConsensusError> {
        // Dense sequences are not implemented in the piggyback example.
        Err(ConsensusError::DenseUnsupported)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn host_peer_has_no_service_endpoint() {
        use tsoracle_driver_openraft::ServiceEndpoint;
        let peer = super::HostPeer {
            addr: "host-1:7000".to_string(),
        };
        assert_eq!(peer.service_endpoint(), None);
    }
}
