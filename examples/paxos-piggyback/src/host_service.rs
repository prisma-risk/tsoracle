//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! The "existing" host service — a tiny in-memory KV — with TSO piggybacked
//! onto the same OmniPaxos log.
//!
//! Envelope shape: `MyAppCommand::{Kv(KvOp), HighWater(HighWaterCommand)}`.
//! Apply path enforces TSO monotonicity (`max(prev, target)`) inside the
//! host's apply pump, sitting next to the KV map. The snapshot type carries
//! both halves.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use omnipaxos::util::LogEntry;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::trace;
use tsoracle_consensus::ConsensusError;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_driver_paxos::host::PaxosHighWaterHost;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

// ---------- Envelope shape ----------

/// Per-key operation on the host's KV store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvOp {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

/// The host's OmniPaxos entry: envelope over its own commands plus TSO.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MyAppCommand {
    Kv(KvOp),
    HighWater(HighWaterCommand),
}

impl From<HighWaterCommand> for MyAppCommand {
    fn from(cmd: HighWaterCommand) -> Self {
        MyAppCommand::HighWater(cmd)
    }
}

/// Snapshot payload — carries BOTH halves (KV map and high-water).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MyAppSnap {
    pub kv: BTreeMap<String, Vec<u8>>,
    pub high_water: u64,
}

impl omnipaxos::storage::Entry for MyAppCommand {
    type Snapshot = MyAppSnap;
}

impl omnipaxos::storage::Snapshot<MyAppCommand> for MyAppSnap {
    fn create(entries: &[MyAppCommand]) -> Self {
        let mut snap = MyAppSnap::default();
        for entry in entries {
            apply_into_snapshot(entry, &mut snap);
        }
        snap
    }

    fn merge(&mut self, other: Self) {
        for (key, value) in other.kv {
            self.kv.insert(key, value);
        }
        self.high_water = self.high_water.max(other.high_water);
    }

    fn use_snapshots() -> bool {
        true
    }
}

fn apply_into_snapshot(entry: &MyAppCommand, snap: &mut MyAppSnap) {
    match entry {
        MyAppCommand::Kv(KvOp::Put { key, value }) => {
            snap.kv.insert(key.clone(), value.clone());
        }
        MyAppCommand::Kv(KvOp::Delete { key }) => {
            snap.kv.remove(key);
        }
        MyAppCommand::HighWater(HighWaterCommand::Advance { at_least }) => {
            if *at_least > snap.high_water {
                snap.high_water = *at_least;
            }
        }
        MyAppCommand::HighWater(HighWaterCommand::Barrier) => {}
    }
}

// ---------- Host state ----------

/// Per-node host state, shared between the apply pump and the
/// [`PiggybackHost`].
///
/// Cheap to clone (every field is `Arc`-wrapped).
#[derive(Clone)]
pub struct HostState {
    kv: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    high_water: Arc<AtomicU64>,
    apply_notify: Arc<Notify>,
}

impl Default for HostState {
    fn default() -> Self {
        Self::new()
    }
}

impl HostState {
    pub fn new() -> Self {
        Self {
            kv: Arc::new(Mutex::new(BTreeMap::new())),
            high_water: Arc::new(AtomicU64::new(0)),
            apply_notify: Arc::new(Notify::new()),
        }
    }

    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::SeqCst)
    }

    pub fn kv_dump(&self) -> BTreeMap<String, Vec<u8>> {
        self.kv.lock().clone()
    }

    /// Cloned `Arc<Notify>` used by readers (`current_high_water`,
    /// `submit_advance`) to wait on the apply pump's next drain.
    pub fn apply_notifier(&self) -> Arc<Notify> {
        self.apply_notify.clone()
    }
}

// ---------- Apply pump ----------

/// Drain decided `MyAppCommand` entries from `omnipaxos` starting at
/// `*cursor`, fold them into `state`, advance `*cursor`, and wake any
/// pollers parked on `state.apply_notifier()`.
///
/// Designed to be called from a per-node apply pump task awoken by the
/// toolkit runner's `apply_notify` after each tick.
pub fn drain_decided_into(
    omnipaxos: &Arc<Mutex<OmniPaxos<MyAppCommand, MemStorage<MyAppCommand>>>>,
    cursor: &mut u64,
    state: &HostState,
) -> u64 {
    let (decided_idx, entries) = {
        let handle = omnipaxos.lock();
        let decided_idx = handle.get_decided_idx();
        if decided_idx <= *cursor {
            return decided_idx;
        }
        let entries = handle.read_decided_suffix(*cursor);
        (decided_idx, entries)
    };

    if let Some(entries) = entries {
        for entry in &entries {
            match entry {
                LogEntry::Decided(MyAppCommand::Kv(KvOp::Put { key, value })) => {
                    state.kv.lock().insert(key.clone(), value.clone());
                }
                LogEntry::Decided(MyAppCommand::Kv(KvOp::Delete { key })) => {
                    state.kv.lock().remove(key);
                }
                LogEntry::Decided(MyAppCommand::HighWater(HighWaterCommand::Advance {
                    at_least,
                })) => {
                    let prev = state.high_water.load(Ordering::SeqCst);
                    if *at_least > prev {
                        state.high_water.store(*at_least, Ordering::SeqCst);
                        trace!(prev, new = at_least, "piggyback high-water advanced");
                    }
                }
                LogEntry::Decided(MyAppCommand::HighWater(HighWaterCommand::Barrier)) => {}
                LogEntry::Snapshotted(snapshotted) => {
                    let snap = &snapshotted.snapshot;
                    let mut kv = state.kv.lock();
                    for (k, v) in &snap.kv {
                        kv.insert(k.clone(), v.clone());
                    }
                    drop(kv);
                    let prev = state.high_water.load(Ordering::SeqCst);
                    if snap.high_water > prev {
                        state.high_water.store(snap.high_water, Ordering::SeqCst);
                    }
                }
                LogEntry::Trimmed(_) | LogEntry::StopSign(_, _) | LogEntry::Undecided(_) => {}
            }
        }
    }

    *cursor = decided_idx;
    state.apply_notify.notify_waiters();
    decided_idx
}

// ---------- PaxosHighWaterHost impl ----------

/// The integration. Owns a clone of the host's OmniPaxos handle and its
/// state; appends `HighWater(...)` envelope variants into the shared log
/// for `current_high_water` / `submit_advance`.
pub struct PiggybackHost {
    omnipaxos: Arc<Mutex<OmniPaxos<MyAppCommand, MemStorage<MyAppCommand>>>>,
    state: HostState,
}

impl PiggybackHost {
    pub fn new(
        omnipaxos: Arc<Mutex<OmniPaxos<MyAppCommand, MemStorage<MyAppCommand>>>>,
        state: HostState,
    ) -> Self {
        Self { omnipaxos, state }
    }
}

#[async_trait]
impl PaxosHighWaterHost for PiggybackHost {
    type Entry = MyAppCommand;
    type Storage = MemStorage<MyAppCommand>;

    fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<MyAppCommand, MemStorage<MyAppCommand>>>> {
        self.omnipaxos.clone()
    }

    async fn current_high_water(&self) -> Result<u64, ConsensusError> {
        let snapshot_decided = self.omnipaxos.lock().get_decided_idx();
        self.omnipaxos
            .lock()
            .append(MyAppCommand::HighWater(HighWaterCommand::Barrier))
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(PiggybackAppendError(format!("{err:?}"))))
            })?;
        let notify = self.state.apply_notifier();
        loop {
            notify.notified().await;
            let new_decided = self.omnipaxos.lock().get_decided_idx();
            if new_decided > snapshot_decided {
                return Ok(self.state.high_water());
            }
        }
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        let snapshot_decided = self.omnipaxos.lock().get_decided_idx();
        self.omnipaxos
            .lock()
            .append(MyAppCommand::HighWater(HighWaterCommand::Advance {
                at_least,
            }))
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(PiggybackAppendError(format!("{err:?}"))))
            })?;
        let notify = self.state.apply_notifier();
        loop {
            notify.notified().await;
            let new_decided = self.omnipaxos.lock().get_decided_idx();
            if new_decided > snapshot_decided && self.state.high_water() >= at_least {
                return Ok(self.state.high_water());
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("piggyback append failed: {0}")]
struct PiggybackAppendError(String);

#[cfg(test)]
mod tests {
    use super::*;
    use omnipaxos::storage::Snapshot;

    #[test]
    fn snapshot_create_folds_kv_and_high_water() {
        let entries = vec![
            MyAppCommand::Kv(KvOp::Put {
                key: "a".into(),
                value: b"1".to_vec(),
            }),
            MyAppCommand::HighWater(HighWaterCommand::Advance { at_least: 10 }),
            MyAppCommand::Kv(KvOp::Put {
                key: "b".into(),
                value: b"2".to_vec(),
            }),
            MyAppCommand::HighWater(HighWaterCommand::Advance { at_least: 30 }),
            MyAppCommand::HighWater(HighWaterCommand::Barrier),
        ];
        let snap = MyAppSnap::create(&entries);
        assert_eq!(snap.high_water, 30);
        assert_eq!(snap.kv.len(), 2);
        assert_eq!(
            snap.kv.get("a").map(|v| v.as_slice()),
            Some(b"1".as_slice())
        );
        assert_eq!(
            snap.kv.get("b").map(|v| v.as_slice()),
            Some(b"2".as_slice())
        );
    }

    #[test]
    fn from_highwatercommand_wraps_into_envelope() {
        let wrapped: MyAppCommand = HighWaterCommand::Advance { at_least: 5 }.into();
        match wrapped {
            MyAppCommand::HighWater(HighWaterCommand::Advance { at_least }) => {
                assert_eq!(at_least, 5);
            }
            other => panic!("expected envelope wrap, got {other:?}"),
        }
    }
}
