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

use std::collections::{BTreeMap, HashMap};
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
use tsoracle_driver_paxos::AdvancePayload;
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

/// Snapshot payload — carries BOTH halves (KV map and high-water) plus
/// the per-appending-node barrier ledger. The ledger is required so a
/// node that catches up via snapshot transfer still sees its own
/// barriers as "applied" — otherwise `current_high_water` would hang
/// after recovery waiting on a seq it can never observe.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MyAppSnap {
    pub kv: BTreeMap<String, Vec<u8>>,
    pub high_water: u64,
    pub applied_barriers: HashMap<u64, u64>,
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
        for (node, seq) in other.applied_barriers {
            let slot = self.applied_barriers.entry(node).or_insert(0);
            if seq > *slot {
                *slot = seq;
            }
        }
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
        MyAppCommand::HighWater(HighWaterCommand::Advance(AdvancePayload { at_least })) => {
            if *at_least > snap.high_water {
                snap.high_water = *at_least;
            }
        }
        MyAppCommand::HighWater(HighWaterCommand::Barrier { node, seq }) => {
            let slot = snap.applied_barriers.entry(*node).or_insert(0);
            if *seq > *slot {
                *slot = *seq;
            }
        }
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
    applied_barriers: Arc<Mutex<HashMap<u64, u64>>>,
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
            applied_barriers: Arc::new(Mutex::new(HashMap::new())),
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

    /// Latest applied barrier sequence for `node`. Returns 0 if the
    /// pump has never folded a `Barrier { node, .. }` entry.
    pub fn applied_barrier_seq(&self, node: u64) -> u64 {
        self.applied_barriers
            .lock()
            .get(&node)
            .copied()
            .unwrap_or(0)
    }
}

// ---------- Apply pump ----------

/// Apply a single decided [`MyAppCommand`] to `state`.
///
/// Pure with respect to consensus — does not lock any OmniPaxos handle, does
/// not signal the apply notifier. Callers that drain a decided suffix should
/// invoke this per entry and then call [`HostState::apply_notify`].
///
/// Half-isolation: `Kv` variants touch only the KV map; `HighWater` variants
/// touch only the high-water cell (or the barrier ledger). `Advance` is
/// monotonic — high_water moves only forward, never backward. `Barrier`
/// records `(node, seq)` so `current_high_water` can wait for its own
/// barrier to be applied instead of trusting `decided_idx` advancement
/// alone.
pub fn apply_decided_into(cmd: &MyAppCommand, state: &HostState) {
    match cmd {
        MyAppCommand::Kv(KvOp::Put { key, value }) => {
            state.kv.lock().insert(key.clone(), value.clone());
        }
        MyAppCommand::Kv(KvOp::Delete { key }) => {
            state.kv.lock().remove(key);
        }
        MyAppCommand::HighWater(HighWaterCommand::Advance(AdvancePayload { at_least })) => {
            let prev = state.high_water.load(Ordering::SeqCst);
            if *at_least > prev {
                state.high_water.store(*at_least, Ordering::SeqCst);
                trace!(prev, new = at_least, "piggyback high-water advanced");
            }
        }
        MyAppCommand::HighWater(HighWaterCommand::Barrier { node, seq }) => {
            let mut ledger = state.applied_barriers.lock();
            let slot = ledger.entry(*node).or_insert(0);
            if *seq > *slot {
                *slot = *seq;
            }
        }
    }
}

/// Apply a [`MyAppSnap`] (e.g., delivered as a `LogEntry::Snapshotted`) to
/// `state`. Merges the snapshot's KV map into the host KV, lifts
/// `high_water` to `max(prev, snap.high_water)`, and merges the
/// per-node barrier ledger (taking the max per node).
pub fn apply_snapshot_into(snap: &MyAppSnap, state: &HostState) {
    let mut kv = state.kv.lock();
    for (k, v) in &snap.kv {
        kv.insert(k.clone(), v.clone());
    }
    drop(kv);
    let prev = state.high_water.load(Ordering::SeqCst);
    if snap.high_water > prev {
        state.high_water.store(snap.high_water, Ordering::SeqCst);
    }
    let mut ledger = state.applied_barriers.lock();
    for (node, seq) in &snap.applied_barriers {
        let slot = ledger.entry(*node).or_insert(0);
        if *seq > *slot {
            *slot = *seq;
        }
    }
}

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

    // `read_decided_suffix` returns None when decided_idx has advanced past
    // our cursor but the local log hasn't yet received the entries (the
    // Decide message can arrive before the AcceptDecide payload on a lagging
    // follower). Leave cursor in place and retry on the next notify — if we
    // advanced cursor here, the entry would be silently dropped when it
    // eventually arrives. See `read_decided_suffix` → `read` → `get_entry_type`
    // in omnipaxos 0.2.2: get_entry_type returns None for idx >= virtual_log_len.
    let Some(entries) = entries else {
        return *cursor;
    };

    for entry in &entries {
        match entry {
            LogEntry::Decided(cmd) => apply_decided_into(cmd, state),
            LogEntry::Snapshotted(snapshotted) => apply_snapshot_into(&snapshotted.snapshot, state),
            LogEntry::Trimmed(_) | LogEntry::StopSign(_, _) | LogEntry::Undecided(_) => {}
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
    my_node_id: u64,
    barrier_seq: AtomicU64,
}

impl PiggybackHost {
    pub fn new(
        omnipaxos: Arc<Mutex<OmniPaxos<MyAppCommand, MemStorage<MyAppCommand>>>>,
        state: HostState,
        my_node_id: u64,
    ) -> Self {
        // Resume the barrier-nonce counter above any seq this node already
        // used in a prior process lifetime. `barrier_seq` is process-local
        // and resets to 0, but `applied_barriers` is durable (restored from
        // decided-log replay + snapshot transfer). `current_high_water`
        // waits for `applied_barrier_seq(self) >= minted_seq`; minting from 0
        // would hand back a seq a recovered `(self, old_seq)` entry already
        // satisfies, letting the read return before its own barrier is
        // applied. Fold the recovered suffix to learn this node's highest
        // durable seq and lift the counter above it. (This example's
        // MemStorage discards state on restart, so the fold is a no-op here;
        // it mirrors the StandaloneHost fix so the pattern stays correct if
        // ported onto durable storage.) The apply pump re-drains from its
        // own cursor; the fold is idempotent.
        let mut recovery_cursor = 0u64;
        drain_decided_into(&omnipaxos, &mut recovery_cursor, &state);
        let recovered_seq = state.applied_barrier_seq(my_node_id);
        Self {
            omnipaxos,
            state,
            my_node_id,
            barrier_seq: AtomicU64::new(recovered_seq),
        }
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
        // Mint a (my_node_id, seq) nonce and wait for *this specific
        // barrier* to be folded into the state — not merely for
        // `decided_idx` to advance past a pre-append snapshot, which
        // would fire on any unrelated earlier entry and let the read
        // return a stale `high_water` while the barrier is still
        // undecided. This mirrors the StandaloneHost fix.
        let seq = self.barrier_seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.omnipaxos
            .lock()
            .append(MyAppCommand::HighWater(HighWaterCommand::Barrier {
                node: self.my_node_id,
                seq,
            }))
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(PiggybackAppendError(format!("{err:?}"))))
            })?;
        let notify = self.state.apply_notifier();
        loop {
            // Register as a waiter before checking state; apply_notifier
            // uses notify_waiters which does not store permits, so an
            // unregistered check would race against the wake.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.applied_barrier_seq(self.my_node_id) >= seq {
                return Ok(self.state.high_water());
            }
            notified.await;
        }
    }

    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
        let snapshot_decided = self.omnipaxos.lock().get_decided_idx();
        self.omnipaxos
            .lock()
            .append(MyAppCommand::HighWater(HighWaterCommand::Advance(
                AdvancePayload { at_least },
            )))
            .map_err(|err| {
                ConsensusError::TransientDriver(Box::new(PiggybackAppendError(format!("{err:?}"))))
            })?;
        let notify = self.state.apply_notifier();
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let new_decided = self.omnipaxos.lock().get_decided_idx();
            if new_decided > snapshot_decided && self.state.high_water() >= at_least {
                return Ok(self.state.high_water());
            }
            notified.await;
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

    fn put(key: &str, value: &[u8]) -> MyAppCommand {
        MyAppCommand::Kv(KvOp::Put {
            key: key.into(),
            value: value.to_vec(),
        })
    }

    fn delete(key: &str) -> MyAppCommand {
        MyAppCommand::Kv(KvOp::Delete { key: key.into() })
    }

    fn advance(at_least: u64) -> MyAppCommand {
        MyAppCommand::HighWater(HighWaterCommand::Advance(AdvancePayload { at_least }))
    }

    fn barrier(node: u64, seq: u64) -> MyAppCommand {
        MyAppCommand::HighWater(HighWaterCommand::Barrier { node, seq })
    }

    // ---------- Envelope ----------

    #[test]
    fn from_highwatercommand_wraps_into_envelope() {
        let wrapped: MyAppCommand =
            HighWaterCommand::Advance(AdvancePayload { at_least: 5 }).into();
        match wrapped {
            MyAppCommand::HighWater(HighWaterCommand::Advance(AdvancePayload { at_least })) => {
                assert_eq!(at_least, 5);
            }
            other => panic!("expected envelope wrap, got {other:?}"),
        }
    }

    // ---------- Snapshot::create fold ----------

    #[test]
    fn snapshot_create_folds_kv_and_high_water() {
        let entries = vec![
            put("a", b"1"),
            advance(10),
            put("b", b"2"),
            advance(30),
            barrier(1, 1),
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
    fn snapshot_create_take_highest_of_repeated_advances() {
        let snap = MyAppSnap::create(&[advance(5), advance(20), advance(12), advance(7)]);
        assert_eq!(snap.high_water, 20);
        assert!(snap.kv.is_empty());
    }

    #[test]
    fn snapshot_create_delete_removes_prior_put() {
        let snap = MyAppSnap::create(&[put("a", b"1"), put("b", b"2"), delete("a")]);
        assert!(!snap.kv.contains_key("a"));
        assert_eq!(
            snap.kv.get("b").map(|v| v.as_slice()),
            Some(b"2".as_slice())
        );
    }

    #[test]
    fn snapshot_create_kv_only_leaves_high_water_zero() {
        let snap = MyAppSnap::create(&[put("a", b"1"), put("b", b"2")]);
        assert_eq!(snap.high_water, 0, "KV writes must not touch the TSO field");
    }

    #[test]
    fn snapshot_create_high_water_only_leaves_kv_empty() {
        let snap = MyAppSnap::create(&[advance(10), advance(20), barrier(1, 1)]);
        assert!(snap.kv.is_empty(), "TSO commands must not touch the KV map");
        assert_eq!(snap.high_water, 20);
    }

    #[test]
    fn snapshot_create_barrier_is_no_op() {
        let snap = MyAppSnap::create(&[barrier(1, 1), barrier(1, 1)]);
        assert!(snap.kv.is_empty());
        assert_eq!(snap.high_water, 0);
    }

    // ---------- Snapshot::merge ----------

    #[test]
    fn snapshot_merge_takes_max_high_water() {
        let mut left = MyAppSnap {
            high_water: 100,
            ..Default::default()
        };
        let right = MyAppSnap {
            high_water: 50,
            ..Default::default()
        };
        left.merge(right);
        assert_eq!(
            left.high_water, 100,
            "merge must not move high_water backward"
        );

        let mut left = MyAppSnap {
            high_water: 50,
            ..Default::default()
        };
        let right = MyAppSnap {
            high_water: 100,
            ..Default::default()
        };
        left.merge(right);
        assert_eq!(left.high_water, 100, "merge takes the higher of the two");
    }

    #[test]
    fn snapshot_merge_overwrites_kv_on_conflict() {
        let mut left = MyAppSnap {
            kv: BTreeMap::from([("a".into(), b"left".to_vec())]),
            ..Default::default()
        };
        let right = MyAppSnap {
            kv: BTreeMap::from([
                ("a".into(), b"right".to_vec()),
                ("b".into(), b"only-right".to_vec()),
            ]),
            ..Default::default()
        };
        left.merge(right);
        assert_eq!(
            left.kv.get("a").map(|v| v.as_slice()),
            Some(b"right".as_slice())
        );
        assert_eq!(
            left.kv.get("b").map(|v| v.as_slice()),
            Some(b"only-right".as_slice())
        );
    }

    // ---------- apply_decided_into ----------

    #[test]
    fn apply_decided_kv_put_inserts_into_state() {
        let state = HostState::new();
        apply_decided_into(&put("alpha", b"v1"), &state);
        assert_eq!(
            state.kv_dump().get("alpha").map(|v| v.as_slice()),
            Some(b"v1".as_slice())
        );
    }

    #[test]
    fn apply_decided_kv_put_overwrites_existing_value() {
        let state = HostState::new();
        apply_decided_into(&put("k", b"first"), &state);
        apply_decided_into(&put("k", b"second"), &state);
        assert_eq!(
            state.kv_dump().get("k").map(|v| v.as_slice()),
            Some(b"second".as_slice())
        );
    }

    #[test]
    fn apply_decided_kv_delete_removes_existing_key() {
        let state = HostState::new();
        apply_decided_into(&put("k", b"v"), &state);
        apply_decided_into(&delete("k"), &state);
        assert!(!state.kv_dump().contains_key("k"));
    }

    #[test]
    fn apply_decided_kv_delete_on_missing_key_is_no_op() {
        let state = HostState::new();
        apply_decided_into(&delete("missing"), &state);
        assert!(state.kv_dump().is_empty());
    }

    #[test]
    fn apply_decided_advance_advances_high_water() {
        let state = HostState::new();
        apply_decided_into(&advance(42), &state);
        assert_eq!(state.high_water(), 42);
    }

    #[test]
    fn apply_decided_advance_is_monotonic() {
        let state = HostState::new();
        apply_decided_into(&advance(100), &state);
        apply_decided_into(&advance(50), &state);
        assert_eq!(
            state.high_water(),
            100,
            "advance must not move high_water backward"
        );
        apply_decided_into(&advance(150), &state);
        assert_eq!(
            state.high_water(),
            150,
            "advance must move high_water forward"
        );
    }

    #[test]
    fn apply_decided_barrier_leaves_kv_and_high_water_unchanged() {
        let state = HostState::new();
        apply_decided_into(&put("k", b"v"), &state);
        apply_decided_into(&advance(99), &state);
        let kv_before = state.kv_dump();
        let hw_before = state.high_water();
        apply_decided_into(&barrier(1, 1), &state);
        assert_eq!(state.kv_dump(), kv_before);
        assert_eq!(state.high_water(), hw_before);
    }

    #[test]
    fn apply_decided_barrier_records_latest_seq_per_node() {
        let state = HostState::new();
        apply_decided_into(&barrier(1, 5), &state);
        apply_decided_into(&barrier(1, 7), &state);
        apply_decided_into(&barrier(2, 3), &state);
        assert_eq!(state.applied_barrier_seq(1), 7);
        assert_eq!(state.applied_barrier_seq(2), 3);
        assert_eq!(state.applied_barrier_seq(99), 0);
    }

    #[test]
    fn apply_decided_barrier_seq_is_monotonic_per_node() {
        let state = HostState::new();
        apply_decided_into(&barrier(1, 10), &state);
        apply_decided_into(&barrier(1, 5), &state);
        assert_eq!(
            state.applied_barrier_seq(1),
            10,
            "an out-of-order replay must not regress the per-node seq",
        );
    }

    // ---------- Half-isolation (the piggyback invariant) ----------

    #[test]
    fn kv_writes_leave_high_water_untouched() {
        let state = HostState::new();
        apply_decided_into(&advance(500), &state);
        let hw_before = state.high_water();
        apply_decided_into(&put("a", b"1"), &state);
        apply_decided_into(&put("b", b"2"), &state);
        apply_decided_into(&delete("a"), &state);
        assert_eq!(
            state.high_water(),
            hw_before,
            "Kv ops must not touch high_water"
        );
    }

    #[test]
    fn high_water_writes_leave_kv_untouched() {
        let state = HostState::new();
        apply_decided_into(&put("a", b"original"), &state);
        let kv_before = state.kv_dump();
        apply_decided_into(&advance(10), &state);
        apply_decided_into(&advance(20), &state);
        apply_decided_into(&barrier(1, 1), &state);
        assert_eq!(
            state.kv_dump(),
            kv_before,
            "HighWater ops must not touch kv"
        );
    }

    // ---------- apply_snapshot_into ----------

    #[test]
    fn apply_snapshot_hydrates_empty_state() {
        let state = HostState::new();
        let snap = MyAppSnap {
            kv: BTreeMap::from([("a".into(), b"1".to_vec()), ("b".into(), b"2".to_vec())]),
            high_water: 99,
            ..Default::default()
        };
        apply_snapshot_into(&snap, &state);
        assert_eq!(state.high_water(), 99);
        let kv = state.kv_dump();
        assert_eq!(kv.len(), 2);
        assert_eq!(kv.get("a").map(|v| v.as_slice()), Some(b"1".as_slice()));
    }

    #[test]
    fn apply_snapshot_merges_into_populated_state() {
        let state = HostState::new();
        apply_decided_into(&put("only-state", b"s"), &state);
        apply_decided_into(&advance(50), &state);

        let snap = MyAppSnap {
            kv: BTreeMap::from([("only-snap".into(), b"x".to_vec())]),
            high_water: 30, // lower than existing 50
            ..Default::default()
        };
        apply_snapshot_into(&snap, &state);

        let kv = state.kv_dump();
        assert!(kv.contains_key("only-state"), "existing keys preserved");
        assert!(kv.contains_key("only-snap"), "snapshot keys merged in");
        assert_eq!(
            state.high_water(),
            50,
            "snapshot must not move high_water backward"
        );
    }

    #[test]
    fn apply_snapshot_advances_high_water_when_higher() {
        let state = HostState::new();
        apply_decided_into(&advance(10), &state);
        apply_snapshot_into(
            &MyAppSnap {
                high_water: 100,
                ..Default::default()
            },
            &state,
        );
        assert_eq!(state.high_water(), 100);
    }

    #[test]
    fn apply_snapshot_merges_barrier_ledger() {
        let state = HostState::new();
        apply_decided_into(&barrier(1, 3), &state);
        apply_snapshot_into(
            &MyAppSnap {
                applied_barriers: HashMap::from([(1, 5), (2, 7)]),
                ..Default::default()
            },
            &state,
        );
        assert_eq!(state.applied_barrier_seq(1), 5, "snapshot lifts seq");
        assert_eq!(state.applied_barrier_seq(2), 7, "snapshot introduces nodes");
    }

    #[test]
    fn apply_snapshot_does_not_regress_barrier_ledger() {
        let state = HostState::new();
        apply_decided_into(&barrier(1, 10), &state);
        apply_snapshot_into(
            &MyAppSnap {
                applied_barriers: HashMap::from([(1, 5)]),
                ..Default::default()
            },
            &state,
        );
        assert_eq!(
            state.applied_barrier_seq(1),
            10,
            "merge must not move seq backward",
        );
    }
}
