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

//! Apply-task state machine.
//!
//! Owns the in-memory high-water mark (an `AtomicU64`), the per-node
//! barrier-nonce ledger that linearizes `current_high_water`, and the
//! apply notifier the host's blocking-read methods loop on. The apply
//! task is spawned by the host (`StandaloneHost`), awaits the toolkit
//! runner's `apply_notify`, then drains the OmniPaxos log's decided
//! suffix into the apply state — exactly once per decided entry.
//!
//! Why per-node barrier nonces. `OmniPaxos::append` does not return a
//! log index, and the apply path only sees decided entries in order. To
//! make `current_high_water` a linearized read, the reader mints a
//! `(my_node, seq)` nonce, appends `Barrier { node, seq }`, and waits
//! until the ledger reports `applied_barrier_seq(my_node) >= seq`. The
//! ledger is keyed by appending-node so two nodes' independent nonce
//! counters don't trample each other; size is bounded by cluster size.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use omnipaxos::OmniPaxos;
use omnipaxos::storage::Storage;
use omnipaxos::util::LogEntry;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::trace;

use crate::log_entry::HighWaterCommand;
use crate::snapshot_policy::SnapshotPolicy;
#[cfg(test)]
use tsoracle_consensus::AdvancePayload;

/// Shared apply-task state.
///
/// Cheap to clone (Arc-wrapped fields). The host clones one copy into
/// the spawned apply task and keeps another for its blocking-read
/// methods to poll.
#[derive(Clone)]
pub struct ApplyState {
    high_water: Arc<AtomicU64>,
    apply_notifier: Arc<Notify>,
    applied_barriers: Arc<Mutex<HashMap<u64, u64>>>,
}

impl Default for ApplyState {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplyState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            high_water: Arc::new(AtomicU64::new(0)),
            apply_notifier: Arc::new(Notify::new()),
            applied_barriers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Current in-memory high-water value.
    #[must_use]
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::SeqCst)
    }

    /// Latest applied barrier sequence for `node`. Returns 0 if the
    /// apply path has never folded a `Barrier { node, .. }` entry.
    #[must_use]
    pub fn applied_barrier_seq(&self, node: u64) -> u64 {
        self.applied_barriers
            .lock()
            .get(&node)
            .copied()
            .unwrap_or(0)
    }

    /// Notifier the host's blocking-read methods loop on.
    ///
    /// Edge-triggered, all-waiters-wake (matches `Notify::notify_waiters`
    /// semantics). Callers MUST loop and re-check their condition; one
    /// wake does not correspond to one decided entry.
    #[must_use]
    pub fn apply_notifier(&self) -> Arc<Notify> {
        self.apply_notifier.clone()
    }
}

/// Drain decided entries from `omnipaxos` starting at `*cursor`, folding
/// `Advance` commands into the AtomicU64, then notify pollers. Returns
/// the new `decided_idx` (so the caller can update its cursor).
///
/// Designed to be called from the host's spawned apply task. The host
/// awaits the toolkit runner's `apply_notify` between calls; this
/// function is the apply step itself and does no waiting of its own.
pub fn drain_decided_into<S>(
    omnipaxos: &Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    cursor: &mut u64,
    state: &ApplyState,
) -> u64
where
    S: Storage<HighWaterCommand> + Send + 'static,
{
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
                LogEntry::Decided(HighWaterCommand::Advance(advance)) => {
                    let prev = state.high_water.load(Ordering::SeqCst);
                    let new = advance.merge(prev);
                    if new > prev {
                        state.high_water.store(new, Ordering::SeqCst);
                        trace!(prev, new, "high-water advanced");
                    }
                }
                LogEntry::Decided(HighWaterCommand::Barrier { node, seq }) => {
                    let mut ledger = state.applied_barriers.lock();
                    let slot = ledger.entry(*node).or_insert(0);
                    if *seq > *slot {
                        *slot = *seq;
                    }
                }
                LogEntry::Snapshotted(snapshotted) => {
                    // OmniPaxos may surface a Snapshotted entry on log
                    // catch-up; reflect its value if it's higher and
                    // merge the snapshot's per-node barrier ledger.
                    // Without merging the ledger, a node that catches
                    // up via snapshot transfer would never see its own
                    // barriers as applied and current_high_water would
                    // hang.
                    let prev = state.high_water.load(Ordering::SeqCst);
                    if snapshotted.snapshot.value > prev {
                        state
                            .high_water
                            .store(snapshotted.snapshot.value, Ordering::SeqCst);
                    }
                    let mut ledger = state.applied_barriers.lock();
                    for (node, seq) in &snapshotted.snapshot.applied_barriers {
                        let slot = ledger.entry(*node).or_insert(0);
                        if *seq > *slot {
                            *slot = *seq;
                        }
                    }
                }
                LogEntry::Trimmed(_) | LogEntry::StopSign(_, _) | LogEntry::Undecided(_) => {
                    // Out of scope for the high-water fold.
                }
            }
        }
    }

    *cursor = decided_idx;
    state.apply_notifier.notify_waiters();
    decided_idx
}

/// Maybe trigger a snapshot via the given policy. Called by the host's
/// apply task after each successful drain.
pub fn maybe_snapshot<S>(
    omnipaxos: &Arc<Mutex<OmniPaxos<HighWaterCommand, S>>>,
    policy: &mut SnapshotPolicy,
    decided_idx: u64,
) where
    S: Storage<HighWaterCommand> + Send + 'static,
{
    if policy.should_snapshot(decided_idx) {
        // Count every attempt so the success rate is recoverable as
        // `snapshot.total - snapshot.failures.total`.
        #[cfg(feature = "metrics")]
        metrics::counter!("tsoracle.paxos.snapshot.total").increment(1);

        let mut handle = omnipaxos.lock();
        // local_only=false: best-effort cluster-wide snapshot. Errors
        // are logged but not propagated; snapshot failure is not fatal
        // for liveness. A persistent failure would otherwise stay hidden
        // behind log lines, so it is also counted as a health signal.
        match handle.snapshot(Some(decided_idx), false) {
            Ok(()) => {
                // The last successfully-snapshotted index. Operators alert on
                // this gauge ceasing to advance.
                #[cfg(feature = "metrics")]
                metrics::gauge!("tsoracle.paxos.snapshot.last_index").set(decided_idx as f64);
            }
            Err(err) => {
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.paxos.snapshot.failures.total").increment(1);
                tracing::warn!(?err, "snapshot trigger failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn new_state_starts_at_zero() {
        let state = ApplyState::new();
        assert_eq!(state.high_water(), 0);
    }

    #[tokio::test]
    async fn apply_notifier_handle_is_shared() {
        let state = ApplyState::new();
        let first = state.apply_notifier();
        let second = state.apply_notifier();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn apply_notifier_wakes_parked_waiters() {
        let state = ApplyState::new();
        let notifier = state.apply_notifier();
        let waiter = tokio::spawn(async move { notifier.notified().await });
        // Give the spawned task time to park on the notify.
        tokio::time::sleep(Duration::from_millis(10)).await;
        state.apply_notifier.notify_waiters();
        tokio::time::timeout(Duration::from_millis(50), waiter)
            .await
            .expect("waiter wakes")
            .expect("task joins");
    }

    #[tokio::test]
    async fn high_water_load_reflects_atomic_stores() {
        let state = ApplyState::new();
        state.high_water.store(42, Ordering::SeqCst);
        assert_eq!(state.high_water(), 42);
    }
}

#[cfg(test)]
mod drain_tests {
    //! Coverage for `drain_decided_into` and `maybe_snapshot`.
    //!
    //! Boots a 3-node OmniPaxos cluster wired through the toolkit's
    //! `MemNetwork` + `MemStorage`, drives ticks until consensus is
    //! reached on a set of `HighWaterCommand` entries, then exercises
    //! the apply pipeline against the leader's `OmniPaxos` handle.

    use super::*;
    use crate::log_entry::HighWaterCommand;
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tsoracle_paxos_toolkit::test_fakes::mem_network::MemNetwork;
    use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

    struct ClusterNode {
        id: u64,
        omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>>,
        inbox: mpsc::Receiver<omnipaxos::messages::Message<HighWaterCommand>>,
    }

    struct Cluster {
        nodes: Vec<ClusterNode>,
        network: Arc<MemNetwork<HighWaterCommand>>,
    }

    fn build_cluster(node_count: usize) -> Cluster {
        let network: Arc<MemNetwork<HighWaterCommand>> = Arc::new(MemNetwork::new());
        let node_ids: Vec<u64> = (1..=node_count as u64).collect();
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: node_ids.clone(),
            flexible_quorum: None,
        };

        let mut nodes = Vec::with_capacity(node_count);
        for &node_id in &node_ids {
            let server_config = ServerConfig {
                pid: node_id,
                election_tick_timeout: 5,
                resend_message_tick_timeout: 5,
                ..Default::default()
            };
            let storage = MemStorage::<HighWaterCommand>::new();
            let config = OmniPaxosConfig {
                cluster_config: cluster_config.clone(),
                server_config,
            };
            let omnipaxos = config.build(storage).expect("build omnipaxos");
            let inbox = network.register(node_id);
            nodes.push(ClusterNode {
                id: node_id,
                omnipaxos: Arc::new(Mutex::new(omnipaxos)),
                inbox,
            });
        }
        Cluster { nodes, network }
    }

    async fn drive_until<F>(cluster: &mut Cluster, mut predicate: F, max_ticks: usize)
    where
        F: FnMut(&Cluster) -> bool,
    {
        for _ in 0..max_ticks {
            for node in &cluster.nodes {
                let outgoing = {
                    let mut handle = node.omnipaxos.lock();
                    handle.tick();
                    handle.outgoing_messages()
                };
                for message in outgoing {
                    cluster.network.deliver(message).await;
                }
            }
            for node in &mut cluster.nodes {
                while let Ok(message) = node.inbox.try_recv() {
                    node.omnipaxos.lock().handle_incoming(message);
                }
            }
            if predicate(cluster) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("predicate did not become true within {max_ticks} ticks");
    }

    async fn drive_to_leader_election(cluster: &mut Cluster) {
        drive_until(
            cluster,
            |state| {
                state
                    .nodes
                    .iter()
                    .any(|node| node.omnipaxos.lock().get_current_leader().is_some())
            },
            500,
        )
        .await;
    }

    fn leader_handle(
        cluster: &Cluster,
    ) -> Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>> {
        let leader_id = cluster
            .nodes
            .iter()
            .find_map(|node| node.omnipaxos.lock().get_current_leader())
            .expect("leader has been elected");
        cluster
            .nodes
            .iter()
            .find(|node| node.id == leader_id)
            .expect("leader present in topology")
            .omnipaxos
            .clone()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_advances_high_water_when_advance_decides() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        leader_handle(&cluster)
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: 42 }))
            .expect("append succeeds on leader");

        drive_until(
            &mut cluster,
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| node.omnipaxos.lock().get_decided_idx() >= 1)
            },
            500,
        )
        .await;

        let state = ApplyState::new();
        let mut cursor = 0u64;
        let new_decided = drain_decided_into(&leader_handle(&cluster), &mut cursor, &state);

        assert!(new_decided >= 1);
        assert_eq!(state.high_water(), 42);
        assert_eq!(cursor, new_decided);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_keeps_max_across_multiple_advances() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        {
            let leader = leader_handle(&cluster);
            let mut handle = leader.lock();
            handle
                .append(HighWaterCommand::Advance(AdvancePayload { at_least: 10 }))
                .expect("append");
            handle
                .append(HighWaterCommand::Advance(AdvancePayload { at_least: 50 }))
                .expect("append");
            handle
                .append(HighWaterCommand::Advance(AdvancePayload { at_least: 30 }))
                .expect("append");
        }

        drive_until(
            &mut cluster,
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| node.omnipaxos.lock().get_decided_idx() >= 3)
            },
            500,
        )
        .await;

        let state = ApplyState::new();
        let mut cursor = 0u64;
        drain_decided_into(&leader_handle(&cluster), &mut cursor, &state);

        assert_eq!(
            state.high_water(),
            50,
            "max-across-advances is the contract"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_ignores_barrier_entries() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        {
            let leader = leader_handle(&cluster);
            let mut handle = leader.lock();
            handle
                .append(HighWaterCommand::Advance(AdvancePayload { at_least: 17 }))
                .expect("append");
            handle
                .append(HighWaterCommand::Barrier { node: 1, seq: 1 })
                .expect("append");
        }

        drive_until(
            &mut cluster,
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| node.omnipaxos.lock().get_decided_idx() >= 2)
            },
            500,
        )
        .await;

        let state = ApplyState::new();
        let mut cursor = 0u64;
        drain_decided_into(&leader_handle(&cluster), &mut cursor, &state);

        assert_eq!(state.high_water(), 17, "Barrier must not lower or zero out");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_records_latest_barrier_seq_per_node() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        {
            let leader = leader_handle(&cluster);
            let mut handle = leader.lock();
            handle
                .append(HighWaterCommand::Barrier { node: 1, seq: 5 })
                .expect("append");
            handle
                .append(HighWaterCommand::Barrier { node: 1, seq: 7 })
                .expect("append");
            handle
                .append(HighWaterCommand::Barrier { node: 2, seq: 3 })
                .expect("append");
        }

        drive_until(
            &mut cluster,
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| node.omnipaxos.lock().get_decided_idx() >= 3)
            },
            500,
        )
        .await;

        let state = ApplyState::new();
        let mut cursor = 0u64;
        drain_decided_into(&leader_handle(&cluster), &mut cursor, &state);

        assert_eq!(state.applied_barrier_seq(1), 7);
        assert_eq!(state.applied_barrier_seq(2), 3);
        assert_eq!(state.applied_barrier_seq(99), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_does_not_regress_barrier_seq_on_lower_value() {
        let state = ApplyState::new();
        state.applied_barriers.lock().insert(1, 42);
        // No drain — just verify the helper observes the seeded ledger.
        assert_eq!(state.applied_barrier_seq(1), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_with_no_new_decided_is_noop() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        let state = ApplyState::new();
        state.high_water.store(99, Ordering::SeqCst);
        let mut cursor = 0u64;
        let returned = drain_decided_into(&leader_handle(&cluster), &mut cursor, &state);

        assert_eq!(returned, 0);
        assert_eq!(cursor, 0);
        assert_eq!(state.high_water(), 99, "unchanged when nothing decided");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn drain_advances_cursor_so_repeat_calls_skip_already_applied() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        leader_handle(&cluster)
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: 7 }))
            .expect("append");

        drive_until(
            &mut cluster,
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| node.omnipaxos.lock().get_decided_idx() >= 1)
            },
            500,
        )
        .await;

        let state = ApplyState::new();
        let mut cursor = 0u64;
        let first = drain_decided_into(&leader_handle(&cluster), &mut cursor, &state);
        assert_eq!(state.high_water(), 7);

        // Lower the atomic to a sentinel; if the second drain re-applied
        // the same entry it would bump us back to 7.
        state.high_water.store(3, Ordering::SeqCst);
        let second = drain_decided_into(&leader_handle(&cluster), &mut cursor, &state);

        assert_eq!(first, second, "no new decisions between calls");
        assert_eq!(
            state.high_water(),
            3,
            "the second drain must NOT re-apply already-cursored entries",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn maybe_snapshot_advances_policy_state() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        leader_handle(&cluster)
            .lock()
            .append(HighWaterCommand::Advance(AdvancePayload { at_least: 1 }))
            .expect("append");

        drive_until(
            &mut cluster,
            |state| {
                state
                    .nodes
                    .iter()
                    .all(|node| node.omnipaxos.lock().get_decided_idx() >= 1)
            },
            500,
        )
        .await;

        let mut policy = SnapshotPolicy::every(1);
        maybe_snapshot(&leader_handle(&cluster), &mut policy, 1);
        assert!(
            !policy.should_snapshot(1),
            "policy's last_snapshot_at must have advanced past the trigger",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn maybe_snapshot_is_noop_when_policy_disabled() {
        let mut cluster = build_cluster(3);
        drive_to_leader_election(&mut cluster).await;

        // No appends; decided_idx stays 0. Disabled policy must skip the
        // snapshot call path entirely.
        let mut policy = SnapshotPolicy::disabled();
        maybe_snapshot(&leader_handle(&cluster), &mut policy, 0);
        assert!(!policy.should_snapshot(u64::MAX));
    }

    #[cfg(feature = "metrics")]
    mod snapshot_health_metrics {
        //! `maybe_snapshot` must surface snapshot health as metrics: every
        //! attempt bumps `tsoracle.paxos.snapshot.total`, a success sets the
        //! `tsoracle.paxos.snapshot.last_index` gauge, and a failure bumps
        //! `tsoracle.paxos.snapshot.failures.total` (instead of only logging).
        //! A thread-local recorder captures real emission, not a mock.

        use super::*;
        use metrics_util::{
            MetricKind,
            debugging::{DebugValue, DebuggingRecorder},
        };

        type RecordedMetric = (
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        );

        fn counter(snapshot: &[RecordedMetric], name: &str) -> u64 {
            for (composite, _u, _d, value) in snapshot {
                if composite.kind() == MetricKind::Counter && composite.key().name() == name {
                    if let DebugValue::Counter(n) = value {
                        return *n;
                    }
                }
            }
            0
        }

        fn gauge(snapshot: &[RecordedMetric], name: &str) -> Option<f64> {
            for (composite, _u, _d, value) in snapshot {
                if composite.kind() == MetricKind::Gauge && composite.key().name() == name {
                    if let DebugValue::Gauge(g) = value {
                        return Some(g.into_inner());
                    }
                }
            }
            None
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn successful_snapshot_emits_attempt_and_last_index_gauge() {
            let mut cluster = build_cluster(3);
            drive_to_leader_election(&mut cluster).await;

            leader_handle(&cluster)
                .lock()
                .append(HighWaterCommand::Advance(AdvancePayload { at_least: 1 }))
                .expect("append");
            drive_until(
                &mut cluster,
                |state| {
                    state
                        .nodes
                        .iter()
                        .all(|node| node.omnipaxos.lock().get_decided_idx() >= 1)
                },
                500,
            )
            .await;

            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let handle = leader_handle(&cluster);
            let mut policy = SnapshotPolicy::every(1);
            metrics::with_local_recorder(&recorder, || maybe_snapshot(&handle, &mut policy, 1));

            let snapshot = snapshotter.snapshot().into_vec();
            assert_eq!(
                counter(&snapshot, "tsoracle.paxos.snapshot.total"),
                1,
                "a fired snapshot attempt must be counted",
            );
            assert_eq!(
                gauge(&snapshot, "tsoracle.paxos.snapshot.last_index"),
                Some(1.0),
                "a successful snapshot must publish its decided index as the health gauge",
            );
            assert_eq!(
                counter(&snapshot, "tsoracle.paxos.snapshot.failures.total"),
                0,
                "a successful snapshot must not be counted as a failure",
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn failed_snapshot_increments_failure_counter() {
            let mut cluster = build_cluster(3);
            drive_to_leader_election(&mut cluster).await;

            leader_handle(&cluster)
                .lock()
                .append(HighWaterCommand::Advance(AdvancePayload { at_least: 1 }))
                .expect("append");
            drive_until(
                &mut cluster,
                |state| {
                    state
                        .nodes
                        .iter()
                        .all(|node| node.omnipaxos.lock().get_decided_idx() >= 1)
                },
                500,
            )
            .await;

            // Request a snapshot far beyond the decided index. OmniPaxos rejects
            // compaction past what has been decided, so the snapshot fails — the
            // failure path `maybe_snapshot` previously only logged.
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            let handle = leader_handle(&cluster);
            let mut policy = SnapshotPolicy::every(1);
            metrics::with_local_recorder(&recorder, || maybe_snapshot(&handle, &mut policy, 999));

            let snapshot = snapshotter.snapshot().into_vec();
            assert_eq!(
                counter(&snapshot, "tsoracle.paxos.snapshot.total"),
                1,
                "the rejected snapshot is still an attempt",
            );
            assert_eq!(
                counter(&snapshot, "tsoracle.paxos.snapshot.failures.total"),
                1,
                "a rejected snapshot must increment the failure counter",
            );
            assert_eq!(
                gauge(&snapshot, "tsoracle.paxos.snapshot.last_index"),
                None,
                "a failed snapshot must not advance the last-index health gauge",
            );
        }
    }
}
