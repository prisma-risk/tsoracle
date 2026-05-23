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
//! Owns the in-memory high-water mark (an `AtomicU64`) and the apply
//! notifier the host's blocking-read methods loop on. The apply task is
//! spawned by the host (`StandaloneHost` in a follow-up sub-issue),
//! awaits the toolkit runner's `apply_notify`, then drains the
//! OmniPaxos log's decided suffix into the AtomicU64 — exactly once per
//! decided entry.
//!
//! No per-proposal tracking. Blocking-read methods (`submit_advance`,
//! `current_high_water`) snapshot `decided_idx` and the AtomicU64
//! before they append, then loop on the `apply_notifier` until their
//! waitcondition is satisfied. This is the consequence of
//! `OmniPaxos::append` not returning a log index for the proposing node.

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

/// Shared apply-task state.
///
/// Cheap to clone (Arc-wrapped fields). The host clones one copy into
/// the spawned apply task and keeps another for its blocking-read
/// methods to poll.
#[derive(Clone)]
pub struct ApplyState {
    high_water: Arc<AtomicU64>,
    apply_notifier: Arc<Notify>,
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
        }
    }

    /// Current in-memory high-water value.
    #[must_use]
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::SeqCst)
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
                LogEntry::Decided(HighWaterCommand::Advance { at_least }) => {
                    let prev = state.high_water.load(Ordering::SeqCst);
                    if *at_least > prev {
                        state.high_water.store(*at_least, Ordering::SeqCst);
                        trace!(prev, new = at_least, "high-water advanced");
                    }
                }
                LogEntry::Decided(HighWaterCommand::Barrier) => {
                    // No-op marker; the AtomicU64 is unchanged.
                }
                LogEntry::Snapshotted(snapshotted) => {
                    // OmniPaxos may surface a Snapshotted entry on log
                    // catch-up; reflect its value if it's higher.
                    let prev = state.high_water.load(Ordering::SeqCst);
                    if snapshotted.snapshot.value > prev {
                        state
                            .high_water
                            .store(snapshotted.snapshot.value, Ordering::SeqCst);
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
        let mut handle = omnipaxos.lock();
        // local_only=false: best-effort cluster-wide snapshot. Errors
        // are logged but not propagated; snapshot failure is not fatal
        // for liveness.
        if let Err(err) = handle.snapshot(Some(decided_idx), false) {
            tracing::warn!(?err, "snapshot trigger failed");
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
            .append(HighWaterCommand::Advance { at_least: 42 })
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
                .append(HighWaterCommand::Advance { at_least: 10 })
                .expect("append");
            handle
                .append(HighWaterCommand::Advance { at_least: 50 })
                .expect("append");
            handle
                .append(HighWaterCommand::Advance { at_least: 30 })
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
                .append(HighWaterCommand::Advance { at_least: 17 })
                .expect("append");
            handle.append(HighWaterCommand::Barrier).expect("append");
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
            .append(HighWaterCommand::Advance { at_least: 7 })
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
            .append(HighWaterCommand::Advance { at_least: 1 })
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
}
