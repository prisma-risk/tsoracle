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

//! Partition gates for in-memory raft test harnesses.
//!
//! Drives reachability between cluster nodes by combining per-node isolation
//! with per-directed-edge cuts. Default reachable; explicit blocks only.

use std::collections::HashSet;
use std::hash::Hash;

use parking_lot::RwLock;

/// Controls reachability between simulated cluster nodes for partition tests.
///
/// The model has two orthogonal blocking mechanisms:
///
/// - **Isolated nodes** — a node is isolated if it's in the `isolated` set.
///   Both directions to/from any isolated node are unreachable.
/// - **Cut edges** — individual directed edges marked unreachable, regardless
///   of node-level state.
///
/// `is_reachable(from, to)` returns `true` only if neither endpoint is
/// isolated **and** the directed edge is not explicitly cut.
pub struct PartitionController<NodeId> {
    isolated: RwLock<HashSet<NodeId>>,
    cut_edges: RwLock<HashSet<(NodeId, NodeId)>>,
}

impl<NodeId> PartitionController<NodeId>
where
    NodeId: Eq + Hash + Copy,
{
    /// Build a controller with no blocks (everything reachable).
    pub fn new() -> Self {
        Self {
            isolated: RwLock::new(HashSet::new()),
            cut_edges: RwLock::new(HashSet::new()),
        }
    }

    /// Isolate `node`: every (node, *) and (*, node) edge becomes unreachable.
    pub fn isolate(&self, node: NodeId) {
        self.isolated.write().insert(node);
    }

    /// Restore `node`: removes the node-level block. Per-edge cuts still apply.
    pub fn heal(&self, node: NodeId) {
        self.isolated.write().remove(&node);
    }

    /// Cut a single directed edge `from -> to`.
    pub fn cut_edge(&self, from: NodeId, to: NodeId) {
        self.cut_edges.write().insert((from, to));
    }

    /// Restore a previously cut directed edge.
    pub fn restore_edge(&self, from: NodeId, to: NodeId) {
        self.cut_edges.write().remove(&(from, to));
    }

    /// True iff neither endpoint is isolated and the directed edge is not cut.
    pub fn is_reachable(&self, from: NodeId, to: NodeId) -> bool {
        let isolated = self.isolated.read();
        if isolated.contains(&from) || isolated.contains(&to) {
            return false;
        }
        !self.cut_edges.read().contains(&(from, to))
    }
}

impl<NodeId> Default for PartitionController<NodeId>
where
    NodeId: Eq + Hash + Copy,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_fully_reachable() {
        let p = PartitionController::<u64>::new();
        assert!(p.is_reachable(1, 2));
        assert!(p.is_reachable(2, 1));
        assert!(p.is_reachable(1, 1));
    }

    #[test]
    fn isolate_blocks_both_directions() {
        let p = PartitionController::<u64>::new();
        p.isolate(1);
        assert!(!p.is_reachable(1, 2));
        assert!(!p.is_reachable(2, 1));
        // edges not involving 1 still reachable
        assert!(p.is_reachable(2, 3));
    }

    #[test]
    fn heal_restores_node_level_reachability() {
        let p = PartitionController::<u64>::new();
        p.isolate(1);
        p.heal(1);
        assert!(p.is_reachable(1, 2));
        assert!(p.is_reachable(2, 1));
    }

    #[test]
    fn cut_edge_blocks_only_one_direction() {
        let p = PartitionController::<u64>::new();
        p.cut_edge(1, 2);
        assert!(!p.is_reachable(1, 2));
        assert!(p.is_reachable(2, 1));
    }

    #[test]
    fn restore_edge_undoes_cut() {
        let p = PartitionController::<u64>::new();
        p.cut_edge(1, 2);
        p.restore_edge(1, 2);
        assert!(p.is_reachable(1, 2));
    }

    #[test]
    fn heal_does_not_restore_individually_cut_edges() {
        let p = PartitionController::<u64>::new();
        p.cut_edge(1, 2);
        p.isolate(1);
        p.heal(1);
        // edge cut survives heal
        assert!(!p.is_reachable(1, 2));
        // reverse direction works (not cut, not isolated)
        assert!(p.is_reachable(2, 1));
    }

    #[test]
    fn default_matches_new() {
        let p = PartitionController::<u64>::default();
        assert!(p.is_reachable(1, 2));
        assert!(p.is_reachable(2, 1));
    }

    /// Regression: a panic that unwinds through a lock guard must not mask the
    /// original failure on subsequent calls. With `std::sync::RwLock` the lock
    /// would poison and every later operation would panic with `PoisonError`,
    /// hiding the real cause — this test pins us to a non-poisoning lock.
    #[test]
    fn lock_does_not_poison_when_a_panic_occurs_inside_a_critical_section() {
        use std::hash::{Hash, Hasher};
        use std::panic::{AssertUnwindSafe, catch_unwind};

        // NodeId whose `Hash` impl panics for a sentinel value, triggering a
        // panic from inside `HashSet::insert` — i.e., while the write guard
        // returned by `RwLock::write()` is still alive.
        #[derive(Clone, Copy, Eq)]
        struct Node(u64);

        impl PartialEq for Node {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }

        impl Hash for Node {
            fn hash<H: Hasher>(&self, state: &mut H) {
                assert_ne!(self.0, 0xDEAD, "simulated downstream panic inside hash()");
                self.0.hash(state);
            }
        }

        let p = PartitionController::<Node>::new();

        // First call panics while the write guard is held.
        let outcome = catch_unwind(AssertUnwindSafe(|| p.isolate(Node(0xDEAD))));
        assert!(
            outcome.is_err(),
            "expected the simulated panic to propagate"
        );

        // With a poisoning lock these calls would re-panic with `PoisonError`,
        // masking the original failure. parking_lot must leave the controller
        // usable so the real panic is what gets reported.
        p.isolate(Node(7));
        assert!(!p.is_reachable(Node(7), Node(1)));
        assert!(p.is_reachable(Node(1), Node(2)));
    }
}
