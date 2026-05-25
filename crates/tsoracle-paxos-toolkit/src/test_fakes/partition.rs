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

//! Partition controller used by [`MemNetwork`](super::mem_network::MemNetwork)
//! to drop messages for a subset of nodes.
//!
//! Intentionally minimal: only full-node isolation, no partial partitions
//! (e.g., "node X can reach A and B but not C"). Partial-connectivity chaos
//! is an explicit non-goal documented in the paxos driver design spec.

use std::collections::HashSet;

use parking_lot::Mutex;

/// Records which node ids are currently isolated. Cheap to clone (the inner
/// state is shared via interior mutability; clones share the same lock).
#[derive(Default)]
pub struct PartitionController {
    isolated: Mutex<HashSet<u64>>,
}

impl PartitionController {
    /// Create a controller with no nodes isolated (all traffic flows).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Isolate `node_id`: every subsequent send originating from OR destined
    /// for this node is treated as blocked by [`Self::is_blocked`].
    pub fn isolate(&self, node_id: u64) {
        self.isolated.lock().insert(node_id);
    }

    /// Restore traffic to a single node previously isolated by
    /// [`Self::isolate`]. Other isolated nodes are unaffected.
    pub fn restore(&self, node_id: u64) {
        self.isolated.lock().remove(&node_id);
    }

    /// Clear all isolation; every node is reachable again.
    pub fn heal(&self) {
        self.isolated.lock().clear();
    }

    /// Returns `true` if a message from `from` to `to` would be dropped.
    /// Traffic is blocked when either endpoint is isolated.
    #[must_use]
    pub fn is_blocked(&self, from: u64, to: u64) -> bool {
        let guard = self.isolated.lock();
        guard.contains(&from) || guard.contains(&to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_partition_allows_all_traffic() {
        let partition = PartitionController::new();
        assert!(!partition.is_blocked(1, 2));
        assert!(!partition.is_blocked(2, 1));
    }

    #[test]
    fn isolate_blocks_outbound_and_inbound_for_the_node() {
        let partition = PartitionController::new();
        partition.isolate(2);
        // Outbound from 2 to anyone is blocked.
        assert!(partition.is_blocked(2, 1));
        assert!(partition.is_blocked(2, 3));
        // Inbound to 2 from anyone is blocked.
        assert!(partition.is_blocked(1, 2));
        assert!(partition.is_blocked(3, 2));
        // Traffic between non-isolated nodes flows.
        assert!(!partition.is_blocked(1, 3));
    }

    #[test]
    fn restore_clears_a_single_node() {
        let partition = PartitionController::new();
        partition.isolate(2);
        partition.isolate(3);
        partition.restore(2);
        // 2 is no longer isolated.
        assert!(!partition.is_blocked(2, 1));
        assert!(!partition.is_blocked(1, 2));
        // 3 is still isolated.
        assert!(partition.is_blocked(3, 1));
    }

    #[test]
    fn heal_clears_all_isolation() {
        let partition = PartitionController::new();
        partition.isolate(2);
        partition.isolate(3);
        partition.heal();
        assert!(!partition.is_blocked(2, 1));
        assert!(!partition.is_blocked(3, 1));
    }
}
