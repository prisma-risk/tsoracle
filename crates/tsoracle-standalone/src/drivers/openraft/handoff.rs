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

//! Graceful leadership handoff on shutdown.
//!
//! When a node is asked to drain (SIGTERM under Kubernetes), the tso gRPC
//! server stops accepting once the shutdown future resolves. If the node being
//! drained is the raft leader, peers would otherwise wait out an election
//! timeout before a successor emerges — a multi-second window where `GetTs`
//! returns `NOT_LEADER`. Transferring leadership to an up-to-date voter first
//! collapses that gap: the successor campaigns immediately, and the draining
//! node redirects in-flight clients to it via the usual leader hint.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use openraft::Raft;
use openraft::async_runtime::watch::WatchReceiver;

use tsoracle_driver_openraft::TypeConfig;

type NodeId = u64;

/// Wall-clock ceiling on waiting for leadership to move after a transfer is
/// triggered. Generous relative to a healthy campaign; if it elapses we drain
/// anyway rather than block shutdown indefinitely.
const HANDOFF_WAIT: Duration = Duration::from_secs(5);

/// Choose the most caught-up voter other than `me` to receive leadership.
///
/// `matched` maps a peer's node id to its highest replicated (matched) log
/// index; a voter missing from the map is treated as having replicated
/// nothing. Picking the most caught-up voter lets the successor become leader
/// without first catching up. Returns `None` when there is no other voter
/// (e.g. a single-node cluster), in which case there is nothing to hand to.
pub(crate) fn pick_handoff_target(
    me: NodeId,
    voters: &BTreeSet<NodeId>,
    matched: &BTreeMap<NodeId, u64>,
) -> Option<NodeId> {
    voters
        .iter()
        .copied()
        .filter(|id| *id != me)
        .max_by_key(|id| matched.get(id).copied().unwrap_or(0))
}

/// If this node is the current leader, transfer leadership to the most
/// caught-up voter and wait (bounded) for it to take over. Best-effort: any
/// error or timeout falls through to a normal drain.
pub(crate) async fn graceful_leader_handoff<SM>(raft: &Raft<TypeConfig, SM>, me: NodeId)
where
    SM: Send + Sync + 'static,
{
    if raft.current_leader().await != Some(me) {
        return;
    }

    let (voters, matched) = {
        let metrics = raft.metrics().borrow_watched().clone();
        let voters: BTreeSet<NodeId> = metrics.membership_config.voter_ids().collect();
        let matched: BTreeMap<NodeId, u64> = metrics
            .replication
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(id, log)| log.map(|log_id| (id, log_id.index)))
            .collect();
        (voters, matched)
    };

    let Some(target) = pick_handoff_target(me, &voters, &matched) else {
        return;
    };

    tracing::info!(target, "transferring leadership before drain");
    if let Err(error) = raft.trigger().transfer_leader(target).await {
        tracing::warn!(
            ?error,
            "leadership transfer failed; draining without handoff"
        );
        return;
    }

    let moved = tokio::time::timeout(HANDOFF_WAIT, async {
        while raft.current_leader().await == Some(me) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    if moved.is_err() {
        tracing::warn!("leadership did not move within the handoff window; draining anyway");
    } else {
        tracing::info!(target, "leadership handed off; draining");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters(ids: &[NodeId]) -> BTreeSet<NodeId> {
        ids.iter().copied().collect()
    }

    #[test]
    fn picks_the_most_caught_up_follower() {
        let v = voters(&[1, 2, 3]);
        let matched = BTreeMap::from([(2, 40), (3, 70)]);
        assert_eq!(pick_handoff_target(1, &v, &matched), Some(3));
    }

    #[test]
    fn excludes_self_even_when_most_caught_up() {
        let v = voters(&[1, 2, 3]);
        let matched = BTreeMap::from([(1, 100), (2, 40), (3, 30)]);
        assert_eq!(pick_handoff_target(1, &v, &matched), Some(2));
    }

    #[test]
    fn treats_missing_replication_as_zero() {
        let v = voters(&[1, 2, 3]);
        let matched = BTreeMap::new();
        assert!(matches!(pick_handoff_target(1, &v, &matched), Some(2 | 3)));
    }

    #[test]
    fn single_voter_cluster_has_no_target() {
        let v = voters(&[1]);
        assert_eq!(pick_handoff_target(1, &v, &BTreeMap::new()), None);
    }

    #[test]
    fn ignores_non_voter_progress() {
        let v = voters(&[1, 2]);
        let matched = BTreeMap::from([(2, 10), (9, 999)]);
        assert_eq!(pick_handoff_target(1, &v, &matched), Some(2));
    }
}
