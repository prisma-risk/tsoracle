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

//! Leadership state surfaced to the server's leader-watch task.

use tsoracle_core::Epoch;

/// Leadership state surfaced to the server's leader-watch task.
///
/// `PartialEq`/`Eq` allow drivers to implement payload-aware debounce on a
/// `watch::Sender<LeaderState>`: emitting only when the value (epoch, endpoint)
/// has actually changed, not just when the variant tag differs. A variant-only
/// check would silently drop term advances within a leadership streak and
/// follower-side leader-endpoint changes, both of which downstream consumers
/// must observe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderState {
    /// This node is the elected leader at the given epoch.
    Leader { epoch: Epoch },
    /// This node is a follower. `leader_endpoint` is the advertised tsoracle
    /// service address of the current leader, when known. `leader_epoch` is
    /// the leader's epoch (raft term) as observed by this follower, used by
    /// clients to reject a stale follower's lower-epoch redirect; `None` when
    /// the driver does not surface it.
    ///
    /// **Contract — endpoint shape:** `leader_endpoint`, when `Some`, MUST be a
    /// scheme-less `host:port` (e.g. `leader:9000`, `10.0.0.7:50551`), never a
    /// `http://...` or `https://...` URL. The follower-redirect path surfaces
    /// this string to clients as a leader hint, and a TLS-configured client
    /// *silently drops* any `http://` hint (`rejects_plaintext_hint`) to refuse
    /// a transport downgrade — so a scheme-bearing endpoint makes this leader
    /// unreachable via redirect, with only a log line as evidence. The shipped
    /// drivers satisfy this by reading the operator-configured membership
    /// service endpoint, which is itself contracted scheme-less.
    ///
    /// **Contract — epoch monotonicity:** `leader_epoch`, across successive
    /// emissions from one node, MUST be non-decreasing. Clients gate redirects
    /// on it (`compare_and_set_leader`): a hint whose epoch cannot outrank the
    /// cached leader is dropped, so a regressing epoch makes clients drop valid
    /// redirects (or, the other way, admit a stale one). Raft/Paxos terms are
    /// monotonic per node, so the shipped drivers satisfy this by construction;
    /// `None` (an epoch-less hint) is always permitted and gates nothing.
    Follower {
        leader_endpoint: Option<String>,
        leader_epoch: Option<Epoch>,
    },
    /// No leader is currently known (election in progress, partition, etc.).
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_state_payload_aware_equality() {
        // `PartialEq` is the basis for `watch::Sender`'s debounce, so
        // verify it discriminates on payload, not just variant tag.
        let l1 = LeaderState::Leader { epoch: Epoch(1) };
        let l2 = LeaderState::Leader { epoch: Epoch(2) };
        assert_ne!(l1, l2, "different epochs must compare unequal");
        assert_eq!(l1, LeaderState::Leader { epoch: Epoch(1) });

        let f_known = LeaderState::Follower {
            leader_endpoint: Some("node-2".into()),
            leader_epoch: Some(Epoch(4)),
        };
        let f_unknown = LeaderState::Follower {
            leader_endpoint: None,
            leader_epoch: None,
        };
        assert_ne!(
            f_known, f_unknown,
            "follower-leader-changes must surface as inequality",
        );
        // Epoch participates in equality so the watch-debounce re-emits on a
        // follower-side epoch change, not just an endpoint change.
        let f_epoch_5 = LeaderState::Follower {
            leader_endpoint: Some("node-2".into()),
            leader_epoch: Some(Epoch(5)),
        };
        assert_ne!(f_known, f_epoch_5, "epoch must discriminate followers");
        assert_ne!(f_known, LeaderState::Unknown);
        assert_eq!(LeaderState::Unknown, LeaderState::Unknown);

        // Debug round-trips through the derive (covers the derive impl).
        let rendered = format!("{l1:?}");
        assert!(rendered.contains("Leader"));
    }
}
