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
            leader_endpoint: Some("http://node-2".into()),
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
            leader_endpoint: Some("http://node-2".into()),
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
