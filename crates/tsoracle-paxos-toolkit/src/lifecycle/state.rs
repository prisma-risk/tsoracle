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

//! Maps OmniPaxos leadership observations to `tsoracle_consensus::LeaderState`.

use tsoracle_consensus::LeaderState;
use tsoracle_core::Epoch;

/// A known peer node and the network endpoint where its TSO service can be
/// reached for follower-redirect hints.
///
/// `node_id` is the OmniPaxos `NodeId` (pid) of the peer. `endpoint` is the
/// advertised tsoracle service address surfaced via
/// `tsoracle_consensus::LeaderState::Follower::leader_endpoint` when this
/// peer becomes the elected leader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub node_id: u64,
    pub endpoint: String,
}

/// Internal leadership observation, mappable to
/// [`tsoracle_consensus::LeaderState`] via [`Self::to_consensus`].
///
/// Distinct from `LeaderState` because the toolkit may need to attach
/// additional diagnostic context per observation; the consensus crate's
/// variant set is the stable interface consumers see.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeadershipState {
    /// This node is the elected leader at the given epoch.
    Leader { epoch: Epoch },
    /// This node is a follower; `leader_endpoint` is the elected leader's
    /// advertised tsoracle service address if known.
    Follower { leader_endpoint: Option<String> },
    /// No leader is currently observed (election in progress, partition,
    /// or local-leader observation without an accompanying epoch).
    Unknown,
}

impl LeadershipState {
    /// Map an OmniPaxos observation to a `LeadershipState`.
    ///
    /// Discrimination rules:
    /// - `(current_leader = Some(my_node_id), epoch = Some(_))` → `Leader { epoch }`.
    /// - `(current_leader = Some(my_node_id), epoch = None)` → `Unknown`. We
    ///   know we are leader by pid but lack the epoch needed to surface a
    ///   `Leader` value; the caller (runner) always pairs them in practice,
    ///   but a permissive signature falls back to `Unknown` rather than
    ///   reporting ourselves as a follower of ourselves.
    /// - `(current_leader = Some(other), _)` → `Follower` with endpoint
    ///   looked up from `peers`, or `None` if the peer isn't listed.
    /// - `(current_leader = None, _)` → `Unknown`.
    pub fn from_omnipaxos(
        my_node_id: u64,
        current_leader: Option<u64>,
        epoch: Option<Epoch>,
        peers: &[Peer],
    ) -> Self {
        match current_leader {
            Some(leader) if leader == my_node_id => match epoch {
                Some(epoch) => Self::Leader { epoch },
                None => Self::Unknown,
            },
            Some(leader) => {
                let endpoint = peers
                    .iter()
                    .find(|peer| peer.node_id == leader)
                    .map(|peer| peer.endpoint.clone());
                Self::Follower {
                    leader_endpoint: endpoint,
                }
            }
            None => Self::Unknown,
        }
    }

    /// Convert to the public `tsoracle_consensus::LeaderState` value. Used
    /// by the runner when emitting events through the leader-event channel.
    #[must_use]
    pub fn to_consensus(&self) -> LeaderState {
        match self {
            Self::Leader { epoch } => LeaderState::Leader { epoch: *epoch },
            Self::Follower { leader_endpoint } => LeaderState::Follower {
                leader_endpoint: leader_endpoint.clone(),
                leader_epoch: None,
            },
            Self::Unknown => LeaderState::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsoracle_consensus::LeaderState;
    use tsoracle_core::Epoch;

    #[test]
    fn leader_is_self_emits_leader_state() {
        let state = LeadershipState::from_omnipaxos(1, Some(1), Some(Epoch(42)), &[]);
        assert_eq!(
            state.to_consensus(),
            LeaderState::Leader { epoch: Epoch(42) },
        );
    }

    #[test]
    fn leader_is_self_without_epoch_emits_unknown() {
        // The runner always pairs current_leader with epoch, but a permissive
        // signature should not produce "follower of self" if epoch is missing.
        let state = LeadershipState::from_omnipaxos(1, Some(1), None, &[]);
        assert_eq!(state.to_consensus(), LeaderState::Unknown);
    }

    #[test]
    fn leader_is_other_emits_follower_with_endpoint() {
        let peers = vec![Peer {
            node_id: 2,
            endpoint: "http://node-2:50051".into(),
        }];
        let state = LeadershipState::from_omnipaxos(1, Some(2), Some(Epoch(7)), &peers);
        assert_eq!(
            state.to_consensus(),
            LeaderState::Follower {
                leader_endpoint: Some("http://node-2:50051".into()),
                leader_epoch: None,
            },
        );
    }

    #[test]
    fn no_leader_emits_unknown() {
        let state = LeadershipState::from_omnipaxos(1, None, None, &[]);
        assert_eq!(state.to_consensus(), LeaderState::Unknown);
    }

    #[test]
    fn follower_with_unknown_peer_endpoint_is_none() {
        let state = LeadershipState::from_omnipaxos(1, Some(2), Some(Epoch(7)), &[]);
        assert_eq!(
            state.to_consensus(),
            LeaderState::Follower {
                leader_endpoint: None,
                leader_epoch: None,
            },
        );
    }
}
