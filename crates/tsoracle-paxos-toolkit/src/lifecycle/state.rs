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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerEntry {
    pub node_id: u64,
    pub endpoint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeadershipState {
    Leader { epoch: Epoch },
    Follower { leader_endpoint: Option<String> },
    Unknown,
}

impl LeadershipState {
    pub fn from_omnipaxos(
        my_node_id: u64,
        current_leader: Option<u64>,
        epoch: Option<Epoch>,
        peers: &[PeerEntry],
    ) -> Self {
        match (current_leader, epoch) {
            (Some(leader), Some(epoch)) if leader == my_node_id => Self::Leader { epoch },
            (Some(leader), _) => {
                let endpoint = peers
                    .iter()
                    .find(|peer| peer.node_id == leader)
                    .map(|peer| peer.endpoint.clone());
                Self::Follower {
                    leader_endpoint: endpoint,
                }
            }
            _ => Self::Unknown,
        }
    }

    #[must_use]
    pub fn to_consensus(&self) -> LeaderState {
        match self {
            Self::Leader { epoch } => LeaderState::Leader { epoch: *epoch },
            Self::Follower { leader_endpoint } => LeaderState::Follower {
                leader_endpoint: leader_endpoint.clone(),
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
    fn leader_is_other_emits_follower_with_endpoint() {
        let peers = vec![PeerEntry {
            node_id: 2,
            endpoint: "http://node-2:50051".into(),
        }];
        let state = LeadershipState::from_omnipaxos(1, Some(2), Some(Epoch(7)), &peers);
        assert_eq!(
            state.to_consensus(),
            LeaderState::Follower {
                leader_endpoint: Some("http://node-2:50051".into()),
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
                leader_endpoint: None
            },
        );
    }
}
