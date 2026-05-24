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

//! A consensus peer and the tsoracle service endpoint it advertises.

/// A known peer node and the network endpoint where its TSO service can be
/// reached for follower-redirect hints.
///
/// `node_id` is the consensus node identity (the OmniPaxos `NodeId`/pid or the
/// openraft `NodeId`). `endpoint` is the advertised tsoracle service address
/// surfaced via `LeaderState::Follower::leader_endpoint` when this peer is the
/// elected leader. This is the cross-backend shape consensus drivers share so
/// the `leader_endpoint` derivation reads the same regardless of the engine
/// underneath.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TsoPeer {
    pub node_id: u64,
    pub endpoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_node_id_and_endpoint() {
        let peer = TsoPeer {
            node_id: 2,
            endpoint: "http://node-2:50051".to_string(),
        };
        assert_eq!(peer.node_id, 2);
        assert_eq!(peer.endpoint, "http://node-2:50051");
    }

    #[test]
    fn equality_is_by_node_id_and_endpoint() {
        let left = TsoPeer {
            node_id: 1,
            endpoint: "http://a".to_string(),
        };
        let same = TsoPeer {
            node_id: 1,
            endpoint: "http://a".to_string(),
        };
        let different_endpoint = TsoPeer {
            node_id: 1,
            endpoint: "http://b".to_string(),
        };
        assert_eq!(left, same);
        assert_ne!(left, different_endpoint);
    }

    #[test]
    fn is_usable_as_a_hash_set_member() {
        use std::collections::HashSet;

        let mut peers = HashSet::new();
        peers.insert(TsoPeer {
            node_id: 1,
            endpoint: "http://a".to_string(),
        });
        let duplicate = peers.insert(TsoPeer {
            node_id: 1,
            endpoint: "http://a".to_string(),
        });
        assert!(!duplicate, "an equal peer must hash to the same bucket");
        assert_eq!(peers.len(), 1);
    }
}
