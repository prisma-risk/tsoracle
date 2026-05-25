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

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

/// openraft's two-address membership entry (#408): the raft peer RPC address
/// and the scheme-less host:port service endpoint clients are redirected to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberAddr {
    pub raft_addr: String,
    pub service_endpoint: String,
}

/// openraft timing knobs (milliseconds), defaulted to the values the example used.
#[derive(Debug, Clone)]
pub struct RaftTuning {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl Default for RaftTuning {
    fn default() -> Self {
        Self {
            heartbeat_ms: 250,
            election_min_ms: 1_000,
            election_max_ms: 2_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FileConfig {
    pub state_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct OpenraftConfig {
    pub id: u64,
    pub raft_addr: std::net::SocketAddr,
    pub raft_dir: PathBuf,
    pub bootstrap: bool,
    /// Required ONLY with `bootstrap`; `None` on a non-bootstrap restart
    /// (membership recovers from raft state, #408).
    pub initial_membership: Option<BTreeMap<u64, MemberAddr>>,
    pub tuning: RaftTuning,
}

#[derive(Debug, Clone)]
pub struct PaxosConfig {
    pub node_id: u64,
    pub peer_listen: std::net::SocketAddr,
    /// id -> paxos peer addr. Required at EVERY start (OmniPaxos has no
    /// membership-driven addressing).
    pub peers: BTreeMap<u64, String>,
    /// id -> tsoracle service endpoint for LeaderHint follower-redirect.
    pub tso_peers: BTreeMap<u64, String>,
    pub data_dir: PathBuf,
    pub tick_interval: Duration,
}

pub enum DriverConfig {
    #[cfg(feature = "file")]
    File(FileConfig),
    #[cfg(feature = "openraft")]
    Openraft(OpenraftConfig),
    #[cfg(feature = "paxos")]
    Paxos(PaxosConfig),
}

/// Parse a comma-separated `id=host:port` peer map. Public so the bin and examples can reuse it instead of duplicating the parser.
pub fn parse_peer_map(input: &str) -> Result<BTreeMap<u64, String>, String> {
    let mut out = BTreeMap::new();
    for pair in input.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let (id, addr) = pair
            .split_once('=')
            .ok_or_else(|| format!("bad peer entry {pair:?}, expected id=host:port"))?;
        let id: u64 = id
            .trim()
            .parse()
            .map_err(|_| format!("bad peer id in {pair:?}"))?;
        out.insert(id, addr.trim().to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peer_map_reads_id_host_port_pairs() {
        let map = parse_peer_map("1=127.0.0.1:5001, 2=127.0.0.1:5002").unwrap();
        assert_eq!(map.get(&1).map(String::as_str), Some("127.0.0.1:5001"));
        assert_eq!(map.get(&2).map(String::as_str), Some("127.0.0.1:5002"));
    }

    #[test]
    fn parse_peer_map_rejects_entry_without_equals() {
        let err = parse_peer_map("1=127.0.0.1:5001,garbage").unwrap_err();
        assert!(err.contains("expected id=host:port"), "got: {err}");
    }

    #[test]
    fn parse_peer_map_skips_blank_entries() {
        let map = parse_peer_map("1=a:1,,2=b:2,").unwrap();
        assert_eq!(map.len(), 2);
    }
}
