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

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

/// openraft's three-address membership entry: the raft peer RPC address, the
/// scheme-less service endpoint clients are redirected to, and the scheme-less
/// admin endpoint the `tsoracle admin` CLI is redirected to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberAddr {
    pub raft_addr: String,
    pub service_endpoint: String,
    pub admin_endpoint: String,
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

/// mTLS material for the peer transport (PEM file paths, read at build()).
/// Presence on a driver config turns the peer transport into mutual TLS:
/// `cert`/`key` is this node's identity (used as both the peer-server identity
/// and the peer-client identity when dialing); `ca` verifies connecting peers.
/// The CA MUST be cluster-dedicated — anyone holding a cert it signed can join
/// replication.
#[derive(Debug, Clone)]
pub struct PeerTlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}

/// mTLS material for the membership-admin gRPC server (PEM file paths,
/// read at build()). Presence on an `OpenraftConfig` turns the admin port
/// into mutual TLS: `cert`/`key` is this node's admin-server identity; `ca`
/// verifies connecting admin clients (operators dialing via `tsoracle admin`).
///
/// Security contract: the admin CA MUST be issued from a CA chain independent
/// of the peer CA. Anyone holding a cert signed by the configured admin CA can
/// change cluster membership (AddLearner / Promote / RemoveNode). `build`
/// enforces the trivial form of this — it rejects sharing the same CA file
/// (by path or by byte content) — but it CANNOT detect two distinct CA files
/// where one is in the trust chain of the other, or both chained under a
/// shared root. Issuing the two CAs from independent roots is the operator's
/// responsibility.
#[derive(Debug, Clone)]
pub struct AdminTlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
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
    pub peer_tls: Option<PeerTlsConfig>,
    /// Bind address for the membership-admin gRPC server. `None` serves no
    /// admin surface.
    pub admin_listen: Option<std::net::SocketAddr>,
    /// mTLS material for the admin gRPC server. `None` means plaintext on
    /// loopback only; routable `admin_listen` without TLS is rejected at
    /// build() (see `StandaloneError::AdminInsecureRoutable`).
    pub admin_tls: Option<AdminTlsConfig>,
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
    pub peer_tls: Option<PeerTlsConfig>,
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

    #[test]
    fn raft_tuning_defaults_match_the_example_values() {
        let tuning = RaftTuning::default();
        assert_eq!(tuning.heartbeat_ms, 250);
        assert_eq!(tuning.election_min_ms, 1_000);
        assert_eq!(tuning.election_max_ms, 2_000);
    }
}
