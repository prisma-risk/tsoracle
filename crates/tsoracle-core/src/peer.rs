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

//! A consensus peer and the tsoracle service endpoint it advertises.

use core::fmt;
use core::str::FromStr;

/// Error returned when a string fails the [`PeerEndpoint`] contract.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PeerEndpointError {
    /// The endpoint was an empty string. `PeerEndpoint` carries no "absent"
    /// semantics; callers that need optionality wrap it in `Option`.
    #[error("peer endpoint must not be empty")]
    Empty,
    /// The endpoint begins with `http://` or `https://`. The TLS client
    /// silently drops scheme-bearing leader hints to refuse a transport
    /// downgrade, so a scheme-bearing endpoint makes its owner unreachable
    /// via redirect.
    #[error("peer endpoint {endpoint:?} must be scheme-less host:port, not a http(s):// URL")]
    SchemeBearing { endpoint: String },
    /// The endpoint has no `:port` suffix. A bare host is technically a valid
    /// authority but is never an intentional production value — the consensus
    /// drivers always advertise an explicit port.
    #[error("peer endpoint {endpoint:?} must include an explicit :port")]
    MissingPort { endpoint: String },
    /// The host portion before `:port` is empty (e.g. `":50051"`).
    #[error("peer endpoint {endpoint:?} has an empty host")]
    EmptyHost { endpoint: String },
    /// The port portion is empty, non-numeric, or outside `1..=65535`.
    #[error("peer endpoint {endpoint:?} has an invalid port: {reason}")]
    InvalidPort {
        endpoint: String,
        reason: &'static str,
    },
}

/// A scheme-less `host:port` advertising the tsoracle service address of a
/// peer node.
///
/// **Contract (enforced at construction):**
///
/// * No `http://` / `https://` prefix. The TLS client silently drops
///   scheme-bearing leader hints (`rejects_plaintext_hint`) to refuse a
///   transport downgrade, so a scheme-bearing endpoint would make its owner
///   unreachable via follower redirect — a silent prod trap. (See
///   `tsoracle_server::fence::endpoint_is_scheme_less` for the historical
///   runtime guard this newtype subsumes.)
/// * Non-empty.
/// * Includes an explicit `:port` with a numeric port in `1..=65535`.
/// * Bracketed IPv6 supported (`[::1]:50051`).
///
/// Consumers that need to dial this endpoint prepend `http://` / `https://`
/// at dial time based on their TLS configuration (see e.g.
/// `tsoracle_client::channel_pool`); the endpoint itself is transport-agnostic.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerEndpoint(String);

impl PeerEndpoint {
    /// Borrow the underlying `host:port` string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume self, returning the underlying `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl TryFrom<String> for PeerEndpoint {
    type Error = PeerEndpointError;

    fn try_from(endpoint: String) -> Result<Self, Self::Error> {
        if endpoint.is_empty() {
            return Err(PeerEndpointError::Empty);
        }
        // ASCII-lowercase only, matching the client's `normalize_uri`. An
        // uppercase variant would already fail to parse downstream.
        if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            return Err(PeerEndpointError::SchemeBearing { endpoint });
        }
        // `rsplit_once(':')` splits on the LAST colon, which is the port
        // separator for both `host:port` and the bracketed-IPv6 form
        // `[::1]:50051` (host=`[::1]`, port=`50051`). `split_once(':')` would
        // break IPv6 by splitting at the first colon inside the address.
        let (host, port_str) = match endpoint.rsplit_once(':') {
            Some(pair) => pair,
            None => return Err(PeerEndpointError::MissingPort { endpoint }),
        };
        if host.is_empty() {
            return Err(PeerEndpointError::EmptyHost { endpoint });
        }
        // `u16::from_str` rejects empty, non-numeric, and `> 65535`. Port 0
        // parses cleanly but is reserved per RFC 6335; reject it explicitly.
        let port = port_str
            .parse::<u16>()
            .map_err(|_| PeerEndpointError::InvalidPort {
                endpoint: endpoint.clone(),
                reason: "must be a base-10 number in 1..=65535",
            })?;
        if port == 0 {
            return Err(PeerEndpointError::InvalidPort {
                endpoint,
                reason: "port 0 is reserved",
            });
        }
        Ok(PeerEndpoint(endpoint))
    }
}

impl TryFrom<&str> for PeerEndpoint {
    type Error = PeerEndpointError;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::try_from(s.to_string())
    }
}

impl FromStr for PeerEndpoint {
    type Err = PeerEndpointError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.to_string())
    }
}

impl fmt::Display for PeerEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for PeerEndpoint {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for PeerEndpoint {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for PeerEndpoint {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Re-validate on the way in: a wire-supplied or persisted endpoint
        // that violates the contract is rejected here, not silently observed
        // as a runtime trap downstream.
        let s = String::deserialize(deserializer)?;
        Self::try_from(s).map_err(serde::de::Error::custom)
    }
}

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
    pub endpoint: PeerEndpoint,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- TsoPeer struct shape (preserved from pre-newtype tests; updated
    // ---- to use contract-conforming endpoints instead of the http://...
    // ---- forms that were latent contract violations).

    #[test]
    fn carries_node_id_and_endpoint() {
        let peer = TsoPeer {
            node_id: 2,
            endpoint: PeerEndpoint::try_from("node-2:50051").unwrap(),
        };
        assert_eq!(peer.node_id, 2);
        assert_eq!(peer.endpoint.as_str(), "node-2:50051");
    }

    #[test]
    fn equality_is_by_node_id_and_endpoint() {
        let left = TsoPeer {
            node_id: 1,
            endpoint: PeerEndpoint::try_from("a:1").unwrap(),
        };
        let same = TsoPeer {
            node_id: 1,
            endpoint: PeerEndpoint::try_from("a:1").unwrap(),
        };
        let different_endpoint = TsoPeer {
            node_id: 1,
            endpoint: PeerEndpoint::try_from("b:1").unwrap(),
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
            endpoint: PeerEndpoint::try_from("a:1").unwrap(),
        });
        let duplicate = peers.insert(TsoPeer {
            node_id: 1,
            endpoint: PeerEndpoint::try_from("a:1").unwrap(),
        });
        assert!(!duplicate, "an equal peer must hash to the same bucket");
        assert_eq!(peers.len(), 1);
    }

    // ---- PeerEndpoint: accepted shapes

    #[test]
    fn accepts_basic_hostname_with_port() {
        let e = PeerEndpoint::try_from("node-1:50051").unwrap();
        assert_eq!(e.as_str(), "node-1:50051");
    }

    #[test]
    fn accepts_ipv4_with_port() {
        let e = PeerEndpoint::try_from("10.0.0.7:50551").unwrap();
        assert_eq!(e.as_str(), "10.0.0.7:50551");
    }

    #[test]
    fn accepts_ipv6_bracketed_with_port() {
        let e = PeerEndpoint::try_from("[::1]:50551").unwrap();
        assert_eq!(e.as_str(), "[::1]:50551");
    }

    #[test]
    fn accepts_fqdn_with_port() {
        let e = PeerEndpoint::try_from("leader.example.com:9000").unwrap();
        assert_eq!(e.as_str(), "leader.example.com:9000");
    }

    #[test]
    fn accepts_max_port() {
        let e = PeerEndpoint::try_from("h:65535").unwrap();
        assert_eq!(e.as_str(), "h:65535");
    }

    #[test]
    fn accepts_min_valid_port() {
        let e = PeerEndpoint::try_from("h:1").unwrap();
        assert_eq!(e.as_str(), "h:1");
    }

    // ---- PeerEndpoint: rejected shapes

    #[test]
    fn rejects_empty() {
        assert_eq!(PeerEndpoint::try_from(""), Err(PeerEndpointError::Empty),);
    }

    #[test]
    fn rejects_http_scheme() {
        let err = PeerEndpoint::try_from("http://node-1:50051").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::SchemeBearing { .. }),
            "expected SchemeBearing, got {err:?}",
        );
    }

    #[test]
    fn rejects_https_scheme() {
        let err = PeerEndpoint::try_from("https://node-1:50051").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::SchemeBearing { .. }),
            "expected SchemeBearing, got {err:?}",
        );
    }

    #[test]
    fn rejects_mem_scheme_via_missing_port() {
        // `mem://foo` is not http(s):// so it bypasses the scheme check, but
        // it has no numeric port — either the missing-port branch or the
        // invalid-port branch must reject it. Either is contract-correct;
        // both are acceptable.
        let err = PeerEndpoint::try_from("mem://foo").unwrap_err();
        assert!(
            matches!(
                err,
                PeerEndpointError::MissingPort { .. }
                    | PeerEndpointError::InvalidPort { .. }
                    | PeerEndpointError::EmptyHost { .. }
            ),
            "expected port-shape rejection, got {err:?}",
        );
    }

    #[test]
    fn rejects_bare_host_no_port() {
        let err = PeerEndpoint::try_from("node-1").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::MissingPort { .. }),
            "expected MissingPort, got {err:?}",
        );
    }

    #[test]
    fn rejects_empty_port() {
        let err = PeerEndpoint::try_from("node-1:").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::InvalidPort { .. }),
            "expected InvalidPort, got {err:?}",
        );
    }

    #[test]
    fn rejects_non_numeric_port() {
        let err = PeerEndpoint::try_from("node-1:abc").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::InvalidPort { .. }),
            "expected InvalidPort, got {err:?}",
        );
    }

    #[test]
    fn rejects_empty_host() {
        let err = PeerEndpoint::try_from(":50051").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::EmptyHost { .. }),
            "expected EmptyHost, got {err:?}",
        );
    }

    #[test]
    fn rejects_port_zero() {
        let err = PeerEndpoint::try_from("h:0").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::InvalidPort { .. }),
            "expected InvalidPort, got {err:?}",
        );
    }

    #[test]
    fn rejects_port_overflow() {
        let err = PeerEndpoint::try_from("h:65536").unwrap_err();
        assert!(
            matches!(err, PeerEndpointError::InvalidPort { .. }),
            "expected InvalidPort, got {err:?}",
        );
    }

    // ---- PeerEndpoint: API surface

    #[test]
    fn display_round_trips() {
        let e = PeerEndpoint::try_from("node-1:50051").unwrap();
        assert_eq!(format!("{e}"), "node-1:50051");
    }

    #[test]
    fn from_str_works() {
        let e: PeerEndpoint = "node-1:50051".parse().unwrap();
        assert_eq!(e.as_str(), "node-1:50051");
    }

    #[test]
    fn try_from_str_ref_works() {
        let e: PeerEndpoint = "node-1:50051".try_into().unwrap();
        assert_eq!(e.as_str(), "node-1:50051");
    }

    #[test]
    fn into_inner_returns_string() {
        let e = PeerEndpoint::try_from("node-1:50051").unwrap();
        assert_eq!(e.into_inner(), String::from("node-1:50051"));
    }

    // ---- PeerEndpoint: serde re-validates on deserialize

    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trip_preserves_value() {
        let e = PeerEndpoint::try_from("10.0.0.7:50551").unwrap();
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(json, "\"10.0.0.7:50551\"");
        let back: PeerEndpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(back, e);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_rejects_scheme_bearing_input() {
        // A scheme-bearing endpoint that snuck onto the wire must NOT be
        // observable as a `PeerEndpoint` — re-validate on deserialize.
        let err = serde_json::from_str::<PeerEndpoint>("\"http://node-1:50051\"")
            .expect_err("deserialize must reject scheme-bearing input");
        let msg = err.to_string();
        assert!(
            msg.contains("scheme-less") || msg.contains("http"),
            "error should mention the scheme contract, got: {msg}",
        );
    }
}
