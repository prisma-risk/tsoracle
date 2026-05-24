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

//! Channel pool with leader-cache and NOT_LEADER redirect handling.

use parking_lot::Mutex;
use prost::Message;
use std::collections::HashMap;
use tokio::time::Instant;
use tonic::Status;
use tonic::metadata::MetadataKey;
use tonic::transport::{Channel, Endpoint};
use tsoracle_proto::v1::LeaderHint;
use tsoracle_proto::v1::tso_service_client::TsoServiceClient;

use crate::RetryPolicy;
use crate::error::ClientError;
use crate::transport::apply_endpoint_config;

const LEADER_HINT_KEY: &str = "tsoracle-leader-hint-bin";

/// Outcome of inspecting a `Status`'s trailers for the leader-hint payload.
///
/// The retry loop treats the three cases differently: `Absent` is the normal
/// "this peer doesn't know who the leader is" signal and stays silent;
/// `Malformed` is a wire-protocol bug worth a warning + counter; `Decoded`
/// is the followable redirect.
pub enum LeaderHintLookup {
    Absent,
    Decoded(LeaderHint),
    Malformed,
}

pub fn decode_leader_hint(status: &Status) -> LeaderHintLookup {
    let Ok(key) = MetadataKey::from_bytes(LEADER_HINT_KEY.as_bytes()) else {
        return LeaderHintLookup::Absent;
    };
    let Some(value) = status.metadata().get_bin(key) else {
        return LeaderHintLookup::Absent;
    };
    let Ok(bytes) = value.to_bytes() else {
        return LeaderHintLookup::Malformed;
    };
    match LeaderHint::decode(bytes.as_ref()) {
        Ok(hint) => LeaderHintLookup::Decoded(hint),
        Err(_) => LeaderHintLookup::Malformed,
    }
}

/// Cached pointer to the endpoint that most recently behaved like the
/// leader, along with the epoch that confirmed it and the instant the
/// cache was last validated. `epoch` is `Option<u128>` (the full leader
/// epoch, reassembled from the wire's two 64-bit halves) so an old server
/// that emits NOT_LEADER hints without an epoch (or a wire payload arriving
/// before any successful GetTs has populated the epoch) can still seat a
/// cache entry; once any source provides an epoch, monotone-forward
/// comparisons take over.
#[derive(Debug, Clone)]
pub(crate) struct CachedLeader {
    pub endpoint: String,
    pub epoch: Option<u128>,
    pub last_used: Instant,
}

pub struct ChannelPool {
    configured: Vec<String>,
    channels: Mutex<HashMap<String, Channel>>,
    leader: Mutex<Option<CachedLeader>>,
    connector: Option<std::sync::Arc<crate::transport::ChannelConnector>>,
    /// Set by `ClientBuilder::tls_config`; cleared by `channel_connector`.
    /// Tells the retry loop to drop wire-supplied `http://` leader hints so
    /// a contacted peer cannot downgrade the transport. Has no effect on
    /// operator-supplied endpoints; those use the documented scheme rule
    /// ("explicit beats configured") unchanged.
    tls_required: bool,
    /// Frozen at builder time. The pool uses `per_attempt_deadline` plus
    /// the keepalive constants to build each `Endpoint`; the retry loop
    /// reads the same policy via [`Self::retry_policy`] to drive its
    /// per-attempt and overall deadlines.
    retry_policy: RetryPolicy,
}

impl ChannelPool {
    pub fn new(
        endpoints: Vec<String>,
        connector: Option<std::sync::Arc<crate::transport::ChannelConnector>>,
        tls_required: bool,
        retry_policy: RetryPolicy,
    ) -> Self {
        ChannelPool {
            configured: endpoints,
            channels: Mutex::new(HashMap::new()),
            leader: Mutex::new(None),
            connector,
            tls_required,
            retry_policy,
        }
    }

    /// True when the built-in TLS connector is in use. The retry loop uses
    /// this to refuse wire-supplied `http://` leader hints; see
    /// `crate::retry::issue_rpc`.
    pub fn tls_required(&self) -> bool {
        self.tls_required
    }

    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    /// The currently-cached leader endpoint, or `None` if no leader has
    /// been observed yet or the cache has aged past `leader_ttl`. The
    /// TTL check is lazy: an expired entry is treated as absent on
    /// read; the underlying slot is cleared on the next mutation.
    pub fn cached_leader(&self) -> Option<String> {
        self.fresh_leader().map(|cached| cached.endpoint)
    }

    /// Internal helper returning the full `CachedLeader` only when it is
    /// within the configured `leader_ttl`. Used by `iter_round_robin`,
    /// `accept_hint`, and the test surface.
    pub(crate) fn fresh_leader(&self) -> Option<CachedLeader> {
        let guard = self.leader.lock();
        match &*guard {
            Some(cached) if cached.last_used.elapsed() < self.retry_policy.leader_ttl => {
                Some(cached.clone())
            }
            _ => None,
        }
    }

    /// Install or refresh the cached leader entry. `epoch` may be `None`
    /// only when the source did not carry one (old-server `NOT_LEADER`
    /// hints). Successful RPCs always carry the leader's epoch via
    /// `GetTsResponse` and go through `record_success`.
    pub(crate) fn set_leader_with(&self, endpoint: String, epoch: Option<u128>) {
        *self.leader.lock() = Some(CachedLeader {
            endpoint,
            epoch,
            last_used: Instant::now(),
        });
    }

    /// Record a successful RPC against `endpoint` that observed the
    /// leader at `epoch`. Touches `last_used` (resetting the TTL clock)
    /// when the cache already points at `endpoint`, and installs a
    /// fresh entry otherwise. Also upgrades a previously-unknown epoch
    /// to the observed one without disturbing TTL semantics.
    pub(crate) fn record_success(&self, endpoint: &str, epoch: u128) {
        let mut guard = self.leader.lock();
        match &mut *guard {
            Some(cached) if cached.endpoint == endpoint => {
                cached.epoch = Some(epoch);
                cached.last_used = Instant::now();
            }
            _ => {
                *guard = Some(CachedLeader {
                    endpoint: endpoint.to_string(),
                    epoch: Some(epoch),
                    last_used: Instant::now(),
                });
            }
        }
    }

    /// Decide whether to honor a `LeaderHint` carrying `hint_epoch`.
    /// The rule is monotone-forward: a hint is rejected only when the
    /// cache is fresh (within `leader_ttl`), both epochs are known, and
    /// the hint's epoch is strictly less than the cached one. All
    /// other combinations accept the hint — including the bootstrap
    /// cases where either side has no epoch yet, so a delayed
    /// NOT_LEADER from an old epoch cannot flap the cache backward
    /// once a higher epoch has been observed.
    pub(crate) fn accept_hint(&self, hint_epoch: Option<u128>) -> bool {
        match self.fresh_leader() {
            None => true,
            Some(cached) => match (cached.epoch, hint_epoch) {
                (Some(cached_epoch), Some(hint_epoch)) => hint_epoch >= cached_epoch,
                _ => true,
            },
        }
    }

    /// Returns a tonic client for `endpoint`, opening the channel on first use.
    pub async fn client(&self, endpoint: &str) -> Result<TsoServiceClient<Channel>, ClientError> {
        if let Some(channel) = self.channels.lock().get(endpoint).cloned() {
            return Ok(TsoServiceClient::new(channel));
        }
        // Cache miss: we are about to actually dial. Time the dial so the
        // `connect.duration` histogram only captures real connect work, not
        // the cache-hit fast path.
        #[cfg(feature = "metrics")]
        let connect_started = std::time::Instant::now();
        let channel = match &self.connector {
            Some(connector) => connector(endpoint).await,
            None => match crate::transport::normalize_uri(endpoint, false).parse::<Endpoint>() {
                Ok(transport_endpoint) => {
                    let transport_endpoint =
                        apply_endpoint_config(transport_endpoint, &self.retry_policy);
                    transport_endpoint
                        .connect()
                        .await
                        .map_err(ClientError::from)
                }
                Err(_) => Err(ClientError::InvalidEndpoint(endpoint.into())),
            },
        };
        match channel {
            Ok(channel) => {
                #[cfg(feature = "metrics")]
                metrics::histogram!("tsoracle.client.connect.duration")
                    .record(connect_started.elapsed().as_secs_f64());
                self.channels
                    .lock()
                    .insert(endpoint.to_string(), channel.clone());
                Ok(TsoServiceClient::new(channel))
            }
            Err(error) => {
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.client.connect.failures.total").increment(1);
                Err(error)
            }
        }
    }

    pub fn iter_round_robin(&self) -> Vec<String> {
        let leader = self.cached_leader();
        let mut endpoints = Vec::with_capacity(self.configured.len());
        if let Some(leader_endpoint) = &leader {
            endpoints.push(leader_endpoint.clone());
        }
        for endpoint in &self.configured {
            if Some(endpoint) != leader.as_ref() {
                endpoints.push(endpoint.clone());
            }
        }
        endpoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryPolicy;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic::Status;
    use tonic::metadata::BinaryMetadataKey;
    use tonic::metadata::BinaryMetadataValue;

    /// A `Status` without a `tsoracle-leader-hint-bin` trailer must
    /// decode to `Absent` — this is the steady-state case (every
    /// response other than NOT_LEADER, plus NOT_LEADER from a server
    /// that has no known leader) and must not surface as `Malformed`,
    /// which would cause the retry loop to count it against the
    /// wire-protocol-bug bucket.
    #[test]
    fn decode_leader_hint_returns_absent_when_no_trailer_present() {
        let status = Status::failed_precondition("not leader");
        assert!(matches!(
            decode_leader_hint(&status),
            LeaderHintLookup::Absent
        ));
    }

    /// A `Status` with a `tsoracle-leader-hint-bin` trailer whose
    /// payload is not a valid `LeaderHint` protobuf must surface as
    /// `Malformed` — the distinction from `Absent` is what lets the
    /// retry loop count wire-protocol bugs separately from "this peer
    /// doesn't know the leader." Without this case the enum would be
    /// observationally equivalent to the prior `Option<LeaderHint>` and
    /// the type-level distinction would be lost.
    #[test]
    fn decode_leader_hint_returns_malformed_on_bad_protobuf() {
        let mut status = Status::failed_precondition("not leader");
        let key = BinaryMetadataKey::from_bytes(LEADER_HINT_KEY.as_bytes())
            .expect("LEADER_HINT_KEY must be a valid binary metadata key");
        // Bytes that are not a valid `LeaderHint` proto. Any sequence
        // that doesn't decode to one or two tagged fields works; we use
        // a wire-tag-shaped run of `0xff` so the decoder enters varint
        // parsing and then fails.
        let value = BinaryMetadataValue::from_bytes(&[0xff, 0xff, 0xff, 0xff]);
        status.metadata_mut().insert_bin(key, value);
        assert!(matches!(
            decode_leader_hint(&status),
            LeaderHintLookup::Malformed
        ));
    }

    /// A well-formed trailer round-trips through `encode` ↔ `decode`
    /// and surfaces as `Decoded(hint)` with the original payload
    /// preserved. This is the client-side companion to the server-side
    /// `roundtrip` test in `tsoracle-server::leader_hint`; both must
    /// agree on the wire shape or NOT_LEADER redirects will silently
    /// degrade.
    #[test]
    fn decode_leader_hint_decodes_well_formed_trailer() {
        let mut status = Status::failed_precondition("not leader");
        let key = BinaryMetadataKey::from_bytes(LEADER_HINT_KEY.as_bytes())
            .expect("LEADER_HINT_KEY must be a valid binary metadata key");
        let hint = LeaderHint {
            leader_endpoint: Some("10.0.0.7:50551".into()),
            leader_epoch_hi: Some(0),
            leader_epoch_lo: Some(42),
        };
        let value = BinaryMetadataValue::from_bytes(&hint.encode_to_vec());
        status.metadata_mut().insert_bin(key, value);

        match decode_leader_hint(&status) {
            LeaderHintLookup::Decoded(decoded) => {
                assert_eq!(decoded.leader_endpoint, hint.leader_endpoint);
                assert_eq!(decoded.leader_epoch_hi, hint.leader_epoch_hi);
                assert_eq!(decoded.leader_epoch_lo, hint.leader_epoch_lo);
            }
            other => panic!(
                "expected Decoded(_), got something else: {}",
                match other {
                    LeaderHintLookup::Absent => "Absent",
                    LeaderHintLookup::Malformed => "Malformed",
                    LeaderHintLookup::Decoded(_) => unreachable!(),
                }
            ),
        }
    }

    #[test]
    fn iter_starts_with_cached_leader() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into(), "c:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("b:1", 1);
        let order = pool.iter_round_robin();
        assert_eq!(order, vec!["b:1", "a:1", "c:1"]);
    }

    #[test]
    fn iter_without_cache_is_configured_order() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into(), "c:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        let order = pool.iter_round_robin();
        assert_eq!(order, vec!["a:1", "b:1", "c:1"]);
    }

    /// Direct table-test of the epoch-monotone rule. The rule is the
    /// safety predicate quoted in the cache-invalidation issue — once
    /// the cache pins a leader at some epoch, a delayed hint from a
    /// lower epoch must be rejected regardless of arrival order. The
    /// bootstrap cases (no cache, no cached epoch, no hint epoch) all
    /// accept so the client remains useful against the current server
    /// that does not populate the leader epoch.
    #[test]
    fn accept_hint_enforces_monotone_forward_epoch() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        // Empty cache: any hint is accepted, including a `None` epoch.
        assert!(pool.accept_hint(None));
        assert!(pool.accept_hint(Some(7)));

        // Pin the cache at epoch 5. A lower-epoch hint is now stale;
        // an equal-or-higher hint, or a hint that carries no epoch at
        // all (old-server fallback), is accepted.
        pool.record_success("b:1", 5);
        assert!(!pool.accept_hint(Some(4)));
        assert!(pool.accept_hint(Some(5)));
        assert!(pool.accept_hint(Some(6)));
        // Old-server fallback: a hint without an epoch must remain
        // acceptable until the server is upgraded to populate one.
        assert!(pool.accept_hint(None));

        // Promoting via a higher-epoch hint must not allow the older
        // epoch to flap the cache backward on a later arrival.
        pool.set_leader_with("a:1".into(), Some(9));
        assert!(!pool.accept_hint(Some(4)));
        assert!(!pool.accept_hint(Some(8)));
    }

    /// A higher-epoch hint must win regardless of arrival order — the
    /// `accept_hint` gate plus `set_leader_with` is what enforces this.
    /// Two orderings of the same pair of hints are exercised here; both
    /// must land the pool on the higher-epoch endpoint.
    #[test]
    fn higher_epoch_hint_wins_regardless_of_arrival_order() {
        for (first_endpoint, first_epoch, second_endpoint, second_epoch, winner) in [
            ("a:1", 7u128, "b:1", 5u128, "a:1"), // higher arrives first
            ("a:1", 5u128, "b:1", 7u128, "b:1"), // higher arrives second
        ] {
            let pool = ChannelPool::new(
                vec!["a:1".into(), "b:1".into()],
                None,
                false,
                RetryPolicy::default(),
            );
            if pool.accept_hint(Some(first_epoch)) {
                pool.set_leader_with(first_endpoint.into(), Some(first_epoch));
            }
            if pool.accept_hint(Some(second_epoch)) {
                pool.set_leader_with(second_endpoint.into(), Some(second_epoch));
            }
            assert_eq!(
                pool.cached_leader().as_deref(),
                Some(winner),
                "ordering {first_endpoint}@{first_epoch} then {second_endpoint}@{second_epoch}"
            );
        }
    }

    /// Per the cache-invalidation issue's acceptance criterion: a cached
    /// leader that has aged past `leader_ttl` must not be re-prepended
    /// to `iter_round_robin`. The next RPC falls back to the configured
    /// endpoint order. Uses a small TTL plus a real sleep — the entry
    /// is still in the slot, but `cached_leader()` reports `None` once
    /// the elapsed time crosses the threshold.
    #[tokio::test(start_paused = true)]
    async fn cached_leader_past_ttl_is_not_prepended_to_worklist() {
        let policy = RetryPolicy {
            leader_ttl: std::time::Duration::from_millis(50),
            ..RetryPolicy::default()
        };
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into(), "c:1".into()],
            None,
            false,
            policy,
        );
        // Fresh cache prepends the leader.
        pool.record_success("b:1", 1);
        assert_eq!(pool.iter_round_robin(), vec!["b:1", "a:1", "c:1"]);
        // Advance virtual time past the TTL. `start_paused = true`
        // makes this deterministic — no real wall-clock sleep, no
        // flake on slow CI runners.
        tokio::time::advance(std::time::Duration::from_millis(75)).await;
        // TTL-expired cache reads as absent and falls back to the
        // configured order.
        assert!(pool.cached_leader().is_none());
        assert_eq!(pool.iter_round_robin(), vec!["a:1", "b:1", "c:1"]);
    }

    /// A successful RPC against the cached leader refreshes the TTL
    /// clock rather than leaving the entry to age out. Without this,
    /// a continuously-busy steady-state leader would re-evaluate the
    /// worklist on a fixed interval and burn the configured-list
    /// prefix on every TTL boundary.
    #[tokio::test(start_paused = true)]
    async fn record_success_against_cached_leader_refreshes_ttl() {
        let policy = RetryPolicy {
            leader_ttl: std::time::Duration::from_millis(100),
            ..RetryPolicy::default()
        };
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], None, false, policy);
        pool.record_success("b:1", 1);
        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        // 60ms in: still fresh. Touch the cache.
        pool.record_success("b:1", 2);
        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        // Total elapsed since the original record_success is 120ms,
        // past TTL — but the touch reset the clock 60ms ago, so the
        // cache must still report `b:1` as fresh.
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
    }

    #[tokio::test]
    async fn pool_with_custom_connector_invokes_closure_per_endpoint() {
        let captured = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let captured_for_closure = captured.clone();
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(move |endpoint: &str| {
            captured_for_closure.lock().push(endpoint.to_string());
            let endpoint_owned = endpoint.to_string();
            Box::pin(async move { Err(crate::error::ClientError::InvalidEndpoint(endpoint_owned)) })
        });
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            Some(connector),
            false,
            RetryPolicy::default(),
        );
        let _ = pool.client("a:1").await;
        let _ = pool.client("b:1").await;
        let seen = captured.lock().clone();
        assert_eq!(seen, vec!["a:1".to_string(), "b:1".to_string()]);
    }

    #[tokio::test]
    async fn pool_caches_channel_from_custom_connector() {
        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_for_closure = call_count.clone();
        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                let n = call_count_for_closure.fetch_add(1, Ordering::SeqCst);
                assert_eq!(n, 0, "connector must only be invoked once per endpoint");
                Box::pin(async {
                    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1")
                        .connect_lazy();
                    Ok(channel)
                })
            });
        let pool = ChannelPool::new(
            vec!["a:1".into()],
            Some(connector),
            false,
            RetryPolicy::default(),
        );
        let _ = pool
            .client("a:1")
            .await
            .expect("first client() must succeed");
        let _ = pool
            .client("a:1")
            .await
            .expect("second client() must hit cache");
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn leader_hint_endpoint_goes_through_same_connector() {
        let captured = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
        let captured_for_closure = captured.clone();
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(move |endpoint: &str| {
            captured_for_closure.lock().push(endpoint.to_string());
            Box::pin(async { Err(crate::error::ClientError::InvalidEndpoint("x".into())) })
        });
        let pool = ChannelPool::new(
            vec!["a:1".into()],
            Some(connector),
            false,
            RetryPolicy::default(),
        );
        let _ = pool.client("hinted:1").await;
        let seen = captured.lock().clone();
        assert_eq!(seen, vec!["hinted:1".to_string()]);
    }
}
