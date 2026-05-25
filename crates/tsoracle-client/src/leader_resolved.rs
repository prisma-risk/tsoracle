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
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::time::Instant;
use tonic::transport::{Channel, Endpoint};
use tsoracle_proto::v1::tso_service_client::TsoServiceClient;

use crate::RetryPolicy;
use crate::error::ClientError;
use crate::transport::apply_endpoint_config;

// The leader-hint trailer key, the `LeaderHintLookup` classifier, and the
// `decode_leader_hint` helper now live in `tsoracle-proto` as the single
// source of truth for the wire contract (the server inserts the trailer; this
// client decodes it). Re-exported here so the retry loop's existing
// `crate::leader_resolved::{LeaderHintLookup, decode_leader_hint}` import path
// is unchanged.
pub use tsoracle_proto::v1::{LeaderHintLookup, decode_leader_hint};

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
    /// One lazily-dialed channel per endpoint. The `parking_lot::Mutex`
    /// guards only the map structure — never an `await` — so it stays a
    /// cheap synchronous lock. Each value is an `Arc<OnceCell<Channel>>`:
    /// concurrent first-callers to the same endpoint look up (or insert) the
    /// shared cell under the lock, drop the lock, then race into the cell's
    /// `get_or_try_init`, which runs the dial exactly once. A failed dial
    /// leaves the cell uninitialized so the next caller retries.
    channels: Mutex<HashMap<String, Arc<OnceCell<Channel>>>>,
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
    /// within the configured `leader_ttl`. Used by `cached_leader` (and
    /// thus `iter_round_robin`) and the test surface. The monotone-forward
    /// freshness check in `compare_and_set_leader` is inlined there instead
    /// of routed through this helper, because that path must hold the lock
    /// across both the check and the write.
    pub(crate) fn fresh_leader(&self) -> Option<CachedLeader> {
        let guard = self.leader.lock();
        match &*guard {
            Some(cached) if cached.last_used.elapsed() < self.retry_policy.leader_ttl => {
                Some(cached.clone())
            }
            _ => None,
        }
    }

    /// Record a successful RPC against `endpoint` that observed the leader at
    /// `epoch`. Like [`Self::compare_and_set_leader`], the cached epoch only
    /// ever moves forward: this method is the *other* writer to the same
    /// `epoch` field, and a monotone-forward gate is only as strong as its
    /// weakest writer (issue #333). A late-completing RPC against a
    /// since-deposed leader — a normal failover artifact, or out-of-order
    /// completion of two coalesced retries — carries a stale epoch that must
    /// not lower the cache, or the CAS gate would then accept a
    /// genuinely-stale hint it was designed to reject.
    ///
    /// - Same endpoint: refresh `last_used` (the endpoint just proved it is
    ///   alive and serving, so it keeps its worklist slot regardless of the
    ///   observed epoch) and `max` the epoch, which also upgrades a
    ///   previously-unknown epoch to the observed one.
    /// - Different endpoint: replace the entry unless the cache is *fresh*,
    ///   both epochs are known, and the new epoch is *strictly below* the
    ///   cached one — the same rule `compare_and_set_leader` applies, so an
    ///   expired entry or an unknown cached epoch still imposes no floor.
    pub(crate) fn record_success(&self, endpoint: &str, epoch: u128) {
        let mut guard = self.leader.lock();
        match &mut *guard {
            Some(cached) if cached.endpoint == endpoint => {
                cached.epoch = Some(cached.epoch.map_or(epoch, |current| current.max(epoch)));
                cached.last_used = Instant::now();
            }
            // A fresh cache at a known, strictly-higher epoch wins: a late
            // success against a now-deposed leader must not install a
            // lower-epoch entry and re-open the CAS gate to stale hints.
            Some(cached)
                if cached.last_used.elapsed() < self.retry_policy.leader_ttl
                    && cached.epoch.is_some_and(|current| epoch < current) => {}
            _ => {
                *guard = Some(CachedLeader {
                    endpoint: endpoint.to_string(),
                    epoch: Some(epoch),
                    last_used: Instant::now(),
                });
            }
        }
    }

    /// Atomically apply the monotone-forward rule and, if it holds, seat
    /// `endpoint`/`epoch` as the cached leader. Returns whether the write
    /// happened.
    ///
    /// The check and the write share one lock acquisition, so a concurrent
    /// `record_success(higher_epoch)` cannot land between "the hint passed
    /// the gate" and "the hint was written" and then be clobbered by the
    /// lower-epoch hint. The rule, evaluated only while the cache is fresh
    /// (within `leader_ttl`):
    ///
    /// - **Both epochs known:** accept iff the hint's epoch is `>=` the
    ///   cached one (the monotone-forward gate; rejects a strictly-stale
    ///   hint).
    /// - **Known cache, epoch-less hint:** there is no epoch to rank, so
    ///   accept only when the hint names a *configured* endpoint — one we
    ///   would dial in round-robin regardless, and that a later successful
    ///   RPC could confirm. An off-list epoch-less hint cannot downgrade the
    ///   confirmed leader (issue #357).
    /// - **Unknown cached epoch:** no monotone floor to defend, so the hint
    ///   is accepted — the bootstrap / old-server path.
    ///
    /// An absent or expired entry also accepts the hint (no floor).
    pub(crate) fn compare_and_set_leader(&self, endpoint: String, epoch: Option<u128>) -> bool {
        let mut guard = self.leader.lock();
        let accept = match &*guard {
            Some(cached) if cached.last_used.elapsed() < self.retry_policy.leader_ttl => {
                match (cached.epoch, epoch) {
                    (Some(cached_epoch), Some(hint_epoch)) => hint_epoch >= cached_epoch,
                    // No epoch to rank against a fresh, known-epoch leader
                    // (issue #357): accept only when the hint names a
                    // configured node — one we would dial in round-robin
                    // anyway, and that a later successful RPC could confirm.
                    // An off-list endpoint (a mixed-version peer's bad guess
                    // or a misbehaving redirect source) cannot downgrade the
                    // confirmed leader on an epoch-less claim.
                    (Some(_), None) => self
                        .configured
                        .iter()
                        .any(|configured| configured == &endpoint),
                    // The cache itself holds no epoch (or is absent/expired):
                    // no monotone floor to defend, so the bootstrap and
                    // old-server paths still accept the hint.
                    _ => true,
                }
            }
            _ => true,
        };
        if accept {
            *guard = Some(CachedLeader {
                endpoint,
                epoch,
                last_used: Instant::now(),
            });
        }
        accept
    }

    /// Test-only convenience wrapper over [`Self::client_with_cell`] for the
    /// callers that only need the client and not the cell handle. Production
    /// code goes through `client_with_cell` so it can evict the exact channel
    /// on a transport-class failure.
    #[cfg(test)]
    pub(crate) async fn client(
        &self,
        endpoint: &str,
    ) -> Result<TsoServiceClient<Channel>, ClientError> {
        self.client_with_cell(endpoint)
            .await
            .map(|(client, _cell)| client)
    }

    /// Returns a tonic client for `endpoint` plus the shared `OnceCell`
    /// backing its channel, opening the channel on first use.
    ///
    /// Look up (or insert) the endpoint's shared `OnceCell` under the
    /// synchronous map lock, release the lock, then drive the dial through
    /// `get_or_try_init`. Concurrent first-callers to the same endpoint share
    /// the one cell, so the dial — and its `connect.duration` / failure
    /// metrics — runs exactly once; later callers and cache hits clone the
    /// already-initialized `Channel` (itself an `Arc`-backed cheap clone).
    ///
    /// A failed dial does not stay cached: the endpoint's entry is evicted on
    /// the error path (via [`Self::evict_if_current`]) so a stream of distinct
    /// *failing* endpoints cannot grow the map without bound. That matters
    /// because wire-supplied leader hints (`LeaderHint.leader_endpoint`) reach
    /// this method as arbitrary endpoint strings, so a contacted peer handing
    /// back a fresh unparseable hint per request would otherwise leak one
    /// uninitialized cell each time. The next caller for the same endpoint
    /// re-inserts a fresh cell and re-dials, so the retry semantics are
    /// unchanged — only the dead slot is reclaimed.
    ///
    /// The returned cell handle lets the retry loop evict the *same* channel
    /// if a later RPC over it fails with a transport error (issue #239); see
    /// [`Self::evict_if_current`] and `crate::retry::attempt`.
    pub(crate) async fn client_with_cell(
        &self,
        endpoint: &str,
    ) -> Result<(TsoServiceClient<Channel>, Arc<OnceCell<Channel>>), ClientError> {
        let cell = {
            let mut guard = self.channels.lock();
            guard
                .entry(endpoint.to_string())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        let result = cell
            .get_or_try_init(|| async {
                // Cache miss: we are about to actually dial. Time the dial so
                // the `connect.duration` histogram only captures real connect
                // work, not the cache-hit fast path.
                #[cfg(feature = "metrics")]
                let connect_started = std::time::Instant::now();
                let result = match &self.connector {
                    Some(connector) => connector(endpoint).await,
                    None => {
                        match crate::transport::normalize_uri(endpoint, false).parse::<Endpoint>() {
                            Ok(transport_endpoint) => {
                                let transport_endpoint =
                                    apply_endpoint_config(transport_endpoint, &self.retry_policy);
                                transport_endpoint
                                    .connect()
                                    .await
                                    .map_err(ClientError::from)
                            }
                            Err(_) => Err(ClientError::InvalidEndpoint(endpoint.into())),
                        }
                    }
                };
                #[cfg(feature = "metrics")]
                match &result {
                    Ok(_) => metrics::histogram!("tsoracle.client.connect.duration")
                        .record(connect_started.elapsed().as_secs_f64()),
                    Err(_) => {
                        metrics::counter!("tsoracle.client.connect.failures.total").increment(1)
                    }
                }
                result
            })
            .await;

        let channel = match result {
            Ok(channel) => channel,
            Err(err) => {
                // The dial failed, so `cell` is still uninitialized — a
                // `OnceCell` only stores a value on a successful init, and
                // `get_or_try_init` runs the closure at most once across every
                // caller sharing this cell, so no concurrent caller initialized
                // it either. Reclaim the map slot (the identity guard inside
                // `evict_if_current` skips it if a concurrent caller already
                // replaced the cell).
                self.evict_if_current(endpoint, &cell);
                return Err(err);
            }
        };

        Ok((TsoServiceClient::new(channel.clone()), cell))
    }

    /// Remove `endpoint`'s cached channel cell, but only while the map still
    /// holds *this* cell. Used on two paths: a failed dial (the cell is still
    /// uninitialized) and a transport-class RPC failure against an
    /// already-dialed channel (issue #239 — a pod-replaced endpoint whose
    /// cached channel and its background tonic reconnect task would otherwise
    /// be reused indefinitely, since a static `Endpoint` resolves its address
    /// once and never re-resolves). Eviction forces the next caller to re-dial
    /// and re-resolve.
    ///
    /// The `Arc::ptr_eq` identity check ensures we only drop the entry while
    /// it still holds the cell we were handed: if a concurrent caller has
    /// meanwhile re-inserted a fresh cell under the same key (and may already
    /// be dialing into it), that cell is left untouched, so a stale failure
    /// cannot evict a freshly-redialed good channel.
    pub(crate) fn evict_if_current(&self, endpoint: &str, cell: &Arc<OnceCell<Channel>>) {
        let mut guard = self.channels.lock();
        if guard
            .get(endpoint)
            .is_some_and(|current| Arc::ptr_eq(current, cell))
        {
            guard.remove(endpoint);
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

    // The `decode_leader_hint` Absent/Malformed/Decoded classification tests
    // live alongside the helper in `tsoracle-proto`; this module covers the
    // channel-pool and leader-cache behavior that is unique to the client.

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

    /// A higher-epoch hint must win regardless of arrival order — the
    /// single-lock `compare_and_set_leader` is what enforces this. Two
    /// orderings of the same pair of hints are exercised here; both must
    /// land the pool on the higher-epoch endpoint.
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
            pool.compare_and_set_leader(first_endpoint.into(), Some(first_epoch));
            pool.compare_and_set_leader(second_endpoint.into(), Some(second_epoch));
            assert_eq!(
                pool.cached_leader().as_deref(),
                Some(winner),
                "ordering {first_endpoint}@{first_epoch} then {second_endpoint}@{second_epoch}"
            );
        }
    }

    /// `compare_and_set_leader` folds the monotone-forward check and the
    /// write into one lock acquisition, closing the TOCTOU window that
    /// existed when the gate (`accept_hint`) and the write
    /// (`set_leader_with`) were separate lock acquisitions. The return
    /// value reports whether the write happened, and — crucially — a
    /// rejected hint must leave the cache exactly where it was.
    #[test]
    fn compare_and_set_leader_checks_and_writes_atomically() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        // Empty cache: any hint is accepted and seated, even a `None` epoch.
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));

        // Pin the cache at epoch 5 via a confirmed RPC.
        pool.record_success("b:1", 5);

        // A lower-epoch hint is rejected and must not move the cache.
        assert!(!pool.compare_and_set_leader("a:1".into(), Some(4)));
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));

        // An equal-epoch hint is accepted (the rule is `>=`) and seated.
        assert!(pool.compare_and_set_leader("a:1".into(), Some(5)));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));

        // A strictly higher epoch promotes the cache forward.
        assert!(pool.compare_and_set_leader("b:1".into(), Some(9)));
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));

        // Once seated forward at epoch 9, an intermediate epoch (8) is
        // still behind and must be rejected without disturbing the cache.
        assert!(!pool.compare_and_set_leader("a:1".into(), Some(8)));
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));

        // Old-server fallback: a hint without an epoch stays acceptable
        // even once a known epoch has been observed.
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
    }

    /// An epoch-less hint must not downgrade a *fresh, known-epoch* cached
    /// leader when the hinted endpoint is **off the configured list** (issue
    /// #357). A mixed-version peer or a misbehaving redirect source can emit a
    /// NOT_LEADER hint carrying no epoch; without a rankable epoch, the only
    /// trust signal left is "is this an endpoint we'd dial anyway?". An
    /// off-list endpoint fails that test, so the confirmed leader stands.
    #[test]
    fn unknown_epoch_offlist_hint_rejected_when_cache_fresh_known() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        // Confirm `a:1` at a known epoch via a successful RPC.
        pool.record_success("a:1", 9);
        // An epoch-less hint to an endpoint that is NOT configured must be
        // rejected — it cannot prove it outranks the confirmed leader.
        assert!(!pool.compare_and_set_leader("attacker:1".into(), None));
        assert_eq!(
            pool.cached_leader().as_deref(),
            Some("a:1"),
            "an off-list epoch-less hint must not unseat a fresh known-epoch leader"
        );
    }

    /// The flip side of the #357 carve-out: an epoch-less hint to a
    /// *configured* endpoint is still accepted over a fresh, known-epoch
    /// leader. A node we would dial in round-robin anyway is trustworthy
    /// enough to redirect to immediately — preserving fast failover in a
    /// mixed-version cluster where the new leader runs an old server that
    /// emits no epoch.
    #[test]
    fn unknown_epoch_configured_hint_accepted_when_cache_fresh_known() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 9);
        // `b:1` is configured, so an epoch-less hint to it is honored.
        assert!(pool.compare_and_set_leader("b:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
    }

    /// When the cache itself holds an *unknown* epoch there is no monotone
    /// floor to defend, so the #357 carve-out does not apply: an epoch-less
    /// hint — even to an off-list endpoint — still seats. This is the
    /// bootstrap / old-server path the gate must keep open.
    #[test]
    fn unknown_epoch_hint_still_accepted_when_cache_unknown_epoch() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        // Seat an unknown-epoch entry (the cache holds `None`).
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        // With no known epoch cached, an off-list epoch-less hint still wins.
        assert!(pool.compare_and_set_leader("attacker:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("attacker:1"));
    }

    /// Once the known-epoch entry has aged past `leader_ttl`, it imposes no
    /// floor, so even an off-list epoch-less hint seats — the #357 carve-out
    /// only guards a *fresh* known-epoch leader. Re-checking freshness under
    /// the write lock is what keeps this consistent.
    #[tokio::test(start_paused = true)]
    async fn unknown_epoch_offlist_hint_accepted_once_cache_expires() {
        let policy = RetryPolicy {
            leader_ttl: std::time::Duration::from_millis(50),
            ..RetryPolicy::default()
        };
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], None, false, policy);
        pool.record_success("a:1", 9);
        // Fresh: the off-list epoch-less hint is rejected.
        assert!(!pool.compare_and_set_leader("attacker:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        // Advance past the TTL; the epoch-9 entry is now stale.
        tokio::time::advance(std::time::Duration::from_millis(75)).await;
        // No floor remains, so the same off-list epoch-less hint now seats.
        assert!(pool.compare_and_set_leader("attacker:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("attacker:1"));
    }

    /// A cache entry that has aged past `leader_ttl` is treated as absent,
    /// so `compare_and_set_leader` accepts and seats any hint — including
    /// one whose epoch is below the stale entry's. Re-checking freshness
    /// under the same lock as the write is what makes this safe; a TOCTOU
    /// between a freshness read and the write could otherwise resurrect a
    /// just-expired entry.
    #[tokio::test(start_paused = true)]
    async fn compare_and_set_leader_accepts_any_hint_once_cache_expires() {
        let policy = RetryPolicy {
            leader_ttl: std::time::Duration::from_millis(50),
            ..RetryPolicy::default()
        };
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], None, false, policy);
        pool.record_success("a:1", 9);
        // Fresh: a lower-epoch hint is still rejected.
        assert!(!pool.compare_and_set_leader("b:1".into(), Some(4)));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        // Advance past the TTL; the epoch-9 entry is now stale.
        tokio::time::advance(std::time::Duration::from_millis(75)).await;
        // The same lower-epoch hint now seats, because an expired entry
        // imposes no monotone-forward floor.
        assert!(pool.compare_and_set_leader("b:1".into(), Some(4)));
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
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

    /// A late-completing RPC against the *cached* leader endpoint must not
    /// lower the cached epoch (issue #333). Out-of-order completion of two
    /// coalesced retries, or a slow response from a since-superseded term,
    /// can arrive carrying an older epoch; `record_success` must `max` it,
    /// never overwrite downward. If it overwrote, the monotone-forward CAS
    /// gate would then accept a genuinely-stale hint it was built to reject.
    #[test]
    fn record_success_same_endpoint_does_not_lower_epoch() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 9);
        // A late, lower-epoch success against the same endpoint.
        pool.record_success("a:1", 4);
        assert_eq!(
            pool.fresh_leader().expect("cache seated").epoch,
            Some(9),
            "a stale same-endpoint success must not lower the cached epoch"
        );
        // The CAS gate must still reject an epoch-5 hint (5 < 9). Were the
        // epoch lowered to 4, this hint would be wrongly accepted.
        assert!(!pool.compare_and_set_leader("b:1".into(), Some(5)));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
    }

    /// A late success against a *different*, now-deposed leader must not
    /// replace a fresh, higher-epoch cache entry (issue #333). The
    /// cross-endpoint replacement mirrors the CAS rule: reject only when the
    /// cache is fresh, both epochs are known, and the new epoch is strictly
    /// below the cached one.
    #[test]
    fn record_success_different_endpoint_lower_epoch_is_rejected() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 9);
        // A late success against a peer that led at an earlier epoch.
        pool.record_success("b:1", 4);
        assert_eq!(
            pool.cached_leader().as_deref(),
            Some("a:1"),
            "a stale cross-endpoint success must not unseat the higher-epoch leader"
        );
        assert_eq!(pool.fresh_leader().expect("cache seated").epoch, Some(9));
    }

    /// A genuine failover — a success against a different endpoint at a
    /// strictly higher epoch — must advance the cache forward.
    #[test]
    fn record_success_different_endpoint_higher_epoch_advances() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 5);
        pool.record_success("b:1", 7);
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
        assert_eq!(pool.fresh_leader().expect("cache seated").epoch, Some(7));
    }

    /// An expired cache imposes no monotone-forward floor: once the entry has
    /// aged past `leader_ttl`, a lower-epoch success against a different
    /// endpoint seats freely, mirroring `compare_and_set_leader`'s handling
    /// of a stale entry. Re-checking freshness under the write lock is what
    /// keeps this consistent.
    #[tokio::test(start_paused = true)]
    async fn record_success_different_endpoint_lower_epoch_seats_once_expired() {
        let policy = RetryPolicy {
            leader_ttl: std::time::Duration::from_millis(50),
            ..RetryPolicy::default()
        };
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], None, false, policy);
        pool.record_success("a:1", 9);
        tokio::time::advance(std::time::Duration::from_millis(75)).await;
        // The epoch-9 entry is now stale, so a lower-epoch success seats.
        pool.record_success("b:1", 4);
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
        assert_eq!(pool.fresh_leader().expect("cache seated").epoch, Some(4));
    }

    /// A same-endpoint success refreshes the TTL even when its epoch is lower
    /// than the cached one: the endpoint just proved it is alive and serving,
    /// so it should keep its place in the worklist. The epoch is held at the
    /// higher value, but `last_used` is reset.
    #[tokio::test(start_paused = true)]
    async fn record_success_same_endpoint_lower_epoch_still_refreshes_ttl() {
        let policy = RetryPolicy {
            leader_ttl: std::time::Duration::from_millis(100),
            ..RetryPolicy::default()
        };
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], None, false, policy);
        pool.record_success("a:1", 9);
        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        // 60ms in: still fresh. A late, lower-epoch success touches the cache.
        pool.record_success("a:1", 4);
        tokio::time::advance(std::time::Duration::from_millis(60)).await;
        // 120ms since the first success but only 60ms since the touch, so the
        // entry must still be fresh — and the epoch must not have dropped.
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        assert_eq!(pool.fresh_leader().expect("cache seated").epoch, Some(9));
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

    /// Per issue #99's acceptance criterion: N concurrent first-callers
    /// racing a fresh pool for the *same* endpoint must observe exactly one
    /// `connect()`. The connector sleeps inside its future to widen the
    /// cache-miss window; under the pre-fix check-without-lock → connect →
    /// lock-and-insert sequence every racer misses the cache and dials, so
    /// the count would equal the number of tasks. The per-endpoint
    /// `OnceCell` collapses them onto a single shared init.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_first_callers_connect_endpoint_exactly_once() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let connect_count_for_closure = connect_count.clone();
        let connector: Arc<crate::transport::ChannelConnector> =
            Arc::new(move |_endpoint: &str| {
                connect_count_for_closure.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    // Hold the dial open long enough that every spawned racer
                    // has cleared the cache-miss check before the first finishes.
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    Ok(
                        tonic::transport::Endpoint::from_static("http://127.0.0.1:1")
                            .connect_lazy(),
                    )
                })
            });
        let pool = Arc::new(ChannelPool::new(
            vec!["a:1".into()],
            Some(connector),
            false,
            RetryPolicy::default(),
        ));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let pool = pool.clone();
            handles.push(tokio::spawn(
                async move { pool.client("a:1").await.map(|_| ()) },
            ));
        }
        for handle in handles {
            handle
                .await
                .expect("racer task must not panic")
                .expect("client() must succeed");
        }

        assert_eq!(
            connect_count.load(Ordering::SeqCst),
            1,
            "concurrent first-callers must share a single connect()"
        );
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

    /// A failed dial must not leave a permanent entry in the channel cache.
    /// #286 began inserting the endpoint's `OnceCell` under the map lock
    /// *before* the parse/dial, so a parse or connect failure left an
    /// uninitialized cell behind forever. Because wire-supplied leader hints
    /// (`LeaderHint.leader_endpoint`) flow into `client()` as arbitrary
    /// endpoint strings, a contacted peer returning a fresh invalid hint per
    /// request could grow this map without bound. Each failed dial must
    /// reclaim its own slot, so a run of distinct failing endpoints leaves
    /// nothing behind.
    #[tokio::test]
    async fn failed_dials_do_not_accumulate_in_channel_cache() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|endpoint: &str| {
            let endpoint_owned = endpoint.to_string();
            Box::pin(async move { Err(crate::error::ClientError::InvalidEndpoint(endpoint_owned)) })
        });
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        for i in 0..8 {
            let endpoint = format!("attacker-hint-{i}:1");
            assert!(
                pool.client(&endpoint).await.is_err(),
                "the failing connector must surface an error for {endpoint}"
            );
        }

        let guard = pool.channels.lock();
        assert_eq!(
            guard.len(),
            0,
            "failed dials must not be retained in the channel cache"
        );
    }

    /// `evict_if_current` clears the endpoint's cached cell when the map
    /// still holds *that* cell. A successful dial seats a cell; evicting it
    /// with the same handle removes the entry, so the next caller re-dials.
    /// This is the primitive the retry loop uses to drop a channel whose RPC
    /// failed with a transport error.
    #[tokio::test]
    async fn evict_if_current_removes_the_cached_cell() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        let (_client, cell) = pool
            .client_with_cell("a:1")
            .await
            .expect("dial against the success connector must succeed");
        assert!(
            pool.channels.lock().contains_key("a:1"),
            "a successful dial must seat a cell"
        );

        pool.evict_if_current("a:1", &cell);
        assert!(
            !pool.channels.lock().contains_key("a:1"),
            "evicting with the live cell must clear the entry"
        );
    }

    /// The identity guard, mirroring the dial-failure eviction: if a
    /// concurrent caller has replaced the endpoint's cell with a fresh one,
    /// evicting with a *stale* handle must spare the live cell. Without it, a
    /// transport failure observed on an old channel could evict a
    /// freshly-redialed good channel, forcing a redundant re-dial.
    #[tokio::test]
    async fn evict_if_current_spares_a_replaced_cell() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        let (_first, cell1) = pool.client_with_cell("a:1").await.expect("first dial");
        // Evict and re-dial so the map holds a different cell than `cell1`.
        pool.evict_if_current("a:1", &cell1);
        let (_second, cell2) = pool.client_with_cell("a:1").await.expect("second dial");
        assert!(
            !Arc::ptr_eq(&cell1, &cell2),
            "the re-dial must seat a fresh cell"
        );

        // A stale-handle eviction must leave the live cell in place.
        pool.evict_if_current("a:1", &cell1);
        assert!(
            pool.channels.lock().contains_key("a:1"),
            "stale-cell eviction must spare the live cell"
        );
    }

    /// The flip side of the eviction-on-failure rule: a successful dial must
    /// still be cached so the single-flight fast path (and the #286
    /// per-endpoint `OnceCell`) keeps working. Removing the entry only when
    /// the cell is still uninitialized is what preserves this.
    #[tokio::test]
    async fn successful_dials_are_retained_in_channel_cache() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        pool.client("a:1")
            .await
            .expect("dial against the success connector must succeed");

        let guard = pool.channels.lock();
        assert_eq!(guard.len(), 1, "a successful dial must stay cached");
        assert!(
            guard
                .get("a:1")
                .expect("the dialed endpoint must have a cache entry")
                .get()
                .is_some(),
            "the retained cell must be initialized"
        );
    }
}
