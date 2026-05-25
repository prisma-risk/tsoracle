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
use std::sync::atomic::{AtomicUsize, Ordering};
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
// `crate::channel_pool::{LeaderHintLookup, decode_leader_hint}` import path
// is unchanged.
pub use tsoracle_proto::v1::{LeaderHintLookup, decode_leader_hint};

/// Upper bound on the number of *non-configured* (wire-supplied leader-hint)
/// channels the pool retains. Configured endpoints are operator-supplied and
/// bounded, so they are exempt; hint endpoints (`LeaderHint.leader_endpoint`)
/// are attacker-influenced strings, so a peer handing back a stream of
/// distinct but reachable decoys would otherwise pin an unbounded number of
/// live `Channel`s and their background reconnect tasks (issue #341). Once the
/// non-configured entries exceed this cap, the least-recently-used one is
/// evicted; the active leader, dialed on every request, stays most-recently-
/// used and is never the victim. The total map size is therefore bounded at
/// `configured.len() + MAX_HINT_CHANNELS`.
const MAX_HINT_CHANNELS: usize = 8;

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

/// Which writer is seating the leader cache. Both writers share one
/// monotone-forward rule (see [`ChannelPool::seat_leader`]); the source only
/// changes the trust model in the ambiguous epoch-less, different-endpoint
/// case.
///
/// A `Confirmed` seat is backed by a *succeeded* RPC: hard proof the endpoint
/// served as leader at the supplied epoch, but the completion can be
/// arbitrarily delayed (a late response from a since-deposed term), so it may
/// be stale by the time it lands. A `Hinted` seat is a third-party NOT_LEADER
/// claim — a *forward-looking* "the leader is now X" assertion from a peer we
/// just contacted. The asymmetry only decides the case where the cache holds
/// no rankable epoch: a hint carrying fresh information seats, but a
/// possibly-stale success that cannot prove it outranks the fresh entry must
/// not clobber it (issue H2).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum SeatSource {
    Confirmed,
    Hinted,
}

/// Merge an incoming epoch into the cached one without ever moving backward:
/// the `max` of two known epochs, and a known epoch is never downgraded to
/// `None`. A `None` cached epoch is upgraded to any incoming `Some`. Used by
/// the same-endpoint path, where a late, lower-epoch (or epoch-less) write
/// must keep the highest epoch the endpoint has been observed at.
fn merge_epoch_forward(cached: Option<u128>, incoming: Option<u128>) -> Option<u128> {
    match (cached, incoming) {
        (Some(cached), Some(incoming)) => Some(cached.max(incoming)),
        (Some(cached), None) => Some(cached),
        (None, incoming) => incoming,
    }
}

/// One pooled channel slot: the lazily-dialed channel cell plus the instant it
/// was last handed out. `last_used` drives the LRU eviction that bounds the
/// number of non-configured (hint-derived) entries (issue #341); it is
/// refreshed on every `client_with_cell` lookup — including cache hits — so the
/// endpoint dialed on every request (the active leader) stays most-recently-
/// used and is never chosen as a victim.
struct PooledChannel {
    cell: Arc<OnceCell<Channel>>,
    last_used: Instant,
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
    ///
    /// Non-configured (hint-derived) entries are capped at
    /// [`MAX_HINT_CHANNELS`] via LRU eviction keyed on each slot's `last_used`;
    /// configured endpoints are exempt. See [`Self::enforce_hint_cap`].
    channels: Mutex<HashMap<String, PooledChannel>>,
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
    /// Per-pool round-robin cursor for `iter_round_robin`. Seeded *randomly*
    /// at construction so that different client instances start their
    /// cold-cache probes at different configured endpoints — a fleet cold
    /// start therefore spreads across nodes instead of every pool opening on
    /// `configured[0]`. Advanced with a `Relaxed` `fetch_add` per worklist;
    /// it carries no cross-thread invariant, so `Relaxed` is sufficient. A
    /// hash of the configured endpoints would *not* distribute load — every
    /// client ships the same endpoint list — so the seed must be random.
    rotation: AtomicUsize,
    /// Test-only count of how many times [`Self::enforce_hint_cap`] has
    /// actually run. The cap sweep is an O(n) `keys().filter()` scan (plus a
    /// `min_by_key` on overflow) under the synchronous map lock, gated to fire
    /// only when a brand-new slot is inserted — a cache hit reuses an existing
    /// slot and cannot push the non-configured count over the cap. That gating
    /// is invisible in the map's final state (a cache-hit sweep would be a
    /// guaranteed no-op), so the only way to assert it is to observe that the
    /// sweep did not run; this counter is that observation. `#[cfg(test)]`, so
    /// it does not exist in production builds.
    #[cfg(test)]
    hint_cap_sweeps: AtomicUsize,
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
            rotation: AtomicUsize::new(rand::random_range(0..=usize::MAX)),
            #[cfg(test)]
            hint_cap_sweeps: AtomicUsize::new(0),
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
    /// freshness check in `seat_leader` is inlined there instead of routed
    /// through this helper, because that path must hold the lock across both
    /// the check and the write.
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
    /// `epoch`. A thin [`SeatSource::Confirmed`] wrapper over
    /// [`Self::seat_leader`], which owns the monotone-forward rule. A
    /// late-completing RPC against a since-deposed leader — a normal failover
    /// artifact, or out-of-order completion of two coalesced retries — carries
    /// a stale epoch, so the rule never lowers the cache: a same-endpoint
    /// success `max`es the epoch, and a different-endpoint success may unseat a
    /// fresh leader only when it proves it outranks it.
    pub(crate) fn record_success(&self, endpoint: &str, epoch: u128) {
        self.seat_leader(endpoint, Some(epoch), SeatSource::Confirmed);
    }

    /// Apply the monotone-forward rule and, if it holds, seat `endpoint`/`epoch`
    /// as the cached leader; returns whether the write happened. A thin
    /// [`SeatSource::Hinted`] wrapper over [`Self::seat_leader`], where the
    /// check and the write share one lock acquisition, so a concurrent
    /// higher-epoch promotion cannot land between "the hint passed the gate"
    /// and "the hint was written" and then be clobbered by this lower-or-equal
    /// hint.
    pub(crate) fn compare_and_set_leader(&self, endpoint: String, epoch: Option<u128>) -> bool {
        self.seat_leader(&endpoint, epoch, SeatSource::Hinted)
    }

    /// The single monotone-forward writer to the leader cache. Both
    /// [`Self::record_success`] and [`Self::compare_and_set_leader`] are thin
    /// typed wrappers over it, so the rule that decides whether a
    /// `(endpoint, epoch)` pair may seat the cache lives in exactly one place.
    /// A monotone-forward gate is only as strong as its weakest writer, and two
    /// hand-mirrored implementations were the root cause of issue H2: the
    /// `record_success` mirror defended a *known*-epoch entry (issue #333) but
    /// left a `None`-epoch entry — the epoch-less / mixed-version case, where
    /// it matters most — unguarded, so a late cross-endpoint success flapped
    /// the cache back to a deposed leader.
    ///
    /// The lock is held across the whole decision-and-write. Returns whether
    /// the cache was (re)seated at `endpoint`. The rule, given the cache state:
    ///
    /// - **Absent or expired entry:** no monotone floor — accept and seat.
    /// - **Same endpoint:** accept, refresh `last_used`, and merge the epoch
    ///   forward (`max` of the known epochs; a known epoch is never downgraded
    ///   to `None`). The endpoint just re-proved itself (a success) or was
    ///   re-named as leader (a hint), so it keeps its slot regardless of the
    ///   observed epoch. Reviving an expired same-endpoint entry carries no
    ///   flap risk, so this path ignores freshness.
    /// - **Different endpoint, fresh entry:** a write may unseat a fresh leader
    ///   only when it can *prove* it outranks it.
    ///   - **Both epochs known:** accept iff the incoming epoch is `>=` the
    ///     cached one (the monotone-forward gate; rejects a strictly-stale
    ///     write — issue #333).
    ///   - **Known cached epoch, epoch-less write:** no epoch to rank. A
    ///     `Hinted` write seats only when it names a *configured* endpoint —
    ///     one we would dial in round-robin anyway (issue #357); a `Confirmed`
    ///     write cannot reach here (a successful GetTs always carries a wire
    ///     epoch) and defends the entry to keep the rule total.
    ///   - **Unknown cached epoch:** no monotone floor. A `Hinted` write seats
    ///     (the bootstrap / old-server path); a `Confirmed` write does *not* —
    ///     a possibly-stale past proof with no rankable epoch must not flap the
    ///     fresh entry back to a possibly-deposed leader (issue H2). The cache
    ///     still converges, because an alive-but-follower endpoint redirects via
    ///     the hint path and a fully-dead one self-heals at `leader_ttl`.
    fn seat_leader(&self, endpoint: &str, epoch: Option<u128>, source: SeatSource) -> bool {
        let mut guard = self.leader.lock();
        match &mut *guard {
            // Same endpoint: refresh and merge the epoch forward, regardless of
            // freshness.
            Some(cached) if cached.endpoint == endpoint => {
                cached.epoch = merge_epoch_forward(cached.epoch, epoch);
                cached.last_used = Instant::now();
                true
            }
            // Different endpoint while the entry is still fresh.
            Some(cached) if cached.last_used.elapsed() < self.retry_policy.leader_ttl => {
                let accept = match (cached.epoch, epoch) {
                    (Some(cached_epoch), Some(incoming)) => incoming >= cached_epoch,
                    (Some(_), None) => match source {
                        SeatSource::Hinted => self.is_configured(endpoint),
                        SeatSource::Confirmed => false,
                    },
                    (None, _) => match source {
                        SeatSource::Hinted => true,
                        SeatSource::Confirmed => false,
                    },
                };
                if accept {
                    *guard = Some(CachedLeader {
                        endpoint: endpoint.to_string(),
                        epoch,
                        last_used: Instant::now(),
                    });
                }
                accept
            }
            // Absent or expired: no floor.
            _ => {
                *guard = Some(CachedLeader {
                    endpoint: endpoint.to_string(),
                    epoch,
                    last_used: Instant::now(),
                });
                true
            }
        }
    }

    /// Pin the round-robin cursor to a known value so the order-asserting
    /// tests are deterministic despite the random production seed. Test-only.
    #[cfg(test)]
    pub(crate) fn pin_rotation_for_test(&self, seed: usize) {
        self.rotation.store(seed, Ordering::Relaxed);
    }

    /// Test-only read of the cap-sweep invocation counter; see the
    /// [`Self::hint_cap_sweeps`] field.
    #[cfg(test)]
    pub(crate) fn hint_cap_sweep_count(&self) -> usize {
        self.hint_cap_sweeps.load(Ordering::Relaxed)
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
            // A brand-new endpoint is the *only* event that can push the
            // non-configured count over the cap; a cache hit reuses an existing
            // slot, leaving the map size unchanged. Captured before the insert
            // so the cap sweep below runs only when a slot is actually added.
            let inserted = !guard.contains_key(endpoint);
            let slot = guard
                .entry(endpoint.to_string())
                .or_insert_with(|| PooledChannel {
                    cell: Arc::new(OnceCell::new()),
                    last_used: Instant::now(),
                });
            // Refresh recency on every lookup (cache hit included) so the
            // endpoint dialed on each request stays most-recently-used (issue
            // #341); this must stay unconditional even though the sweep below
            // is gated on insertion.
            slot.last_used = Instant::now();
            let cell = slot.cell.clone();
            // Reclaim any non-configured slot that pushed us over the cap, but
            // only on a real insert: on a cache hit the count is unchanged and
            // was already trimmed to `<= MAX_HINT_CHANNELS` by the insert that
            // seated this slot, so the O(n) sweep would be a no-op under the
            // map lock. The freshly-touched slot is the most-recently-used, so
            // it is never its own victim.
            if inserted {
                self.enforce_hint_cap(&mut guard, endpoint);
            }
            cell
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
            .is_some_and(|current| Arc::ptr_eq(&current.cell, cell))
        {
            guard.remove(endpoint);
        }
    }

    /// True when `endpoint` is one of the operator-supplied configured
    /// endpoints. Configured endpoints are bounded and trusted, so they are
    /// exempt from the hint-channel cap; only wire-supplied hint endpoints are
    /// eligible for LRU eviction.
    fn is_configured(&self, endpoint: &str) -> bool {
        self.configured
            .iter()
            .any(|configured| configured == endpoint)
    }

    /// Evict least-recently-used non-configured channels until at most
    /// [`MAX_HINT_CHANNELS`] remain, bounding the resources a peer can pin by
    /// streaming distinct but reachable hint endpoints (issue #341).
    /// `protected` — the endpoint just touched by the caller — is never chosen
    /// as a victim, so a same-instant `last_used` tie cannot evict the very
    /// slot we are about to hand out.
    ///
    /// Evicting only drops the map's reference; a caller mid-dial holds its own
    /// clone of the `Arc<OnceCell<Channel>>`, so the channel survives until that
    /// caller drops it — eviction merely forces a future re-dial, matching the
    /// [`Self::evict_if_current`] contract.
    fn enforce_hint_cap(&self, channels: &mut HashMap<String, PooledChannel>, protected: &str) {
        // Record that the sweep ran so tests can assert it fires only on a real
        // insert; the count is otherwise invisible in the map's final state.
        #[cfg(test)]
        self.hint_cap_sweeps.fetch_add(1, Ordering::Relaxed);
        loop {
            let non_configured = channels
                .keys()
                .filter(|endpoint| !self.is_configured(endpoint))
                .count();
            if non_configured <= MAX_HINT_CHANNELS {
                return;
            }
            let victim = channels
                .iter()
                .filter(|(endpoint, _)| {
                    endpoint.as_str() != protected && !self.is_configured(endpoint)
                })
                .min_by_key(|(_, slot)| slot.last_used)
                .map(|(endpoint, _)| endpoint.clone());
            match victim {
                Some(endpoint) => {
                    channels.remove(&endpoint);
                }
                // Only the protected slot remains evictable; leave it so the
                // caller still gets its channel (cap is restored on the next
                // insert against a different endpoint).
                None => return,
            }
        }
    }

    /// The retry loop's dial order: the fresh cached leader first (if any),
    /// then the configured endpoints in genuine round-robin order.
    ///
    /// Each call advances a per-pool cursor and starts the configured tail at
    /// `cursor % configured.len()`, so successive cold-cache worklists from
    /// one pool rotate across nodes rather than always opening on
    /// `configured[0]`. The cursor is seeded randomly per pool (see the
    /// `rotation` field), which is what spreads a *fleet* cold start — every
    /// client otherwise starts at the same offset. The cached leader, when
    /// fresh, is still dialed first and is removed from the rotated tail so it
    /// is not dialed twice.
    pub fn iter_round_robin(&self) -> Vec<String> {
        let leader = self.cached_leader();
        let mut endpoints = Vec::with_capacity(self.configured.len() + 1);
        if let Some(leader_endpoint) = &leader {
            endpoints.push(leader_endpoint.clone());
        }
        // Guard the modulo: an empty configured list has nothing to rotate
        // (the hint-only path already pushed the leader above).
        if self.configured.is_empty() {
            return endpoints;
        }
        let offset = self.rotation.fetch_add(1, Ordering::Relaxed) % self.configured.len();
        for endpoint in self.configured[offset..]
            .iter()
            .chain(&self.configured[..offset])
        {
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
        pool.pin_rotation_for_test(0);
        pool.record_success("b:1", 1);
        let order = pool.iter_round_robin();
        assert_eq!(order, vec!["b:1", "a:1", "c:1"]);
    }

    #[test]
    fn iter_without_cache_rotates_configured() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into(), "c:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.pin_rotation_for_test(0);
        // Successive cold-cache worklists rotate the configured order; the
        // offset wraps at `configured.len()`.
        assert_eq!(pool.iter_round_robin(), vec!["a:1", "b:1", "c:1"]); // offset 0
        assert_eq!(pool.iter_round_robin(), vec!["b:1", "c:1", "a:1"]); // offset 1
        assert_eq!(pool.iter_round_robin(), vec!["c:1", "a:1", "b:1"]); // offset 2
        assert_eq!(pool.iter_round_robin(), vec!["a:1", "b:1", "c:1"]); // offset 3 % 3
    }

    /// The starting offset comes from the (seeded) cursor, not a hardcoded
    /// zero — this is the mechanism behind fleet cold-start distribution. The
    /// randomness of the production seed is trusted and intentionally not
    /// asserted (a "different pools differ" test would be a CI-flake risk).
    #[test]
    fn iter_round_robin_honors_seeded_offset() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into(), "c:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.pin_rotation_for_test(1);
        assert_eq!(pool.iter_round_robin(), vec!["b:1", "c:1", "a:1"]);
    }

    /// An empty configured list must not panic on the `% configured.len()`
    /// rotation (modulo-by-zero) and yields an empty worklist; a cached
    /// hint-only leader is still surfaced.
    #[test]
    fn iter_round_robin_empty_configured_does_not_panic() {
        let pool = ChannelPool::new(Vec::new(), None, false, RetryPolicy::default());
        assert!(pool.iter_round_robin().is_empty());
        pool.record_success("hint:1", 1);
        assert_eq!(pool.iter_round_robin(), vec!["hint:1"]);
    }

    /// A single configured endpoint is the degenerate rotation case (offset is
    /// always `0 % 1 == 0`): every worklist is just that endpoint, and a
    /// cached leader equal to it is emitted once, never duplicated.
    #[test]
    fn iter_round_robin_single_configured_endpoint() {
        let pool = ChannelPool::new(vec!["a:1".into()], None, false, RetryPolicy::default());
        pool.pin_rotation_for_test(0);
        assert_eq!(pool.iter_round_robin(), vec!["a:1"]);
        assert_eq!(pool.iter_round_robin(), vec!["a:1"]);
        pool.record_success("a:1", 1);
        assert_eq!(pool.iter_round_robin(), vec!["a:1"]);
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

        // Seat the cache at `b:1` epoch 5. A *confirmed* RPC could not
        // cross-seat over the fresh `a:1` unknown-epoch entry — that defense is
        // issue H2 — but a hint carrying a known epoch is new information and
        // seats over an unknown-epoch cache.
        assert!(pool.compare_and_set_leader("b:1".into(), Some(5)));
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));

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

    /// A hint naming the *same* endpoint as the cached leader is accepted and
    /// holds the higher epoch, even when the hint's epoch is below the cached
    /// one. Unifying both writers behind `seat_leader` made the same-endpoint
    /// path uniform with `record_success`: the endpoint is the cached leader
    /// either way, so it keeps its slot (and its higher epoch) and the worklist
    /// still steers to it. The old `compare_and_set_leader` returned `false`
    /// here (a `StaleLeaderHint`); the unified rule returns `true`.
    #[test]
    fn same_endpoint_lower_epoch_hint_is_accepted_and_holds_higher_epoch() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 9);
        assert!(pool.compare_and_set_leader("a:1".into(), Some(4)));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        assert_eq!(
            pool.fresh_leader().expect("cache seated").epoch,
            Some(9),
            "a same-endpoint hint must not lower the cached epoch"
        );
    }

    /// A same-endpoint *epoch-less* hint must not downgrade a known cached
    /// epoch to `None`. The old `compare_and_set_leader` routed a same-endpoint
    /// `(Some, None)` pair through the configured-list arm and overwrote the
    /// entry, dropping the epoch; the unified `merge_epoch_forward` keeps it.
    #[test]
    fn same_endpoint_epochless_hint_keeps_known_epoch() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        pool.record_success("a:1", 9);
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        assert_eq!(
            pool.fresh_leader().expect("cache seated").epoch,
            Some(9),
            "an epoch-less same-endpoint hint must not drop the known epoch"
        );
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
        pool.pin_rotation_for_test(0);
        // Fresh cache prepends leader c; the offset-0 tail is [a, b].
        pool.record_success("c:1", 1);
        assert_eq!(pool.iter_round_robin(), vec!["c:1", "a:1", "b:1"]);
        // Advance virtual time past the TTL; the entry now reads as absent.
        tokio::time::advance(std::time::Duration::from_millis(75)).await;
        // No prepend now: the worklist is the pure offset-1 rotation, in which
        // c sits mid-list rather than being forced to the front.
        assert!(pool.cached_leader().is_none());
        assert_eq!(pool.iter_round_robin(), vec!["b:1", "c:1", "a:1"]);
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

    /// The epoch-less flap (issue H2): a `None`-epoch cache entry — seated by
    /// an epoch-less leader hint or at bootstrap — has no monotone floor, so a
    /// late, genuinely-stale success against a *different*, now-deposed leader
    /// must still not clobber it while it is fresh. Before the fix the
    /// cross-endpoint reject arm only fired when the cached epoch was known
    /// (`is_some_and`), so a `None`-epoch entry fell straight through to the
    /// replace arm — exactly the backward flap the monotone-forward gate
    /// exists to prevent, unguarded in the deployment (epoch-less / mixed
    /// version) where it matters. A success is a *past* proof whose completion
    /// can be arbitrarily delayed, so when it cannot prove it outranks the
    /// fresh entry the entry is defended.
    #[test]
    fn record_success_different_endpoint_does_not_clobber_fresh_unknown_epoch_cache() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        // Seat `a:1` with an *unknown* epoch via an epoch-less hint (the
        // mixed-version / bootstrap path).
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        // A late success against a different, now-deposed leader must not
        // unseat the fresh entry, even though the cache has no epoch to rank.
        pool.record_success("b:1", 5);
        assert_eq!(
            pool.cached_leader().as_deref(),
            Some("a:1"),
            "a stale cross-endpoint success must not clobber a fresh unknown-epoch entry"
        );
    }

    /// The issue's literal "both epoch-less" case: an epoch-less cache plus a
    /// success that carries the wire's zero epoch (an epoch-less / old server
    /// sends `(0, 0)`, which reassembles to `Some(0)`). A different-endpoint
    /// success still must not flap the fresh entry backward.
    #[test]
    fn record_success_epochless_does_not_clobber_fresh_unknown_epoch_cache() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        // Epoch-less server: the success reassembles to epoch 0.
        pool.record_success("b:1", 0);
        assert_eq!(
            pool.cached_leader().as_deref(),
            Some("a:1"),
            "an epoch-0 cross-endpoint success must not clobber a fresh unknown-epoch entry"
        );
    }

    /// The flip side: once the unknown-epoch entry ages past `leader_ttl` it
    /// imposes no floor, so a different-endpoint success seats freely — the
    /// defense only guards a *fresh* entry, and the cache self-heals at TTL.
    #[tokio::test(start_paused = true)]
    async fn record_success_seats_against_unknown_epoch_cache_once_expired() {
        let policy = RetryPolicy {
            leader_ttl: std::time::Duration::from_millis(50),
            ..RetryPolicy::default()
        };
        let pool = ChannelPool::new(vec!["a:1".into(), "b:1".into()], None, false, policy);
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        // Fresh: the different-endpoint success is rejected.
        pool.record_success("b:1", 5);
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        // Advance past the TTL; the unknown-epoch entry is now stale.
        tokio::time::advance(std::time::Duration::from_millis(75)).await;
        // No floor remains, so the same success now seats.
        pool.record_success("b:1", 5);
        assert_eq!(pool.cached_leader().as_deref(), Some("b:1"));
        assert_eq!(pool.fresh_leader().expect("cache seated").epoch, Some(5));
    }

    /// A success against the *same* endpoint as a fresh unknown-epoch entry
    /// upgrades it to the observed epoch (and refreshes the TTL) — the
    /// bootstrap-then-first-success path. The endpoint is unchanged, so there
    /// is no flap concern; the cache simply gains the epoch it was missing.
    #[test]
    fn record_success_same_endpoint_upgrades_unknown_epoch() {
        let pool = ChannelPool::new(
            vec!["a:1".into(), "b:1".into()],
            None,
            false,
            RetryPolicy::default(),
        );
        assert!(pool.compare_and_set_leader("a:1".into(), None));
        assert_eq!(pool.fresh_leader().expect("cache seated").epoch, None);
        pool.record_success("a:1", 7);
        assert_eq!(pool.cached_leader().as_deref(), Some("a:1"));
        assert_eq!(
            pool.fresh_leader().expect("cache seated").epoch,
            Some(7),
            "a same-endpoint success must upgrade an unknown cached epoch"
        );
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
                .cell
                .get()
                .is_some(),
            "the retained cell must be initialized"
        );
    }

    /// A stream of distinct but *reachable* wire-supplied hint endpoints must
    /// not grow the channel map without bound (issue #341). The failed-dial
    /// leak (#290) and transport-failure eviction (#239) only reclaim on
    /// failure; a hint that dials *successfully* and never has a transport
    /// failure was retained forever, each entry pinning a live `Channel` and
    /// its background reconnect task. Non-configured (hint-derived) entries are
    /// now capped at `MAX_HINT_CHANNELS`, so a peer pointing at live decoys
    /// cannot pin unbounded resources.
    #[tokio::test]
    async fn successful_hint_dials_are_bounded() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        // No configured endpoints: every dial below is a hint-derived entry.
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        for i in 0..50 {
            let endpoint = format!("decoy-{i}:1");
            pool.client(&endpoint)
                .await
                .expect("the success connector must seat a channel");
        }

        let guard = pool.channels.lock();
        assert!(
            guard.len() <= MAX_HINT_CHANNELS,
            "non-configured channels must be capped at {MAX_HINT_CHANNELS}, found {}",
            guard.len()
        );
    }

    /// Configured (operator-supplied) endpoints are exempt from the hint cap:
    /// they are bounded and trusted, so a flood of distinct hint endpoints must
    /// never evict them. The total map stays bounded at
    /// `configured.len() + MAX_HINT_CHANNELS`.
    #[tokio::test]
    async fn configured_endpoints_are_never_evicted() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        let pool = ChannelPool::new(
            vec!["cfg-a:1".into(), "cfg-b:1".into()],
            Some(connector),
            false,
            RetryPolicy::default(),
        );
        // Seat the configured endpoints, then never touch them again.
        pool.client("cfg-a:1").await.expect("dial cfg-a");
        pool.client("cfg-b:1").await.expect("dial cfg-b");
        // Flood with distinct, reachable hint endpoints.
        for i in 0..50 {
            pool.client(&format!("decoy-{i}:1"))
                .await
                .expect("dial decoy");
        }

        let guard = pool.channels.lock();
        assert!(
            guard.contains_key("cfg-a:1") && guard.contains_key("cfg-b:1"),
            "configured endpoints must survive a hint flood"
        );
        assert!(
            guard.len() <= 2 + MAX_HINT_CHANNELS,
            "total map size must stay bounded at configured.len() + cap, found {}",
            guard.len()
        );
    }

    /// LRU policy: an endpoint dialed on every round (standing in for the
    /// active leader, which is dialed on each request) stays most-recently-used
    /// and survives, while older idle hint endpoints are evicted. This is what
    /// distinguishes LRU from a naive insertion-order/FIFO eviction — the
    /// leader is seated *first*, so a FIFO policy would discard it.
    #[tokio::test]
    async fn lru_retains_the_repeatedly_dialed_endpoint() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        // Seat the "leader" first so a FIFO policy would evict it earliest.
        pool.client("leader:1").await.expect("dial leader");
        for i in 0..50 {
            pool.client(&format!("decoy-{i}:1"))
                .await
                .expect("dial decoy");
            // Touch the leader every round, keeping it most-recently-used.
            pool.client("leader:1").await.expect("redial leader");
        }

        let guard = pool.channels.lock();
        assert!(
            guard.contains_key("leader:1"),
            "the repeatedly-dialed endpoint must survive LRU eviction"
        );
        assert!(
            guard.len() <= MAX_HINT_CHANNELS,
            "non-configured channels must stay capped, found {}",
            guard.len()
        );
    }

    /// The cap sweep is the pool's hottest-path cost — an O(n) `keys().filter()`
    /// scan (plus a `min_by_key` on overflow) under the synchronous map lock —
    /// and it can only ever evict on a *new* insert: a cache hit reuses an
    /// existing slot, so the non-configured count is unchanged and already
    /// `<= MAX_HINT_CHANNELS` from the insert that seated it. The sweep is
    /// therefore gated to run only when a brand-new slot is added. That contract
    /// is invisible in the map's final state — a cache-hit sweep would be a
    /// guaranteed no-op — so this asserts it directly via the sweep counter:
    /// each genuine insert bumps it, each cache hit leaves it untouched.
    #[tokio::test]
    async fn cap_sweep_runs_on_insert_not_on_cache_hit() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        // No configured endpoints, so every dial below is a hint-derived slot
        // eligible for the cap sweep.
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        // First dial of `a:1` inserts a slot, so the cap sweep runs once.
        pool.client("a:1").await.expect("insert a:1");
        assert_eq!(
            pool.hint_cap_sweep_count(),
            1,
            "the first dial inserts a slot and must sweep once"
        );

        // Re-dialing `a:1` is a cache hit: no new slot, so no sweep.
        pool.client("a:1").await.expect("cache hit a:1");
        assert_eq!(
            pool.hint_cap_sweep_count(),
            1,
            "a cache hit must not run the O(n)-under-lock cap sweep"
        );

        // A different endpoint inserts again, so the sweep runs a second time.
        pool.client("b:1").await.expect("insert b:1");
        assert_eq!(
            pool.hint_cap_sweep_count(),
            2,
            "a dial against a brand-new endpoint must sweep"
        );

        // Cache hits on either seated endpoint still add no sweeps.
        pool.client("a:1").await.expect("cache hit a:1");
        pool.client("b:1").await.expect("cache hit b:1");
        assert_eq!(
            pool.hint_cap_sweep_count(),
            2,
            "cache hits never sweep, regardless of which seated slot is reused"
        );
    }

    /// The cap sweep is gated on insertion, but the `last_used` recency refresh
    /// must stay *unconditional*: it is what keeps the active leader — re-dialed
    /// on every request and thus almost always a cache hit — most-recently-used,
    /// so the next insert's sweep never picks it as a victim (issue #341). This
    /// guards that the insert gate did not also swallow the refresh: a cache hit
    /// advances the slot's `last_used` while running no sweep and leaving the
    /// map size unchanged. Were the refresh folded under the same gate, the
    /// leader would age and the eviction protection of issue #341 would reopen.
    #[tokio::test(start_paused = true)]
    async fn cache_hit_refreshes_recency_without_sweeping() {
        let connector: Arc<crate::transport::ChannelConnector> = Arc::new(|_endpoint: &str| {
            Box::pin(async {
                Ok(tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy())
            })
        });
        let pool = ChannelPool::new(Vec::new(), Some(connector), false, RetryPolicy::default());

        pool.client("a:1").await.expect("insert a:1");
        let seated_at = pool
            .channels
            .lock()
            .get("a:1")
            .expect("a:1 must be seated")
            .last_used;
        let sweeps_after_insert = pool.hint_cap_sweep_count();

        // Let virtual time pass, then hit the cache for the same endpoint.
        tokio::time::advance(std::time::Duration::from_millis(10)).await;
        pool.client("a:1").await.expect("cache hit a:1");

        let guard = pool.channels.lock();
        assert_eq!(guard.len(), 1, "a cache hit must not grow the map");
        assert!(
            guard
                .get("a:1")
                .expect("a:1 must still be seated")
                .last_used
                > seated_at,
            "a cache hit must refresh last_used unconditionally"
        );
        drop(guard);
        assert_eq!(
            pool.hint_cap_sweep_count(),
            sweeps_after_insert,
            "the recency refresh must not drag the cap sweep along with it"
        );
    }
}
