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

//! `ConsensusDriver` impl that wraps any [`PaxosHighWaterHost`].
//!
//! The driver layer is thin: it derives the fence-aligned epoch from
//! the OmniPaxos handle on every leader observation and on every fence
//! check, so the value the server sees matches the value
//! `persist_high_water` compares against.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_paxos_toolkit::lifecycle::LeaderEventSubscriber;

use crate::host::PaxosHighWaterHost;
use crate::type_config::encode_epoch;

/// `ConsensusDriver` for any `PaxosHighWaterHost`.
///
/// The toolkit's `PaxosRunner` emits leader events with a placeholder
/// epoch (a process-local monotonic counter). This driver maps that
/// stream to emit ballot-derived epochs that match what
/// `persist_high_water`'s fence check sees, so leaders that pass their
/// own epoch back into persist do not fence themselves out.
pub struct PaxosDriver<H>
where
    H: PaxosHighWaterHost,
{
    host: H,
    /// Handle over the runner's leader-event channel. Each
    /// [`ConsensusDriver::leadership_events`] call mints a fresh stream from it,
    /// gated by `active_generation` so at most one is live at a time: a
    /// *sequential* re-subscription (e.g. an in-process restart that fully stops
    /// the prior server before starting the replacement) is well-defined, while
    /// a *concurrent* second subscription fails closed.
    leader_subscriber: LeaderEventSubscriber,
    /// Source of holder generation ids for every
    /// [`ConsensusDriver::leadership_events`] acquisition attempt, starting at
    /// `1` (so `0` is reserved as the "slot free" sentinel). Minted via
    /// [`PaxosDriver::mint_generation`], which skips the `0` the `fetch_add`
    /// yields once per `2^64`-wide overflow so the sentinel is never handed out
    /// as a holder. Distinct live generations are what make the lease's release
    /// ABA-proof: a slot released and re-acquired holds a *different* generation,
    /// so a stale lease can never free a newer holder.
    next_generation: AtomicU64,
    /// Single-active-stream lease slot. `0` means free; any other value is the
    /// id of the generation currently holding a live stream.
    /// `leadership_events` CASes `0 -> my_generation` when it hands out a live
    /// stream, and the stream's [`StreamLease`] CASes `my_generation -> 0` on
    /// `Drop`. A second call observing a non-zero slot returns a fail-closed
    /// empty stream instead of a second live one, so two `Server`s built from
    /// one `Arc<PaxosDriver>` cannot both seed allocators and issue overlapping
    /// timestamps. Shared via `Arc` so the released-on-`Drop` guard outlives the
    /// borrow that minted it.
    active_generation: Arc<AtomicU64>,
}

impl<H> PaxosDriver<H>
where
    H: PaxosHighWaterHost,
{
    /// Build a driver around a host.
    ///
    /// `leader_subscriber` is taken from the host (typically via
    /// `StandaloneHost::take_leader_subscriber`). [`ConsensusDriver::leadership_events`]
    /// mints a fresh, live stream that synchronously yields the current leadership
    /// state, but only one such stream may be live at a time (see `active_generation`):
    /// a *sequential* re-subscription after the prior stream is dropped is
    /// well-defined, while a *concurrent* second subscription fails closed.
    pub fn new(host: H, leader_subscriber: LeaderEventSubscriber) -> Self {
        Self {
            host,
            leader_subscriber,
            // Generation ids start at 1 so the `active_generation` slot can use 0
            // as its unambiguous "free" sentinel.
            next_generation: AtomicU64::new(1),
            active_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Borrow the wrapped host for direct interaction (e.g., to call
    /// `omnipaxos()` from outside the driver path).
    pub fn host(&self) -> &H {
        &self.host
    }

    /// Mint the next holder generation, skipping the reserved `0` sentinel.
    ///
    /// `fetch_add` wraps silently on overflow, so once every `2^64` acquisitions
    /// the counter returns `0`. Handing `0` out as a holder generation would
    /// turn the acquiring `compare_exchange(0, my_generation)` into
    /// `compare_exchange(0, 0)`: a no-op store that "succeeds" while leaving the
    /// slot visibly free, so a concurrent second subscription could also acquire
    /// a live stream and defeat the single-active lease. Skipping the sentinel
    /// keeps the invariant "every minted generation is non-zero" intact; the
    /// extra `fetch_add` fires at most once per wrap, never on the hot path.
    fn mint_generation(&self) -> u64 {
        loop {
            let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
            if generation != 0 {
                return generation;
            }
        }
    }
}

#[async_trait]
impl<H> ConsensusDriver for PaxosDriver<H>
where
    H: PaxosHighWaterHost,
{
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        // Single-active-stream lease. Claim a fresh generation id and try to
        // occupy the lease slot before minting a stream, so the driver hands out
        // at most one live leadership stream at a time.
        //
        // A *sequential* re-subscription (the prior stream already dropped, e.g.
        // an in-process restart) finds the slot free (`0`) and occupies it with
        // its new generation. A *concurrent* second subscription (two `Server`s
        // built from one `Arc<PaxosDriver>`, or a replacement `Server` started
        // before the old one's stream is dropped) finds the slot held and FAILS
        // CLOSED: it returns an empty stream. The server's leader-watch loop
        // reads that EOF as `WatchStreamClosed`, poisons serving state, and stays
        // `NotServing`, so the second server never seeds an independent allocator
        // and the two endpoints cannot issue overlapping (duplicate) timestamps.
        //
        // A leaked lease (a server torn down without dropping its stream) leaves
        // the slot occupied forever: recovery is operator-driven (process
        // restart), but the rejection is now counted on
        // `tsoracle.leadership_stream.rejected.total` so the stuck node is an
        // alertable signal rather than a single log line.
        let my_generation = self.mint_generation();
        if let Err(active_generation) = self.active_generation.compare_exchange(
            0,
            my_generation,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            #[cfg(feature = "metrics")]
            metrics::counter!("tsoracle.leadership_stream.rejected.total").increment(1);
            tracing::error!(
                rejected_generation = my_generation,
                active_generation,
                "PaxosDriver::leadership_events called while a leadership stream is already \
                 active; refusing the concurrent second subscription and returning a \
                 fail-closed empty stream. Build exactly one Server per consensus driver, and \
                 fully stop a server (drop its WatchGuard) before starting a replacement."
            );
            return Box::pin(futures::stream::empty());
        }
        // Released on `Drop` of the returned stream, which frees the slot for the
        // next (sequential) subscription.
        let lease = StreamLease {
            active: Arc::clone(&self.active_generation),
            generation: my_generation,
        };

        // Mint the live stream. The subscriber's first poll yields the channel's
        // current state synchronously (the trait's first-item contract).
        let stream = self.leader_subscriber.subscribe();
        let omnipaxos = self.host.omnipaxos();
        let mapped = stream.map(move |state| match state {
            LeaderState::Leader { .. } => {
                // Re-derive epoch from the host's view of the current ballot.
                // This is what fence checks see.
                let ballot = omnipaxos.lock().get_promise();
                LeaderState::Leader {
                    epoch: encode_epoch(ballot),
                }
            }
            other => other,
        });
        Box::pin(LeasedStream {
            _lease: lease,
            inner: Box::pin(mapped),
        })
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.host.current_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        // Reject an out-of-range value before the Advance is appended: the
        // apply path computes an unchecked `max(prev, at_least)`, so a decided
        // poison value can never be served and cannot self-heal. This is
        // value-intrinsic, so it precedes the epoch fence — an out-of-range
        // request is permanently bad regardless of which epoch issued it.
        tsoracle_consensus::reject_out_of_range_advance(at_least)?;

        // Fence: reject the call if the supplied epoch does not match
        // the host's current ballot-derived epoch. The check + append
        // are NOT atomic across the OmniPaxos handle, but a stale leader
        // whose ballot has been superseded will see its append rejected
        // downstream anyway; this is a cheap pre-flight to avoid wasting
        // a log slot.
        let current_epoch = {
            let handle = self.host.omnipaxos();
            let guard = handle.lock();
            encode_epoch(guard.get_promise())
        };
        if epoch != current_epoch {
            return Err(ConsensusError::Fenced {
                expected: epoch,
                current: current_epoch,
            });
        }
        self.host.submit_advance(at_least).await
    }
}

/// RAII guard for the driver's single-active-stream lease.
///
/// Held by the live [`LeasedStream`] `leadership_events` returns, and carries the
/// `generation` id that occupies the lease slot. Freeing the slot on `Drop` is
/// what permits *sequential* re-subscription: once the live stream is dropped the
/// next `leadership_events` call finds the slot free and mints a fresh live stream.
struct StreamLease {
    active: Arc<AtomicU64>,
    generation: u64,
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        // Free the slot only if it still holds *our* generation. Under the
        // fail-closed acquire this is always the case, but the conditional store
        // makes release ABA-proof: a stale lease whose generation was already
        // superseded becomes a no-op rather than clobbering a newer holder. The
        // `Release` success-ordering pairs with the `Acquire` failure-ordering on
        // the next acquiring `compare_exchange`, so a re-subscription observes the
        // freed slot.
        let _ =
            self.active
                .compare_exchange(self.generation, 0, Ordering::Release, Ordering::Relaxed);
    }
}

/// The live leadership stream `leadership_events` hands out, pairing the mapped
/// event stream with the [`StreamLease`] that keeps the driver's single-active
/// slot occupied for exactly as long as this stream lives.
///
/// Both fields are `Unpin` (the lease is a plain `Arc` handle; `inner` is an
/// already-pinned box), so the `Stream` impl projects to `inner` without
/// `unsafe` or a pin-projection dependency.
struct LeasedStream {
    _lease: StreamLease,
    inner: Pin<Box<dyn Stream<Item = LeaderState> + Send>>,
}

impl Stream for LeasedStream {
    type Item = LeaderState;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_entry::HighWaterCommand;
    use omnipaxos::OmniPaxos;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use tsoracle_paxos_toolkit::lifecycle::leader_event_channel;
    use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;

    /// Install a process-global `TRACE` subscriber so the driver's
    /// `tracing::warn!` sites format their fields under test instead of
    /// short-circuiting. Idempotent across tests (see the client crate's twin).
    fn enable_tracing() {
        use tracing_subscriber::filter::LevelFilter;
        let _ = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::TRACE)
            .with_test_writer()
            .try_init();
    }

    // A minimal stub host for tests that need to construct a PaxosDriver
    // without booting a real cluster. The omnipaxos handle is real but
    // never ticked; current_high_water / submit_advance return errors.
    struct StubHost {
        omnipaxos: Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>>,
    }

    impl StubHost {
        fn new() -> Self {
            use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
            let cluster_config = ClusterConfig {
                configuration_id: 1,
                nodes: vec![1, 2, 3],
                flexible_quorum: None,
            };
            let server_config = ServerConfig {
                pid: 1,
                ..Default::default()
            };
            let config = OmniPaxosConfig {
                cluster_config,
                server_config,
            };
            let omnipaxos = config
                .build(MemStorage::<HighWaterCommand>::new())
                .expect("build");
            Self {
                omnipaxos: Arc::new(Mutex::new(omnipaxos)),
            }
        }
    }

    #[async_trait]
    impl PaxosHighWaterHost for StubHost {
        type Entry = HighWaterCommand;
        type Storage = MemStorage<HighWaterCommand>;

        fn omnipaxos(
            &self,
        ) -> Arc<Mutex<OmniPaxos<HighWaterCommand, MemStorage<HighWaterCommand>>>> {
            self.omnipaxos.clone()
        }

        async fn current_high_water(&self) -> Result<u64, ConsensusError> {
            Ok(0)
        }

        async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError> {
            Ok(at_least)
        }
    }

    /// `host()` hands back a reference to the wrapped host — the seam piggyback
    /// embedders use to reach their own state machine alongside the high-water
    /// one. Proven by the shared `omnipaxos` handle surviving the move into the
    /// driver.
    #[tokio::test]
    async fn host_accessor_returns_the_wrapped_host() {
        let host = StubHost::new();
        let omnipaxos_handle = host.omnipaxos();
        let (_sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);
        assert!(
            Arc::ptr_eq(&driver.host().omnipaxos(), &omnipaxos_handle),
            "host() must return the same host the driver was constructed with",
        );
    }

    #[tokio::test]
    async fn persist_with_stale_epoch_returns_fenced() {
        let host = StubHost::new();
        let (_sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);

        let stale_epoch = Epoch(0xDEAD_BEEF);
        let result = driver.persist_high_water(42, stale_epoch).await;
        match result {
            Err(ConsensusError::Fenced { expected, current }) => {
                assert_eq!(expected, stale_epoch);
                assert_ne!(current, stale_epoch);
            }
            other => panic!("expected Fenced, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn persist_rejects_out_of_range_before_append() {
        use tsoracle_core::PHYSICAL_MS_MAX;

        let host = StubHost::new();
        let current_ballot = host.omnipaxos().lock().get_promise();
        let current_epoch = encode_epoch(current_ballot);

        let (_sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);

        // The range guard must run before the Advance is appended to the log,
        // so an out-of-range fence value is never durably committed. StubHost's
        // submit_advance echoes Ok(at_least), so a returned Err proves the
        // value was rejected before it reached the append path.
        let err = driver
            .persist_high_water(PHYSICAL_MS_MAX + 1, current_epoch)
            .await
            .expect_err("an out-of-range advance must be rejected, not appended");
        assert!(
            matches!(err, ConsensusError::AdvanceOutOfRange(at_least) if at_least == PHYSICAL_MS_MAX + 1),
            "out-of-range advance must surface as AdvanceOutOfRange carrying the offending value, got {err:?}"
        );

        // The boundary value is in range and still reaches the host.
        assert_eq!(
            driver
                .persist_high_water(PHYSICAL_MS_MAX, current_epoch)
                .await
                .expect("the maximum in-range value must persist"),
            PHYSICAL_MS_MAX
        );
    }

    #[tokio::test]
    async fn persist_with_matching_epoch_calls_submit_advance() {
        let host = StubHost::new();
        let current_ballot = host.omnipaxos().lock().get_promise();
        let current_epoch = encode_epoch(current_ballot);

        let (_sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);

        let result = driver.persist_high_water(99, current_epoch).await;
        // StubHost::submit_advance returns Ok(at_least).
        assert_eq!(result.unwrap(), 99);
    }

    #[tokio::test]
    async fn leadership_events_resubscribes_after_drop() {
        let host = StubHost::new();
        let (sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);

        // Drive the channel to a leader state so a fresh subscription has a
        // non-`Unknown` current value to surface synchronously.
        sender
            .send(LeaderState::Leader { epoch: Epoch(7) })
            .unwrap();

        // The first subscription yields the current leader state...
        let mut first = driver.leadership_events();
        assert!(
            matches!(first.next().await, Some(LeaderState::Leader { .. })),
            "first subscription must yield the current leader state",
        );

        // ...and once it is dropped, the single-active lease is released, so a
        // SEQUENTIAL re-subscription (e.g. an in-process restart that fully
        // stops the prior server before starting the replacement) re-derives a
        // fresh, live stream rather than a one-shot blackout. This is the #396
        // behavior we must preserve.
        drop(first);
        let mut second = driver.leadership_events();
        assert!(
            matches!(second.next().await, Some(LeaderState::Leader { .. })),
            "a sequential re-subscription after drop must re-derive a live stream",
        );
    }

    #[tokio::test]
    async fn leadership_events_second_concurrent_subscription_fails_closed() {
        enable_tracing();
        let host = StubHost::new();
        let (sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);

        sender
            .send(LeaderState::Leader { epoch: Epoch(7) })
            .unwrap();

        // The first subscription holds the single-active lease for as long as
        // its stream is alive.
        let mut first = driver.leadership_events();

        // A SECOND subscription taken while the first is still live must fail
        // closed: it returns an empty stream (yields `None` immediately) rather
        // than a second live stream. The server's leader-watch loop treats that
        // EOF as `WatchStreamClosed` and stays `NotServing`, so two `Server`s
        // built from one `Arc<PaxosDriver>` cannot both seed allocators and
        // issue overlapping timestamps.
        let mut second = driver.leadership_events();
        assert!(
            second.next().await.is_none(),
            "a concurrent second subscription must fail closed with an empty stream",
        );

        // The first stream is unaffected by the rejected second subscription.
        assert!(
            matches!(first.next().await, Some(LeaderState::Leader { .. })),
            "the live first stream must keep yielding after a rejected second subscription",
        );
    }

    #[tokio::test]
    async fn leadership_events_lease_round_trips_across_generations() {
        let host = StubHost::new();
        let (sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);

        sender
            .send(LeaderState::Leader { epoch: Epoch(7) })
            .unwrap();

        // Acquire and drop the lease repeatedly: each generation is live so long
        // as the prior one was fully dropped first. This guards the lease's
        // Drop-release against a single-shot regression.
        for _ in 0..3 {
            let mut stream = driver.leadership_events();
            assert!(
                matches!(stream.next().await, Some(LeaderState::Leader { .. })),
                "each sequential generation must re-derive a live stream",
            );
            drop(stream);
        }
    }

    /// Generation ids are minted with `fetch_add`, which wraps silently on
    /// overflow. The reserved `0` sentinel ("slot free") must never be handed
    /// out as a holder generation: if it were, the acquiring
    /// `compare_exchange(0, my_generation)` becomes `compare_exchange(0, 0)`,
    /// which "succeeds" while leaving the slot visibly free. A concurrent second
    /// subscription would then also acquire a live stream, silently defeating
    /// the single-active lease that stops two `Server`s built from one
    /// `Arc<PaxosDriver>` from seeding allocators and issuing overlapping
    /// timestamps.
    #[tokio::test]
    async fn leadership_events_never_mints_the_zero_sentinel_on_counter_wrap() {
        let host = StubHost::new();
        let (sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);
        sender
            .send(LeaderState::Leader { epoch: Epoch(7) })
            .unwrap();

        // Drive the generation counter to the brink of overflow, then consume
        // the last pre-wrap id so the *next* `fetch_add` wraps the counter to 0
        // — the value that, if minted as a holder generation, collides with the
        // free sentinel.
        driver.next_generation.store(u64::MAX, Ordering::SeqCst);
        drop(driver.leadership_events()); // mints u64::MAX, wraps counter to 0

        // The next acquisition's `fetch_add` returns 0. A correct mint must skip
        // the sentinel so this live stream still occupies the lease slot.
        let mut first = driver.leadership_events();
        assert!(
            matches!(first.next().await, Some(LeaderState::Leader { .. })),
            "the post-wrap subscription must still yield a live stream",
        );
        assert_ne!(
            driver.active_generation.load(Ordering::SeqCst),
            0,
            "a live stream must occupy the lease slot; minting the 0 sentinel \
             would leave it visibly free",
        );

        // ...and because the slot is genuinely occupied, a concurrent second
        // subscription fails closed instead of handing out a second live stream.
        let mut second = driver.leadership_events();
        assert!(
            second.next().await.is_none(),
            "a concurrent second subscription after counter wrap must fail closed",
        );
    }

    #[tokio::test]
    async fn load_high_water_delegates_to_host() {
        let host = StubHost::new();
        let (_sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);
        assert_eq!(driver.load_high_water().await.unwrap(), 0);
    }

    /// `StreamLease::drop` must free the single-active slot only when it still
    /// owns the generation occupying it. This is the ABA guard generation-numbering
    /// buys over a bare boolean: a stale lease whose generation has already been
    /// superseded is a no-op on `Drop`, so it can never clobber a newer holder's
    /// lease. Unreachable through the fail-closed public API today, but correct
    /// by construction — and the property that keeps the lease safe if reused.
    #[test]
    fn stream_lease_drop_frees_slot_only_for_its_own_generation() {
        let slot = Arc::new(AtomicU64::new(0));

        // Generation 9 currently holds the slot.
        slot.store(9, Ordering::SeqCst);

        // A stale lease for an older generation (5) dropping must NOT free gen 9.
        let stale = StreamLease {
            active: Arc::clone(&slot),
            generation: 5,
        };
        drop(stale);
        assert_eq!(
            slot.load(Ordering::SeqCst),
            9,
            "a stale generation's Drop must not free a slot held by a newer generation",
        );

        // The rightful holder's Drop releases the slot back to free (0).
        let rightful = StreamLease {
            active: Arc::clone(&slot),
            generation: 9,
        };
        drop(rightful);
        assert_eq!(
            slot.load(Ordering::SeqCst),
            0,
            "the owning generation's Drop must release the slot",
        );
    }

    /// The fail-closed rejection of a concurrent second subscription must be
    /// observable as a metric, not just a `tracing::error!`. A leaked lease
    /// turns the node into a silent `NotServing` sink; the counter makes that
    /// sink alertable. Mirrors `state_machine.rs::snapshot_health_metrics` —
    /// a thread-local recorder captures real emission, not a mock.
    #[cfg(feature = "metrics")]
    mod rejection_metrics {
        use super::*;
        use metrics_util::{
            MetricKind,
            debugging::{DebugValue, DebuggingRecorder},
        };

        type RecordedMetric = (
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        );

        fn counter(snapshot: &[RecordedMetric], name: &str) -> u64 {
            for (composite, _u, _d, value) in snapshot {
                if composite.kind() == MetricKind::Counter && composite.key().name() == name {
                    if let DebugValue::Counter(n) = value {
                        return *n;
                    }
                }
            }
            0
        }

        #[tokio::test]
        async fn rejected_concurrent_subscription_increments_counter() {
            const REJECTED: &str = "tsoracle.leadership_stream.rejected.total";

            let host = StubHost::new();
            let (sender, subscriber) = leader_event_channel();
            let driver = PaxosDriver::new(host, subscriber);
            sender
                .send(LeaderState::Leader { epoch: Epoch(7) })
                .unwrap();

            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();

            // A single, clean live subscription is not a rejection.
            let first = metrics::with_local_recorder(&recorder, || driver.leadership_events());
            assert_eq!(
                counter(&snapshotter.snapshot().into_vec(), REJECTED),
                0,
                "a lone live subscription must not count as a rejection",
            );

            // A concurrent second subscription fails closed AND is counted.
            let second = metrics::with_local_recorder(&recorder, || driver.leadership_events());
            assert_eq!(
                counter(&snapshotter.snapshot().into_vec(), REJECTED),
                1,
                "a fail-closed rejection must increment the counter",
            );

            // A SEQUENTIAL re-subscription after the first is dropped succeeds and
            // must NOT count — only genuine rejections move the counter.
            drop(first);
            drop(second);
            let _resubscribed =
                metrics::with_local_recorder(&recorder, || driver.leadership_events());
            assert_eq!(
                counter(&snapshotter.snapshot().into_vec(), REJECTED),
                1,
                "a clean sequential re-subscription must not be counted as a rejection",
            );
        }
    }
}
