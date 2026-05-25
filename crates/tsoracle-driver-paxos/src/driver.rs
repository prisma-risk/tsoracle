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

//! `ConsensusDriver` impl that wraps any [`PaxosHighWaterHost`].
//!
//! The driver layer is thin: it derives the fence-aligned epoch from
//! the OmniPaxos handle on every leader observation and on every fence
//! check, so the value the server sees matches the value
//! `persist_high_water` compares against.

use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
    /// gated by `stream_active` so at most one is live at a time: a *sequential*
    /// re-subscription (e.g. an in-process restart that fully stops the prior
    /// server before starting the replacement) is well-defined, while a
    /// *concurrent* second subscription fails closed.
    leader_subscriber: LeaderEventSubscriber,
    /// Single-active-stream lease. `leadership_events` flips this `false -> true`
    /// when it hands out a live stream and the stream flips it back on `Drop`.
    /// A second call observing `true` returns a fail-closed empty stream instead
    /// of a second live one, so two `Server`s built from one `Arc<PaxosDriver>`
    /// cannot both seed allocators and issue overlapping timestamps. Shared via
    /// `Arc` so the released-on-`Drop` guard outlives the borrow that minted it.
    stream_active: Arc<AtomicBool>,
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
    /// state, but only one such stream may be live at a time (see `stream_active`):
    /// a *sequential* re-subscription after the prior stream is dropped is
    /// well-defined, while a *concurrent* second subscription fails closed.
    pub fn new(host: H, leader_subscriber: LeaderEventSubscriber) -> Self {
        Self {
            host,
            leader_subscriber,
            stream_active: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Borrow the wrapped host for direct interaction (e.g., to call
    /// `omnipaxos()` from outside the driver path).
    pub fn host(&self) -> &H {
        &self.host
    }
}

#[async_trait]
impl<H> ConsensusDriver for PaxosDriver<H>
where
    H: PaxosHighWaterHost,
{
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        // Single-active-stream lease. Acquire it before minting a stream so the
        // driver hands out at most one live leadership stream at a time.
        //
        // A *sequential* re-subscription (the prior stream already dropped, e.g.
        // an in-process restart) re-acquires the freed lease and is live. A
        // *concurrent* second subscription (two `Server`s built from one
        // `Arc<PaxosDriver>`, or a replacement `Server` started before the old
        // one's stream is dropped) observes the lease held and FAILS CLOSED: it
        // returns an empty stream. The server's leader-watch loop reads that EOF
        // as `WatchStreamClosed`, poisons serving state, and stays `NotServing`,
        // so the second server never seeds an independent allocator and the two
        // endpoints cannot issue overlapping (duplicate) timestamps.
        if self
            .stream_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            tracing::error!(
                "PaxosDriver::leadership_events called while a leadership stream is already \
                 active; refusing the concurrent second subscription and returning a \
                 fail-closed empty stream. Build exactly one Server per consensus driver, and \
                 fully stop a server (drop its WatchGuard) before starting a replacement."
            );
            return Box::pin(futures::stream::empty());
        }
        // Released on `Drop` of the returned stream, which re-opens the lease for
        // the next (sequential) subscription.
        let lease = StreamLease {
            active: Arc::clone(&self.stream_active),
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
/// Held by the live [`LeasedStream`] `leadership_events` returns. Storing
/// `false` back on `Drop` is what permits *sequential* re-subscription: once the
/// live stream is dropped the next `leadership_events` call re-acquires the lease
/// and mints a fresh live stream.
struct StreamLease {
    active: Arc<AtomicBool>,
}

impl Drop for StreamLease {
    fn drop(&mut self) {
        // Release pairs with the `Acquire` failure-ordering on the next
        // `compare_exchange`, so a re-subscription observes the freed lease.
        self.active.store(false, Ordering::Release);
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
            matches!(err, ConsensusError::PermanentDriver(_)),
            "out-of-range advance must classify as PermanentDriver, got {err:?}"
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

    #[tokio::test]
    async fn load_high_water_delegates_to_host() {
        let host = StubHost::new();
        let (_sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);
        assert_eq!(driver.load_high_water().await.unwrap(), 0);
    }
}
