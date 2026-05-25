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
    /// Re-subscribable handle over the runner's leader-event channel. Each
    /// [`ConsensusDriver::leadership_events`] call mints a fresh stream from it,
    /// so a second subscriber (e.g. an in-process restart) is never blacked out
    /// — mirroring the openraft driver, which re-derives its stream from the
    /// raft metrics watch on every call.
    leader_subscriber: LeaderEventSubscriber,
}

impl<H> PaxosDriver<H>
where
    H: PaxosHighWaterHost,
{
    /// Build a driver around a host.
    ///
    /// `leader_subscriber` is taken from the host (typically via
    /// `StandaloneHost::take_leader_subscriber`). It is re-subscribable: every
    /// call to [`ConsensusDriver::leadership_events`] mints a fresh, live stream
    /// that synchronously yields the current leadership state, so repeated
    /// subscriptions are well-defined rather than a one-shot take.
    pub fn new(host: H, leader_subscriber: LeaderEventSubscriber) -> Self {
        Self {
            host,
            leader_subscriber,
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
        // Mint a fresh stream on every call. The subscriber's first poll yields
        // the channel's current state synchronously (the trait's first-item
        // contract), so a second subscriber sees the live leadership state
        // rather than an empty stream.
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
        Box::pin(mapped)
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
    async fn leadership_events_resubscribes_on_each_call() {
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

        // ...and a SECOND subscription is NOT a silent blackout: it re-derives
        // a fresh stream that synchronously yields the current state too. The
        // pre-fix driver `take()`s the stream once and returns `stream::empty()`
        // here, so this would observe `None`.
        let mut second = driver.leadership_events();
        assert!(
            matches!(second.next().await, Some(LeaderState::Leader { .. })),
            "a second subscription must re-derive a live stream, not blackout",
        );
    }

    #[tokio::test]
    async fn load_high_water_delegates_to_host() {
        let host = StubHost::new();
        let (_sender, subscriber) = leader_event_channel();
        let driver = PaxosDriver::new(host, subscriber);
        assert_eq!(driver.load_high_water().await.unwrap(), 0);
    }
}
