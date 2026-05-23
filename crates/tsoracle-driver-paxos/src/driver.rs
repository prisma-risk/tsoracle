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
use parking_lot::Mutex;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::Epoch;
use tsoracle_paxos_toolkit::lifecycle::LeaderEventStream;

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
    leader_stream: Mutex<Option<LeaderEventStream>>,
}

impl<H> PaxosDriver<H>
where
    H: PaxosHighWaterHost,
{
    /// Build a driver around a host.
    ///
    /// `leader_stream` is taken from the host (typically via
    /// `StandaloneHost::take_leader_stream`). The driver consumes it
    /// once on the first call to [`ConsensusDriver::leadership_events`];
    /// subsequent calls return an empty stream that closes immediately
    /// (the trait permits this since it is documented as a once-per-
    /// lifetime subscription).
    pub fn new(host: H, leader_stream: LeaderEventStream) -> Self {
        Self {
            host,
            leader_stream: Mutex::new(Some(leader_stream)),
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
        let raw = self.leader_stream.lock().take();
        let omnipaxos = self.host.omnipaxos();
        match raw {
            Some(stream) => {
                let mapped = stream.map(move |state| match state {
                    LeaderState::Leader { .. } => {
                        // Re-derive epoch from the host's view of the
                        // current ballot. This is what fence checks see.
                        let ballot = omnipaxos.lock().get_promise();
                        LeaderState::Leader {
                            epoch: encode_epoch(ballot),
                        }
                    }
                    other => other,
                });
                Box::pin(mapped)
            }
            None => Box::pin(futures::stream::empty()),
        }
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.host.current_high_water().await
    }

    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
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
        let (_sender, stream) = leader_event_channel();
        let driver = PaxosDriver::new(host, stream);

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
    async fn persist_with_matching_epoch_calls_submit_advance() {
        let host = StubHost::new();
        let current_ballot = host.omnipaxos().lock().get_promise();
        let current_epoch = encode_epoch(current_ballot);

        let (_sender, stream) = leader_event_channel();
        let driver = PaxosDriver::new(host, stream);

        let result = driver.persist_high_water(99, current_epoch).await;
        // StubHost::submit_advance returns Ok(at_least).
        assert_eq!(result.unwrap(), 99);
    }

    #[tokio::test]
    async fn leadership_events_is_take_once() {
        let host = StubHost::new();
        let (_sender, stream) = leader_event_channel();
        let driver = PaxosDriver::new(host, stream);

        // First call returns a real stream.
        let _first = driver.leadership_events();
        // Second call returns an empty stream that closes immediately.
        let mut second = driver.leadership_events();
        assert!(second.next().await.is_none());
    }

    #[tokio::test]
    async fn load_high_water_delegates_to_host() {
        let host = StubHost::new();
        let (_sender, stream) = leader_event_channel();
        let driver = PaxosDriver::new(host, stream);
        assert_eq!(driver.load_high_water().await.unwrap(), 0);
    }
}
