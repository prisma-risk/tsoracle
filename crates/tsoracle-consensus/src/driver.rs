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

//! The [`ConsensusDriver`] trait: the single injection point for HA and
//! durable persistence.

use core::pin::Pin;

use futures::Stream;
use tsoracle_core::Epoch;

use crate::error::ConsensusError;
use crate::leadership::LeaderState;

/// The single injection point for HA and durable persistence.
///
/// Implementations own leadership election, peer transport, durable storage,
/// and the topology knowledge needed to populate `LeaderHint` on follower
/// redirects. The library never names peers or opens peer sockets.
#[async_trait::async_trait]
pub trait ConsensusDriver: Send + Sync + 'static {
    /// Stream of leadership transitions. The server holds this for its lifetime.
    ///
    /// **Contract — synchronous first item:** the stream MUST emit the current
    /// `LeaderState` as its first item, available on the first poll without
    /// waiting for an external transition; subsequent items reflect later
    /// transitions. The server's leader-watch task blocks on this first item to
    /// seed its state, so a driver that emits the initial state lazily (e.g.
    /// only once the next transition fires) stalls the fence — a node that is
    /// already leader at subscription time would never seed Serving. When no
    /// leader is known yet, emit `LeaderState::Unknown` rather than withholding.
    /// `tokio::sync::watch` + `tokio_stream::wrappers::WatchStream` satisfy this
    /// because the watch is seeded with an initial value the stream yields
    /// immediately.
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>>;

    /// Read the durably-persisted high-water.
    ///
    /// **Contract — linearized:** the returned value must reflect all writes
    /// durably committed before this call started, from any prior leader at
    /// any prior epoch. Consensus-backed implementations satisfy this by
    /// writing a no-op barrier through the consensus log and reading the
    /// state machine after that barrier commits. Single-node implementations
    /// satisfy this trivially.
    async fn load_high_water(&self) -> Result<u64, ConsensusError>;

    /// Durably advance the high-water to **at least** `at_least`. Returns the
    /// actual durably-persisted value, which is `max(stored_value, at_least)`.
    ///
    /// **Contract — monotonic-advance:** a stale or reordered call MUST be
    /// silently absorbed without regression. The trait is "advance to at
    /// least," never "absolute set." For consensus-backed implementations,
    /// the state-machine apply path computes `max(prev, at_least)`.
    ///
    /// The `epoch` lets the driver reject stale-leader writes; single-node
    /// drivers may ignore it.
    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use futures::stream;

    struct Dummy;

    #[async_trait::async_trait]
    impl ConsensusDriver for Dummy {
        fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
            // Honour the first-item contract: synchronously emit the current
            // state. `Dummy` never elects, so that state is `Unknown`, with no
            // transitions to follow. `stream::empty()` would model a contract
            // violation that stalls the server's leader-watch task.
            Box::pin(stream::once(async { LeaderState::Unknown }))
        }
        async fn load_high_water(&self) -> Result<u64, ConsensusError> {
            Ok(0)
        }
        async fn persist_high_water(
            &self,
            at_least: u64,
            _epoch: Epoch,
        ) -> Result<u64, ConsensusError> {
            Ok(at_least)
        }
    }

    #[test]
    fn dummy_is_object_safe() {
        let _: Box<dyn ConsensusDriver> = Box::new(Dummy);
    }

    #[test]
    fn dummy_driver_methods_return_documented_defaults() {
        // Calling each method covers the trait-object dispatch path and
        // confirms the Dummy's contract: the stream synchronously emits the
        // current state (`Unknown`) as its first item then ends, zero
        // high-water, monotonic-advance returns the input. Uses futures'
        // built-in executor to keep the crate free of a tokio dev-dependency.
        let driver: Box<dyn ConsensusDriver> = Box::new(Dummy);
        futures::executor::block_on(async {
            let mut events = driver.leadership_events();
            assert_eq!(
                events.next().await,
                Some(LeaderState::Unknown),
                "first item must be the current state, synchronously",
            );
            assert!(events.next().await.is_none(), "no transitions after");
            assert_eq!(driver.load_high_water().await.unwrap(), 0);
            assert_eq!(
                driver.persist_high_water(42, Epoch(7)).await.unwrap(),
                42,
                "persist returns the at_least argument unchanged",
            );
        });
    }
}
