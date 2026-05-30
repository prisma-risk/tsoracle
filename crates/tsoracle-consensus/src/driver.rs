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
///
/// **Contract — cancellation & latency (`load_high_water` / `persist_high_water`):**
/// the server drives these inside its leader-watch fence, which it may stop at
/// any time — most importantly on graceful shutdown. That stop is cooperative
/// and observed only *between* fence steps, never inside an in-flight call, so a
/// method that blocks indefinitely (e.g. waiting out a lost quorum) stalls the
/// fence and, on shutdown, forces the server to abort the watch task once its
/// shutdown grace elapses, dropping the in-flight future. Implementations
/// therefore MUST honour two rules:
///   * **Cancel-safety.** Dropping an in-flight `load_high_water` /
///     `persist_high_water` future MUST NOT corrupt durable state. A
///     `persist_high_water` whose proposal was already submitted MAY still
///     commit after its future is dropped; the monotonic-advance contract on
///     that method makes the late apply safe — it can only raise the
///     high-water, never regress it.
///   * **Latency bound.** Prefer returning [`ConsensusError::TransientDriver`]
///     once a round-trip exceeds a sane internal deadline over blocking
///     forever. The fence's retry loop is cancel-observant: a `TransientDriver`
///     lets it back off, race the leadership stream, and honour a pending stop,
///     whereas an unbounded await can only be broken by a forced abort during
///     shutdown.
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
    ///
    /// **Contract — `Follower` payload shape:** the `leader_endpoint` and
    /// `leader_epoch` a driver places on [`LeaderState::Follower`] are
    /// load-bearing for client redirects and carry their own rules — a
    /// scheme-less `host:port` endpoint and a per-node non-decreasing epoch.
    /// Violations are silent in production (dropped redirects); see the
    /// `LeaderState::Follower` field docs for the full contract. The server's
    /// leader-watch task `debug_assert!`s both in debug builds, so a
    /// contract-violating driver trips loudly in its own tests.
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
    /// **Contract — epoch-fencing (driver's choice of mechanism):** the
    /// `epoch` names the leadership term the caller believed it held when it
    /// issued the write. A consensus-backed driver MAY reject a stale-leader
    /// write through *either* of two trait-compliant mechanisms, and which one
    /// it uses is a driver-internal detail callers must not depend on:
    ///   * **Pre-check** — compare `epoch` against the driver's current term
    ///     before proposing and return [`ConsensusError::Fenced`] on mismatch.
    ///     The paxos driver does this against its current ballot-derived epoch
    ///     to avoid wasting a log slot on a write that would be superseded
    ///     downstream anyway.
    ///   * **Apply-time monotonicity + leader-forwarding** — admit the
    ///     proposal and rely on the `max(prev, at_least)` apply (above) plus
    ///     consensus leader-forwarding, which surfaces a superseded leader as
    ///     [`ConsensusError::NotLeader`]. The openraft driver does this and
    ///     ignores `epoch` entirely: a stale write either no-ops under `max`
    ///     or is rejected because the node is no longer leader.
    ///
    /// Both outcomes carry the same meaning to the caller — *this leader is
    /// stale; retry against the new leader* — so callers MUST treat
    /// [`ConsensusError::Fenced`] and [`ConsensusError::NotLeader`]
    /// identically (the server fence collapses them into one step-down arm,
    /// and both map to `FAILED_PRECONDITION` + `LeaderHint`; see
    /// [`ConsensusError`]). Single-node drivers have no term to fence against
    /// and may ignore `epoch`.
    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError>;

    /// Read a key's durably-committed dense counter (0 if absent). Linearized,
    /// like `load_high_water`. Default: unsupported.
    async fn load_dense_seq(&self, key: &tsoracle_core::SeqKey) -> Result<u64, ConsensusError> {
        let _ = key;
        Err(ConsensusError::DenseUnsupported)
    }

    /// Durable, linearized fetch-add for one key. Lazily creates the key at 0 on
    /// first use. Commits `seq[key] += count` and returns the pre-advance value
    /// as the block start. `expected_epoch` fences a stale proposer. Default:
    /// unsupported.
    async fn advance_dense(
        &self,
        key: &tsoracle_core::SeqKey,
        count: u32,
        expected_epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        let (_, _, _) = (key, count, expected_epoch);
        Err(ConsensusError::DenseUnsupported)
    }
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
            // The dense methods are not overridden by `Dummy`, so these exercise
            // the trait's default bodies, which report the capability as absent.
            let key = tsoracle_core::SeqKey::try_new("k").unwrap();
            assert!(
                matches!(
                    driver.load_dense_seq(&key).await,
                    Err(ConsensusError::DenseUnsupported)
                ),
                "default load_dense_seq is DenseUnsupported",
            );
            assert!(
                matches!(
                    driver.advance_dense(&key, 1, Epoch(1)).await,
                    Err(ConsensusError::DenseUnsupported)
                ),
                "default advance_dense is DenseUnsupported",
            );
        });
    }
}
