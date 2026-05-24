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

#![doc = include_str!("../README.md")]
// Panic policy (see CONTRIBUTING.md). `cfg_attr(not(test), ...)` skips the lint
// for the lib's own unit tests; integration tests are separate compilation units.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod docs;

use core::pin::Pin;
use futures::Stream;
use tsoracle_core::Epoch;

/// Leadership state surfaced to the server's leader-watch task.
///
/// `PartialEq`/`Eq` allow drivers to implement payload-aware debounce on a
/// `watch::Sender<LeaderState>`: emitting only when the value (epoch, endpoint)
/// has actually changed, not just when the variant tag differs. A variant-only
/// check would silently drop term advances within a leadership streak and
/// follower-side leader-endpoint changes, both of which downstream consumers
/// must observe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaderState {
    /// This node is the elected leader at the given epoch.
    Leader { epoch: Epoch },
    /// This node is a follower. `leader_endpoint` is the advertised tsoracle
    /// service address of the current leader, when known. `leader_epoch` is
    /// the leader's epoch (raft term) as observed by this follower, used by
    /// clients to reject a stale follower's lower-epoch redirect; `None` when
    /// the driver does not surface it.
    Follower {
        leader_endpoint: Option<String>,
        leader_epoch: Option<Epoch>,
    },
    /// No leader is currently known (election in progress, partition, etc.).
    Unknown,
}

/// Errors returned by `ConsensusDriver` operations.
///
/// Driver implementations classify their internal failures into one of
/// `TransientDriver` or `PermanentDriver`. The server uses that classification
/// directly to pick a gRPC status code:
///
/// | Variant            | gRPC code            | Client expectation         |
/// |--------------------|----------------------|----------------------------|
/// | `NotLeader`        | `FAILED_PRECONDITION` + `LeaderHint` | Retry against the new leader |
/// | `Fenced`           | `FAILED_PRECONDITION` + `LeaderHint` | Retry against the new leader |
/// | `TransientDriver`  | `UNAVAILABLE`        | Safe to retry              |
/// | `PermanentDriver`  | `INTERNAL`           | Do NOT silently retry      |
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    #[error("not leader (current epoch: {observed:?})")]
    NotLeader { observed: Option<Epoch> },
    #[error("epoch fenced: expected {expected:?}, current {current:?}")]
    Fenced { expected: Epoch, current: Epoch },
    /// A driver-level failure the caller MAY retry. Use for errors that are
    /// reasonably expected to clear on their own: storage I/O hiccup, peer
    /// transport flap, transient quorum loss.
    #[error("transient driver error: {0}")]
    TransientDriver(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A driver-level failure the caller MUST NOT silently retry. Use for
    /// persistent local fault: read-only filesystem, corruption, gone
    /// storage device, bad driver implementation, invariant violation.
    #[error("permanent driver error: {0}")]
    PermanentDriver(#[source] Box<dyn std::error::Error + Send + Sync>),
}

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

    #[test]
    fn leader_state_payload_aware_equality() {
        // `PartialEq` is the basis for `watch::Sender`'s debounce, so
        // verify it discriminates on payload, not just variant tag.
        let l1 = LeaderState::Leader { epoch: Epoch(1) };
        let l2 = LeaderState::Leader { epoch: Epoch(2) };
        assert_ne!(l1, l2, "different epochs must compare unequal");
        assert_eq!(l1, LeaderState::Leader { epoch: Epoch(1) });

        let f_known = LeaderState::Follower {
            leader_endpoint: Some("http://node-2".into()),
            leader_epoch: Some(Epoch(4)),
        };
        let f_unknown = LeaderState::Follower {
            leader_endpoint: None,
            leader_epoch: None,
        };
        assert_ne!(
            f_known, f_unknown,
            "follower-leader-changes must surface as inequality",
        );
        // Epoch participates in equality so the watch-debounce re-emits on a
        // follower-side epoch change, not just an endpoint change.
        let f_epoch_5 = LeaderState::Follower {
            leader_endpoint: Some("http://node-2".into()),
            leader_epoch: Some(Epoch(5)),
        };
        assert_ne!(f_known, f_epoch_5, "epoch must discriminate followers");
        assert_ne!(f_known, LeaderState::Unknown);
        assert_eq!(LeaderState::Unknown, LeaderState::Unknown);

        // Debug round-trips through the derive (covers the derive impl).
        let rendered = format!("{l1:?}");
        assert!(rendered.contains("Leader"));
    }

    #[test]
    fn consensus_error_display_text() {
        let not_leader = ConsensusError::NotLeader {
            observed: Some(Epoch(3)),
        };
        assert!(not_leader.to_string().contains("not leader"));

        let fenced = ConsensusError::Fenced {
            expected: Epoch(2),
            current: Epoch(5),
        };
        let fenced_text = fenced.to_string();
        assert!(fenced_text.contains("fenced"));
        assert!(fenced_text.contains('2'));
        assert!(fenced_text.contains('5'));

        let transient = ConsensusError::TransientDriver(Box::new(std::io::Error::other("flap")));
        assert!(transient.to_string().contains("transient"));
        assert!(transient.to_string().contains("flap"));

        let permanent = ConsensusError::PermanentDriver(Box::new(std::io::Error::other("corrupt")));
        assert!(permanent.to_string().contains("permanent"));
        assert!(permanent.to_string().contains("corrupt"));
    }
}
