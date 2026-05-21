//! The `ConsensusDriver` trait — the single injection point for HA and persistence.
//!
//! Consumers implement this trait against their preferred mechanism (openraft,
//! etcd, a single-node file, etc.). The library itself does not run consensus.

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
    /// service address of the current leader, when known.
    Follower { leader_endpoint: Option<String> },
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
    use futures::stream;

    // Compile-time smoke: a trivial implementer fits the trait bounds.
    struct Dummy;

    #[async_trait::async_trait]
    impl ConsensusDriver for Dummy {
        fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
            Box::pin(stream::empty())
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
}
