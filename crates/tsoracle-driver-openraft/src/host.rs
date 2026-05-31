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

//! Host abstraction the openraft-backed `ConsensusDriver` builds on.
//!
//! Implementations decide where the high-water mark is stored and replicated.
//! The bundled [`StandaloneHost`](crate::standalone::StandaloneHost) owns its
//! own raft cluster + state machine. A larger service (e.g. one that already
//! runs an openraft cluster for other state) can implement this trait directly
//! and route TSO commands through its existing log.

use async_trait::async_trait;
use openraft::RaftMetrics;
use openraft::RaftTypeConfig;
use openraft::type_config::alias::WatchReceiverOf;
use tsoracle_consensus::ConsensusError;

/// Host that knows how to read and advance the TSO high-water mark via openraft.
///
/// The driver crate handles `ConsensusDriver` trait shape and leadership-event
/// mapping; the host supplies the storage / submission semantics.
#[async_trait]
pub trait OpenraftHighWaterHost: Send + Sync + 'static {
    /// The host's `RaftTypeConfig`. Each host picks its own — the standalone
    /// host uses [`crate::TypeConfig`], a piggy-back host uses whatever
    /// `TypeConfig` its larger raft is built on.
    type Config: RaftTypeConfig;

    /// The metrics watch the driver reads leadership transitions from.
    ///
    /// This is the only window the driver needs onto the host's raft, so the
    /// trait hands out the watch receiver directly rather than a
    /// `&Raft<C, SM>`. A piggy-back host whose raft binds to its own state
    /// machine can satisfy the trait without that state machine having to match
    /// the standalone host's — the trait carries no `StateMachine` type. Hosts
    /// that own a `Raft` implement this as `self.raft.metrics()`.
    fn metrics(&self) -> WatchReceiverOf<Self::Config, RaftMetrics<Self::Config>>;

    /// Read the current high-water mark linearizably. Implementations are
    /// expected to issue an `ensure_linearizable` barrier before reading their
    /// underlying state machine if reads should reflect all committed writes.
    async fn current_high_water(&self) -> Result<u64, ConsensusError>;

    /// Submit a "bump to at_least" proposal through the host's raft log and
    /// return the new high-water value after apply. The state machine must
    /// enforce monotonicity (`max(prev, at_least)`); the driver does not.
    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError>;

    /// The active write version (read from the shared `ActiveWriteVersion`
    /// cell). The driver gates `advance_dense` on this being >=
    /// `DENSE_WRITE_VERSION`.
    fn active_write_version(&self) -> u8;

    /// Submit an `AdvanceDense { key, count }` proposal and return the applied
    /// block start. The proposer reads `ApplyOutcome` from the `client_write`
    /// response: `DenseAdvanced { start }` → `Ok(start)`; the two rejection
    /// outcomes map to the corresponding `ConsensusError`.
    async fn submit_advance_dense(
        &self,
        key: &tsoracle_core::SeqKey,
        count: u32,
    ) -> Result<u64, ConsensusError>;

    /// Submit an `AdvanceDenseBatch { entries }` proposal and return the applied
    /// per-key block starts in request order. The proposer reads `ApplyOutcome`
    /// from the `client_write` response: `DenseBatchAdvanced { starts }` →
    /// `Ok(starts)`; the two rejection outcomes map to the corresponding
    /// `ConsensusError`.
    async fn submit_advance_dense_batch(
        &self,
        entries: &[(tsoracle_core::SeqKey, u32)],
    ) -> Result<Vec<u64>, ConsensusError>;

    /// Read a key's durably-committed dense counter (0 if absent),
    /// linearized. Implementations issue the same read barrier as
    /// `current_high_water` before reading the local state machine.
    async fn current_dense_seq(&self, key: &tsoracle_core::SeqKey) -> Result<u64, ConsensusError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_config::TypeConfig;
    use openraft::RaftMetrics;
    use openraft::type_config::TypeConfigExt;
    use openraft::type_config::alias::WatchReceiverOf;

    /// A host backed *only* by a metrics watch receiver — it owns no
    /// `Raft<TypeConfig, HighWaterStateMachine>`. This is the piggy-back shape
    /// the narrowed trait must admit: a larger service whose raft binds to its
    /// own state machine can satisfy the trait because the driver only needs a
    /// metrics watch, never a `&Raft<C, SM>`.
    struct MetricsOnlyHost {
        rx: WatchReceiverOf<TypeConfig, RaftMetrics<TypeConfig>>,
    }

    #[async_trait]
    impl OpenraftHighWaterHost for MetricsOnlyHost {
        type Config = TypeConfig;

        fn metrics(&self) -> WatchReceiverOf<Self::Config, RaftMetrics<Self::Config>> {
            self.rx.clone()
        }

        async fn current_high_water(&self) -> Result<u64, ConsensusError> {
            Ok(0)
        }

        async fn submit_advance(&self, _at_least: u64) -> Result<u64, ConsensusError> {
            Ok(0)
        }

        fn active_write_version(&self) -> u8 {
            tsoracle_openraft_toolkit::BASELINE_WRITE_VERSION
        }

        async fn submit_advance_dense(
            &self,
            _key: &tsoracle_core::SeqKey,
            _count: u32,
        ) -> Result<u64, ConsensusError> {
            Err(ConsensusError::DenseUnsupported)
        }

        async fn submit_advance_dense_batch(
            &self,
            _entries: &[(tsoracle_core::SeqKey, u32)],
        ) -> Result<Vec<u64>, ConsensusError> {
            Err(ConsensusError::DenseUnsupported)
        }

        async fn current_dense_seq(
            &self,
            _key: &tsoracle_core::SeqKey,
        ) -> Result<u64, ConsensusError> {
            Err(ConsensusError::DenseUnsupported)
        }
    }

    /// Compile-time proof the trait is satisfiable without a `StateMachine`
    /// associated type or a `&Raft<C, SM>` accessor.
    fn assert_implements_host<H: OpenraftHighWaterHost>() {}

    #[tokio::test]
    async fn metrics_only_host_satisfies_and_drives_trait() {
        assert_implements_host::<MetricsOnlyHost>();

        // The narrowed accessor yields a usable receiver built from a synthetic
        // metrics watch — no raft cluster required.
        let metrics: RaftMetrics<TypeConfig> = RaftMetrics::new_initial(1u64);
        let (_tx, rx) = <TypeConfig as TypeConfigExt>::watch_channel(metrics);
        let host = MetricsOnlyHost { rx };

        // Drive every method on the trait surface, not just `metrics()`, to
        // prove a host owning no `Raft<C, HighWaterStateMachine>` is fully
        // usable through the trait — type-checking alone wouldn't exercise the
        // async read/submit path.
        let _rx = host.metrics();
        assert!(host.current_high_water().await.is_ok());
        assert!(host.submit_advance(7).await.is_ok());
        let _ = host.active_write_version();
        let key = tsoracle_core::SeqKey::try_new("k").unwrap();
        assert!(host.submit_advance_dense(&key, 1).await.is_err());
        assert!(
            host.submit_advance_dense_batch(&[(key.clone(), 1)])
                .await
                .is_err()
        );
        assert!(host.current_dense_seq(&key).await.is_err());
    }
}
