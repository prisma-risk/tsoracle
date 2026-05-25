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

//! Host abstraction the OmniPaxos-backed `ConsensusDriver` builds on.
//!
//! Implementations decide where the OmniPaxos handle lives, where storage
//! is persisted, and how `current_high_water` / `submit_advance` interact
//! with the underlying paxos log. The bundled [`crate::StandaloneHost`]
//! owns its own OmniPaxos cluster + apply pipeline keyed on
//! [`HighWaterCommand`]. A larger service that already runs OmniPaxos for
//! other state can implement this trait against its existing handle and
//! pick its own [`Entry`](omnipaxos::storage::Entry) — typically an
//! envelope enum that carries `HighWaterCommand` as one variant alongside
//! the service's own commands.

use std::sync::Arc;

use async_trait::async_trait;
use omnipaxos::OmniPaxos;
use omnipaxos::storage::{Entry, Storage};
use parking_lot::Mutex;
use tsoracle_consensus::ConsensusError;

/// Host that knows how to read and advance the TSO high-water mark via
/// OmniPaxos.
///
/// The driver crate handles the `ConsensusDriver` trait shape and
/// leadership-event mapping; the host supplies the entry shape, the
/// storage, and the submission semantics.
#[async_trait]
pub trait PaxosHighWaterHost: Send + Sync + 'static {
    /// The OmniPaxos entry type this host's log replicates.
    ///
    /// The bundled [`crate::StandaloneHost`] sets this to
    /// [`crate::log_entry::HighWaterCommand`] directly. A piggyback host
    /// typically picks a wider envelope enum (e.g.
    /// `MyAppCommand::{App(_), HighWater(HighWaterCommand)}`) so its TSO
    /// proposals ride the same log as its existing commands.
    ///
    /// The `Snapshot: Send` bound lets the driver move the OmniPaxos
    /// handle across `.await` points when mapping leader observations.
    type Entry: Entry<Snapshot: Send> + Send + 'static;

    /// The storage type backing this host's OmniPaxos handle. Each host
    /// picks its own — the standalone host uses the toolkit's
    /// `RocksdbStorage`, a piggyback host uses whatever Storage its
    /// larger OmniPaxos instance is built on.
    type Storage: Storage<Self::Entry> + Send + 'static;

    /// The OmniPaxos handle the driver reads leadership state from.
    fn omnipaxos(&self) -> Arc<Mutex<OmniPaxos<Self::Entry, Self::Storage>>>;

    /// Read the current high-water mark linearizably.
    ///
    /// Implementations append a barrier entry (the standalone host uses
    /// [`crate::log_entry::HighWaterCommand::Barrier`]; piggyback hosts
    /// wrap it in their envelope variant), await the apply task's
    /// notification that the cluster's `decided_idx` has advanced past
    /// the call's snapshot, and then return the in-memory high-water.
    async fn current_high_water(&self) -> Result<u64, ConsensusError>;

    /// Submit a "bump to at_least" proposal through the host's OmniPaxos
    /// log and return the new high-water value once the cluster has
    /// applied it (or a later higher value).
    ///
    /// Implementations append an advance entry (the standalone host uses
    /// [`crate::log_entry::HighWaterCommand::Advance`]; piggyback hosts
    /// wrap it in their envelope variant) and wait until both (a) the
    /// cluster's `decided_idx` has advanced past the call's snapshot AND
    /// (b) the in-memory high-water is at least `at_least`.
    async fn submit_advance(&self, at_least: u64) -> Result<u64, ConsensusError>;
}
