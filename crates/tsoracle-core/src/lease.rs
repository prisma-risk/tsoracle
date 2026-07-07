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

//! Lease records and the pure lease-table state machine (sync, no I/O).
//!
//! A lease grants an opaque `holder` the right to stamp timestamps with
//! `physical_ms <= ts_upper_bound` until `expires_at_ms`. The table decides
//! acquire/renew/release/supersede outcomes; durability and clocks live in
//! the server + driver layers (prepare/commit split, like the allocator).
//!
//! Holder is opaque bytes; the table only compares holders for byte equality
//! (the holder group key). `holder_epoch` is the supersede ordering within a
//! holder group. Callers choose how to encode a holder group and epoch into
//! those two fields; the table treats them as opaque ordering inputs.
//!
//! The record carries `holder_epoch` (required to evaluate supersede) and
//! `ttl_ms` (required because `RenewLease { lease_id }` carries no TTL:
//! renewal re-arms the acquire-time TTL). Expiry is `now_ms >= expires_at_ms`.
//! Superseded-but-unexpired leases still count toward the frontier; timestamps
//! are never reissued, so there is no reclamation, only frontier accounting.

use std::collections::BTreeMap;

/// Maximum accepted length of the opaque holder key, in bytes.
pub const MAX_LEASE_HOLDER_LEN: usize = 128;
/// Default server-enforced lower bound on requested lease TTLs (5 s).
pub const DEFAULT_LEASE_TTL_FLOOR_MS: u64 = 5_000;
/// Default server-enforced upper bound on requested lease TTLs (300 s).
pub const DEFAULT_LEASE_TTL_CEILING_MS: u64 = 300_000;

/// Durable lease state.
///
/// `lease_id` is the acquire-time `ts_upper_bound` (a committed-high-water
/// value); strict monotonicity of the durable high-water makes it unique across
/// grants, epochs, and failovers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseRecord {
    pub lease_id: u64,
    /// Opaque holder-group key (1..=MAX_LEASE_HOLDER_LEN bytes); compared only
    /// for byte equality.
    pub holder: Vec<u8>,
    /// Supersede ordering within a holder group.
    pub holder_epoch: u64,
    /// Acquire-time TTL, re-armed verbatim by every renewal.
    pub ttl_ms: u64,
    /// Highest physical_ms the holder may stamp. Always a value the durable
    /// high-water has reached.
    pub ts_upper_bound: u64,
    /// Server-clock expiry; the lease stops counting toward the safe frontier
    /// at `now_ms >= expires_at_ms`.
    pub expires_at_ms: u64,
    /// True once a higher-epoch acquire for the same holder group landed.
    pub superseded: bool,
}

impl LeaseRecord {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LeaseError {
    #[error("lease holder must be 1..={MAX_LEASE_HOLDER_LEN} bytes, got {0}")]
    HolderLenOutOfRange(usize),
    #[error("ttl_ms {ttl_ms} outside server bounds [{floor_ms}, {ceiling_ms}]")]
    TtlOutOfRange {
        ttl_ms: u64,
        floor_ms: u64,
        ceiling_ms: u64,
    },
    #[error("holder epoch {requested} is not above the active lease's epoch {held}")]
    HolderEpochStale { requested: u64, held: u64 },
    #[error("unknown lease id {0}")]
    UnknownLease(u64),
    #[error("lease {lease_id} expired at {expired_at_ms} ms; acquire a new lease")]
    LeaseExpired { lease_id: u64, expired_at_ms: u64 },
    #[error("lease {lease_id} was superseded by holder epoch {by_epoch}")]
    LeaseSuperseded { lease_id: u64, by_epoch: u64 },
}

/// Validate an AcquireLease request against server TTL policy. Floor and
/// ceiling are server configuration.
pub fn validate_lease_request(
    holder: &[u8],
    ttl_ms: u64,
    floor_ms: u64,
    ceiling_ms: u64,
) -> Result<(), LeaseError> {
    if holder.is_empty() || holder.len() > MAX_LEASE_HOLDER_LEN {
        return Err(LeaseError::HolderLenOutOfRange(holder.len()));
    }
    if ttl_ms < floor_ms || ttl_ms > ceiling_ms {
        return Err(LeaseError::TtlOutOfRange {
            ttl_ms,
            floor_ms,
            ceiling_ms,
        });
    }
    Ok(())
}

/// Outcome of `prepare_acquire`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AcquireDecision {
    /// Same holder + same epoch with a live active lease: no side effects,
    /// return the live lease as-is.
    Idempotent(LeaseRecord),
    /// Grant a fresh lease; `supersedes` names the active lease being
    /// atomically superseded by a strictly-higher holder epoch, if any.
    Grant { supersedes: Option<u64> },
}

/// Pure lease table keyed by lease_id.
///
/// Prepare methods are read-only decisions; commit methods apply state the
/// caller has already persisted.
#[derive(Debug, Default)]
pub struct LeaseTable {
    records: BTreeMap<u64, LeaseRecord>,
}

impl LeaseTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fence-time seeding from the durable record set; entries already expired
    /// at `now_ms` are dropped because they can never count again.
    pub fn seed(&mut self, records: Vec<LeaseRecord>, now_ms: u64) {
        self.records = records
            .into_iter()
            .filter(|r| !r.is_expired(now_ms))
            .map(|r| (r.lease_id, r))
            .collect();
    }

    pub fn clear(&mut self) {
        self.records.clear();
    }

    fn active_for(&self, holder: &[u8], now_ms: u64) -> Option<&LeaseRecord> {
        self.records
            .values()
            .find(|r| !r.superseded && !r.is_expired(now_ms) && r.holder == holder)
    }

    pub fn prepare_acquire(
        &self,
        holder: &[u8],
        holder_epoch: u64,
        now_ms: u64,
    ) -> Result<AcquireDecision, LeaseError> {
        match self.active_for(holder, now_ms) {
            None => Ok(AcquireDecision::Grant { supersedes: None }),
            Some(active) if active.holder_epoch == holder_epoch => {
                Ok(AcquireDecision::Idempotent(active.clone()))
            }
            Some(active) if active.holder_epoch < holder_epoch => Ok(AcquireDecision::Grant {
                supersedes: Some(active.lease_id),
            }),
            Some(active) => Err(LeaseError::HolderEpochStale {
                requested: holder_epoch,
                held: active.holder_epoch,
            }),
        }
    }

    pub fn commit_acquire(&mut self, record: LeaseRecord, supersedes: Option<u64>, now_ms: u64) {
        if let Some(id) = supersedes
            && let Some(old) = self.records.get_mut(&id)
        {
            old.superseded = true;
        }
        self.records.insert(record.lease_id, record);
        self.prune_expired(now_ms);
    }

    pub fn prepare_renew(&self, lease_id: u64, now_ms: u64) -> Result<LeaseRecord, LeaseError> {
        let record = self
            .records
            .get(&lease_id)
            .ok_or(LeaseError::UnknownLease(lease_id))?;
        if record.is_expired(now_ms) {
            return Err(LeaseError::LeaseExpired {
                lease_id,
                expired_at_ms: record.expires_at_ms,
            });
        }
        if record.superseded {
            let by_epoch = self
                .records
                .values()
                .filter(|r| r.holder == record.holder && r.holder_epoch > record.holder_epoch)
                .map(|r| r.holder_epoch)
                .max()
                .unwrap_or(record.holder_epoch);
            return Err(LeaseError::LeaseSuperseded { lease_id, by_epoch });
        }
        Ok(record.clone())
    }

    pub fn commit_renew(&mut self, record: LeaseRecord, now_ms: u64) {
        self.records.insert(record.lease_id, record);
        self.prune_expired(now_ms);
    }

    /// Live record to surrender, or `None` when the release is a no-op.
    pub fn prepare_release(&self, lease_id: u64) -> Option<LeaseRecord> {
        self.records.get(&lease_id).cloned()
    }

    pub fn commit_release(&mut self, lease_id: u64, now_ms: u64) {
        self.records.remove(&lease_id);
        self.prune_expired(now_ms);
    }

    /// Frontier lease term: min over unexpired leases, active or superseded.
    pub fn min_unexpired_bound(&self, now_ms: u64) -> Option<u64> {
        self.records
            .values()
            .filter(|r| !r.is_expired(now_ms))
            .map(|r| r.ts_upper_bound)
            .min()
    }

    /// The unexpired record set, for durable persistence as absolute state.
    pub fn live_set(&self, now_ms: u64) -> Vec<LeaseRecord> {
        self.records
            .values()
            .filter(|r| !r.is_expired(now_ms))
            .cloned()
            .collect()
    }

    /// The live set as it will be after applying an upsert, supersede, or
    /// removal. The server persists this projection, then commits the same
    /// mutation.
    pub fn projected_live_set(
        &self,
        upsert: Option<&LeaseRecord>,
        supersede: Option<u64>,
        remove: Option<u64>,
        now_ms: u64,
    ) -> Vec<LeaseRecord> {
        let mut set: BTreeMap<u64, LeaseRecord> = self
            .records
            .iter()
            .filter(|(_, r)| !r.is_expired(now_ms))
            .map(|(id, r)| (*id, r.clone()))
            .collect();
        if let Some(id) = supersede
            && let Some(old) = set.get_mut(&id)
        {
            old.superseded = true;
        }
        if let Some(id) = remove {
            set.remove(&id);
        }
        if let Some(record) = upsert {
            set.insert(record.lease_id, record.clone());
        }
        set.into_values().collect()
    }

    fn prune_expired(&mut self, now_ms: u64) {
        self.records.retain(|_, r| !r.is_expired(now_ms));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: u64, holder: &[u8], epoch: u64, bound: u64, expires: u64) -> LeaseRecord {
        LeaseRecord {
            lease_id: id,
            holder: holder.to_vec(),
            holder_epoch: epoch,
            ttl_ms: 10_000,
            ts_upper_bound: bound,
            expires_at_ms: expires,
            superseded: false,
        }
    }

    #[test]
    fn validate_rejects_bad_holder_and_ttl() {
        assert!(matches!(
            validate_lease_request(&[], 10_000, 5_000, 300_000),
            Err(LeaseError::HolderLenOutOfRange(0))
        ));
        assert!(matches!(
            validate_lease_request(&[0u8; MAX_LEASE_HOLDER_LEN + 1], 10_000, 5_000, 300_000),
            Err(LeaseError::HolderLenOutOfRange(_))
        ));
        assert!(matches!(
            validate_lease_request(b"g1", 4_999, 5_000, 300_000),
            Err(LeaseError::TtlOutOfRange { .. })
        ));
        assert!(matches!(
            validate_lease_request(b"g1", 300_001, 5_000, 300_000),
            Err(LeaseError::TtlOutOfRange { .. })
        ));
        assert!(validate_lease_request(b"g1", 5_000, 5_000, 300_000).is_ok());
        assert!(validate_lease_request(b"g1", 300_000, 5_000, 300_000).is_ok());
    }

    #[test]
    fn fresh_holder_gets_grant_without_supersede() {
        let table = LeaseTable::new();
        assert_eq!(
            table.prepare_acquire(b"g1", 1, 1_000).unwrap(),
            AcquireDecision::Grant { supersedes: None }
        );
    }

    #[test]
    fn same_epoch_reacquire_is_idempotent() {
        let mut table = LeaseTable::new();
        let live = rec(100, b"g1", 1, 100, 11_000);
        table.commit_acquire(live.clone(), None, 1_000);
        assert_eq!(
            table.prepare_acquire(b"g1", 1, 2_000).unwrap(),
            AcquireDecision::Idempotent(live)
        );
    }

    #[test]
    fn higher_epoch_supersedes_and_lower_epoch_is_rejected() {
        let mut table = LeaseTable::new();
        table.commit_acquire(rec(100, b"g1", 5, 100, 11_000), None, 1_000);
        assert_eq!(
            table.prepare_acquire(b"g1", 6, 2_000).unwrap(),
            AcquireDecision::Grant {
                supersedes: Some(100)
            }
        );
        assert!(matches!(
            table.prepare_acquire(b"g1", 4, 2_000),
            Err(LeaseError::HolderEpochStale {
                requested: 4,
                held: 5
            })
        ));
    }

    #[test]
    fn superseded_lease_counts_toward_frontier_until_expiry() {
        let mut table = LeaseTable::new();
        table.commit_acquire(rec(100, b"g1", 1, 100, 11_000), None, 1_000);
        table.commit_acquire(rec(200, b"g1", 2, 200, 12_000), Some(100), 2_000);
        assert_eq!(table.min_unexpired_bound(3_000), Some(100));
        assert_eq!(table.min_unexpired_bound(11_000), Some(200));
        assert!(matches!(
            table.prepare_renew(100, 3_000),
            Err(LeaseError::LeaseSuperseded {
                lease_id: 100,
                by_epoch: 2
            })
        ));
    }

    #[test]
    fn renew_unknown_and_expired_are_distinct_errors() {
        let mut table = LeaseTable::new();
        table.commit_acquire(rec(100, b"g1", 1, 100, 11_000), None, 1_000);
        assert!(matches!(
            table.prepare_renew(999, 2_000),
            Err(LeaseError::UnknownLease(999))
        ));
        assert!(matches!(
            table.prepare_renew(100, 11_000),
            Err(LeaseError::LeaseExpired {
                lease_id: 100,
                expired_at_ms: 11_000
            })
        ));
        assert_eq!(table.prepare_renew(100, 10_999).unwrap().lease_id, 100);
    }

    #[test]
    fn release_is_idempotent_and_removes_from_frontier() {
        let mut table = LeaseTable::new();
        table.commit_acquire(rec(100, b"g1", 1, 100, 11_000), None, 1_000);
        assert!(table.prepare_release(100).is_some());
        table.commit_release(100, 2_000);
        assert_eq!(table.min_unexpired_bound(2_000), None);
        assert!(
            table.prepare_release(100).is_none(),
            "second release is a no-op"
        );
        assert!(
            table.prepare_release(42).is_none(),
            "unknown release is a no-op"
        );
    }

    #[test]
    fn seed_drops_expired_records() {
        let mut table = LeaseTable::new();
        table.seed(
            vec![rec(1, b"g1", 1, 10, 5_000), rec(2, b"g2", 1, 20, 50_000)],
            6_000,
        );
        assert_eq!(table.min_unexpired_bound(6_000), Some(20));
        assert!(matches!(
            table.prepare_renew(1, 6_000),
            Err(LeaseError::UnknownLease(1))
        ));
    }

    #[test]
    fn projected_live_set_matches_committed_state() {
        let mut table = LeaseTable::new();
        table.commit_acquire(rec(100, b"g1", 1, 100, 11_000), None, 1_000);
        let incoming = rec(200, b"g1", 2, 200, 12_000);
        let projected = table.projected_live_set(Some(&incoming), Some(100), None, 2_000);
        table.commit_acquire(incoming, Some(100), 2_000);
        let mut committed = table.live_set(2_000);
        let mut projected = projected;
        committed.sort_by_key(|r| r.lease_id);
        projected.sort_by_key(|r| r.lease_id);
        assert_eq!(projected, committed);
        assert!(committed.iter().any(|r| r.lease_id == 100 && r.superseded));
    }

    #[test]
    fn expired_active_holder_gets_fresh_grant() {
        let mut table = LeaseTable::new();
        table.commit_acquire(rec(100, b"g1", 3, 100, 11_000), None, 1_000);
        assert_eq!(
            table.prepare_acquire(b"g1", 3, 11_000).unwrap(),
            AcquireDecision::Grant { supersedes: None }
        );
    }
}
