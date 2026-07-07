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

//! Serving flow for lease RPCs.
//!
//! Lease grants and renewals allocate their `ts_upper_bound` through the same
//! window-extension prepare → persist → commit sequence as direct timestamp
//! issuance. The extension slot is therefore also the lease-mutation lock:
//! it serializes lease mutations with each other and with window extension.
//! Lease records are persisted before successful RPC responses so a failover
//! can seed the lease table from durable state.

use std::sync::Arc;

use tonic::Status;
use tsoracle_consensus::ConsensusError;
use tsoracle_core::{
    AcquireDecision, CommitOutcome, CoreError, Epoch, LeaseError, LeaseRecord,
    validate_lease_request,
};
use tsoracle_proto::v1::{
    AcquireLeaseRequest, AcquireLeaseResponse, EpochWire, ReleaseLeaseRequest,
    ReleaseLeaseResponse, RenewLeaseRequest, RenewLeaseResponse,
};

use crate::leader_hint::not_leader_status;
use crate::persist_disposition::{PersistDisposition, classify};
use crate::server::Server;
use crate::service::{core_status, leader_hint_from};
use crate::serving_core::ExtensionSlot;

pub(crate) async fn acquire_lease(
    server: &Arc<Server>,
    req: AcquireLeaseRequest,
) -> Result<AcquireLeaseResponse, Status> {
    server.reporter.lease_acquire_requests.increment(1);
    ensure_serving(server)?;
    validate_lease_request(
        &req.holder,
        req.ttl_ms,
        server.lease_ttl_floor.as_millis() as u64,
        server.lease_ttl_ceiling.as_millis() as u64,
    )
    .map_err(lease_status)?;

    let now_ms = server.clock.now_ms();
    let slot = server.core.extension_slot().await;
    let supersedes = match server
        .core
        .lease_prepare_acquire(&req.holder, req.holder_epoch, now_ms)
        .map_err(lease_status)?
    {
        AcquireDecision::Idempotent(live) => {
            let epoch = current_epoch_or_not_leader(server)?;
            server.reporter.lease_acquire_success.increment(1);
            return Ok(AcquireLeaseResponse {
                lease_id: live.lease_id,
                ts_upper_bound: live.ts_upper_bound,
                expires_at_ms: live.expires_at_ms,
                epoch: wire(epoch),
            });
        }
        AcquireDecision::Grant { supersedes } => supersedes,
    };

    let _gate = slot.drain_barrier().await;
    let (actual, epoch) = persist_extension_bound(server, &slot, now_ms, req.ttl_ms).await?;
    let record = LeaseRecord {
        lease_id: actual,
        holder: req.holder,
        holder_epoch: req.holder_epoch,
        ttl_ms: req.ttl_ms,
        ts_upper_bound: actual,
        expires_at_ms: now_ms.saturating_add(req.ttl_ms),
        superseded: false,
    };
    // A crash after the high-water advance but before this persist burns a
    // forward range and records no lease. That is safe: timestamps are never
    // reissued, and a retry receives a fresh grant.
    persist_lease_mutation(
        server,
        LeaseMutation {
            upsert: Some(record.clone()),
            supersede: supersedes,
            remove: None,
        },
        epoch,
        now_ms,
    )
    .await?;
    server.reporter.lease_acquire_success.increment(1);
    Ok(AcquireLeaseResponse {
        lease_id: record.lease_id,
        ts_upper_bound: record.ts_upper_bound,
        expires_at_ms: record.expires_at_ms,
        epoch: wire(epoch),
    })
}

pub(crate) async fn renew_lease(
    server: &Arc<Server>,
    req: RenewLeaseRequest,
) -> Result<RenewLeaseResponse, Status> {
    server.reporter.lease_renew_requests.increment(1);
    ensure_serving(server)?;

    let now_ms = server.clock.now_ms();
    let slot = server.core.extension_slot().await;
    let mut record = server
        .core
        .lease_prepare_renew(req.lease_id, now_ms)
        .map_err(lease_status)?;

    let _gate = slot.drain_barrier().await;
    let (actual, epoch) = persist_extension_bound(server, &slot, now_ms, record.ttl_ms).await?;
    record.ts_upper_bound = actual;
    record.expires_at_ms = now_ms.saturating_add(record.ttl_ms);
    persist_lease_mutation(
        server,
        LeaseMutation {
            upsert: Some(record.clone()),
            supersede: None,
            remove: None,
        },
        epoch,
        now_ms,
    )
    .await?;
    server.reporter.lease_renew_success.increment(1);
    Ok(RenewLeaseResponse {
        ts_upper_bound: record.ts_upper_bound,
        expires_at_ms: record.expires_at_ms,
        epoch: wire(epoch),
    })
}

pub(crate) async fn release_lease(
    server: &Arc<Server>,
    req: ReleaseLeaseRequest,
) -> Result<ReleaseLeaseResponse, Status> {
    server.reporter.lease_release_requests.increment(1);
    ensure_serving(server)?;

    let now_ms = server.clock.now_ms();
    let slot = server.core.extension_slot().await;
    if server.core.lease_prepare_release(req.lease_id).is_none() {
        return Ok(ReleaseLeaseResponse {});
    }
    let epoch = current_epoch_or_not_leader(server)?;

    let _gate = slot.drain_barrier().await;
    persist_lease_mutation(
        server,
        LeaseMutation {
            upsert: None,
            supersede: None,
            remove: Some(req.lease_id),
        },
        epoch,
        now_ms,
    )
    .await?;
    Ok(ReleaseLeaseResponse {})
}

struct LeaseMutation {
    upsert: Option<LeaseRecord>,
    supersede: Option<u64>,
    remove: Option<u64>,
}

fn not_leader(server: &Arc<Server>) -> Status {
    not_leader_status(&server.reporter, leader_hint_from(server))
}

fn ensure_serving(server: &Arc<Server>) -> Result<(), Status> {
    if server.core.is_serving() {
        Ok(())
    } else {
        Err(not_leader(server))
    }
}

fn current_epoch_or_not_leader(server: &Arc<Server>) -> Result<Epoch, Status> {
    server
        .core
        .current_epoch()
        .ok_or_else(|| not_leader(server))
}

fn lease_status(error: LeaseError) -> Status {
    match error {
        LeaseError::HolderLenOutOfRange(_) | LeaseError::TtlOutOfRange { .. } => {
            Status::invalid_argument(error.to_string())
        }
        LeaseError::HolderEpochStale { .. }
        | LeaseError::LeaseExpired { .. }
        | LeaseError::LeaseSuperseded { .. } => Status::failed_precondition(error.to_string()),
        LeaseError::UnknownLease(_) => Status::not_found(error.to_string()),
    }
}

async fn persist_bound(server: &Arc<Server>, requested: u64, epoch: Epoch) -> Result<u64, Status> {
    match server.consensus.persist_high_water(requested, epoch).await {
        Ok(actual) => Ok(actual),
        Err(error) => Err(persist_error_status(server, "persist", error)),
    }
}

async fn persist_extension_bound(
    server: &Arc<Server>,
    slot: &ExtensionSlot<'_>,
    now_ms: u64,
    ttl_ms: u64,
) -> Result<(u64, Epoch), Status> {
    let (requested, epoch) = match slot.prepare_extension(now_ms, ttl_ms) {
        Ok(prepared) => prepared,
        Err(CoreError::NotLeader) => return Err(not_leader(server)),
        Err(other) => return Err(core_status(other)),
    };
    let actual = persist_bound(server, requested, epoch).await?;
    if let CommitOutcome::Ignored(_) = server
        .core
        .commit_extension(actual, epoch)
        .map_err(core_status)?
    {
        return Err(not_leader(server));
    }
    Ok((actual, epoch))
}

async fn persist_lease_mutation(
    server: &Arc<Server>,
    mutation: LeaseMutation,
    epoch: Epoch,
    now_ms: u64,
) -> Result<(), Status> {
    let set = server.core.lease_projected_live_set(
        mutation.upsert.as_ref(),
        mutation.supersede,
        mutation.remove,
        now_ms,
    );
    persist_lease_set(server, &set, epoch).await?;
    server
        .core
        .lease_commit(
            mutation.upsert,
            mutation.supersede,
            mutation.remove,
            epoch,
            now_ms,
        )
        .map_err(core_status)
}

async fn persist_lease_set(
    server: &Arc<Server>,
    set: &[LeaseRecord],
    epoch: Epoch,
) -> Result<(), Status> {
    match server.consensus.persist_leases(set, epoch).await {
        Ok(()) => Ok(()),
        Err(ConsensusError::LeasesUnsupported) => Err(Status::unimplemented(
            "leases are not supported by this consensus driver",
        )),
        Err(error) => Err(persist_error_status(server, "persist leases", error)),
    }
}

fn persist_error_status(
    server: &Arc<Server>,
    context: &'static str,
    error: ConsensusError,
) -> Status {
    match classify(error) {
        PersistDisposition::SteppedDown { fenced_by } => {
            server.core.step_down(None, fenced_by);
            not_leader(server)
        }
        PersistDisposition::Transient(source) => {
            Status::unavailable(format!("{context}: {source}"))
        }
        PersistDisposition::Permanent(source) => Status::internal(format!("{context}: {source}")),
        PersistDisposition::OutOfRange(at_least) => Status::internal(format!(
            "{context}: {}",
            ConsensusError::AdvanceOutOfRange(at_least)
        )),
    }
}

fn wire(epoch: Epoch) -> Option<EpochWire> {
    let (hi, lo) = epoch.to_wire();
    Some(EpochWire { hi, lo })
}
