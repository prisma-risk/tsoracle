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

// #[PerformanceCriticalPath]

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tsoracle_consensus::ConsensusError;
use tsoracle_core::{CommitOutcome, CoreError, Epoch, PeerEndpoint};
use tsoracle_proto::v1::{
    EpochWire, GetCurrentMaxSafeRequest, GetCurrentMaxSafeResponse, GetSeqRequest, GetSeqResponse,
    GetTsRequest, GetTsResponse, LeaderHint, tso_service_server::TsoService,
};

use crate::leader_hint::not_leader_status;
use crate::persist_disposition::{PersistDisposition, classify};
use crate::server::{Server, ServingState};

/// Convert an optional leader epoch into the nested wire form carried by
/// `LeaderHint`. Bundling the two 64-bit halves in one `EpochWire` means the
/// epoch is present in full or absent entirely — a half-populated epoch is
/// unrepresentable, so the client never has to reason about a partial pair.
fn wire_epoch(epoch: Option<Epoch>) -> Option<EpochWire> {
    epoch.map(|epoch| {
        let (hi, lo) = epoch.to_wire();
        EpochWire { hi, lo }
    })
}

/// Snapshot the best-available leader hint from the serving-state channel. Used
/// wherever we need to surface a `FAILED_PRECONDITION` "not leader" response
/// from a service-layer code path; matches what the fast NOT_LEADER gate emits.
fn leader_hint_from(server: &Server) -> LeaderHint {
    let (leader_endpoint, leader_epoch) = match server.core.serving_state() {
        ServingState::NotServing {
            leader_endpoint,
            leader_epoch,
        } => (leader_endpoint, leader_epoch),
        ServingState::Serving => (None, None),
    };
    LeaderHint {
        // Wire-format `LeaderHint.leader_endpoint` stays `Option<String>`
        // (prost-generated) — the boundary out of the typed `PeerEndpoint` is
        // here; the boundary back in is in `tsoracle_client::leader_hint`
        // where every wire-supplied hint runs through `PeerEndpoint::try_from`.
        leader_endpoint: leader_endpoint.map(PeerEndpoint::into_inner),
        leader_epoch: wire_epoch(leader_epoch),
    }
}

fn core_status(error: CoreError) -> Status {
    match error {
        CoreError::NotLeader => Status::failed_precondition("not leader"),
        CoreError::WindowExhausted => Status::internal("window exhausted"),
        CoreError::InvalidCount(count) => {
            Status::invalid_argument(format!("invalid count: {count}"))
        }
        CoreError::PhysicalMsOutOfRange(physical_ms) => Status::out_of_range(format!(
            "physical_ms {physical_ms} exceeds 46-bit timestamp field"
        )),
        CoreError::LogicalRangeOutOfRange {
            logical_start,
            count,
        } => Status::out_of_range(format!(
            "logical range [{logical_start}, +{count}) exceeds 18-bit timestamp field"
        )),
        CoreError::InvalidLeadershipWindow {
            fence_floor,
            committed_ceiling,
        } => Status::internal(format!(
            "invalid leadership window: fence_floor {fence_floor} exceeds committed_ceiling {committed_ceiling}"
        )),
        CoreError::WindowExtensionOverflow {
            floor,
            now_ms,
            ahead_ms,
        } => Status::internal(format!(
            "window extension overflow: max(floor {floor}, now_ms {now_ms}) + ahead_ms {ahead_ms} exceeds u64::MAX"
        )),
        CoreError::SeqKeyEmpty
        | CoreError::SeqKeyTooLong { .. }
        | CoreError::SeqCountZero
        | CoreError::SeqCountTooLarge { .. } => Status::invalid_argument(error.to_string()),
    }
}

pub struct TsoServiceImpl {
    pub(crate) server: Arc<Server>,
}

#[tonic::async_trait]
impl TsoService for TsoServiceImpl {
    async fn get_ts(&self, req: Request<GetTsRequest>) -> Result<Response<GetTsResponse>, Status> {
        tsoracle_failpoint::failpoint!("server::service::before_allocate");
        let count = req.into_inner().count;
        if count == 0 {
            return Err(Status::invalid_argument("count must be >= 1"));
        }

        // Offered load: count every well-formed request exactly once, here at
        // entry before the NOT_LEADER gate and the allocator. `success.total`
        // below bumps only on the Ok arm, so `requests.total - success.total`
        // is the failure count — a node rejecting every request shows this
        // total climbing while successes stay flat (vs. flat-at-zero, which is
        // indistinguishable from no traffic). Outside the retry loop, so a
        // single-extend-retry call still counts exactly once.
        self.server.reporter.get_ts_requests.increment(1);

        // Fast NOT_LEADER gate. `is_serving` answers the gate without cloning a
        // `ServingState`; only the rejected path re-reads (via `leader_hint_from`)
        // to build the redirect hint. The two reads are not atomic, but the hint
        // is best-effort either way — this mirrors the NotLeader arm below, which
        // also re-reads after `try_grant`.
        if !self.server.core.is_serving() {
            return Err(not_leader_status(
                &self.server.reporter,
                leader_hint_from(&self.server),
            ));
        }

        // At most two attempts: the first may return WindowExhausted, in which
        // case we extend the window and retry once. Every error other than a
        // first-attempt WindowExhausted — and NotLeader, which needs a metadata
        // trailer core_status cannot attach — routes through the single
        // exhaustive CoreError -> Status mapping in `core_status`, so a new
        // variant compiles and is handled here without editing this match. A
        // second WindowExhausted (the extension did not help — a driver bug)
        // therefore surfaces as `core_status`'s Internal mapping.
        //
        // This is a divergent `loop` (no `break`, only `return`/`continue`) so
        // it has type `!` and needs no trailing expression: the `attempt`
        // counter bounds it to two iterations, and the second iteration's
        // WindowExhausted falls through the guard into the `core_status` arm
        // rather than continuing.
        // Sample the wall clock once for the whole get_ts. The retry's
        // try_grant and the extension's would_grant / try_prepare all observe
        // this single instant, so the would_grant recheck predicts the retry
        // try_grant exactly. Re-reading the clock per call let it advance
        // between the recheck and the retry, exhausting a zero-slack (small
        // window_ahead) window — a timing race that surfaced intermittently as
        // `Internal "window exhausted"`.
        let now_ms = self.server.clock.now_ms();
        let mut attempt = 0;
        loop {
            let outcome = self.server.core.try_grant(now_ms, count);
            match outcome {
                Ok(grant) => {
                    self.server.reporter.get_ts_success.increment(1);
                    self.server
                        .reporter
                        .timestamps_issued
                        .increment(u64::from(grant.count()));
                    let (epoch_hi, epoch_lo) = grant.epoch().to_wire();
                    return Ok(Response::new(GetTsResponse {
                        physical_ms: grant.physical_ms(),
                        logical_start: grant.logical_start(),
                        count: grant.count(),
                        epoch_hi,
                        epoch_lo,
                    }));
                }
                Err(CoreError::NotLeader) => {
                    return Err(not_leader_status(
                        &self.server.reporter,
                        leader_hint_from(&self.server),
                    ));
                }
                Err(CoreError::WindowExhausted) if attempt == 0 => {
                    self.extend_window(now_ms, count).await?;
                    attempt += 1;
                    continue;
                }
                Err(other) => return Err(core_status(other)),
            }
        }
    }

    async fn get_current_max_safe(
        &self,
        _request: Request<GetCurrentMaxSafeRequest>,
    ) -> Result<Response<GetCurrentMaxSafeResponse>, Status> {
        let max_safe_physical_ms = self.server.core.current_max_safe_physical_ms();
        let (epoch_hi, epoch_lo) = self
            .server
            .core
            .current_epoch()
            .unwrap_or(Epoch::ZERO)
            .to_wire();
        Ok(Response::new(GetCurrentMaxSafeResponse {
            max_safe_physical_ms,
            epoch_hi,
            epoch_lo,
        }))
    }

    async fn get_seq(
        &self,
        req: Request<GetSeqRequest>,
    ) -> Result<Response<GetSeqResponse>, Status> {
        let GetSeqRequest { key, count } = req.into_inner();

        self.server.reporter.get_seq_requests.increment(1);

        if !self.server.core.is_serving() {
            return Err(not_leader_status(
                &self.server.reporter,
                leader_hint_from(&self.server),
            ));
        }

        // Core gate: leadership + key/count validation. NotLeader → redirect.
        let seq_key = match self.server.core.seq_validate(&key, count) {
            Ok(k) => k,
            Err(tsoracle_core::CoreError::NotLeader) => {
                return Err(not_leader_status(
                    &self.server.reporter,
                    leader_hint_from(&self.server),
                ));
            }
            Err(tsoracle_core::CoreError::SeqKeyEmpty)
            | Err(tsoracle_core::CoreError::SeqKeyTooLong { .. }) => {
                return Err(Status::invalid_argument("invalid sequence key"));
            }
            Err(tsoracle_core::CoreError::SeqCountZero)
            | Err(tsoracle_core::CoreError::SeqCountTooLarge { .. }) => {
                return Err(Status::invalid_argument(
                    "count must be between 1 and the maximum",
                ));
            }
            Err(other) => return Err(Status::internal(other.to_string())),
        };

        let epoch = match self.server.core.current_epoch() {
            Some(e) => e,
            None => {
                return Err(not_leader_status(
                    &self.server.reporter,
                    leader_hint_from(&self.server),
                ));
            }
        };

        match self
            .server
            .consensus
            .advance_dense(&seq_key, count, epoch)
            .await
        {
            Ok(start) => {
                self.server.reporter.get_seq_success.increment(1);
                self.server
                    .reporter
                    .seq_values_issued
                    .increment(u64::from(count));
                // Route through the core SeqGrant (spec §7.2) so the response is
                // derived from the validated grant, not assembled ad hoc.
                let grant = tsoracle_core::SeqGrant::new(seq_key, start, count, epoch);
                let (hi, lo) = grant.epoch().to_wire();
                Ok(Response::new(GetSeqResponse {
                    key: grant.key().as_str().to_string(),
                    start: grant.start(),
                    count: grant.count(),
                    // EpochWire is already imported (used by the leader-hint
                    // path's `wire_epoch` helper).
                    epoch: Some(EpochWire { hi, lo }),
                }))
            }
            Err(ConsensusError::SeqKeyCardinalityExceeded { cap }) => {
                self.server.reporter.seq_cardinality_rejected.increment(1);
                Err(Status::resource_exhausted(format!(
                    "dense key cardinality cap {cap} reached"
                )))
            }
            Err(ConsensusError::SeqOverflow) => {
                Err(Status::failed_precondition("dense counter overflow"))
            }
            Err(ConsensusError::NotLeader { .. })
            | Err(ConsensusError::Fenced { .. })
            | Err(ConsensusError::DenseUnsupported) => Err(not_leader_status(
                &self.server.reporter,
                leader_hint_from(&self.server),
            )),
            Err(other) => Err(Status::unavailable(other.to_string())),
        }
    }
}

impl TsoServiceImpl {
    /// Extend the window with single-flight coalescing.
    ///
    /// `extension_lock` (a `tokio::sync::Mutex`) is acquired first so only one
    /// caller in any concurrent burst proceeds into the prepare/persist/commit
    /// sequence. After acquiring, the caller rechecks whether the window has
    /// already been extended enough to satisfy its own `count` — if yes, it
    /// returns without contacting consensus. `count` is the caller's own
    /// request count, used so the recheck mirrors the outer loop's next
    /// `try_grant` exactly (a coarser check could skip an extension that the
    /// outer retry still actually needs).
    ///
    /// `now_ms` is the single wall-clock sample taken by `get_ts` for the whole
    /// operation. Both the recheck and the prepare use it (rather than re-reading
    /// the clock) so the would_grant predicate matches the retry try_grant at the
    /// same logical instant — see the sampling comment in `get_ts`.
    async fn extend_window(&self, now_ms: u64, count: u32) -> Result<(), Status> {
        // Single-flight gate: serialize peer extenders so consensus is hit once
        // per stampede, not once per stampeder. The slot holds `extension_lock`.
        let slot = self.server.core.extension_slot().await;

        // Recheck-after-acquire: a peer extender may have run prepare → persist →
        // commit while we waited for the slot. If the outer try_grant retry would
        // now succeed, skip the consensus round-trip. Uses get_ts's single
        // `now_ms` sample so the predicate matches the retry try_grant.
        if slot.would_grant(now_ms, count) {
            return Ok(());
        }

        // Drain barrier: the fence's write() waits behind this read until our
        // commit applies (or is dropped by the epoch check). Reachable only
        // through the slot, so `extension_lock` → `extension_gate` cannot invert.
        let _gate = slot.drain_barrier().await;
        tsoracle_failpoint::failpoint!("server::service::extension_gate_held");

        let (requested, epoch) =
            match slot.prepare_extension(now_ms, self.server.window_ahead.as_millis() as u64) {
                Ok(prepared) => prepared,
                // Lost leadership between the outer fast-gate check and here.
                // Surface as a leader redirect (with the hint the serving-state
                // channel knows about), not a bare FAILED_PRECONDITION without
                // metadata.
                Err(CoreError::NotLeader) => {
                    return Err(not_leader_status(
                        &self.server.reporter,
                        leader_hint_from(&self.server),
                    ));
                }
                Err(other) => return Err(core_status(other)),
            };
        // Count and time only the consensus round-trip itself: the
        // recheck-after-acquire short-circuit above skips it, and operators
        // tuning `window_ahead` care about how often a stampede actually
        // reached persist + how long that took (success or failure).
        let extension_started_at = std::time::Instant::now();
        let persist_outcome = self
            .server
            .consensus
            .persist_high_water(requested, epoch)
            .await;
        self.server.reporter.window_extensions.increment(1);
        self.server
            .reporter
            .window_extension_latency
            .record(extension_started_at.elapsed().as_secs_f64());
        let actual = match persist_outcome {
            Ok(v) => v,
            // Route the failure through the shared classifier and apply the
            // *extend path's* policy to each disposition. The fence path
            // (`fence::run_leader_watch`) maps the same dispositions
            // differently — that divergence is the whole point of factoring
            // the classification out: it now lives at these two call sites
            // explicitly rather than as two near-identical variant matches.
            Err(error) => match classify(error) {
                // Leadership moved under us — authoritative proof this node's
                // epoch is stale. Step down immediately: letting subsequent
                // try_grant calls keep serving from a fenced epoch, even
                // briefly, is the wrong tradeoff for a TSO. step_down clears
                // the allocator and publishes NotServing under the single
                // transition API; leader_hint_from then snapshots that
                // freshly-published state for the redirect. `fenced_by` is the
                // epoch the client can validate its next leader against —
                // present for Fenced, absent for NotLeader.
                PersistDisposition::SteppedDown { fenced_by } => {
                    self.server.core.step_down(None, fenced_by);
                    return Err(not_leader_status(
                        &self.server.reporter,
                        leader_hint_from(&self.server),
                    ));
                }
                // Transient driver failure: storage hiccup, peer transport
                // flap, quorum momentarily lost. Tell the client it MAY retry;
                // do NOT step down — there is no proof the epoch is stale.
                PersistDisposition::Transient(source) => {
                    return Err(Status::unavailable(format!("persist: {source}")));
                }
                // Permanent driver failure: read-only filesystem, corruption,
                // gone storage device, invariant violation. Surface honestly
                // so clients do not silently retry into a tarpit; do NOT step
                // down — the driver is sick, not fenced.
                PersistDisposition::Permanent(source) => {
                    return Err(Status::internal(format!("persist: {source}")));
                }
                // Proposed advance exceeded the 46-bit physical_ms cap. Same
                // surface policy as Permanent (INTERNAL, no step-down), but the
                // offending value is carried structurally. Reuse the variant's
                // `Display` (a single source of truth in `ConsensusError`) so
                // the message text stays pinned to the consensus crate.
                PersistDisposition::OutOfRange(at_least) => {
                    return Err(Status::internal(format!(
                        "persist: {}",
                        ConsensusError::AdvanceOutOfRange(at_least)
                    )));
                }
            },
        };
        let commit_outcome = self
            .server
            .core
            .commit_extension(actual, epoch)
            .map_err(core_status)?;
        // A dropped commit after a paid-for persist round-trip is benign but
        // worth surfacing: the epoch-fencing / monotonic-bound logic discarded
        // a durably-persisted value, a leading indicator of epoch churn
        // (NotLeader / EpochMismatch) or persist reordering (NotAdvanced).
        if let CommitOutcome::Ignored(_reason) = commit_outcome {
            #[cfg(feature = "tracing")]
            tracing::debug!(reason = ?_reason, "window extension commit ignored after persist");
            self.server
                .reporter
                .ignored_commits
                .for_reason(_reason)
                .increment(1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_status_maps_each_variant_to_documented_code() {
        // Every CoreError variant has a distinct gRPC status code; if a
        // future edit drops a branch the mapping table here catches it.
        assert_eq!(
            core_status(CoreError::NotLeader).code(),
            tonic::Code::FailedPrecondition,
        );

        assert_eq!(
            core_status(CoreError::WindowExhausted).code(),
            tonic::Code::Internal,
        );

        let invalid = core_status(CoreError::InvalidCount(7));
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
        assert!(invalid.message().contains("invalid count: 7"));

        let oor = core_status(CoreError::PhysicalMsOutOfRange(1 << 47));
        assert_eq!(oor.code(), tonic::Code::OutOfRange);
        assert!(oor.message().contains("46-bit"));

        let invalid_window = core_status(CoreError::InvalidLeadershipWindow {
            fence_floor: 9,
            committed_ceiling: 4,
        });
        assert_eq!(invalid_window.code(), tonic::Code::Internal);
        assert!(invalid_window.message().contains("fence_floor 9"));
        assert!(invalid_window.message().contains("committed_ceiling 4"));

        let logical_oor = core_status(CoreError::LogicalRangeOutOfRange {
            logical_start: 5,
            count: 11,
        });
        assert_eq!(logical_oor.code(), tonic::Code::OutOfRange);
        assert!(logical_oor.message().contains("logical range [5, +11)"));
        assert!(logical_oor.message().contains("18-bit"));

        let extension_overflow = core_status(CoreError::WindowExtensionOverflow {
            floor: 7,
            now_ms: 8,
            ahead_ms: u64::MAX,
        });
        assert_eq!(extension_overflow.code(), tonic::Code::Internal);
        assert!(extension_overflow.message().contains("floor 7"));
        assert!(extension_overflow.message().contains("now_ms 8"));
        assert!(
            extension_overflow
                .message()
                .contains(&format!("ahead_ms {}", u64::MAX))
        );

        assert_eq!(
            core_status(CoreError::SeqKeyEmpty).code(),
            tonic::Code::InvalidArgument
        );
        let too_long = core_status(CoreError::SeqKeyTooLong { len: 200, max: 128 });
        assert_eq!(too_long.code(), tonic::Code::InvalidArgument);
        assert!(too_long.message().contains("200"));
        assert_eq!(
            core_status(CoreError::SeqCountZero).code(),
            tonic::Code::InvalidArgument
        );
        let too_large = core_status(CoreError::SeqCountTooLarge {
            count: 70_000,
            max: 65_536,
        });
        assert_eq!(too_large.code(), tonic::Code::InvalidArgument);
        assert!(too_large.message().contains("70000"));
    }

    #[test]
    fn leader_hint_from_returns_endpoint_and_epoch_when_not_serving() {
        let server = Server::builder()
            .consensus_driver(std::sync::Arc::new(crate::test_fakes::InMemoryDriver::new()))
            .clock(std::sync::Arc::new(crate::SystemClock))
            .build()
            .unwrap();
        server.core.publish_not_serving(
            Some(PeerEndpoint::try_from("other-node:9000").unwrap()),
            Some(Epoch(7)),
        );
        let hint = leader_hint_from(&server);
        assert_eq!(hint.leader_endpoint.as_deref(), Some("other-node:9000"));
        let (hi, lo) = Epoch(7).to_wire();
        assert_eq!(hint.leader_epoch, Some(EpochWire { hi, lo }));

        // The Serving branch flips endpoint and epoch to None.
        server.core.publish_serving();
        let hint = leader_hint_from(&server);
        assert!(hint.leader_endpoint.is_none());
        assert!(hint.leader_epoch.is_none());
    }

    #[test]
    fn wire_epoch_bundles_some_and_passes_through_none() {
        // Fits in the low 64 bits (hi == 0).
        let (hi, lo) = Epoch(7).to_wire();
        assert_eq!(wire_epoch(Some(Epoch(7))), Some(EpochWire { hi, lo }));

        // Crosses the 64-bit boundary so hi is non-zero — guards against a
        // hi/lo swap that the all-low-bits case above cannot detect.
        let cross = Epoch((1u128 << 64) | 3);
        let (hi, lo) = cross.to_wire();
        assert_eq!(wire_epoch(Some(cross)), Some(EpochWire { hi, lo }));

        assert_eq!(wire_epoch(None), None);
    }
}
