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

// #[PerformanceCriticalPath]

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tsoracle_consensus::ConsensusError;
#[cfg(feature = "metrics")]
use tsoracle_core::IgnoreReason;
use tsoracle_core::{CommitOutcome, CoreError, Epoch};
use tsoracle_proto::v1::{
    EpochWire, GetTsRequest, GetTsResponse, LeaderHint, tso_service_server::TsoService,
};

use crate::leader_hint::not_leader_status;
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
    let (leader_endpoint, leader_epoch) = match server.state_tx.borrow().clone() {
        ServingState::NotServing {
            leader_endpoint,
            leader_epoch,
        } => (leader_endpoint, leader_epoch),
        ServingState::Serving => (None, None),
    };
    LeaderHint {
        leader_endpoint,
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
    }
}

/// The per-reason counter name for an ignored window-extension commit. Each
/// reason gets its own flat counter (matching the rest of the catalog) so an
/// operator can alert on epoch churn (`not_leader` / `epoch_mismatch`)
/// separately from persist reordering (`not_advanced`).
#[cfg(feature = "metrics")]
fn ignored_commit_metric(reason: IgnoreReason) -> &'static str {
    match reason {
        IgnoreReason::NotLeader => "tsoracle.window.extensions.ignored.not_leader.total",
        IgnoreReason::EpochMismatch { .. } => {
            "tsoracle.window.extensions.ignored.epoch_mismatch.total"
        }
        IgnoreReason::NotAdvanced { .. } => "tsoracle.window.extensions.ignored.not_advanced.total",
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

        // Fast NOT_LEADER gate.
        if let ServingState::NotServing {
            leader_endpoint,
            leader_epoch,
        } = self.server.state_tx.borrow().clone()
        {
            return Err(not_leader_status(LeaderHint {
                leader_endpoint,
                leader_epoch: wire_epoch(leader_epoch),
            }));
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
            let outcome = {
                let mut allocator = self.server.allocator.lock();
                allocator.try_grant(now_ms, count)
            };
            match outcome {
                Ok(grant) => {
                    #[cfg(feature = "metrics")]
                    {
                        metrics::counter!("tsoracle.get_ts.total").increment(1);
                        metrics::counter!("tsoracle.get_ts.timestamps_issued")
                            .increment(u64::from(grant.count()));
                    }
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
                    return Err(not_leader_status(leader_hint_from(&self.server)));
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
        // Single-flight gate: serialize peer extenders so consensus is hit
        // once per stampede, not once per stampeder.
        let _extension_lock = self.server.extension_lock.lock().await;

        // Recheck-after-acquire: a peer extender may have run prepare →
        // persist → commit while we waited for the lock. If the outer
        // try_grant retry would now succeed, skip the consensus round-trip.
        // Using get_ts's single `now_ms` sample keeps the predicate aligned
        // with what the outer loop's retry try_grant will observe.
        if self.server.allocator.lock().would_grant(now_ms, count) {
            return Ok(());
        }

        // Drain barrier: leader-watch's write() waits behind this read until
        // our commit applies (or is silently dropped by the epoch check).
        let _gate = self.server.extension_gate.read().await;
        tsoracle_failpoint::failpoint!("server::service::extension_gate_held");

        let (requested, epoch) = {
            let allocator = self.server.allocator.lock();
            let Some(epoch) = allocator.epoch() else {
                // Lost leadership between the outer fast-gate check and here.
                // Surface as a leader redirect (with the hint the serving-state
                // channel knows about), not a bare FAILED_PRECONDITION without
                // metadata.
                return Err(not_leader_status(leader_hint_from(&self.server)));
            };
            let target = allocator
                .try_prepare_window_extension(now_ms, self.server.window_ahead.as_millis() as u64)
                .map_err(core_status)?;
            (target, epoch)
        };
        // Count and time only the consensus round-trip itself: the
        // recheck-after-acquire short-circuit above skips it, and operators
        // tuning `window_ahead` care about how often a stampede actually
        // reached persist + how long that took (success or failure).
        #[cfg(feature = "metrics")]
        let extension_started_at = std::time::Instant::now();
        let persist_outcome = self
            .server
            .consensus
            .persist_high_water(requested, epoch)
            .await;
        #[cfg(feature = "metrics")]
        {
            metrics::counter!("tsoracle.window.extensions.total").increment(1);
            metrics::histogram!("tsoracle.window.extension_latency")
                .record(extension_started_at.elapsed().as_secs_f64());
        }
        let actual = match persist_outcome {
            Ok(v) => v,
            // NotLeader / Fenced are authoritative proof from the consensus
            // driver that this node's epoch is stale. Step down immediately
            // — letting subsequent try_grant calls keep serving from a
            // fenced epoch, even briefly, is the wrong tradeoff for a TSO.
            // The step_down helper clears the allocator and publishes
            // NotServing under the single transition API; leader_hint_from
            // then snapshots that freshly-published state for the redirect.
            //
            // Fenced names the epoch that fenced us as `current`; publish it
            // so the NOT_LEADER hint carries an epoch the client can validate
            // its next leader against. NotLeader during persist exposes no
            // such epoch here, so its hint omits one.
            Err(ConsensusError::Fenced { current, .. }) => {
                self.server
                    .step_down_due_to_consensus_rejection(None, Some(current));
                return Err(not_leader_status(leader_hint_from(&self.server)));
            }
            Err(ConsensusError::NotLeader { .. }) => {
                self.server.step_down_due_to_consensus_rejection(None, None);
                return Err(not_leader_status(leader_hint_from(&self.server)));
            }
            // Transient driver failure: storage hiccup, peer transport flap,
            // quorum momentarily lost. Tell the client it MAY retry.
            Err(ConsensusError::TransientDriver(e)) => {
                return Err(Status::unavailable(format!("persist: {e}")));
            }
            // Permanent driver failure: read-only filesystem, corruption,
            // gone storage device, invariant violation. Surface honestly so
            // clients do not silently retry into a tarpit.
            Err(ConsensusError::PermanentDriver(e)) => {
                return Err(Status::internal(format!("persist: {e}")));
            }
        };
        let commit_outcome = self
            .server
            .allocator
            .lock()
            .try_commit_window_extension(actual, epoch)
            .map_err(core_status)?;
        // A dropped commit after a paid-for persist round-trip is benign but
        // worth surfacing: the epoch-fencing / monotonic-bound logic discarded
        // a durably-persisted value, a leading indicator of epoch churn
        // (NotLeader / EpochMismatch) or persist reordering (NotAdvanced).
        if let CommitOutcome::Ignored(_reason) = commit_outcome {
            #[cfg(feature = "tracing")]
            tracing::debug!(reason = ?_reason, "window extension commit ignored after persist");
            #[cfg(feature = "metrics")]
            metrics::counter!(ignored_commit_metric(_reason)).increment(1);
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
    }

    #[test]
    fn leader_hint_from_returns_endpoint_and_epoch_when_not_serving() {
        let server = Server::builder()
            .consensus_driver(std::sync::Arc::new(crate::test_fakes::InMemoryDriver::new()))
            .clock(std::sync::Arc::new(tsoracle_core::SystemClock))
            .build()
            .unwrap();
        server.state_tx.send_replace(ServingState::NotServing {
            leader_endpoint: Some("http://other-node:9000".into()),
            leader_epoch: Some(Epoch(7)),
        });
        let hint = leader_hint_from(&server);
        assert_eq!(
            hint.leader_endpoint.as_deref(),
            Some("http://other-node:9000")
        );
        let (hi, lo) = Epoch(7).to_wire();
        assert_eq!(hint.leader_epoch, Some(EpochWire { hi, lo }));

        // The Serving branch flips endpoint and epoch to None.
        server.state_tx.send_replace(ServingState::Serving);
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

    #[cfg(feature = "metrics")]
    #[test]
    fn ignored_commit_metric_names_each_reason_distinctly() {
        use tsoracle_core::IgnoreReason;
        // Each ignore reason maps to its own counter so an operator can tell
        // epoch churn (not_leader / epoch_mismatch) from persist reordering
        // (not_advanced) at the metric layer, not just in the logs.
        assert_eq!(
            ignored_commit_metric(IgnoreReason::NotLeader),
            "tsoracle.window.extensions.ignored.not_leader.total"
        );
        assert_eq!(
            ignored_commit_metric(IgnoreReason::EpochMismatch {
                expected: Epoch(4),
                current: Epoch(5),
            }),
            "tsoracle.window.extensions.ignored.epoch_mismatch.total"
        );
        assert_eq!(
            ignored_commit_metric(IgnoreReason::NotAdvanced {
                persisted: 3_000,
                committed: 5_000,
            }),
            "tsoracle.window.extensions.ignored.not_advanced.total"
        );
    }
}
