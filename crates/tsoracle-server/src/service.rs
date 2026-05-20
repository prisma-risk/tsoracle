use std::sync::Arc;
use tonic::{Request, Response, Status};
use tsoracle_consensus::ConsensusError;
use tsoracle_core::CoreError;
use tsoracle_proto::v1::{GetTsRequest, GetTsResponse, LeaderHint, tso_service_server::TsoService};

use crate::leader_hint::not_leader_status;
use crate::server::{Server, ServingState};

/// Snapshot the best-available leader hint from `state_rx`. Used wherever we
/// need to surface a `FAILED_PRECONDITION` "not leader" response from a
/// service-layer code path; matches what the fast NOT_LEADER gate emits.
fn leader_hint_from(server: &Server) -> LeaderHint {
    let endpoint = match server.state_rx.borrow().clone() {
        ServingState::NotServing { leader_endpoint } => leader_endpoint,
        ServingState::Serving => None,
    };
    LeaderHint {
        leader_endpoint: endpoint,
        leader_epoch: None,
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
        CoreError::InvalidLeadershipWindow {
            fence_floor,
            committed_ceiling,
        } => Status::internal(format!(
            "invalid leadership window: fence_floor {fence_floor} exceeds committed_ceiling {committed_ceiling}"
        )),
    }
}

pub struct TsoServiceImpl {
    pub(crate) server: Arc<Server>,
}

#[tonic::async_trait]
impl TsoService for TsoServiceImpl {
    async fn get_ts(&self, req: Request<GetTsRequest>) -> Result<Response<GetTsResponse>, Status> {
        let count = req.into_inner().count;
        if count == 0 {
            return Err(Status::invalid_argument("count must be >= 1"));
        }

        // Fast NOT_LEADER gate.
        if let ServingState::NotServing { leader_endpoint } = self.server.state_rx.borrow().clone()
        {
            return Err(not_leader_status(LeaderHint {
                leader_endpoint,
                leader_epoch: None,
            }));
        }

        // Two attempts: first one may return WindowExhausted, in which case we
        // extend and retry once. A second WindowExhausted is a driver bug.
        for attempt in 0..2 {
            let outcome = {
                let mut allocator = self.server.allocator.lock();
                allocator.try_grant(self.server.clock.now_ms(), count)
            };
            match outcome {
                Ok(grant) => {
                    return Ok(Response::new(GetTsResponse {
                        physical_ms: grant.physical_ms,
                        logical_start: grant.logical_start,
                        count: grant.count,
                        epoch: grant.epoch.0,
                    }));
                }
                Err(CoreError::NotLeader) => {
                    return Err(not_leader_status(leader_hint_from(&self.server)));
                }
                Err(CoreError::InvalidCount(c)) => {
                    return Err(Status::invalid_argument(format!("invalid count: {c}")));
                }
                Err(CoreError::PhysicalMsOutOfRange(v)) => {
                    return Err(Status::out_of_range(format!(
                        "physical_ms {v} exceeds 46-bit timestamp field"
                    )));
                }
                Err(e @ CoreError::InvalidLeadershipWindow { .. }) => {
                    return Err(core_status(e));
                }
                Err(CoreError::WindowExhausted) if attempt == 0 => {
                    self.extend_window(count).await?;
                    continue;
                }
                Err(CoreError::WindowExhausted) => {
                    return Err(Status::internal("window still exhausted after extension"));
                }
            }
        }
        unreachable!("loop returns or continues exactly twice")
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
    async fn extend_window(&self, count: u32) -> Result<(), Status> {
        // Single-flight gate: serialize peer extenders so consensus is hit
        // once per stampede, not once per stampeder.
        let _extension_lock = self.server.extension_lock.lock().await;

        // Recheck-after-acquire: a peer extender may have run prepare →
        // persist → commit while we waited for the lock. If the outer
        // try_grant retry would now succeed, skip the consensus round-trip.
        // Reading now_ms fresh inside the lock keeps the predicate aligned
        // with what the outer loop's next try_grant will observe.
        if self
            .server
            .allocator
            .lock()
            .would_grant(self.server.clock.now_ms(), count)
        {
            return Ok(());
        }

        // Drain barrier: leader-watch's write() waits behind this read until
        // our commit applies (or is silently dropped by the epoch check).
        let _gate = self.server.extension_gate.read().await;

        let (requested, epoch) = {
            let allocator = self.server.allocator.lock();
            let Some(epoch) = allocator.epoch() else {
                // Lost leadership between the outer fast-gate check and here.
                // Surface as a leader redirect (with the hint state_rx knows
                // about), not a bare FAILED_PRECONDITION without metadata.
                return Err(not_leader_status(leader_hint_from(&self.server)));
            };
            let now = self.server.clock.now_ms();
            let target = allocator
                .try_prepare_window_extension(now, self.server.window_ahead.as_millis() as u64)
                .map_err(core_status)?;
            (target, epoch)
        };
        let actual = match self
            .server
            .consensus
            .persist_high_water(requested, epoch)
            .await
        {
            Ok(v) => v,
            // NotLeader / Fenced are authoritative proof from the consensus
            // driver that this node's epoch is stale. Step down immediately
            // — letting subsequent try_grant calls keep serving from a
            // fenced epoch, even briefly, is the wrong tradeoff for a TSO.
            // The step_down helper clears the allocator and publishes
            // NotServing under the single transition API; the hint we pass
            // back is whatever state_rx most recently knew about.
            Err(ConsensusError::NotLeader { .. }) | Err(ConsensusError::Fenced { .. }) => {
                self.server.step_down_due_to_consensus_rejection(None);
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
        self.server
            .allocator
            .lock()
            .try_commit_window_extension(actual, epoch)
            .map_err(core_status)?;
        Ok(())
    }
}
