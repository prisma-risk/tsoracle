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

//! Fence orchestration. Runs on each leadership transition.
//!
//! The arithmetic here is the inclusive-high-water invariant:
//!   serving_floor = max(prior_max + 1, now)
//!   requested     = serving_floor + failover_advance
//!   actual        = consensus.persist_high_water(requested, epoch)
//!   allocator.become_leader(serving_floor, actual, epoch)
//!
//! The `+1` is load-bearing: a prior leader at physical_ms = prior_max could
//! have served logical = LOGICAL_MAX, so the new leader MUST start strictly
//! above prior_max. The `leader_watch` integration tests pin this arithmetically.

use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use tsoracle_consensus::{ConsensusError, LeaderState};
use tsoracle_core::Epoch;

use crate::persist_disposition::{PersistDisposition, classify};
use crate::server::{Server, ServerError};

// Fence retry tuning for the volatile post-election window. When a fence hits
// `TransientDriver`, the retry loop does NOT give up: a still-elected leader
// has no fresh leadership event coming to re-drive it, so abandoning the fence
// would strand the node `NotServing` indefinitely. Instead the loop retries
// with capped exponential backoff and, on every backoff, concurrently watches
// the leadership stream — so a genuine leadership change (which some drivers,
// e.g. the paxos host, report as `TransientDriver` rather than `NotLeader`) is
// observed and dispatched instead of spun past. The loop exits only on success,
// a `NotLeader`/`Fenced` classification, a permanent fault, or an observed
// leadership transition. Backoff grows exponentially from `FENCE_RETRY_BASE`,
// capped at `FENCE_RETRY_CAP`.
const FENCE_RETRY_BASE: Duration = Duration::from_millis(25);
const FENCE_RETRY_CAP: Duration = Duration::from_millis(250);

// Rate-limiting for the "fence still stuck" warning (tracing only): warn once
// when retries first reach WARN_AFTER, then again every WARN_INTERVAL retries
// (~5s of stuck time at FENCE_RETRY_CAP). Gated with the warn so they don't
// read as dead code when `tracing` is disabled.
#[cfg(feature = "tracing")]
const FENCE_TRANSIENT_RETRY_WARN_AFTER: u32 = 8;
#[cfg(feature = "tracing")]
const FENCE_TRANSIENT_RETRY_WARN_INTERVAL: u32 = 20;

/// Whether the stuck-fence warning should fire at this retry count: once at
/// `FENCE_TRANSIENT_RETRY_WARN_AFTER`, then every `FENCE_TRANSIENT_RETRY_WARN_INTERVAL`
/// retries after that. The `>=` guards the subtraction against underflow.
#[cfg(feature = "tracing")]
fn warn_on_stuck_fence(transient_retries: u32) -> bool {
    transient_retries >= FENCE_TRANSIENT_RETRY_WARN_AFTER
        && (transient_retries - FENCE_TRANSIENT_RETRY_WARN_AFTER)
            .is_multiple_of(FENCE_TRANSIENT_RETRY_WARN_INTERVAL)
}

/// Debug-only guard that a driver-surfaced [`LeaderState`] honors the
/// `LeaderState::Follower` epoch-monotonicity contract (see
/// [`tsoracle_consensus::LeaderState`]): `leader_epoch` is non-decreasing
/// across emissions. The client's monotone-forward gate
/// (`compare_and_set_leader`) drops a hint that cannot outrank the cached
/// leader, so a regressing epoch makes clients drop valid redirects.
///
/// The companion scheme-less `leader_endpoint` contract is enforced at the
/// type level by [`tsoracle_core::PeerEndpoint`]: a scheme-bearing string
/// cannot inhabit the field, so the historical runtime check folded into the
/// type's constructor.
///
/// Compiled out in release builds (`debug_assert!`), so it costs nothing in
/// production and fires only in a driver author's own test suite — turning a
/// silent production trap into a loud test-time failure. `last_epoch` carries
/// the highest epoch observed so far across the watch loop; `None` epochs
/// (epoch-less paxos hints, `Unknown`) are a documented valid case and advance
/// nothing.
fn debug_assert_leader_state_contract(evt: &LeaderState, last_epoch: &mut Option<Epoch>) {
    let epoch = match evt {
        LeaderState::Leader { epoch } => Some(*epoch),
        LeaderState::Follower { leader_epoch, .. } => *leader_epoch,
        LeaderState::Unknown => None,
    };
    if let Some(epoch) = epoch {
        if let Some(prev) = *last_epoch {
            debug_assert!(
                epoch >= prev,
                "consensus driver surfaced a leader_epoch that regressed \
                 ({epoch:?} < {prev:?}); LeaderState epochs must be non-decreasing — \
                 the client's monotone-forward leader cache drops lower-epoch redirects",
            );
        }
        *last_epoch = Some(epoch);
    }
}

pub(crate) async fn run_leader_watch(
    server: Arc<Server>,
    cancel: impl std::future::Future<Output = ()>,
) -> Result<(), ServerError> {
    let mut stream = server.consensus.leadership_events();
    // A leadership event observed while a fence is mid-retry is stashed here and
    // dispatched on the next iteration instead of being awaited fresh.
    let mut pending: Option<LeaderState> = None;
    // Highest leader epoch observed so far, threaded through the debug-only
    // driver-contract guard below. Carries no production behavior; the guard's
    // `debug_assert!`s compile out in release.
    let mut last_epoch: Option<Epoch> = None;
    // Cooperative cancellation (see `Server::into_router` / `WatchGuard`).
    // Pinned once and observed only at the `select!` boundaries below — the
    // event wait and the transient-retry backoff — never inside a fence
    // attempt, so an in-flight fence always runs to completion rather than
    // being torn down mid-`extension_gate.write()`. On cancel we publish
    // `NotServing` (the same fail-safe every other termination performs) and
    // return `Ok(())`: the stop was requested, so it is not an error.
    tokio::pin!(cancel);
    loop {
        let evt = match pending.take() {
            Some(evt) => evt,
            None => {
                tokio::select! {
                    // Bias toward cancel so a pending stop is honored promptly
                    // instead of dispatching one more leadership event first.
                    biased;
                    _ = &mut cancel => {
                        server.core.step_down(None, None);
                        return Ok(());
                    }
                    next = stream.next() => match next {
                        Some(evt) => evt,
                        None => break,
                    },
                }
            }
        };
        // Debug-only driver-contract check: a scheme-bearing leader_endpoint or
        // a regressing leader_epoch is a silent trap in production (dropped
        // redirects) but a loud failure in a driver author's test suite. Costs
        // nothing in release — the asserts compile out.
        debug_assert_leader_state_contract(&evt, &mut last_epoch);
        // `tsoracle.leader_transition.total` is emitted per-arm *after* that
        // arm's safety state change (clear/publish), never here before the
        // `match`. The counter answers "how often is leadership churning"; one
        // increment per observed event (Leader, Follower, Unknown), same as a
        // pre-`match` emission, but ordered so a (contract-violating) blocking
        // recorder cannot delay the safety-critical transition. The `metrics`
        // crate's de facto contract is non-blocking O(1); this placement keeps
        // the "safety before metrics" invariant structural, not conventional.
        match evt {
            LeaderState::Leader { epoch } => {
                // Time the full fence: drain-barrier wait, durable persist, and
                // allocator seed are all included. Recorded only when the fence
                // reaches Serving.
                let fence_started_at = std::time::Instant::now();

                // Enter the fence: clear the allocator and publish NotServing so
                // new GetTs requests return NOT_LEADER until the fence
                // republishes Serving below. `enter_fencing` clears *before* it
                // publishes (invariant 2), so the order cannot be inverted here.
                server.core.enter_fencing();

                // Count the transition after the clear above, not on the fence's
                // success path below: a Leader event is "observed" the moment we
                // begin handling it, so it must increment once even if the fence
                // then steps down (NotLeader/Fenced) or faults — matching the
                // prior pre-`match` placement exactly. Emitting after the clear
                // (not before) keeps a blocking recorder from delaying it.
                server.reporter.leader_transitions.increment(1);
                server.reporter.last_leader_transition.touch_now();

                // Fence with retry. A consensus error here is not automatically
                // fatal: the post-election window is volatile and `ConsensusError`
                // separates the recoverable classes from the permanent ones.
                //
                //   * TransientDriver — momentary quorum loss / transport flap.
                //     Retry with backoff, racing the leadership stream so a real
                //     leadership change is observed even if the driver keeps
                //     classifying it as transient.
                //   * NotLeader / Fenced — leadership moved under us. Step down to
                //     NotServing and continue the watch loop.
                //   * anything else (PermanentDriver, allocator invariant) —
                //     fatal: propagate so `into_router` poisons serving state.
                //
                // Serving stays NotServing until an attempt fully succeeds, so the
                // invariant "never publish Serving at a stale epoch" holds.
                let mut transient_retries: u32 = 0;
                loop {
                    // One fence attempt. `?` short-circuits to `attempt` (the async
                    // block's output), NOT out of run_leader_watch, so the error
                    // can be classified below.
                    let attempt: Result<(), ServerError> = async {
                        // Drain in-flight extensions from the prior epoch.
                        //
                        // Lock-ordering invariant: the fence takes ONLY
                        // `extension_gate.write()` here and intentionally does
                        // NOT acquire `extension_lock`. Extenders acquire in the
                        // order `extension_lock` → `extension_gate.read()` (see
                        // `service::extend_window`), so the safe global order is
                        // `extension_lock` before `extension_gate`. The fence
                        // holds no `extension_lock`, so it cannot close a cycle:
                        // it just waits for in-flight readers to drain. A future
                        // path that took these in the opposite order — grabbing
                        // `extension_gate.write()` and then `extension_lock` —
                        // would deadlock against an extender holding
                        // `extension_lock` and waiting on `read()`. Keep the
                        // fence `extension_lock`-free.
                        let drain_guard = server.core.drain_barrier_write().await;

                        // Linearized load of the durably-persisted high-water.
                        // prior_max is an INCLUSIVE high-water: the prior leader
                        // could have issued a timestamp at (prior_max,
                        // LOGICAL_MAX), so the new leader must start strictly
                        // above prior_max — hence the +1 below.
                        let prior_max = server.consensus.load_high_water().await?;
                        let lease_records = match server.consensus.load_leases().await {
                            Ok(records) => records,
                            Err(ConsensusError::LeasesUnsupported) => Vec::new(),
                            Err(err) => return Err(err.into()),
                        };

                        tsoracle_failpoint::failpoint!(
                            "server::fence::after_load_before_persist",
                            |arg: Option<String>| -> Result<(), ServerError> {
                                let _ = arg;
                                Err(ServerError::Consensus(ConsensusError::TransientDriver(
                                    Box::new(std::io::Error::other(
                                        "failpoint: server::fence::after_load_before_persist",
                                    )),
                                )))
                            }
                        );
                        tsoracle_yieldpoint::yieldpoint!(
                            "server::fence::after_load_before_persist"
                        );

                        // serving_floor is the first physical_ms the new leader
                        // may issue at — strictly above anything the prior
                        // leader could have served. requested = floor + advance
                        // is the pre-extended ceiling we ask the driver to
                        // persist.
                        let now = server.clock.now_ms();
                        let serving_floor = core::cmp::max(prior_max.saturating_add(1), now);
                        let requested = serving_floor
                            .saturating_add(server.failover_advance.as_millis() as u64);

                        // Persist the fence at the new epoch.
                        let actual = server
                            .consensus
                            .persist_high_water(requested, epoch)
                            .await?;

                        tsoracle_failpoint::failpoint!(
                            "server::fence::after_persist_before_publish"
                        );

                        // Seed the allocator with both bounds: the floor pins
                        // the lower bound; committed_ceiling = actual is the
                        // post-persist upper bound the allocator can serve
                        // through without an extra extension round-trip.
                        server
                            .core
                            .seed_on_leadership_gained(serving_floor, actual, epoch)?;
                        server.core.seed_leases(lease_records, now);

                        // Publish serving, then release the drain guard.
                        server.core.publish_serving();
                        drop(drain_guard);

                        tsoracle_failpoint::failpoint!("server::fence::after_serving_published");
                        Ok(())
                    }
                    .await;

                    match attempt {
                        Ok(()) => {
                            server
                                .reporter
                                .fence_latency
                                .record(fence_started_at.elapsed().as_secs_f64());
                            break;
                        }
                        // A consensus error (from load_high_water or
                        // persist_high_water) routes through the shared
                        // classifier; the fence then applies its *own* policy to
                        // each disposition. The extend path
                        // (`service::extend_window`) maps these same dispositions
                        // differently — that deliberate divergence is now explicit
                        // at the two call sites instead of duplicated as variant
                        // matches.
                        Err(ServerError::Consensus(consensus_error)) => {
                            match classify(consensus_error) {
                                // Leadership moved under us. Already NotServing via
                                // enter_fencing; republish it (no hint — the fence
                                // has no caller to redirect, it just awaits the next
                                // leadership event) and stop retrying a persist that
                                // can only keep failing at this epoch. `fenced_by` is
                                // unused here for that reason.
                                PersistDisposition::SteppedDown { .. } => {
                                    server.core.publish_not_serving(None, None);
                                    break;
                                }
                                // Recoverable driver hiccup. Retry, but race the
                                // backoff against the leadership stream so a genuine
                                // transition is observed instead of spun past.
                                PersistDisposition::Transient(_source) => {
                                    transient_retries += 1;
                                    server.reporter.fence_transient_retries.increment(1);
                                    #[cfg(feature = "tracing")]
                                    if warn_on_stuck_fence(transient_retries) {
                                        tracing::warn!(
                                            error = %_source,
                                            retries = transient_retries,
                                            "fence still retrying a transient consensus error; serving is paused while this node remains leader"
                                        );
                                    }
                                    let backoff = core::cmp::min(
                                        FENCE_RETRY_BASE.saturating_mul(
                                            1u32 << (transient_retries - 1).min(16),
                                        ),
                                        FENCE_RETRY_CAP,
                                    );
                                    tokio::select! {
                                        // Bias toward cancel so a stop requested
                                        // while a fence is stuck retrying a transient
                                        // error is honored rather than spun past.
                                        biased;
                                        _ = &mut cancel => {
                                            server.core.step_down(None, None);
                                            return Ok(());
                                        }
                                        _ = tokio::time::sleep(backoff) => {}
                                        next = stream.next() => {
                                            match next {
                                                Some(evt) => {
                                                    pending = Some(evt);
                                                    break;
                                                }
                                                None => return Err(ServerError::WatchStreamClosed),
                                            }
                                        }
                                    }
                                }
                                // Permanent fault: fail fast so into_router poisons
                                // serving state and stops serving. Reconstitute the
                                // original error as the propagated cause.
                                PersistDisposition::Permanent(source) => {
                                    return Err(ServerError::Consensus(
                                        ConsensusError::PermanentDriver(source),
                                    ));
                                }
                                // Same surface policy as Permanent (fatal, poison
                                // serving), but the variant carries the offending
                                // u64 structurally, so we reconstruct the original
                                // ConsensusError variant — not collapse it into
                                // PermanentDriver — so propagated diagnostics keep
                                // their typed identity.
                                PersistDisposition::OutOfRange(at_least) => {
                                    return Err(ServerError::Consensus(
                                        ConsensusError::AdvanceOutOfRange(at_least),
                                    ));
                                }
                            }
                        }
                        // A non-consensus fault (e.g. a Core allocator invariant
                        // from seed_on_leadership_gained): never recoverable here,
                        // so fail fast and let into_router poison serving state.
                        Err(e) => return Err(e),
                    }
                }
            }
            LeaderState::Follower {
                leader_endpoint,
                leader_epoch,
            } => {
                server.core.step_down(leader_endpoint, leader_epoch);
                // Emit after the NotServing publish so a blocking recorder
                // cannot delay the safety state change.
                server.reporter.leader_transitions.increment(1);
                server.reporter.last_leader_transition.touch_now();
            }
            LeaderState::Unknown => {
                server.core.step_down(None, None);
                // Emit after the NotServing publish so a blocking recorder
                // cannot delay the safety state change.
                server.reporter.leader_transitions.increment(1);
                server.reporter.last_leader_transition.touch_now();
            }
        }
    }
    // The leadership stream is contracted to live for the life of the server;
    // reaching here means the driver dropped it (shutdown, lost session,
    // partition recovery). Surface that as an explicit error so the watch-task
    // termination always routes through the poisoning branch in `into_router`
    // and is observable to callers of `serve_with_*`. See #72.
    Err(ServerError::WatchStreamClosed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsoracle_core::{Epoch, PeerEndpoint};

    #[test]
    fn monotone_sequence_with_bare_endpoints_passes_guard() {
        let mut last_epoch = None;
        for evt in [
            LeaderState::Unknown,
            LeaderState::Leader { epoch: Epoch(5) },
            LeaderState::Follower {
                leader_endpoint: Some(PeerEndpoint::try_from("leader:9000").unwrap()),
                leader_epoch: Some(Epoch(5)),
            },
            LeaderState::Follower {
                leader_endpoint: None,
                leader_epoch: None,
            },
            LeaderState::Leader { epoch: Epoch(6) },
        ] {
            debug_assert_leader_state_contract(&evt, &mut last_epoch);
        }
        assert_eq!(last_epoch, Some(Epoch(6)));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "leader_epoch that regressed")]
    fn regressing_epoch_trips_guard() {
        let mut last_epoch = None;
        debug_assert_leader_state_contract(
            &LeaderState::Leader { epoch: Epoch(7) },
            &mut last_epoch,
        );
        debug_assert_leader_state_contract(
            &LeaderState::Follower {
                leader_endpoint: Some(PeerEndpoint::try_from("leader:9000").unwrap()),
                leader_epoch: Some(Epoch(6)),
            },
            &mut last_epoch,
        );
    }
}
