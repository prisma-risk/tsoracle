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

//! Fence orchestration. Runs on each leadership transition.
//!
//! The arithmetic here is the inclusive-high-water invariant:
//!   serving_floor = max(prior_max + 1, now)
//!   requested     = serving_floor + failover_advance
//!   actual        = consensus.persist_high_water(requested, epoch)
//!   allocator.try_on_leadership_gained(serving_floor, actual, epoch)
//!
//! The `+1` is load-bearing: a prior leader at physical_ms = prior_max could
//! have served logical = LOGICAL_MAX, so the new leader MUST start strictly
//! above prior_max. `tests/leader_watch.rs:46` pins this arithmetically.

use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

use tsoracle_consensus::{ConsensusError, LeaderState};

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
            % FENCE_TRANSIENT_RETRY_WARN_INTERVAL
            == 0
}

pub(crate) async fn run_leader_watch(
    server: Arc<Server>,
    cancel: impl std::future::Future<Output = ()>,
) -> Result<(), ServerError> {
    let mut stream = server.consensus.leadership_events();
    // A leadership event observed while a fence is mid-retry is stashed here and
    // dispatched on the next iteration instead of being awaited fresh.
    let mut pending: Option<LeaderState> = None;
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
                #[cfg(feature = "metrics")]
                let fence_started_at = std::time::Instant::now();

                // Clear serving so new GetTs requests return NOT_LEADER until the
                // fence republishes Serving below.
                server.core.publish_not_serving(None, None);
                server.core.clear_allocator();

                // Count the transition after the clear above, not on the fence's
                // success path below: a Leader event is "observed" the moment we
                // begin handling it, so it must increment once even if the fence
                // then steps down (NotLeader/Fenced) or faults — matching the
                // prior pre-`match` placement exactly. Emitting after the clear
                // (not before) keeps a blocking recorder from delaying it.
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.leader_transition.total").increment(1);

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

                        // Publish serving, then release the drain guard.
                        server.core.publish_serving();
                        drop(drain_guard);

                        tsoracle_failpoint::failpoint!("server::fence::after_serving_published");
                        Ok(())
                    }
                    .await;

                    match attempt {
                        Ok(()) => {
                            #[cfg(feature = "metrics")]
                            metrics::histogram!("tsoracle.leader_transition.fence_latency")
                                .record(fence_started_at.elapsed().as_secs_f64());
                            break;
                        }
                        // Leadership moved under us. Already NotServing; await the
                        // next leadership event rather than retrying a persist that
                        // can only keep failing at this epoch.
                        Err(ServerError::Consensus(
                            ConsensusError::NotLeader { .. } | ConsensusError::Fenced { .. },
                        )) => {
                            server.core.publish_not_serving(None, None);
                            break;
                        }
                        // Recoverable driver hiccup. Retry, but race the backoff
                        // against the leadership stream so a genuine transition is
                        // observed instead of spun past.
                        Err(ServerError::Consensus(ConsensusError::TransientDriver(_source))) => {
                            transient_retries += 1;
                            #[cfg(feature = "metrics")]
                            metrics::counter!(
                                "tsoracle.leader_transition.fence_transient_retries.total"
                            )
                            .increment(1);
                            #[cfg(feature = "tracing")]
                            if warn_on_stuck_fence(transient_retries) {
                                tracing::warn!(
                                    error = %_source,
                                    retries = transient_retries,
                                    "fence still retrying a transient consensus error; serving is paused while this node remains leader"
                                );
                            }
                            let backoff = core::cmp::min(
                                FENCE_RETRY_BASE
                                    .saturating_mul(1u32 << (transient_retries - 1).min(16)),
                                FENCE_RETRY_CAP,
                            );
                            tokio::select! {
                                // Bias toward cancel so a stop requested while
                                // a fence is stuck retrying a transient error
                                // is honored rather than spun past.
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
                        // Permanent fault or allocator invariant: fail fast so
                        // into_router poisons serving state and stops serving.
                        Err(e) => return Err(e),
                    }
                }
            }
            LeaderState::Follower {
                leader_endpoint,
                leader_epoch,
            } => {
                server.core.clear_allocator();
                server
                    .core
                    .publish_not_serving(leader_endpoint, leader_epoch);
                // Emit after the NotServing publish so a blocking recorder
                // cannot delay the safety state change.
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.leader_transition.total").increment(1);
            }
            LeaderState::Unknown => {
                server.core.clear_allocator();
                server.core.publish_not_serving(None, None);
                // Emit after the NotServing publish so a blocking recorder
                // cannot delay the safety state change.
                #[cfg(feature = "metrics")]
                metrics::counter!("tsoracle.leader_transition.total").increment(1);
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
