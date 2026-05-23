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

use crate::server::{Server, ServerError, ServingState};

/// Bounded retry budget for a fence that hits a recoverable (`TransientDriver`)
/// consensus error in the volatile post-election window. After this many
/// retries the fence stops trying for the current leadership event, steps down
/// to `NotServing`, and awaits the next event rather than tearing the server
/// down. Backoff grows exponentially from `FENCE_RETRY_BASE`, capped at
/// `FENCE_RETRY_CAP`; the default budget spans well under two seconds — enough
/// to ride out a failover flap without masking a sustained outage.
const FENCE_MAX_TRANSIENT_RETRIES: u32 = 8;
const FENCE_RETRY_BASE: Duration = Duration::from_millis(25);
const FENCE_RETRY_CAP: Duration = Duration::from_millis(250);

pub(crate) async fn run_leader_watch(server: Arc<Server>) -> Result<(), ServerError> {
    let mut stream = server.consensus.leadership_events();
    while let Some(evt) = stream.next().await {
        // Every state change (Leader, Follower, Unknown) counts as a
        // transition observed from the driver: the total counter answers
        // "how often is leadership churning".
        #[cfg(feature = "metrics")]
        metrics::counter!("tsoracle.leader_transition.total").increment(1);
        match evt {
            LeaderState::Leader { epoch } => {
                // Time the full fence: drain-barrier wait, durable persist, and
                // allocator seed are all included. Recorded only when the fence
                // reaches Serving.
                #[cfg(feature = "metrics")]
                let fence_started_at = std::time::Instant::now();

                // Clear serving so new GetTs requests return NOT_LEADER until
                // the fence republishes Serving below.
                let _ = server.state_tx.send(ServingState::NotServing {
                    leader_endpoint: None,
                });
                server.allocator.lock().on_leadership_lost();

                // Fence with bounded retry. A consensus error here is not
                // automatically fatal: the post-election window is volatile and
                // `ConsensusError` already separates the recoverable classes
                // from the permanent ones. We honor that split instead of
                // tearing the whole server down on the first hiccup:
                //
                //   * TransientDriver — momentary quorum loss / transport flap
                //     while this node is still the elected leader. Retry with
                //     backoff; no fresh leadership event is coming to re-drive
                //     us.
                //   * NotLeader / Fenced — leadership moved under us. Step down
                //     to NotServing and continue the watch loop; the stream
                //     will deliver the new state (and another Leader event if
                //     we re-win).
                //   * anything else (PermanentDriver, allocator invariant) —
                //     fatal: propagate so `into_router` poisons serving state
                //     and stops serving.
                //
                // Serving stays NotServing until an attempt fully succeeds, so
                // the invariant "never publish Serving at a stale epoch" holds
                // on every path.
                let mut transient_retries: u32 = 0;
                loop {
                    // One fence attempt. `?` short-circuits to `attempt`
                    // (the async block's output), NOT out of run_leader_watch,
                    // so the error can be classified below.
                    let attempt: Result<(), ServerError> = async {
                        // Drain in-flight extensions from the prior epoch.
                        let drain_guard = server.extension_gate.write().await;

                        // Linearized load of the durably-persisted high-water.
                        // prior_max is an INCLUSIVE high-water: the prior leader
                        // could have issued a timestamp at (prior_max,
                        // LOGICAL_MAX), so the new leader must start strictly
                        // above prior_max — hence the +1 below.
                        let prior_max = server.consensus.load_high_water().await?;

                        crate::failpoint!(
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

                        crate::failpoint!("server::fence::after_persist_before_publish");

                        // Seed the allocator with both bounds: the floor pins
                        // the lower bound; committed_ceiling = actual is the
                        // post-persist upper bound the allocator can serve
                        // through without an extra extension round-trip.
                        server.allocator.lock().try_on_leadership_gained(
                            serving_floor,
                            actual,
                            epoch,
                        )?;

                        // Publish serving, then release the drain guard.
                        let _ = server.state_tx.send(ServingState::Serving);
                        drop(drain_guard);

                        crate::failpoint!("server::fence::after_serving_published");
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
                        // Leadership moved under us. Already NotServing; await
                        // the next leadership event rather than retrying a
                        // persist that can only keep failing at this epoch.
                        Err(ServerError::Consensus(
                            ConsensusError::NotLeader { .. } | ConsensusError::Fenced { .. },
                        )) => {
                            let _ = server.state_tx.send(ServingState::NotServing {
                                leader_endpoint: None,
                            });
                            break;
                        }
                        // Recoverable driver hiccup; retry while still leader.
                        Err(ServerError::Consensus(ConsensusError::TransientDriver(_source))) => {
                            transient_retries += 1;
                            if transient_retries > FENCE_MAX_TRANSIENT_RETRIES {
                                // Budget exhausted: stay up, NotServing, and
                                // await the next leadership event rather than
                                // tearing down a still-elected leader.
                                #[cfg(feature = "tracing")]
                                tracing::warn!(
                                    error = %_source,
                                    retries = transient_retries,
                                    "fence exhausted transient retries; awaiting next leadership event"
                                );
                                let _ = server.state_tx.send(ServingState::NotServing {
                                    leader_endpoint: None,
                                });
                                break;
                            }
                            let backoff = core::cmp::min(
                                FENCE_RETRY_BASE
                                    .saturating_mul(1u32 << (transient_retries - 1).min(16)),
                                FENCE_RETRY_CAP,
                            );
                            tokio::time::sleep(backoff).await;
                        }
                        // Permanent fault or allocator invariant: fail fast so
                        // into_router poisons serving state and stops serving.
                        Err(e) => return Err(e),
                    }
                }
            }
            LeaderState::Follower { leader_endpoint } => {
                server.allocator.lock().on_leadership_lost();
                let _ = server
                    .state_tx
                    .send(ServingState::NotServing { leader_endpoint });
            }
            LeaderState::Unknown => {
                server.allocator.lock().on_leadership_lost();
                let _ = server.state_tx.send(ServingState::NotServing {
                    leader_endpoint: None,
                });
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
