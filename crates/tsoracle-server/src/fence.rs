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

#[cfg(feature = "failpoints")]
use tsoracle_consensus::ConsensusError;
use tsoracle_consensus::LeaderState;

use crate::server::{Server, ServerError, ServingState};

pub(crate) async fn run_leader_watch(server: Arc<Server>) -> Result<(), ServerError> {
    let mut stream = server.consensus.leadership_events();
    while let Some(evt) = stream.next().await {
        match evt {
            LeaderState::Leader { epoch } => {
                // Clear serving so new GetTs requests return NOT_LEADER.
                let _ = server.state_tx.send(ServingState::NotServing {
                    leader_endpoint: None,
                });
                server.allocator.lock().on_leadership_lost();

                // Drain in-flight extensions from the prior epoch.
                let drain_guard = server.extension_gate.write().await;

                // Linearized load of the durably-persisted high-water.
                // prior_max is an INCLUSIVE high-water: the prior leader could
                // have issued a timestamp at (prior_max, LOGICAL_MAX). The new
                // leader must therefore start strictly above prior_max, hence
                // the +1 below.
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

                // Compute the serving floor and the requested ceiling.
                // serving_floor is the first physical_ms the new leader may
                // issue at — strictly above any timestamp the prior leader
                // could have served. requested = floor + advance is the
                // pre-extended ceiling we ask the driver to persist.
                let now = server.clock.now_ms();
                let serving_floor = core::cmp::max(prior_max.saturating_add(1), now);
                let requested =
                    serving_floor.saturating_add(server.failover_advance.as_millis() as u64);

                // Persist the fence at the new epoch.
                let actual = server
                    .consensus
                    .persist_high_water(requested, epoch)
                    .await?;

                crate::failpoint!("server::fence::after_persist_before_publish");

                // Seed the allocator with both bounds. The floor pins the
                // lower bound; committed_ceiling = actual is the post-persist
                // upper bound the allocator can serve through without an extra
                // extension round-trip.
                server
                    .allocator
                    .lock()
                    .try_on_leadership_gained(serving_floor, actual, epoch)?;

                // Publish serving, then release the drain guard.
                let _ = server.state_tx.send(ServingState::Serving);
                drop(drain_guard);

                crate::failpoint!("server::fence::after_serving_published");
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
    Ok(())
}
