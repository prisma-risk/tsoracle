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

//! Regression: `submit_advance` must not return until *this call's own*
//! proposal is provably in the decided log.
//!
//! The pre-fix wait was a threshold check —
//! `new_decided > snapshot_decided && high_water() >= at_least` — which a
//! racing caller's `Advance` can satisfy before this call's own entry is
//! applied: both `decided_idx` moving and the floor being met can be
//! caused by an unrelated entry, so the returned high-water was not
//! provably attributable to this call.
//!
//! The fix mirrors `current_high_water`: it plants a `(my_node_id, seq)`
//! barrier nonce immediately after the `Advance` and waits until the apply
//! path folds *that specific* barrier. Once `submit_advance` returns, the
//! decided log therefore contains a `Barrier` authored by this node
//! sitting after its `Advance` — an identifiable marker the call planted
//! and waited to commit. The pre-fix path appended no barrier at all, so
//! this assertion fails against it.

use std::time::Duration;

use omnipaxos::util::LogEntry;
use tsoracle_consensus::AdvancePayload;
use tsoracle_driver_paxos::HighWaterCommand;
use tsoracle_driver_paxos::host::PaxosHighWaterHost;

#[path = "common/mod.rs"]
mod common;

// Runs under tokio virtual time (`start_paused`): the runner's `interval`
// ticks, the apply task's `notified()`, the test's `drive_until` polls, and
// the `timeout(5s)` guard all advance in simulated time the moment the
// runtime goes idle — so the async apply task that folds the attributable
// barrier still runs (the regression lives in that path), but without
// wall-clock variance. `start_paused` implies the current-thread runtime.
#[tokio::test(start_paused = true)]
async fn submit_advance_commits_an_attributable_barrier_for_this_call() {
    let mut cluster = common::build_mem_cluster(3);
    cluster.start_all();
    cluster
        .drive_until(common::some_leader_elected(), 1_000)
        .await;
    let leader_id = cluster.leader();

    let host_ref = cluster
        .node(leader_id)
        .host
        .as_ref()
        .expect("leader host present");

    let returned = tokio::time::timeout(Duration::from_secs(5), host_ref.submit_advance(42))
        .await
        .expect("submit_advance must complete within 5s")
        .expect("submit_advance must not return an error");
    assert!(
        returned >= 42,
        "submit_advance must honor the requested floor (saw {returned})",
    );

    // Once submit_advance returns, the leader's decided log must contain a
    // Barrier authored by this node, positioned after this call's
    // Advance{at_least: 42}. That ordering is exactly the attribution
    // guarantee: the call planted an identifiable marker right after its
    // own Advance and only returned once that marker committed, so the
    // returned value is provably this call's, not a racing caller's.
    let handle = cluster.node(leader_id).omnipaxos();
    let entries = handle
        .lock()
        .read_decided_suffix(0)
        .expect("leader has a decided suffix after submit_advance returns");

    let mut saw_this_calls_advance = false;
    let mut barrier_from_this_node_after_advance = false;
    for entry in &entries {
        match entry {
            LogEntry::Decided(HighWaterCommand::Advance(AdvancePayload { at_least: 42 })) => {
                saw_this_calls_advance = true;
            }
            LogEntry::Decided(HighWaterCommand::Barrier { node, .. }) if *node == leader_id => {
                if saw_this_calls_advance {
                    barrier_from_this_node_after_advance = true;
                }
            }
            _ => {}
        }
    }

    assert!(
        saw_this_calls_advance,
        "this call's Advance{{42}} must be in the decided log",
    );
    assert!(
        barrier_from_this_node_after_advance,
        "a Barrier authored by this node must follow this call's Advance in the decided log",
    );

    cluster.stop_all().await;
}
