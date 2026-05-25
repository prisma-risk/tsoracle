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

//! Regression: `StandaloneHost::stop` must observe the apply-task
//! shutdown signal even when the apply task is mid-iteration at the
//! moment `stop` fires.
//!
//! The original implementation used `Notify::notify_waiters` which only
//! wakes tasks currently inside `.notified().await`. If the apply task
//! was inside its `drain_decided_into` / `maybe_snapshot` body when
//! `stop` ran, the notification was silently lost — and because the
//! runner's tick task keeps firing `apply_notify` every couple of
//! milliseconds, the apply task would livelock through the
//! apply-notify branch forever, never observing shutdown.
//!
//! The fix uses `Notify::notify_one`, which stores a permit consumed
//! by the next `.notified()` future. This test arms the
//! `standalone_host::apply_task::between_iterations` yield point so
//! the apply task is deterministically parked off-shutdown when
//! `host.stop()` runs.

#![cfg(feature = "yieldpoints")]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use omnipaxos::messages::Message;
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use parking_lot::Mutex;
use tsoracle_driver_paxos::{HighWaterCommand, StandaloneHost};
use tsoracle_paxos_toolkit::lifecycle::MessageSink;
use tsoracle_paxos_toolkit::test_fakes::mem_storage::MemStorage;
use tsoracle_yieldpoint as yieldpoint;

const APPLY_TASK_YIELD: &str = "standalone_host::apply_task::between_iterations";

struct NoopSink;

#[async_trait]
impl MessageSink<HighWaterCommand> for NoopSink {
    async fn send(&self, _: Message<HighWaterCommand>) {}
}

fn build_host() -> StandaloneHost<MemStorage<HighWaterCommand>> {
    let cluster_config = ClusterConfig {
        configuration_id: 1,
        nodes: vec![1, 2, 3],
        flexible_quorum: None,
    };
    let server_config = ServerConfig {
        pid: 1,
        ..Default::default()
    };
    let config = OmniPaxosConfig {
        cluster_config,
        server_config,
    };
    let omnipaxos = config
        .build(MemStorage::<HighWaterCommand>::new())
        .expect("build omnipaxos handle");
    StandaloneHost::builder()
        .omnipaxos(Arc::new(Mutex::new(omnipaxos)))
        .my_node_id(1)
        .tick_interval(Duration::from_millis(2))
        .build()
        .expect("build standalone host")
}

// Runs under tokio virtual time (`start_paused`). The livelock this regression
// guards is in the async apply task, so the test keeps the real `start` /
// `stop` machinery and only removes wall-clock variance: the runner's
// `interval` ticks, the coordinating `sleep`s, and the `timeout` guard all
// advance in simulated time. `start_paused` implies the current-thread runtime.
#[tokio::test(start_paused = true)]
async fn stop_delivers_shutdown_when_apply_task_is_mid_iteration() {
    // Arm the yield point. The apply task will await this Notify
    // between iterations of its `apply_notify` branch — yielding its
    // worker so the rest of the runtime keeps running, but parked
    // off any `shutdown.notified()` await.
    let gate = yieldpoint::cfg(APPLY_TASK_YIELD);

    let mut host = build_host();
    host.start(Arc::new(NoopSink))
        .expect("fresh host is not already running");

    // The runner's first tick (~2 ms in) fires apply_notify; the apply
    // task wakes, runs drain+maybe_snapshot, then parks at the yield
    // point. 50 ms is comfortably enough for that sequence.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let stop_handle = tokio::spawn(async move {
        host.stop().await;
    });

    // Let stop() send its shutdown notify while the apply task is
    // still parked on the gate — the timing that loses the signal
    // under the old notify_waiters() implementation.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Release the apply task. It returns from the yield point, loops
    // back to select!. The fix's stored permit on `apply_shutdown` is
    // consumed by the fresh `shutdown.notified()` future, breaking
    // the loop.
    gate.notify_one();

    // Liveness guard, not a latency assertion. Once the gate releases, the
    // apply task consumes the stored `apply_shutdown` permit on its next
    // `select!` turn and `stop()` returns. The pre-fix `notify_waiters`
    // regression livelocked here *forever*; under virtual time the runner
    // ticks pace that livelock, so the runtime still goes idle between ticks
    // and the paused clock advances to this (simulated) deadline, tripping the
    // timeout. Any finite bound distinguishes "fixed" from "regressed"; the
    // duration is sim-time, so it costs no wall clock.
    tokio::time::timeout(Duration::from_secs(10), stop_handle)
        .await
        .expect("host.stop() must complete after yield-point release (shutdown livelock?)")
        .expect("stop task must not panic");

    yieldpoint::remove(APPLY_TASK_YIELD);
}
