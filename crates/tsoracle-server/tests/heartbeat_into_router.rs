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

//! Embedder-path coverage: a heartbeat must appear when the caller mounts
//! routes via `Server::into_router()` and never calls any `serve*` method.
//! Also covers the zero-interval (no task spawned) and drop-stops-task paths.
//!
//! All tests use `flavor = "current_thread"` so that `tokio::spawn`-ed tasks
//! poll on the same OS thread as the test driver. This is essential: the
//! tracing subscriber is installed via `set_default` (thread-local), and with
//! `current_thread` the spawned heartbeat task inherits that subscriber.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing_subscriber::fmt::MakeWriter;

use tsoracle_server::Server;
use tsoracle_server::test_fakes::InMemoryDriver;

// ---------------------------------------------------------------------------
// Shared buffer writer (copied from heartbeat.rs unit tests, placed here for
// the integration test binary which cannot reach crate-private types).
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct BufWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for BufWriter {
    type Writer = BufWriterHandle;
    fn make_writer(&'a self) -> Self::Writer {
        BufWriterHandle(self.0.clone())
    }
}

struct BufWriterHandle(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for BufWriterHandle {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Install a `tracing_subscriber` that writes into `buf`, returning a
/// `DefaultGuard` that unregisters it on drop. Uses `set_default` (thread-
/// local), safe to call once per test because each integration-test binary
/// runs in its own process.
fn install_subscriber(buf: BufWriter) -> tracing::dispatcher::DefaultGuard {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(buf)
        .with_max_level(tracing::Level::INFO)
        .with_target(true)
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_default(subscriber)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Heartbeat lines must appear when the server is mounted via `into_router()`
/// and no `serve*` method is ever called. This is the embedder path: a larger
/// application owns the listener and calls `axum::serve` (or equivalent) with
/// the returned `Routes`. The heartbeat task is spawned inside
/// `into_router_parts()`, so it runs regardless of how the routes are served.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn heartbeat_emits_from_into_router_embedder_path() {
    let buf = BufWriter::default();
    let _guard = install_subscriber(buf.clone());

    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver)
        .heartbeat_interval(Duration::from_millis(50))
        .build()
        .expect("build");

    // Embedder mounts routes directly — never calls serve_*.
    let (_routes, watch_guard) = server
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    // Yield first so the spawned heartbeat task gets its initial poll and
    // registers its `sleep(interval)` timer before we advance the clock.
    tokio::task::yield_now().await;

    // Advance several intervals, yielding multiple times per step so that the
    // spawned heartbeat task has enough polls to wake from each sleep.
    // With current_thread + start_paused, spawned tasks poll only when the
    // test task yields, so we yield generously between advances.
    for _ in 0..3 {
        tokio::time::advance(Duration::from_millis(60)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
    }

    // Drop the guard — cancels both the watch task and the heartbeat task.
    drop(watch_guard);

    let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let lines = output
        .lines()
        .filter(|l| l.contains("tsoracle::heartbeat"))
        .count();
    assert!(
        lines >= 2,
        "expected >= 2 heartbeat lines via into_router path, got {lines}.\n{output}"
    );
}

/// When the server is configured with `heartbeat_interval(Duration::ZERO)`,
/// no heartbeat task is spawned; advancing time must produce zero heartbeat
/// log lines.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn heartbeat_interval_zero_spawns_no_task() {
    let buf = BufWriter::default();
    let _guard = install_subscriber(buf.clone());

    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver)
        .heartbeat_interval(Duration::ZERO)
        .build()
        .expect("build");

    let (_routes, watch_guard) = server
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::task::yield_now().await;
    drop(watch_guard);

    let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    let lines = output
        .lines()
        .filter(|l| l.contains("tsoracle::heartbeat"))
        .count();
    assert_eq!(
        lines, 0,
        "no heartbeat lines expected with interval=0, got {lines}.\n{output}"
    );
}

/// Dropping the `WatchGuard` must stop the heartbeat task: after the drop, no
/// additional heartbeat lines must appear even if more time is advanced.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn dropping_watch_guard_stops_heartbeat() {
    let buf = BufWriter::default();
    let _guard = install_subscriber(buf.clone());

    let driver = Arc::new(InMemoryDriver::new());
    let server = Server::builder()
        .consensus_driver(driver)
        .heartbeat_interval(Duration::from_millis(50))
        .build()
        .expect("build");

    let (_routes, watch_guard) = server
        .into_router()
        .expect("into_router is infallible without the reflection feature");

    // Yield first so the spawned heartbeat task gets its initial poll and
    // registers its `sleep(interval)` timer before we advance the clock.
    tokio::task::yield_now().await;

    // One interval — at least one heartbeat line should appear. Yield
    // multiple times after advancing so the woken task can emit its line.
    tokio::time::advance(Duration::from_millis(60)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    let lines_before_drop = String::from_utf8(buf.0.lock().unwrap().clone())
        .unwrap()
        .lines()
        .filter(|l| l.contains("tsoracle::heartbeat"))
        .count();

    // Cancel both the watch task and the heartbeat task.
    drop(watch_guard);

    // Yield once more to allow any in-flight polls to settle.
    tokio::task::yield_now().await;

    // Five more intervals worth of time; the cancelled task must not emit.
    tokio::time::advance(Duration::from_millis(300)).await;
    tokio::task::yield_now().await;

    let lines_after_drop = String::from_utf8(buf.0.lock().unwrap().clone())
        .unwrap()
        .lines()
        .filter(|l| l.contains("tsoracle::heartbeat"))
        .count();

    assert!(
        lines_before_drop >= 1,
        "expected at least one heartbeat before drop, got {lines_before_drop}"
    );
    assert_eq!(
        lines_before_drop, lines_after_drop,
        "heartbeat continued after WatchGuard drop \
         ({lines_before_drop} lines before → {lines_after_drop} after)"
    );
}
