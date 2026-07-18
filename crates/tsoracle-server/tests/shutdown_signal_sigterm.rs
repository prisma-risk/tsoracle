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

//! `shutdown_signal()` must resolve when the process receives SIGTERM — the
//! disposition a container orchestrator (Kubernetes, `docker stop`) uses to ask
//! a node to drain.
//!
//! This lives in its own integration-test binary, alone, on purpose: the test
//! raises a process-wide signal, and isolating it in a dedicated process means
//! that signal can never perturb another test sharing the runtime. The
//! end-to-end "a real server drains on SIGTERM" behaviour is covered separately
//! by `examples/openraft-standalone/tests/sigterm.rs`; this test pins the
//! library helper itself.

#![cfg(unix)]

use std::time::Duration;

use nix::sys::signal::{Signal, raise};
use tracing_subscriber::filter::LevelFilter;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_signal_resolves_on_sigterm() {
    // A `TRACE`-level subscriber so the helper's "shutdown signal received"
    // `tracing::info!` actually formats its fields under test rather than
    // short-circuiting.
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .with_test_writer()
        .try_init();

    // Construct but do not poll the future before raising SIGTERM. The helper
    // must replace the default terminate-the-process disposition synchronously
    // so a signal received during process startup is retained for later.
    let waiter = tsoracle_server::shutdown_signal();
    raise(Signal::SIGTERM).expect("raising SIGTERM at our own process must succeed");

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("shutdown_signal must resolve within 5s of SIGTERM");
}
