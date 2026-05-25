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

    let waiter = tokio::spawn(tsoracle_server::shutdown_signal());

    // `shutdown_signal` installs the SIGTERM handler the first time the spawned
    // task is polled — replacing the default "terminate the process"
    // disposition. Wait until it is parked on the handler before raising the
    // signal so the raise cannot kill the test process through the default
    // disposition.
    tokio::time::sleep(Duration::from_millis(250)).await;
    raise(Signal::SIGTERM).expect("raising SIGTERM at our own process must succeed");

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("shutdown_signal must resolve within 5s of SIGTERM")
        .expect("the shutdown_signal task must not panic");
}
