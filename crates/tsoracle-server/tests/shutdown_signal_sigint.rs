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

//! `shutdown_signal()` must also resolve on SIGINT — the disposition an
//! interactive Ctrl-C sends. Isolated in its own process for the same reason as
//! the SIGTERM companion: it raises a process-wide signal, and a dedicated
//! process keeps that from disturbing any other test.

#![cfg(unix)]

use std::time::Duration;

use nix::sys::signal::{Signal, raise};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_signal_resolves_on_sigint() {
    let waiter = tokio::spawn(tsoracle_server::shutdown_signal());

    // The `select!` polls `ctrl_c()` (SIGINT) on first poll, installing the
    // handler that replaces the default terminate-the-process disposition. Wait
    // for that before raising SIGINT.
    tokio::time::sleep(Duration::from_millis(250)).await;
    raise(Signal::SIGINT).expect("raising SIGINT at our own process must succeed");

    tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .expect("shutdown_signal must resolve within 5s of SIGINT")
        .expect("the shutdown_signal task must not panic");
}
