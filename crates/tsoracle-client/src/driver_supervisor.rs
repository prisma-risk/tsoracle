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

//! One-shot supervisor that observes the client driver task's `JoinHandle`
//! and emits a death-rattle log if it panics or terminates abnormally.
//!
//! Lives in its own file rather than alongside `driver_task` because the
//! sibling `driver` module carries the performance-critical-path marker,
//! which bans info-or-higher log macros: the per-request select loop in
//! `driver_task` must not contribute to log volume, but the supervisor
//! fires at most once per `Client` lifetime, so an `error!` death-rattle
//! is the correct severity. The marker's grep-based guard cannot
//! distinguish one-shot supervisory logs from per-request logs, so the
//! file split is what keeps both rules satisfied. Same pattern as the
//! leader-watch supervisor in `tsoracle_server::server`.
//!
//! See `docs/performance-critical-path.md` for the marker rules.

use tokio::task::JoinHandle;

/// Await the driver task's `JoinHandle` and log if it panicked or was
/// cancelled. Without this, `tokio::spawn`'s default "drop the handle on
/// the floor" behaviour would absorb the panic silently and callers would
/// see only a stream of `ClientError::DriverGone` errors with no
/// breadcrumb pointing to the panicking RPC closure.
pub(crate) async fn observe_driver_handle(handle: JoinHandle<()>) {
    match handle.await {
        Ok(()) => {}
        Err(err) if err.is_panic() => {
            #[cfg(feature = "tracing")]
            tracing::error!(
                error = %err,
                "tsoracle client driver task panicked; subsequent requests will fail with ClientError::DriverGone"
            );
            // Suppress the panic in non-tracing builds: re-panicking here
            // would abort the runtime worker, which is strictly worse
            // than surfacing DriverGone to callers.
            #[cfg(not(feature = "tracing"))]
            let _ = err;
        }
        Err(err) => {
            #[cfg(feature = "tracing")]
            tracing::error!(
                error = %err,
                "tsoracle client driver task terminated abnormally (cancelled?)"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = err;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Install a process-global `TRACE` subscriber so the supervisor's
    /// `tracing::error!` death-rattle actually formats its fields under test —
    /// without a subscriber the macro short-circuits before evaluating its
    /// arguments, leaving those lines unexecuted. Idempotent across tests;
    /// mirrors the helper in `retry`.
    fn enable_tracing() {
        use tracing_subscriber::filter::LevelFilter;
        let _ = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::TRACE)
            .with_test_writer()
            .try_init();
    }

    /// Normal-exit arm: a `JoinHandle` that completes cleanly drops
    /// through the supervisor without logging. The supervisor must
    /// return, not park.
    #[tokio::test]
    async fn observe_returns_on_clean_completion() {
        let handle = tokio::spawn(async {});
        observe_driver_handle(handle).await;
    }

    /// Panic arm: a `JoinHandle` whose task panicked must not propagate
    /// the panic out of the supervisor — re-panicking inside an observer
    /// spawned with `tokio::spawn` would crash the runtime worker
    /// instead of surfacing `DriverGone` to callers, which is strictly
    /// worse. The supervisor must absorb the panic and return.
    #[tokio::test]
    async fn observe_absorbs_panic_without_repanicking() {
        enable_tracing();
        let handle = tokio::spawn(async {
            panic!("synthetic driver-task panic");
        });
        // If `observe_driver_handle` re-panicked here, the test would
        // fail with a panic message rather than completing.
        observe_driver_handle(handle).await;
    }

    /// Abnormal-termination arm: a `JoinHandle` aborted externally
    /// returns a non-panic `JoinError` (`is_panic() == false`). The
    /// supervisor must take the second `Err` arm rather than the
    /// guarded `Err(err) if err.is_panic()` arm — same absorb-and-log
    /// behaviour, different code path.
    #[tokio::test]
    async fn observe_handles_aborted_handle() {
        enable_tracing();
        let handle = tokio::spawn(async {
            // Long enough that `abort()` reliably wins over the body.
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        handle.abort();
        observe_driver_handle(handle).await;
    }
}
