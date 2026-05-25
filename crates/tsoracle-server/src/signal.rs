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

//! A graceful-shutdown signal future for embedders that run their own `main`.

/// Resolves when the process receives an OS request to terminate.
///
/// Container orchestrators (Kubernetes, `docker stop`) and systemd stop
/// processes with SIGTERM, while an interactive Ctrl-C sends SIGINT. Feed this
/// to [`Server::serve_with_shutdown`](crate::Server::serve_with_shutdown) (or
/// [`Server::serve_with_listener`](crate::Server::serve_with_listener)) so both
/// drive tonic's graceful drain; otherwise the default SIGTERM disposition
/// kills the process mid-flight and a supervisor SIGKILLs it after the grace
/// period. On non-unix targets only Ctrl-C is available.
///
/// ```no_run
/// # async fn run(server: tsoracle_server::Server) -> Result<(), tsoracle_server::ServerError> {
/// let addr = "0.0.0.0:50551".parse().unwrap();
/// server
///     .serve_with_shutdown(addr, tsoracle_server::shutdown_signal())
///     .await
/// # }
/// ```
#[cfg(unix)]
pub async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(stream) => stream,
        Err(_error) => {
            // Installing the SIGTERM handler only fails on resource exhaustion
            // or an invalid signal — neither expected here. Degrade to Ctrl-C
            // rather than refuse to shut down an otherwise-healthy node.
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_error, "could not install SIGTERM handler; only Ctrl-C will trigger shutdown");
            let _ = tokio::signal::ctrl_c().await;
            #[cfg(feature = "tracing")]
            tracing::info!(signal = "SIGINT", "shutdown signal received");
            return;
        }
    };

    let _signal_name = tokio::select! {
        _ = tokio::signal::ctrl_c() => "SIGINT",
        _ = sigterm.recv() => "SIGTERM",
    };
    #[cfg(feature = "tracing")]
    tracing::info!(signal = _signal_name, "shutdown signal received");
}

/// Resolves when the process receives Ctrl-C. See the unix variant for the full
/// contract; non-unix targets expose only SIGINT.
#[cfg(not(unix))]
pub async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    #[cfg(feature = "tracing")]
    tracing::info!(signal = "SIGINT", "shutdown signal received");
}
