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
/// On Unix, calling this function installs both signal handlers synchronously,
/// before the returned future is first polled. Call it before advertising
/// process readiness so a shutdown request cannot race handler registration.
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
pub fn shutdown_signal() -> impl std::future::Future<Output = ()> + Send + 'static {
    use tokio::signal::unix::{SignalKind, signal};

    // `signal(...)` registers with Tokio synchronously. Keep both calls outside
    // the async block so the default terminate-the-process dispositions are
    // replaced before the returned future can be parked behind server startup.
    let sigterm = signal(SignalKind::terminate());
    let sigint = signal(SignalKind::interrupt());

    async move {
        let _signal_name = match (sigterm, sigint) {
            (Ok(mut sigterm), Ok(mut sigint)) => {
                tokio::select! {
                    _ = sigint.recv() => "SIGINT",
                    _ = sigterm.recv() => "SIGTERM",
                }
            }
            (Ok(mut sigterm), Err(_sigint_error)) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    error = %_sigint_error,
                    "could not install SIGINT handler; only SIGTERM will trigger shutdown"
                );
                let _ = sigterm.recv().await;
                "SIGTERM"
            }
            (Err(_sigterm_error), Ok(mut sigint)) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    error = %_sigterm_error,
                    "could not install SIGTERM handler; only SIGINT will trigger shutdown"
                );
                let _ = sigint.recv().await;
                "SIGINT"
            }
            (Err(_sigterm_error), Err(_sigint_error)) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    sigterm_error = %_sigterm_error,
                    sigint_error = %_sigint_error,
                    "could not install SIGTERM or SIGINT handler"
                );
                std::future::pending::<&'static str>().await
            }
        };
        #[cfg(feature = "tracing")]
        tracing::info!(signal = _signal_name, "shutdown signal received");
    }
}

/// Resolves when the process receives Ctrl-C. See the unix variant for the full
/// contract; non-unix targets expose only SIGINT.
#[cfg(not(unix))]
pub async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    #[cfg(feature = "tracing")]
    tracing::info!(signal = "SIGINT", "shutdown signal received");
}
