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

//! Embed the tsoracle server with the metrics feature enabled, install a
//! Prometheus recorder, and expose `/metrics` for scraping.
//!
//! Run: `cargo run -p example-metrics-prometheus`
//! Then in another terminal: `curl http://127.0.0.1:9552/metrics`.
//! Ctrl-C drains in-flight RPCs and exits cleanly.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::broadcast;
use tsoracle_client::Client;
use tsoracle_driver_file::FileDriver;
use tsoracle_server::Server;

const GRPC_ADDR: &str = "127.0.0.1:50552";
const METRICS_ADDR: &str = "127.0.0.1:9552";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let metrics_addr: SocketAddr = METRICS_ADDR.parse()?;
    let grpc_addr: SocketAddr = GRPC_ADDR.parse()?;

    // The `metrics` crate resolves the global recorder lazily per call site
    // and caches the result on first emission. An emission that lands before
    // install pins that call site to the noop recorder for the rest of the
    // process, so the exporter MUST be installed before any tsoracle code
    // runs.
    PrometheusBuilder::new()
        .with_http_listener(metrics_addr)
        .install()?;

    let driver = FileDriver::open_or_init(PathBuf::from("./tsoracle-prometheus-data"))?;
    let server = Server::builder().consensus_driver(driver).build()?;

    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let load_handle = tokio::spawn(drive_load(grpc_addr, shutdown_tx.subscribe()));

    let shutdown_signal = {
        let shutdown_tx = shutdown_tx.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("ctrl-c received, draining");
            let _ = shutdown_tx.send(());
        }
    };

    println!(
        "gRPC on http://{grpc_addr} \u{2014} scrape http://{metrics_addr}/metrics \u{2014} Ctrl-C to exit"
    );

    server
        .serve_with_shutdown(grpc_addr, shutdown_signal)
        .await?;
    let _ = load_handle.await;
    Ok(())
}

/// Calls `get_ts_batch` on a loop so a fresh scrape shows non-zero counters
/// and at least one histogram sample, instead of an empty exposition body.
async fn drive_load(addr: SocketAddr, mut shutdown: broadcast::Receiver<()>) {
    let client = loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => return,
            attempt = Client::connect(vec![addr.to_string()]) => match attempt {
                Ok(client) => break client,
                Err(err) => {
                    tracing::debug!(%err, "client connect failed, retrying");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    };

    let mut interval = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            biased;
            _ = shutdown.recv() => return,
            _ = interval.tick() => {
                if let Err(err) = client.get_ts_batch(5).await {
                    tracing::debug!(%err, "get_ts_batch failed");
                }
            }
        }
    }
}
