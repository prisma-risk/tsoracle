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

//! Bench harness: spawns server + clients, drives load, returns a Report.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tokio::net::TcpListener;
use tokio::runtime::Builder;
use tokio::sync::Barrier;
use tonic::Code;
use tsoracle_client::{Client, ClientError};
use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::Epoch;
use tsoracle_server::{Server, test_fakes::InMemoryDriver};

use crate::{GitInfo, LatencyStats, RecordedCounts, Report, RunConfig, Throughput};

/// Upper bound on recorded latencies (microseconds). Must match the upper
/// bound passed to `Histogram::new_with_bounds` in `run`. Samples that exceed
/// this are clamped to the bound and counted in `oor_samples` so callers know
/// the percentiles read as a lower bound on the tail.
pub const HISTO_MAX_US: u64 = 60_000_000;

/// Classify a `ClientError` as transient (worth retrying once) or fatal.
///
/// Transient cases for a single-server, single-leader, no-real-network bench:
/// - `Transport(_)` / `TransportFanout(_)`: typically a connection blip during
///   server startup or graceful shutdown drain (the latter is the same failure
///   fanned out to a coalesced sibling waiter). Retry is harmless and usually quick.
/// - `Rpc` with `Unavailable`/`DeadlineExceeded`/`ResourceExhausted`: the
///   server is up but unhealthy or backpressuring; retry.
///
/// Everything else — `InvalidEndpoint`, `InvalidCount`, `NoReachableEndpoints`,
/// or any other gRPC `Code` — is a programmer or configuration bug and fails
/// the run loudly.
pub fn is_transient(err: &ClientError) -> bool {
    match err {
        ClientError::Transport(_) | ClientError::TransportFanout(_) => true,
        ClientError::Rpc(status) => matches!(
            status.code(),
            Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted
        ),
        ClientError::NoReachableEndpoints
        | ClientError::InvalidEndpoint(_)
        | ClientError::InvalidCount(_)
        | ClientError::Connector(_)
        | ClientError::DriverGone => false,
    }
}

/// Issue one call, retrying transient errors. Returns the number of
/// timestamps received on success.
#[cfg_attr(feature = "flamegraph", tracing::instrument(skip_all))]
pub async fn run_one(
    client: &Client,
    batch_size: u32,
    transient_retries: &AtomicU64,
) -> Result<u64, ClientError> {
    loop {
        let result = if batch_size == 1 {
            client.get_ts().await.map(|_| 1u64)
        } else {
            client
                .get_ts_batch(batch_size)
                .await
                .map(|b| b.len() as u64)
        };
        match result {
            Ok(n) => return Ok(n),
            Err(e) if is_transient(&e) => {
                transient_retries.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// One bench client task. Runs `warmup_iters` untimed warmup calls, awaits
/// the cross-task barrier, then runs `recorded_iters` timed calls and feeds
/// latencies into the per-task histogram.
///
/// Returns the count of *recorded* timestamps issued by this task (after
/// warmup, post-clamp).
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "flamegraph", tracing::instrument(skip_all))]
pub async fn client_task(
    client: Arc<Client>,
    warmup_iters: u64,
    recorded_iters: u64,
    batch_size: u32,
    histo: &mut Histogram<u64>,
    barrier: Arc<Barrier>,
    recorded_count: &AtomicU64,
    transient_retries: &AtomicU64,
    oor_samples: &mut u64,
) -> Result<u64, ClientError> {
    for _ in 0..warmup_iters {
        run_one(&client, batch_size, transient_retries).await?;
    }
    barrier.wait().await;

    let mut timestamps_issued: u64 = 0;
    for _ in 0..recorded_iters {
        let start = Instant::now();
        let n = run_one(&client, batch_size, transient_retries).await?;
        let elapsed_us = start.elapsed().as_micros() as u64;
        let clamped = elapsed_us.clamp(1, HISTO_MAX_US);
        if clamped != elapsed_us {
            *oor_samples += 1;
        }
        histo
            .record(clamped)
            .expect("value was clamped to histogram bounds");
        timestamps_issued += n;
        recorded_count.fetch_add(1, Ordering::Relaxed);
    }
    Ok(timestamps_issued)
}

/// Per-task histogram bounds. Lower bound 1 µs, upper `HISTO_MAX_US`,
/// 3 significant figures.
fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, HISTO_MAX_US, 3).expect("constant bounds must be valid")
}

/// Run a single benchmark to completion. The supervisor blocks the caller's
/// thread; both runtimes are owned by this function.
#[cfg_attr(feature = "flamegraph", tracing::instrument(skip_all))]
pub fn run(cfg: RunConfig) -> anyhow::Result<Report> {
    cfg.validate().map_err(anyhow::Error::msg)?;

    let git = GitInfo::capture();
    let hostname = hostname().unwrap_or_else(|| "unknown".to_string());

    let recorded_per_task = (cfg.ops - cfg.warmup) / (cfg.batch_size as u64) / (cfg.clients as u64);
    let warmup_per_task = cfg.warmup / (cfg.batch_size as u64) / (cfg.clients as u64);

    // Build the two runtimes. 3 MB stack matches openraft's minimal.
    let server_rt = Builder::new_multi_thread()
        .worker_threads(cfg.server_threads)
        .thread_stack_size(3 * 1024 * 1024)
        .thread_name("bench-server")
        .enable_all()
        .build()?;
    let client_rt = Builder::new_multi_thread()
        .worker_threads(cfg.client_threads)
        .thread_stack_size(3 * 1024 * 1024)
        .thread_name("bench-client")
        .enable_all()
        .build()?;

    // ---- Spawn the server on the server runtime. ----
    let driver = Arc::new(InMemoryDriver::new());
    driver.become_leader(Epoch(1));

    let bind_addr = cfg.bind;
    let listener = server_rt.block_on(async move { TcpListener::bind(bind_addr).await })?;
    let resolved_addr = listener.local_addr()?;

    let server = Server::builder()
        .consensus_driver(driver.clone() as Arc<dyn ConsensusDriver>)
        .build()
        .map_err(|e| anyhow::anyhow!("server build: {e:?}"))?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server_handle = server_rt.spawn(async move {
        server
            .serve_with_listener(listener, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    // ---- Run the client side. ----
    let cfg_for_client = cfg.clone();
    let report = client_rt.block_on(async move {
        // Connect a single shared client.
        let endpoint = format!("http://{resolved_addr}");
        let client = Arc::new(Client::connect(vec![endpoint]).await?);

        let barrier = Arc::new(Barrier::new(cfg_for_client.clients + 1));
        let recorded_count = Arc::new(AtomicU64::new(0));
        let transient_retries = Arc::new(AtomicU64::new(0));

        // Spawn the printer.
        let printer_stop = Arc::new(AtomicBool::new(false));
        let printer = spawn_printer(
            recorded_count.clone(),
            printer_stop.clone(),
            cfg_for_client.print_interval,
        );

        // Spawn each task and collect its results via channel.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<TaskResult>();
        let mut handles = Vec::with_capacity(cfg_for_client.clients);
        for _ in 0..cfg_for_client.clients {
            let client = client.clone();
            let barrier = barrier.clone();
            let recorded_count = recorded_count.clone();
            let transient_retries = transient_retries.clone();
            let tx = tx.clone();
            let batch_size = cfg_for_client.batch_size;
            handles.push(tokio::spawn(async move {
                let mut histo = new_histogram();
                let mut oor: u64 = 0;
                let outcome = client_task(
                    client,
                    warmup_per_task,
                    recorded_per_task,
                    batch_size,
                    &mut histo,
                    barrier,
                    &recorded_count,
                    &transient_retries,
                    &mut oor,
                )
                .await;
                let failure = outcome.as_ref().err().map(|e| format!("{e:?}"));
                let timestamps = outcome.unwrap_or(0);
                let _ = tx.send(TaskResult {
                    timestamps,
                    histo,
                    oor_samples: oor,
                    failure,
                });
            }));
        }
        drop(tx);

        // Barrier release: when this awaits, all tasks have finished warmup.
        barrier.wait().await;
        let t0 = Instant::now();

        // Drain every task's result.
        let mut merged = new_histogram();
        let mut total_timestamps: u64 = 0;
        let mut total_oor: u64 = 0;
        let mut first_failure: Option<String> = None;
        while let Some(result) = rx.recv().await {
            total_timestamps += result.timestamps;
            total_oor += result.oor_samples;
            merged.add(&result.histo).expect("compatible bounds");
            if first_failure.is_none() && result.failure.is_some() {
                first_failure = result.failure;
            }
        }
        // t1 is stamped after the recv loop drains, not as each task finishes
        // — so it includes the supervisor's serial `merged.add(...)` overhead
        // for results that arrive while later tasks are still running. For
        // typical multi-second runs this is sub-millisecond noise; for very
        // short runs (smoke test scale) it pessimizes throughput by a few %.
        // A more precise design would have each task stamp its own end Instant
        // and take the max; deferring until someone needs the precision.
        let t1 = Instant::now();

        // Stop the printer and join handles.
        printer_stop.store(true, Ordering::Relaxed);
        let _ = printer.await;
        for h in handles {
            let _ = h.await;
        }

        if let Some(failure) = first_failure {
            return Err(anyhow::anyhow!("a client task failed: {failure}"));
        }

        let elapsed = t1.saturating_duration_since(t0);
        let total_calls = recorded_count.load(Ordering::Relaxed);
        let throughput = Throughput {
            client_calls_per_sec: total_calls as f64 / elapsed.as_secs_f64(),
            timestamps_per_sec: total_timestamps as f64 / elapsed.as_secs_f64(),
        };
        let latency = LatencyStats {
            p50: merged.value_at_quantile(0.50),
            p90: merged.value_at_quantile(0.90),
            p99: merged.value_at_quantile(0.99),
            p999: merged.value_at_quantile(0.999),
            min: merged.min(),
            max: merged.max(),
            mean: merged.mean() as u64,
        };
        Ok::<Report, anyhow::Error>(Report {
            config: cfg_for_client.clone(),
            git,
            profile: profile(),
            hostname,
            resolved_addr,
            elapsed,
            recorded: RecordedCounts {
                client_calls: total_calls,
                timestamps: total_timestamps,
            },
            throughput,
            latency_per_call_us: latency,
            transient_retries: transient_retries.load(Ordering::Relaxed),
            out_of_range_samples: total_oor,
        })
    })?;

    // ---- Shut the server down cleanly. ----
    let _ = shutdown_tx.send(());
    let _ = server_rt.block_on(server_handle);

    // Tokio runtimes must be dropped from a non-async context. block_on returned;
    // we're back in the sync top-level. Explicit drop for clarity:
    drop(server_rt);
    drop(client_rt);

    Ok(report)
}

struct TaskResult {
    timestamps: u64,
    histo: Histogram<u64>,
    oor_samples: u64,
    failure: Option<String>,
}

fn spawn_printer(
    recorded_count: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    // Floor the interval at 50 ms so `--print-interval 0s` doesn't starve.
    let interval = interval.max(Duration::from_millis(50));
    tokio::spawn(async move {
        let start = Instant::now();
        let mut last = 0u64;
        while !stop.load(Ordering::Relaxed) {
            tokio::time::sleep(interval).await;
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let now = recorded_count.load(Ordering::Relaxed);
            let delta = now.saturating_sub(last);
            let elapsed = start.elapsed().as_secs_f64();
            let rate = delta as f64 / interval.as_secs_f64();
            eprintln!(
                "[{elapsed:>6.2}s] recorded={now} (+{delta} in last {interval:?}, \u{2248}{rate:.0} calls/s)"
            );
            last = now;
        }
    })
}

fn hostname() -> Option<String> {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .filter(|s| !s.is_empty())
}

fn profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}
