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

mod tracker;

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use tsoracle_client::{ClientBuilder, RetryPolicy};

use crate::tracker::Tracker;

#[derive(Clone, Debug, ValueEnum)]
enum Mode {
    /// Probe each endpoint in turn; every one must serve or redirect to success.
    ColdStart,
    /// Hammer one client for a fixed duration; emit a sentinel on first success.
    Soak,
}

#[derive(Parser, Debug)]
#[command(name = "kube-e2e-driver")]
struct Cli {
    #[arg(long, value_enum)]
    mode: Mode,

    /// Comma-separated bare `host:port` endpoints. cold-start probes each in
    /// turn; soak builds one client over all of them.
    #[arg(long, value_delimiter = ',', required = true)]
    endpoints: Vec<String>,

    /// cold-start: number of GetTs calls per endpoint.
    #[arg(long, default_value_t = 5)]
    count: u32,

    /// soak: how long to sustain load, in seconds.
    #[arg(long, default_value_t = 120)]
    duration_secs: u64,
}

/// Soak error budget: monotonicity is the hard invariant (zero tolerance), but
/// a graceful rolling restart has an irreducible window where an in-flight RPC
/// to a terminating pod is severed (a transport error the client's retry cannot
/// always mask, fanned out across coalesced waiters). 0.5% leaves ample room
/// above the observed teardown rate while still catching a real regression.
const MAX_SOAK_ERROR_RATE: f64 = 0.005;

/// A budget generous enough that a single-pod restart's brief re-election is
/// masked by the client's retry + leader-redirect, so a "final error" really
/// means the client gave up.
fn generous_policy() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 50,
        overall_deadline: Duration::from_secs(20),
        ..RetryPolicy::default()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let passed = match cli.mode {
        Mode::ColdStart => run_cold_start(&cli.endpoints, cli.count).await?,
        Mode::Soak => run_soak(&cli.endpoints, Duration::from_secs(cli.duration_secs)).await?,
    };
    if !passed {
        std::process::exit(1);
    }
    Ok(())
}

/// Probe each ordinal endpoint independently. A fresh single-endpoint client
/// forces traffic at that specific pod; a follower replies with a leader-hint
/// the in-cluster client follows. Any endpoint that cannot ultimately produce a
/// timestamp fails the run, which is what proves all three nodes participate.
async fn run_cold_start(endpoints: &[String], count: u32) -> Result<bool> {
    let mut tracker: Tracker<_> = Tracker::new();
    for endpoint in endpoints {
        let client = ClientBuilder::endpoints(vec![endpoint.clone()])
            .retry_policy(generous_policy())
            .build()
            .await
            .with_context(|| format!("build client for {endpoint}"))?;
        for _ in 0..count {
            match client.get_ts().await {
                Ok(ts) => tracker.record_ok(ts),
                Err(error) => {
                    eprintln!("cold-start: {endpoint} get_ts error: {error}");
                    tracker.record_err();
                }
            }
        }
    }
    Ok(tracker.report("cold-start"))
}

/// Sustain GetTs load across the whole list for `duration`. Prints a sentinel
/// on the first success so the workflow knows load is live before it restarts
/// the StatefulSet.
async fn run_soak(endpoints: &[String], duration: Duration) -> Result<bool> {
    let client = ClientBuilder::endpoints(endpoints.to_vec())
        .retry_policy(generous_policy())
        .build()
        .await
        .context("build soak client")?;
    let mut tracker: Tracker<_> = Tracker::new();
    let mut announced = false;
    // The deadline is checked before each call, so the loop can overrun by up
    // to one client `overall_deadline` (the in-flight get_ts) against a fully
    // unreachable cluster; size any Job activeDeadlineSeconds with that slack.
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        match client.get_ts().await {
            Ok(ts) => {
                if !announced {
                    println!("soak: first GetTs ok");
                    announced = true;
                }
                tracker.record_ok(ts);
            }
            Err(error) => {
                eprintln!("soak: get_ts error: {error}");
                tracker.record_err();
            }
        }
    }
    Ok(tracker.report_within_error_tolerance("soak", MAX_SOAK_ERROR_RATE))
}
