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

mod tracker;

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use tsoracle_client::{ClientBuilder, RetryPolicy};

use crate::tracker::Tracker;

#[derive(Clone, Debug, ValueEnum, PartialEq, Eq)]
enum Mode {
    /// Probe each endpoint in turn; every one must serve or redirect to success.
    ColdStart,
    /// Hammer one client for a fixed duration; emit a sentinel on first success.
    Soak,
    /// Soak variant for the mixed-version rollout: sustain load with the same
    /// zero-monotonicity-violation invariant and 0.05% error tolerance while
    /// the orchestrator rolls a subset of replicas to the next image and drives
    /// activation. Prints a distinct sentinel (`mixed-version-soak: first GetTs
    /// ok`) so the orchestrator only perturbs the cluster after load is provably
    /// live. The driver itself is a pure observer of monotonicity — it does not
    /// perform the rollout or the activation; the orchestrator does.
    MixedVersionSoak,
    /// Soak variant for the SIGKILL-leader assertion (issue #485): sustain load
    /// with the same zero-monotonicity-violation invariant and the same final
    /// error tolerance ([`MAX_SIGKILL_SOAK_ERROR_RATE`]) while the orchestrator
    /// force-deletes the current leader pod (`kubectl delete pod ... --force
    /// --grace-period=0`) and waits for the StatefulSet to bring a replacement
    /// up. Distinct sentinel (`sigkill-soak: first GetTs ok`) so the
    /// orchestrator only severs the leader once load is provably live. The
    /// driver remains a pure observer of monotonicity; the orchestrator picks
    /// the leader and issues the kill.
    SigkillSoak,
    /// Soak variant for the dynamic-membership assertion (issue #487): sustain
    /// load with the same zero-monotonicity-violation invariant and the
    /// graceful [`MAX_SOAK_ERROR_RATE`] budget while the orchestrator scales
    /// the StatefulSet by one, runs `tsoracle admin add-learner`, `promote`,
    /// and `remove` against the leader, and verifies the load stays live and
    /// monotone throughout. Distinct sentinel (`membership-soak: first GetTs
    /// ok`) so the orchestrator only begins the membership churn after load
    /// is provably live. openraft-only — paxos returns `AdminError::Unsupported`.
    /// The driver remains a pure observer; the orchestrator drives every
    /// membership transition over the loopback admin port via `kubectl exec`.
    MembershipSoak,
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
/// always mask, fanned out across coalesced waiters). 0.05% leaves ~18x
/// headroom above the baseline observed in a local kind run (1 / 37,410 ≈
/// 0.0027%) while still catching a real regression.
const MAX_SOAK_ERROR_RATE: f64 = 0.0005;

/// SIGKILL-leader soak error budget. A force-deleted leader severs every
/// in-flight RPC to that pod at once (no graceful shutdown, no
/// `transfer_leader`, no draining), so this was expected to require a looser
/// budget than the graceful soak. In practice the client's retry +
/// leader-redirect masks the entire kill window: a local kind baseline saw
/// 0 / 71,261 errors (0.0000%). 0.05% (same as the graceful soak) is the
/// tightest budget the baseline supports while leaving room for transport-
/// layer jitter that the test cluster's network might not exhibit.
const MAX_SIGKILL_SOAK_ERROR_RATE: f64 = 0.0005;

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
        Mode::MixedVersionSoak => {
            run_mixed_version_soak(&cli.endpoints, Duration::from_secs(cli.duration_secs)).await?
        }
        Mode::SigkillSoak => {
            run_sigkill_soak(&cli.endpoints, Duration::from_secs(cli.duration_secs)).await?
        }
        Mode::MembershipSoak => {
            run_membership_soak(&cli.endpoints, Duration::from_secs(cli.duration_secs)).await?
        }
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
    sustain_load(endpoints, duration, "soak", MAX_SOAK_ERROR_RATE).await
}

/// Sustain GetTs load while the orchestrator performs a mixed-version rolling
/// restart and drives activation. Identical invariants to [`run_soak`] — zero
/// monotonicity violations, final error rate below [`MAX_SOAK_ERROR_RATE`] —
/// because timestamp monotonicity is exactly the property a format activation
/// must not break. The distinct sentinel (`mixed-version-soak: first GetTs ok`)
/// lets the orchestrator sequence the partial upgrade + activation only after
/// load is provably live, mirroring how the existing soak lane sequences a
/// rolling restart.
async fn run_mixed_version_soak(endpoints: &[String], duration: Duration) -> Result<bool> {
    sustain_load(
        endpoints,
        duration,
        "mixed-version-soak",
        MAX_SOAK_ERROR_RATE,
    )
    .await
}

/// Sustain GetTs load while the orchestrator force-deletes the current leader
/// pod. Same zero-monotonicity-violation invariant as the other soak modes,
/// against [`MAX_SIGKILL_SOAK_ERROR_RATE`] — which the kind baseline showed
/// the client's retry + leader-redirect masks entirely (0 errors observed).
/// The distinct sentinel (`sigkill-soak: first GetTs ok`) lets the orchestrator
/// only kill the leader after load is provably live.
async fn run_sigkill_soak(endpoints: &[String], duration: Duration) -> Result<bool> {
    sustain_load(
        endpoints,
        duration,
        "sigkill-soak",
        MAX_SIGKILL_SOAK_ERROR_RATE,
    )
    .await
}

/// Sustain GetTs load while the orchestrator scales the StatefulSet by one,
/// adds the new replica as a learner (`add-learner` blocks until catch-up),
/// promotes it to voter, and removes a pre-existing non-leader voter. Same
/// zero-monotonicity-violation invariant and graceful [`MAX_SOAK_ERROR_RATE`]
/// budget as [`run_soak`] — none of the membership ops sever an active client
/// connection (the leader stays leader, the removed voter is not the
/// leader). The distinct sentinel (`membership-soak: first GetTs ok`) lets
/// the orchestrator only begin the churn after load is provably live.
async fn run_membership_soak(endpoints: &[String], duration: Duration) -> Result<bool> {
    sustain_load(endpoints, duration, "membership-soak", MAX_SOAK_ERROR_RATE).await
}

/// Shared body of the three soak modes. The `label` is the per-mode prefix
/// used in the first-success sentinel, the per-error log line, and the final-
/// verdict tracker report — so the orchestrator can grep for the right mode
/// unambiguously. `max_error_rate` is passed through to the tracker so each
/// mode can pick its own budget against its baseline.
async fn sustain_load(
    endpoints: &[String],
    duration: Duration,
    label: &str,
    max_error_rate: f64,
) -> Result<bool> {
    let client = ClientBuilder::endpoints(endpoints.to_vec())
        .retry_policy(generous_policy())
        .build()
        .await
        .with_context(|| format!("build {label} client"))?;
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
                    println!("{label}: first GetTs ok");
                    announced = true;
                }
                tracker.record_ok(ts);
            }
            Err(error) => {
                eprintln!("{label}: get_ts error: {error}");
                tracker.record_err();
            }
        }
    }
    Ok(tracker.report_within_error_tolerance(label, max_error_rate))
}

#[cfg(test)]
mod cli_tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn cold_start_mode_parses() {
        let cli = Cli::parse_from([
            "kube-e2e-driver",
            "--mode=cold-start",
            "--endpoints=a:5051,b:5051,c:5051",
            "--count=3",
        ]);
        assert_eq!(cli.mode, Mode::ColdStart);
        assert_eq!(cli.endpoints.len(), 3);
        assert_eq!(cli.count, 3);
    }

    #[test]
    fn soak_mode_parses() {
        let cli = Cli::parse_from([
            "kube-e2e-driver",
            "--mode=soak",
            "--endpoints=a:5051,b:5051,c:5051",
            "--duration-secs=30",
        ]);
        assert_eq!(cli.mode, Mode::Soak);
        assert_eq!(cli.duration_secs, 30);
    }

    #[test]
    fn mixed_version_soak_mode_parses() {
        // The new mode shares the CLI surface of `soak` (a list of endpoints
        // + a duration); the orchestrator only needs to switch the value of
        // `--mode` and route to a job manifest using the matching sentinel.
        let cli = Cli::parse_from([
            "kube-e2e-driver",
            "--mode=mixed-version-soak",
            "--endpoints=a:5051,b:5051,c:5051",
            "--duration-secs=180",
        ]);
        assert_eq!(cli.mode, Mode::MixedVersionSoak);
        assert_eq!(cli.endpoints.len(), 3);
        assert_eq!(cli.duration_secs, 180);
    }

    #[test]
    fn sigkill_soak_mode_parses() {
        // Like the other soaks, the SIGKILL mode shares the soak CLI surface;
        // only `--mode` differs. The orchestrator picks the leader pod via
        // `tsoracle admin members` (loopback admin port; see issue #485) and
        // routes traffic at the same three replica endpoints the soak uses.
        let cli = Cli::parse_from([
            "kube-e2e-driver",
            "--mode=sigkill-soak",
            "--endpoints=a:5051,b:5051,c:5051",
            "--duration-secs=180",
        ]);
        assert_eq!(cli.mode, Mode::SigkillSoak);
        assert_eq!(cli.endpoints.len(), 3);
        assert_eq!(cli.duration_secs, 180);
    }

    #[test]
    fn membership_soak_mode_parses() {
        // Membership-soak shares the soak CLI surface; the orchestrator drives
        // the add-learner/promote/remove sequence through `kubectl exec
        // LEADER_POD -- tsoracle admin ... --endpoint http://127.0.0.1:51002`
        // (the chart's loopback-only admin bind). The endpoints list is the
        // three original replicas; the new pod tsoracle-3 reaches the client
        // via leader-hint after promote.
        let cli = Cli::parse_from([
            "kube-e2e-driver",
            "--mode=membership-soak",
            "--endpoints=a:5051,b:5051,c:5051",
            "--duration-secs=180",
        ]);
        assert_eq!(cli.mode, Mode::MembershipSoak);
        assert_eq!(cli.endpoints.len(), 3);
        assert_eq!(cli.duration_secs, 180);
    }
}
