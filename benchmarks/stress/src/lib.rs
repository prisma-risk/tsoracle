#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

//! tsoracle stress + chaos harness.

pub mod chaos;
pub mod config;
pub mod event;
pub mod git;
pub mod loadgen;
pub mod nemesis;
pub mod report;
pub mod sample;
pub mod schedule;
pub mod supervisor;
pub mod topology;
pub mod types;
pub mod violation;

/// MIRRORS `bench-minimal::parse_count` — kept in sync manually.
///
/// Accepts underscore digit separators and a single trailing lowercase
/// `k`/`m`/`g`: `1k` → 1_000, `2m` → 2_000_000, `1g` → 1_000_000_000.
pub fn parse_count(input: &str) -> Result<u64, String> {
    if input.is_empty() {
        return Err("empty input".into());
    }
    let (digits, multiplier) = match input.as_bytes().last().copied() {
        Some(b'k') => (&input[..input.len() - 1], 1_000u64),
        Some(b'm') => (&input[..input.len() - 1], 1_000_000u64),
        Some(b'g') => (&input[..input.len() - 1], 1_000_000_000u64),
        _ => (input, 1u64),
    };
    let cleaned: String = digits.chars().filter(|c| *c != '_').collect();
    if cleaned.is_empty() {
        return Err(format!("no digits in {input:?}"));
    }
    let base: u64 = cleaned
        .parse()
        .map_err(|e| format!("invalid number {input:?}: {e}"))?;
    base.checked_mul(multiplier)
        .ok_or_else(|| format!("overflow parsing {input:?}"))
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tokio::runtime::Builder;
use tokio::sync::mpsc;

use crate::config::{ScenarioKind, StressConfig, TopologyKind};
use crate::event::SupervisorEvent;
use crate::git::GitInfo;
use crate::loadgen::{ClientTaskCfg, client_task};
use crate::nemesis::{play, random, scenario};
use crate::report::{LatencyStats, Outcome, RecordedCounts, Report, Throughput};
use crate::schedule::{RandomParams, Schedule};
use crate::supervisor::Supervisor;
use crate::topology::ChaosController;
use crate::topology::mem::MemTopology;
use crate::types::ClientId;

/// Tuple returned by topology spawn helpers below: the chaos controller, the
/// endpoint strings clients should dial, and the join handle for the spawned
/// server task. Aliased to suppress `clippy::type_complexity`.
type SpawnedTopology = (
    Box<dyn ChaosController>,
    Vec<String>,
    tokio::task::JoinHandle<Result<(), tsoracle_server::ServerError>>,
);

/// Histogram upper bound (60 seconds, microseconds).
pub(crate) const HISTO_MAX_US: u64 = 60_000_000;

/// Build a histogram with the standard bounds.
pub(crate) fn new_histogram() -> Histogram<u64> {
    // Bounds are compile-time constants; failure here would indicate a bug in
    // hdrhistogram itself. We use `unwrap_or_else` plus a fallback rather than
    // `.expect(..)` to stay within the crate's lint policy (warn on expect_used
    // in non-test code).
    Histogram::new_with_bounds(1, HISTO_MAX_US, 3).unwrap_or_else(|_| {
        // Fallback to a smaller, definitely-valid configuration. This branch
        // is unreachable for the constants above.
        #[allow(clippy::unwrap_used)]
        Histogram::new(1).unwrap()
    })
}

/// Top-level: resolve schedule, spawn topology, drive load, collect outcome.
///
/// Returns the `Report`. The `Outcome` carried inside maps to the exit code
/// the CLI uses for process exit (see `Outcome::exit_code`).
pub fn run(cfg: StressConfig) -> Result<Report, anyhow::Error> {
    cfg.validate().map_err(anyhow::Error::msg)?;

    // --- Build runtimes (server, client, control). ---
    let server_rt = Builder::new_multi_thread()
        .worker_threads(cfg.server_threads)
        .thread_name("stress-server")
        .enable_all()
        .build()?;
    let client_rt = Builder::new_multi_thread()
        .worker_threads(cfg.client_threads)
        .thread_name("stress-client")
        .enable_all()
        .build()?;
    let control_rt = Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("stress-control")
        .enable_all()
        .build()?;

    // --- Resolve schedule. ---
    let schedule = resolve_schedule(&cfg)?;
    if let Some(path) = cfg.schedule_out.as_ref() {
        let json = serde_json::to_string_pretty(&schedule)?;
        std::fs::write(path, json)?;
    }

    // --- Spawn topology on the server runtime. ---
    let grace = cfg.grace();
    let (controller, endpoints, server_handle): SpawnedTopology = match cfg.topology {
        TopologyKind::Mem => {
            let topo = server_rt.block_on(MemTopology::spawn(grace))?;
            let endpoints = topo.controller.endpoints();
            let server_handle = topo.server_handle;
            (Box::new(topo.controller), endpoints, server_handle)
        }
        TopologyKind::Raft => anyhow::bail!("raft topology not yet implemented"),
        TopologyKind::Process => anyhow::bail!("process topology not yet implemented"),
    };

    // --- Supervisor channel + task on control runtime. ---
    let (event_tx, event_rx) = mpsc::channel::<SupervisorEvent>(65_536);
    let supervisor_handle = control_rt.spawn(Supervisor::new().run(event_rx));

    // --- Loadgen on the client runtime. ---
    let stop = Arc::new(AtomicBool::new(false));
    let transient_retries = Arc::new(AtomicU64::new(0));

    // The controller must remain alive across the loadgen `block_on` (for
    // nemesis playback) and then be consumed by `shutdown(self: Box<Self>)`
    // afterwards. `block_on` requires a `'static` future, which precludes
    // borrowing `controller`; Arc<dyn ChaosController> would forbid the
    // by-value shutdown. We park the box in `Arc<Mutex<Option<...>>>` and
    // `.take()` it once playback finishes, then call `shutdown` on the
    // recovered box.
    let controller_slot: Arc<parking_lot::Mutex<Option<Box<dyn ChaosController>>>> =
        Arc::new(parking_lot::Mutex::new(Some(controller)));

    let cfg_for_client = cfg.clone();
    let schedule_for_client = schedule.clone();
    let stop_for_client = stop.clone();
    let transient_retries_for_client = transient_retries.clone();
    let event_tx_for_client = event_tx.clone();
    let controller_for_nemesis = controller_slot.clone();
    let endpoints_for_client = endpoints.clone();

    let load_result = client_rt.block_on(async move {
        let client = Arc::new(tsoracle_client::Client::connect(endpoints_for_client).await?);
        let mut handles = Vec::with_capacity(cfg_for_client.clients);

        for client_idx in 0..cfg_for_client.clients {
            let task_cfg = ClientTaskCfg {
                client_id: ClientId(client_idx as u32),
                client: client.clone(),
                batch_size: cfg_for_client.batch_size,
                warmup_iters: cfg_for_client.warmup,
                liveness_deadline: cfg_for_client.liveness_deadline,
                stop: stop_for_client.clone(),
                tx: event_tx_for_client.clone(),
                transient_retries: transient_retries_for_client.clone(),
            };
            handles.push(tokio::spawn(client_task(task_cfg)));
        }

        let t0 = Instant::now();

        // Apply burst loadgen pause if any.
        let stop_for_burst = stop_for_client.clone();
        let schedule_for_burst = schedule_for_client.clone();
        let burst_fut = async move {
            if let Some(pause) = schedule_for_burst.loadgen_pause.clone() {
                tokio::time::sleep(pause.at).await;
                stop_for_burst.store(true, Ordering::Relaxed);
                tokio::time::sleep(pause.dur).await;
                stop_for_burst.store(false, Ordering::Relaxed);
            } else {
                std::future::pending::<()>().await
            }
        };

        let nemesis_fut = {
            let schedule = schedule_for_client.clone();
            let event_tx = event_tx_for_client.clone();
            let controller_slot = controller_for_nemesis.clone();
            async move {
                // We hold the controller out of the slot for the duration of
                // playback. After playback, we put it back so the outer
                // function can `take()` it and call shutdown.
                let controller_box = controller_slot.lock().take();
                if let Some(controller_box) = controller_box {
                    play(&schedule, controller_box.as_ref(), event_tx, t0).await;
                    *controller_slot.lock() = Some(controller_box);
                }
                // Nemesis completion must NOT terminate the run: when the
                // scenario has zero ops (e.g. `steady`), playback returns
                // immediately, and tripping the outer `select!` here would
                // make `--duration` a no-op. Park forever so the timer (or
                // burst-pause) drives termination.
                std::future::pending::<()>().await
            }
        };

        let timer_fut = async {
            if let Some(d) = cfg_for_client.duration {
                tokio::time::sleep(d).await;
            } else {
                // --ops mode is not yet wired; sleep a placeholder.
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        };

        tokio::select! {
            _ = nemesis_fut => {},
            _ = timer_fut => {},
            _ = burst_fut => {},
        }
        stop_for_client.store(true, Ordering::Relaxed);

        for handle in handles {
            let _ = handle.await;
        }
        let _ = event_tx_for_client.send(SupervisorEvent::End).await;
        drop(event_tx_for_client);

        anyhow::Ok(t0)
    })?;

    let t0 = load_result;
    let elapsed = t0.elapsed();

    // Drop the outer event_tx so the supervisor sees channel closure.
    drop(event_tx);

    // Wait for supervisor to drain.
    let supervisor_outcome = control_rt
        .block_on(supervisor_handle)
        .map_err(|err| anyhow::anyhow!("supervisor join: {err:?}"))?;

    // Build report.
    let timestamps = supervisor_outcome.events_observed;
    let batch_size = cfg.batch_size.max(1) as u64;
    let client_calls = timestamps / batch_size;
    let elapsed_secs = elapsed.as_secs_f64().max(1e-9);
    let throughput = Throughput {
        client_calls_per_sec: client_calls as f64 / elapsed_secs,
        timestamps_per_sec: timestamps as f64 / elapsed_secs,
    };
    let merged = &supervisor_outcome.latency;
    let latency = LatencyStats {
        p50: merged.value_at_quantile(0.50),
        p90: merged.value_at_quantile(0.90),
        p99: merged.value_at_quantile(0.99),
        p999: merged.value_at_quantile(0.999),
        min: merged.min(),
        max: merged.max(),
        mean: merged.mean() as u64,
    };
    let outcome = if supervisor_outcome.violations.is_empty() {
        Outcome::Ok
    } else {
        Outcome::InvariantViolation
    };

    let hostname_str = hostname().unwrap_or_else(|| "unknown".into());
    let topology = cfg.topology;
    let report = Report {
        config: cfg,
        git: GitInfo::capture(),
        hostname: hostname_str,
        topology,
        elapsed,
        recorded: RecordedCounts {
            client_calls,
            timestamps,
        },
        throughput,
        latency_per_call_us: latency,
        transient_retries: transient_retries.load(Ordering::Relaxed),
        out_of_range_samples: 0,
        violations: supervisor_outcome.violations,
        chaos_events: Vec::new(),
        schedule,
        outcome,
    };

    // Shut topology down cleanly.
    let final_controller = controller_slot.lock().take();
    server_rt.block_on(async move {
        if let Some(controller) = final_controller {
            controller.shutdown().await;
        }
        let _ = server_handle.await;
    });
    drop(server_rt);
    drop(client_rt);
    drop(control_rt);
    Ok(report)
}

/// Self-test entry point: like `run` but forces an `InvariantViolation`
/// outcome. Used by CI as a positive control for the report + exit-code
/// pipeline. Returns a `Report` whose `outcome` MUST be
/// `Outcome::InvariantViolation`.
pub fn run_inject_violation(mut cfg: StressConfig) -> Result<Report, anyhow::Error> {
    cfg.duration = Some(Duration::from_secs(3));
    let mut report = run(cfg)?;
    report.violations.push(crate::violation::Violation {
        kind: crate::violation::ViolationKind::Monotonicity {
            prev: tsoracle_core::Timestamp(u64::MAX),
            got: tsoracle_core::Timestamp(1),
            sample: crate::sample::IssuedSample {
                client_id: ClientId(0),
                batch_id: 0,
                batch_idx: 0,
                is_last: true,
                ts: tsoracle_core::Timestamp(1),
                issued_at: Instant::now(),
                recv_time: Instant::now(),
            },
        },
        at: Instant::now(),
    });
    report.outcome = Outcome::InvariantViolation;
    Ok(report)
}

/// Re-run from a saved schedule. Pins the schedule's source (named or
/// random-seeded) and replays the ops bit-for-bit.
pub fn load_schedule(path: &std::path::Path) -> Result<Schedule, anyhow::Error> {
    let raw = std::fs::read_to_string(path)?;
    let schedule: Schedule = serde_json::from_str(&raw)?;
    Ok(schedule)
}

fn resolve_schedule(cfg: &StressConfig) -> Result<Schedule, anyhow::Error> {
    let total = cfg
        .duration
        .or_else(|| cfg.ops.map(|_| Duration::from_secs(30)))
        .ok_or_else(|| anyhow::anyhow!("config has neither duration nor ops"))?;
    match &cfg.scenario {
        ScenarioKind::Named(name) => scenario::build(name, total).map_err(anyhow::Error::msg),
        ScenarioKind::Random { seed } => {
            let params = RandomParams {
                mean_gap: Duration::from_millis(500),
                total,
                weight_kill: 1.0,
                weight_pause: 1.0,
                weight_failpoint: 0.5,
            };
            Ok(random::build(*seed, params))
        }
    }
}

fn hostname() -> Option<String> {
    std::process::Command::new("hostname")
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
            } else {
                None
            }
        })
        .filter(|host| !host.is_empty())
}

#[cfg(test)]
mod parse_count_tests {
    use super::parse_count;

    #[test]
    fn plain_number() {
        assert_eq!(parse_count("1").unwrap(), 1);
    }
    #[test]
    fn k_suffix() {
        assert_eq!(parse_count("1k").unwrap(), 1_000);
    }
    #[test]
    fn m_suffix() {
        assert_eq!(parse_count("2m").unwrap(), 2_000_000);
    }
    #[test]
    fn g_suffix() {
        assert_eq!(parse_count("1g").unwrap(), 1_000_000_000);
    }
    #[test]
    fn underscores() {
        assert_eq!(parse_count("1_500k").unwrap(), 1_500_000);
    }
    #[test]
    fn empty_rejected() {
        assert!(parse_count("").is_err());
    }
    #[test]
    fn uppercase_rejected() {
        assert!(parse_count("1K").is_err());
    }
    #[test]
    fn bare_suffix_rejected() {
        let err = parse_count("k").unwrap_err();
        assert!(err.contains("no digits"), "got: {err}");
    }
    #[test]
    fn non_numeric_rejected() {
        let err = parse_count("abc").unwrap_err();
        assert!(err.contains("invalid number"), "got: {err}");
    }
    #[test]
    fn overflow_rejected() {
        // 999...g (max u64 is ~1.8e19, so 99e9 base * 1e9 multiplier overflows).
        let err = parse_count("99999999999g").unwrap_err();
        assert!(err.contains("overflow"), "got: {err}");
    }
}

#[cfg(test)]
mod resolve_schedule_tests {
    use super::*;
    use crate::config::{ScenarioKind, TopologyKind};
    use std::net::SocketAddr;

    fn cfg_with(scenario: ScenarioKind, duration: Option<Duration>) -> StressConfig {
        StressConfig {
            topology: TopologyKind::Mem,
            scenario,
            duration,
            ops: None,
            clients: 1,
            batch_size: 1,
            warmup: 1,
            client_threads: 1,
            server_threads: 1,
            liveness_deadline: Duration::from_secs(5),
            grace_mem: Duration::from_millis(100),
            grace_raft: Duration::from_millis(750),
            grace_process: Duration::from_secs(2),
            nodes: 1,
            bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            json: false,
            json_stream: false,
            print_interval: Duration::from_secs(1),
            seed: 0,
            schedule_out: None,
            ci_smoke: false,
        }
    }

    #[test]
    fn random_scenario_resolves_to_random_source() {
        let cfg = cfg_with(
            ScenarioKind::Random { seed: 42 },
            Some(Duration::from_secs(10)),
        );
        let s = resolve_schedule(&cfg).unwrap();
        match s.source {
            crate::schedule::ScheduleSource::Random { seed, .. } => assert_eq!(seed, 42),
            other => panic!("expected Random source, got {other:?}"),
        }
    }

    #[test]
    fn named_scenario_resolves_via_catalog() {
        let cfg = cfg_with(
            ScenarioKind::Named("steady".into()),
            Some(Duration::from_secs(10)),
        );
        let s = resolve_schedule(&cfg).unwrap();
        match s.source {
            crate::schedule::ScheduleSource::Named { scenario } => {
                assert_eq!(scenario, "steady");
            }
            other => panic!("expected Named source, got {other:?}"),
        }
    }

    #[test]
    fn unknown_named_scenario_errors() {
        let cfg = cfg_with(
            ScenarioKind::Named("does-not-exist".into()),
            Some(Duration::from_secs(10)),
        );
        assert!(resolve_schedule(&cfg).is_err());
    }

    #[test]
    fn no_duration_and_no_ops_errors() {
        let cfg = cfg_with(ScenarioKind::Named("steady".into()), None);
        let err = resolve_schedule(&cfg).unwrap_err().to_string();
        assert!(err.contains("neither duration nor ops"), "got: {err}");
    }

    #[test]
    fn ops_only_uses_default_total() {
        // `ops`-only mode (no duration) takes the 30s placeholder total.
        let mut cfg = cfg_with(ScenarioKind::Named("steady".into()), None);
        cfg.ops = Some(1_000);
        let s = resolve_schedule(&cfg).unwrap();
        assert!(matches!(
            s.source,
            crate::schedule::ScheduleSource::Named { .. }
        ));
    }
}

#[cfg(test)]
mod load_schedule_tests {
    use super::*;
    use crate::schedule::{Schedule, ScheduleSource};
    use std::io::Write;

    #[test]
    fn load_schedule_round_trips_from_disk() {
        let original = Schedule {
            source: ScheduleSource::Named {
                scenario: "killer-loop".into(),
            },
            ops: Vec::new(),
            total: Duration::from_secs(15),
            loadgen_pause: None,
        };
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(serde_json::to_string(&original).unwrap().as_bytes())
            .unwrap();
        let loaded = load_schedule(file.path()).unwrap();
        assert_eq!(loaded.total, Duration::from_secs(15));
        match loaded.source {
            ScheduleSource::Named { scenario } => assert_eq!(scenario, "killer-loop"),
            _ => panic!("wrong source"),
        }
    }

    #[test]
    fn load_schedule_errors_on_missing_file() {
        assert!(load_schedule(std::path::Path::new("/no/such/path.json")).is_err());
    }

    #[test]
    fn load_schedule_errors_on_invalid_json() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"not json").unwrap();
        assert!(load_schedule(file.path()).is_err());
    }
}

#[cfg(test)]
mod histogram_tests {
    use super::*;

    #[test]
    fn new_histogram_has_expected_bounds() {
        let h = new_histogram();
        assert_eq!(h.high(), HISTO_MAX_US);
        assert_eq!(h.low(), 1);
    }
}
