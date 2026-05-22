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

//! tsoracle overhead benchmark.

use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

pub mod harness;

/// Parse a `u64` argument that accepts underscore digit separators and a
/// single trailing `k`/`m`/`g` suffix (lowercase only). The parser is small
/// on purpose; richer suffix sets can come later.
///
/// `1` → 1, `1_000` → 1_000, `1k` → 1_000, `1m` → 1_000_000, `1g` → 1_000_000_000,
/// `1_500k` → 1_500_000.
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

#[cfg(test)]
mod parse_count_tests {
    use super::parse_count;

    #[test]
    fn plain_number() {
        assert_eq!(parse_count("1").unwrap(), 1);
        assert_eq!(parse_count("1234567890").unwrap(), 1_234_567_890);
    }

    #[test]
    fn underscores_are_separators() {
        assert_eq!(parse_count("1_000").unwrap(), 1_000);
        assert_eq!(parse_count("1_000_000").unwrap(), 1_000_000);
    }

    #[test]
    fn k_m_g_suffixes() {
        assert_eq!(parse_count("1k").unwrap(), 1_000);
        assert_eq!(parse_count("2m").unwrap(), 2_000_000);
        assert_eq!(parse_count("1g").unwrap(), 1_000_000_000);
    }

    #[test]
    fn suffix_with_underscores() {
        assert_eq!(parse_count("1_500k").unwrap(), 1_500_000);
    }

    #[test]
    fn upper_case_suffix_rejected() {
        assert!(parse_count("1K").is_err());
    }

    #[test]
    fn empty_input_rejected() {
        assert!(parse_count("").is_err());
    }

    #[test]
    fn suffix_only_rejected() {
        assert!(parse_count("k").is_err());
    }

    #[test]
    fn alphabetic_in_middle_rejected() {
        assert!(parse_count("1k2").is_err());
    }

    #[test]
    fn zero_returns_ok() {
        assert_eq!(parse_count("0").unwrap(), 0);
    }

    #[test]
    fn underscore_only_rejected() {
        assert!(parse_count("_").is_err());
    }

    #[test]
    fn multiply_overflow_rejected() {
        assert!(parse_count("18446744073709551g").is_err());
    }
}

/// All configuration for a single benchmark run.
#[derive(Debug, Clone)]
pub struct RunConfig {
    pub clients: usize,
    pub ops: u64,
    pub batch_size: u32,
    pub client_threads: usize,
    pub server_threads: usize,
    pub warmup: u64,
    pub bind: SocketAddr,
    pub print_interval: Duration,
    pub json: bool,
    pub seed: u64,
}

impl RunConfig {
    /// Validate that no degenerate combination would panic or produce zero
    /// recorded samples. Returns a human-readable error suitable for printing
    /// to stderr.
    pub fn validate(&self) -> Result<(), String> {
        if self.clients == 0 {
            return Err("--clients must be >= 1".into());
        }
        if self.batch_size == 0 {
            return Err("--batch-size must be >= 1".into());
        }
        if self.client_threads == 0 {
            return Err("--client-threads must be >= 1".into());
        }
        if self.server_threads == 0 {
            return Err("--server-threads must be >= 1".into());
        }
        if self.ops == 0 {
            return Err("--ops must be >= 1".into());
        }
        if self.warmup >= self.ops {
            return Err(format!(
                "--warmup ({}) must be strictly less than --ops ({})",
                self.warmup, self.ops
            ));
        }
        let recorded = self.ops - self.warmup;
        let per_task = recorded / (self.batch_size as u64) / (self.clients as u64);
        if per_task == 0 {
            return Err(format!(
                "no recorded samples per task: \
                 (ops - warmup) / batch_size / clients = \
                 ({recorded}) / {} / {} = 0. \
                 Increase --ops or decrease --clients/--batch-size.",
                self.batch_size, self.clients
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod run_config_tests {
    use super::RunConfig;
    use std::net::SocketAddr;
    use std::time::Duration;

    fn ok_config() -> RunConfig {
        RunConfig {
            clients: 2,
            ops: 200,
            batch_size: 1,
            client_threads: 1,
            server_threads: 1,
            warmup: 10,
            bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
            print_interval: Duration::from_secs(1),
            json: false,
            seed: 0,
        }
    }

    #[test]
    fn known_good_config_validates() {
        ok_config().validate().unwrap();
    }

    #[test]
    fn zero_clients_rejected() {
        let cfg = RunConfig {
            clients: 0,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("--clients"));
    }

    #[test]
    fn zero_batch_rejected() {
        let cfg = RunConfig {
            batch_size: 0,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("--batch-size"));
    }

    #[test]
    fn zero_client_threads_rejected() {
        let cfg = RunConfig {
            client_threads: 0,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("--client-threads"));
    }

    #[test]
    fn zero_server_threads_rejected() {
        let cfg = RunConfig {
            server_threads: 0,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("--server-threads"));
    }

    #[test]
    fn zero_ops_rejected() {
        let cfg = RunConfig {
            ops: 0,
            warmup: 0,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("--ops"));
    }

    #[test]
    fn warmup_equal_to_ops_rejected() {
        let cfg = RunConfig {
            warmup: 200,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("--warmup"));
    }

    #[test]
    fn warmup_greater_than_ops_rejected() {
        let cfg = RunConfig {
            warmup: 500,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("--warmup"));
    }

    #[test]
    fn no_recorded_samples_rejected() {
        // (200 - 10) / 1 / 1000 = 0
        let cfg = RunConfig {
            clients: 1000,
            ..ok_config()
        };
        assert!(cfg.validate().unwrap_err().contains("no recorded samples"));
    }
}

/// Git metadata captured at runtime. `rev` is "unknown" and `dirty` is `false`
/// when `git` isn't available, the directory isn't a checkout, or any command
/// fails. Capturing at runtime (rather than via `build.rs` + env vars) avoids
/// the stale-cache problem where commits to the current branch (which update
/// `refs/heads/<branch>`, not `HEAD`) or working-tree edits don't trigger a
/// rebuild.
#[derive(Debug, Clone)]
pub struct GitInfo {
    pub rev: String,
    pub dirty: bool,
}

impl GitInfo {
    pub fn capture() -> Self {
        let rev = Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
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
            .unwrap_or_else(|| "unknown".to_string());

        // `git diff-index --quiet HEAD --` exits 0 if clean, 1 if dirty,
        // anything else on error. We treat error as "clean" to avoid false
        // positives in environments without a checkout.
        let dirty = Command::new("git")
            .args(["diff-index", "--quiet", "HEAD", "--"])
            .status()
            .ok()
            .map(|s| s.code() == Some(1))
            .unwrap_or(false);

        GitInfo { rev, dirty }
    }
}

#[cfg(test)]
mod git_info_tests {
    use super::GitInfo;

    #[test]
    fn capture_returns_non_empty_rev() {
        // In CI / dev this should be a real rev; in a fresh tarball it will
        // be "unknown". Either way, `rev` is non-empty.
        let info = GitInfo::capture();
        assert!(!info.rev.is_empty(), "rev should never be empty");
    }
}

/// Final report returned by `harness::run` and rendered to text/JSON.
#[derive(Debug, Clone)]
pub struct Report {
    pub config: RunConfig,
    pub git: GitInfo,
    pub profile: &'static str,
    pub hostname: String,
    pub resolved_addr: SocketAddr,
    pub elapsed: Duration,
    pub recorded: RecordedCounts,
    pub throughput: Throughput,
    pub latency_per_call_us: LatencyStats,
    pub transient_retries: u64,
    pub out_of_range_samples: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct RecordedCounts {
    pub client_calls: u64,
    pub timestamps: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Throughput {
    pub client_calls_per_sec: f64,
    pub timestamps_per_sec: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct LatencyStats {
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub min: u64,
    pub max: u64,
    pub mean: u64,
}

impl Report {
    pub fn render_text(&self) -> String {
        let dirty = if self.git.dirty { "true" } else { "false" };
        format!(
            "tsoracle bench-minimal — git rev {} (dirty={}), profile={}, hostname={}\n\
             config:        clients={} ops={} batch={} client_threads={} server_threads={} warmup={}\n\
             recorded:      client_calls={} timestamps={}\n\
             elapsed:       {:.3} s        (post-warmup, from barrier release to last task done)\n\
             throughput:    client_calls/s: {:.0}        timestamps/s: {:.0}\n\
             latency per client call:\n  \
             p50: {} µs          p90: {} µs           p99: {} µs           p999: {} µs\n  \
             min: {} µs           max: {} µs          mean: {} µs\n\
             transient retries:    {}\n\
             out-of-range samples: {}\n",
            self.git.rev,
            dirty,
            self.profile,
            self.hostname,
            self.config.clients,
            self.config.ops,
            self.config.batch_size,
            self.config.client_threads,
            self.config.server_threads,
            self.config.warmup,
            self.recorded.client_calls,
            self.recorded.timestamps,
            self.elapsed.as_secs_f64(),
            self.throughput.client_calls_per_sec,
            self.throughput.timestamps_per_sec,
            self.latency_per_call_us.p50,
            self.latency_per_call_us.p90,
            self.latency_per_call_us.p99,
            self.latency_per_call_us.p999,
            self.latency_per_call_us.min,
            self.latency_per_call_us.max,
            self.latency_per_call_us.mean,
            self.transient_retries,
            self.out_of_range_samples,
        )
    }

    pub fn render_json(&self) -> String {
        let value = serde_json::json!({
            "config": {
                "clients": self.config.clients,
                "ops_nominal": self.config.ops,
                "batch_size": self.config.batch_size,
                "client_threads": self.config.client_threads,
                "server_threads": self.config.server_threads,
                "warmup_nominal": self.config.warmup,
                "bind": self.config.bind.to_string(),
                "resolved_addr": self.resolved_addr.to_string(),
            },
            "git_rev": self.git.rev,
            "git_dirty": self.git.dirty,
            "profile": self.profile,
            "hostname": self.hostname,
            "elapsed_s": self.elapsed.as_secs_f64(),
            "recorded": {
                "client_calls": self.recorded.client_calls,
                "timestamps": self.recorded.timestamps,
            },
            "throughput": {
                "client_calls_per_sec": self.throughput.client_calls_per_sec,
                "timestamps_per_sec": self.throughput.timestamps_per_sec,
            },
            "latency_per_call_us": {
                "p50": self.latency_per_call_us.p50,
                "p90": self.latency_per_call_us.p90,
                "p99": self.latency_per_call_us.p99,
                "p999": self.latency_per_call_us.p999,
                "min": self.latency_per_call_us.min,
                "max": self.latency_per_call_us.max,
                "mean": self.latency_per_call_us.mean,
            },
            "transient_retries": self.transient_retries,
            "out_of_range_samples": self.out_of_range_samples,
        });
        value.to_string()
    }
}

#[cfg(test)]
mod report_tests {
    use super::*;

    fn sample_report() -> Report {
        Report {
            config: RunConfig {
                clients: 64,
                ops: 1_000_000,
                batch_size: 4,
                client_threads: 1,
                server_threads: 8,
                warmup: 100_000,
                bind: "127.0.0.1:0".parse().unwrap(),
                print_interval: Duration::from_secs(1),
                json: true,
                seed: 0,
            },
            git: GitInfo {
                rev: "c0ffee".into(),
                dirty: false,
            },
            profile: "release",
            hostname: "mac-m1".into(),
            resolved_addr: "127.0.0.1:58219".parse().unwrap(),
            elapsed: Duration::from_micros(3_069_000),
            recorded: RecordedCounts {
                client_calls: 224_960,
                timestamps: 899_840,
            },
            throughput: Throughput {
                client_calls_per_sec: 73_300.0,
                timestamps_per_sec: 293_200.0,
            },
            latency_per_call_us: LatencyStats {
                p50: 186,
                p90: 312,
                p99: 854,
                p999: 2_410,
                min: 94,
                max: 7_120,
                mean: 219,
            },
            transient_retries: 0,
            out_of_range_samples: 0,
        }
    }

    #[test]
    fn text_contains_key_fields() {
        let s = sample_report().render_text();
        assert!(
            s.contains("client_calls=224960") || s.contains("client_calls=224_960"),
            "text was: {s}"
        );
        assert!(
            s.contains("timestamps=899840") || s.contains("timestamps=899_840"),
            "text was: {s}"
        );
        assert!(s.contains("p50: 186"), "text was: {s}");
        assert!(s.contains("out-of-range samples: 0"), "text was: {s}");
    }

    #[test]
    fn json_round_trips() {
        let raw = sample_report().render_json();
        let parsed: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["recorded"]["client_calls"], 224_960);
        assert_eq!(parsed["recorded"]["timestamps"], 899_840);
        assert_eq!(parsed["config"]["ops_nominal"], 1_000_000);
        assert_eq!(parsed["git_dirty"], false);
        assert!(parsed["throughput"]["timestamps_per_sec"].as_f64().unwrap() > 0.0);
    }

    #[test]
    fn json_is_single_line() {
        let raw = sample_report().render_json();
        assert!(
            !raw.contains('\n'),
            "JSON must be one line for jq -- got: {raw}"
        );
    }
}
