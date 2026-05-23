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

//! tsoracle stress harness — see ../README.md.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread::available_parallelism;
use std::time::Duration;

use clap::{Parser, Subcommand};
use humantime::Duration as HumantimeDuration;
use stress::config::{ScenarioKind, StressConfig, TopologyKind};
use stress::nemesis::scenario;

#[derive(Parser, Debug)]
#[command(name = "stress", about = "tsoracle stress + chaos harness")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
// `RunArgs` is large (many flags); the variants are intentionally unbalanced.
// The enum is parsed exactly once at startup and immediately matched, so the
// "boxing for size" advice from clippy doesn't apply here.
#[allow(clippy::large_enum_variant)]
enum Cmd {
    Run(RunArgs),
    Replay(ReplayArgs),
    ListScenarios,
    InjectViolation(InjectArgs),
}

#[derive(Parser, Debug, Clone)]
struct RunArgs {
    #[arg(long, value_enum)]
    topology: TopologyArg,
    #[arg(long, default_value = "steady")]
    scenario: String,
    #[arg(long)]
    duration: Option<HumantimeDuration>,
    #[arg(long)]
    ops: Option<u64>,
    #[arg(long, default_value_t = 16)]
    clients: usize,
    #[arg(long, default_value_t = 1)]
    batch_size: u32,
    #[arg(long, default_value_t = 1000)]
    warmup: u64,
    #[arg(long, default_value_t = 1)]
    client_threads: usize,
    #[arg(long, default_value_t = available_parallelism().map(|n| n.get()).unwrap_or(1))]
    server_threads: usize,
    #[arg(long, default_value = "5s")]
    liveness_deadline: HumantimeDuration,
    #[arg(long, default_value = "100ms")]
    grace_mem: HumantimeDuration,
    #[arg(long, default_value = "750ms")]
    grace_raft: HumantimeDuration,
    #[arg(long, default_value = "1s")]
    grace_paxos: HumantimeDuration,
    #[arg(long, default_value = "2s")]
    grace_process: HumantimeDuration,
    #[arg(long, default_value_t = 3)]
    nodes: usize,
    #[arg(long, default_value = "127.0.0.1:0")]
    bind: SocketAddr,
    #[arg(long, default_value_t = false)]
    json: bool,
    #[arg(long, default_value_t = false)]
    json_stream: bool,
    #[arg(long, default_value = "1s")]
    print_interval: HumantimeDuration,
    #[arg(long, default_value_t = 0)]
    seed: u64,
    #[arg(long)]
    schedule_out: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    ci_smoke: bool,
}

#[derive(Parser, Debug)]
struct ReplayArgs {
    schedule: PathBuf,
}

#[derive(Parser, Debug)]
struct InjectArgs {
    #[arg(long, value_enum)]
    topology: TopologyArg,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum TopologyArg {
    Mem,
    Raft,
    Paxos,
    Process,
}

impl From<TopologyArg> for TopologyKind {
    fn from(t: TopologyArg) -> Self {
        match t {
            TopologyArg::Mem => TopologyKind::Mem,
            TopologyArg::Raft => TopologyKind::Raft,
            TopologyArg::Paxos => TopologyKind::Paxos,
            TopologyArg::Process => TopologyKind::Process,
        }
    }
}

fn main() -> ExitCode {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run(a) => run_cmd(a),
        Cmd::Replay(a) => replay_cmd(a),
        Cmd::ListScenarios => list_scenarios_cmd(),
        Cmd::InjectViolation(a) => inject_violation_cmd(a),
    }
}

fn build_config(a: &RunArgs) -> StressConfig {
    let mut cfg = StressConfig {
        topology: a.topology.into(),
        scenario: if a.seed != 0 {
            ScenarioKind::Random { seed: a.seed }
        } else {
            ScenarioKind::Named(a.scenario.clone())
        },
        duration: a.duration.map(Duration::from),
        ops: a.ops,
        clients: a.clients,
        batch_size: a.batch_size,
        warmup: a.warmup,
        client_threads: a.client_threads,
        server_threads: a.server_threads,
        liveness_deadline: Duration::from(a.liveness_deadline),
        grace_mem: Duration::from(a.grace_mem),
        grace_raft: Duration::from(a.grace_raft),
        grace_paxos: Duration::from(a.grace_paxos),
        grace_process: Duration::from(a.grace_process),
        nodes: a.nodes,
        bind: a.bind,
        json: a.json,
        json_stream: a.json_stream,
        print_interval: Duration::from(a.print_interval),
        seed: a.seed,
        schedule_out: a.schedule_out.clone(),
        ci_smoke: a.ci_smoke,
    };
    if cfg.ci_smoke {
        // Tuned so the smoke completes its warmup and measurement phase
        // even on the worst-case topology (process + 1 node + killer-loop),
        // where the single child is killed every 2s and healthy windows
        // are only hundreds of milliseconds. The previous warmup of 1000
        // RPCs across 16 clients was unachievable in those windows, so the
        // smoke reported `outcome=Ok` with zero timestamps — a useless gate.
        cfg.duration = Some(Duration::from_secs(20));
        cfg.clients = 8;
        cfg.batch_size = 4;
        cfg.warmup = 100;
        cfg.scenario = ScenarioKind::Named("killer-loop".into());
    }
    cfg
}

fn run_cmd(args: RunArgs) -> ExitCode {
    let cfg = build_config(&args);
    let json_mode = cfg.json;
    match stress::run(cfg) {
        Ok(report) => {
            if json_mode {
                println!("{}", report.render_json());
            } else {
                print!("{}", report.render_text());
            }
            let code = report.outcome.exit_code();
            ExitCode::from(code as u8)
        }
        Err(err) => {
            eprintln!("stress run failed: {err:#}");
            ExitCode::from(2)
        }
    }
}

fn replay_cmd(args: ReplayArgs) -> ExitCode {
    let schedule = match stress::load_schedule(&args.schedule) {
        Ok(s) => s,
        Err(err) => {
            eprintln!(
                "replay: failed to load schedule {:?}: {err:#}",
                args.schedule
            );
            return ExitCode::from(2);
        }
    };
    let scenario = match &schedule.source {
        stress::schedule::ScheduleSource::Named { scenario } => {
            ScenarioKind::Named(scenario.clone())
        }
        stress::schedule::ScheduleSource::Random { seed, .. } => {
            ScenarioKind::Random { seed: *seed }
        }
    };
    // Replay duration = recorded total + a small safety margin. The margin
    // gives the replay headroom to observe post-chaos samples even when it
    // runs slightly slower than the original (scheduler jitter, cold caches,
    // CI noise). 2s handles short runs (high relative jitter) without being
    // visible on long runs.
    let replay_margin = Duration::from_secs(2);
    let cfg = StressConfig {
        topology: TopologyKind::Mem,
        scenario,
        duration: Some(schedule.total + replay_margin),
        ops: None,
        clients: 16,
        batch_size: 1,
        warmup: 100,
        client_threads: 1,
        server_threads: available_parallelism().map(|n| n.get()).unwrap_or(1),
        liveness_deadline: Duration::from_secs(5),
        grace_mem: Duration::from_millis(100),
        grace_raft: Duration::from_millis(750),
        grace_paxos: Duration::from_millis(1000),
        grace_process: Duration::from_secs(2),
        nodes: 1,
        bind: "127.0.0.1:0"
            .parse::<SocketAddr>()
            .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 0))),
        json: false,
        json_stream: false,
        print_interval: Duration::from_secs(1),
        seed: 0,
        schedule_out: None,
        ci_smoke: false,
    };
    match stress::run(cfg) {
        Ok(report) => {
            print!("{}", report.render_text());
            let code = report.outcome.exit_code();
            ExitCode::from(code as u8)
        }
        Err(err) => {
            eprintln!("replay run failed: {err:#}");
            ExitCode::from(2)
        }
    }
}

fn list_scenarios_cmd() -> ExitCode {
    for info in scenario::catalog() {
        println!("{:18}  {}", info.name, info.summary);
    }
    ExitCode::from(0)
}

fn inject_violation_cmd(args: InjectArgs) -> ExitCode {
    let cfg = StressConfig {
        topology: args.topology.into(),
        scenario: ScenarioKind::Named("steady".into()),
        duration: Some(Duration::from_secs(3)),
        ops: None,
        clients: 4,
        batch_size: 1,
        warmup: 100,
        client_threads: 1,
        server_threads: available_parallelism().map(|n| n.get()).unwrap_or(1),
        liveness_deadline: Duration::from_secs(5),
        grace_mem: Duration::from_millis(100),
        grace_raft: Duration::from_millis(750),
        grace_paxos: Duration::from_millis(1000),
        grace_process: Duration::from_secs(2),
        nodes: 1,
        bind: SocketAddr::from(([127, 0, 0, 1], 0)),
        json: false,
        json_stream: false,
        print_interval: Duration::from_secs(1),
        seed: 0,
        schedule_out: None,
        ci_smoke: false,
    };
    match stress::run_inject_violation(cfg) {
        Ok(report) => {
            print!("{}", report.render_text());
            // Must be 1 if supervisor + exit-code mapping is wired correctly.
            let code = report.outcome.exit_code();
            ExitCode::from(code as u8)
        }
        Err(err) => {
            eprintln!("inject-violation failed: {err:#}");
            ExitCode::from(2)
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(filter);
    tracing_subscriber::registry().with(fmt_layer).init();
}
