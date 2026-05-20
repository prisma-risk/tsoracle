//! tsoracle bench-minimal — see ../README.md and the design doc at
//! docs/superpowers/specs/2026-05-19-benchmarks-minimal-design.md.

use std::net::SocketAddr;
use std::thread::available_parallelism;
use std::time::Duration;

use bench_minimal::{RunConfig, harness, parse_count};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "bench",
    about = "tsoracle overhead benchmark (end-to-end through tonic, in-memory ConsensusDriver)"
)]
struct Cli {
    #[arg(long, default_value_t = 1)]
    clients: usize,

    #[arg(long, default_value = "1_000_000", value_parser = parse_count)]
    ops: u64,

    #[arg(long, default_value_t = 1)]
    batch_size: u32,

    #[arg(long, default_value_t = 1)]
    client_threads: usize,

    #[arg(long, default_value_t = available_parallelism().map(|n| n.get()).unwrap_or(1))]
    server_threads: usize,

    #[arg(long, default_value = "100_000", value_parser = parse_count)]
    warmup: u64,

    #[arg(long, default_value = "127.0.0.1:0")]
    bind: SocketAddr,

    #[arg(long, default_value = "1s")]
    print_interval: humantime::Duration,

    #[arg(long, default_value_t = false)]
    json: bool,

    #[arg(long, default_value_t = 0)]
    seed: u64,
}

impl Cli {
    fn into_config(self) -> RunConfig {
        RunConfig {
            clients: self.clients,
            ops: self.ops,
            batch_size: self.batch_size,
            client_threads: self.client_threads,
            server_threads: self.server_threads,
            warmup: self.warmup,
            bind: self.bind,
            print_interval: Duration::from(self.print_interval),
            json: self.json,
            seed: self.seed,
        }
    }
}

fn main() {
    // When the `flamegraph` feature is active, _flame_guard owns the open
    // tracing.folded file; it must live until main returns so the writer is
    // flushed before the process exits.
    #[cfg(feature = "flamegraph")]
    let _flame_guard = init_tracing_with_flamegraph();
    #[cfg(not(feature = "flamegraph"))]
    init_tracing();

    let cfg = Cli::parse().into_config();
    if let Err(msg) = cfg.validate() {
        eprintln!("invalid config: {msg}");
        std::process::exit(2);
    }
    let json_mode = cfg.json;

    match harness::run(cfg) {
        Ok(report) => {
            if json_mode {
                eprint!("{}", report.render_text());
                println!("{}", report.render_json());
            } else {
                print!("{}", report.render_text());
            }
        }
        Err(e) => {
            eprintln!("bench failed: {e:#}");
            std::process::exit(1);
        }
    }
}

#[cfg(not(feature = "flamegraph"))]
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

/// Install the fmt subscriber AND a `tracing_flame::FlameLayer` writing to
/// `./tracing.folded`. The returned `FlushGuard` must be kept alive until
/// the process exits — drop it before main returns and you lose pending
/// span data.
///
/// Convert the folded output to SVG with `inferno-flamegraph`:
///
/// ```sh
/// cargo install inferno
/// inferno-flamegraph < tracing.folded > flamegraph.svg
/// ```
///
/// The fmt layer keeps the project's standard EnvFilter ("warn" default).
/// The flame layer is NOT filtered — it sees every instrumented span so the
/// `#[tracing::instrument]` annotations on the harness (gated behind this
/// same feature) all get captured.
#[cfg(feature = "flamegraph")]
fn init_tracing_with_flamegraph() -> tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>> {
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(filter);
    let (flame_layer, guard) = tracing_flame::FlameLayer::with_file("./tracing.folded")
        .expect("failed to open ./tracing.folded for flamegraph output");
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(flame_layer)
        .init();
    guard
}
