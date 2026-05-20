# bench-minimal

A tsoracle overhead benchmark, measures the cost added by tsoracle itself — the gRPC service, the server, the leader-watch task, the failover fence, and the window allocator — with disk fsync and real consensus replication stubbed out via the in-memory `ConsensusDriver` exported by `tsoracle-server` under the `test-fakes` feature.

This crate is a characterization tool, not a CI gate. Run it when you want a number. It is `publish = false` and excluded from `make coverage`.

## What it measures (and what it doesn't)

Stays in the path: bench task → `tsoracle-client` (with coalescing) → loopback TCP → tonic → `tsoracle-server` (leader-watch, failover fence) → `tsoracle-core` window allocator → `InMemoryDriver`.

Stripped: no fsync, no real consensus, no real network. The result tells you "how fast is tsoracle itself, separated from disk and consensus?" — *not* "how fast will my deployment be." Loopback ≠ network; single process ≠ HA; `InMemoryDriver` ≠ a real driver.

## How to run

```bash
make bench   # the headline config
cargo run --release -p bench-minimal --bin bench -- --help   # the full surface
```

A representative custom invocation:

```bash
cargo run --release -p bench-minimal --bin bench -- \
  --clients 64 --ops 1m --batch-size 4 --warmup 100k
```

Number arguments accept underscores and a single trailing `k`/`m`/`g`: `1m`, `1_000_000`, and `1000k` all equal one million.

## Reading the numbers

- `client_calls/s` counts invocations at the `tsoracle-client` boundary. The client's request-coalescing layer may collapse concurrent calls into fewer server RPCs, so `client_calls/s` is the user-observed rate, not the server-side RPC rate. Latency percentiles are per *client call*, not per server RPC.
- `timestamps/s = client_calls/s × batch_size`. If you ran with `--batch-size 4`, multiply mentally.
- `out_of_range_samples > 0` means at least one call took more than 60 s; those latencies were clamped to the histogram's max, so percentiles read as a lower bound on the tail. The counter going non-zero is a signal worth investigating.
- The `recorded:` line shows actuals (post-warmup, integer-floored). The `config:` line shows nominal targets. They differ by design; the JSON output names them `recorded.client_calls` vs `config.ops_nominal` so machine consumers can't conflate them.
- `elapsed` is post-warmup wall-clock, from the moment all tasks cross the barrier to the moment the last task finishes. Throughput is computed against this `elapsed`, not against the wall-clock from program start.

## Latest results

```
host: Apple M4 Max (arm64)        date: 2026-05-19     git rev: b63fccf (feat/benchmarks-minimal)
config                                             client_calls/s    timestamps/s   p50       p99
--clients 1    --batch-size 1                                 419             419   2395 µs   2583 µs
--clients 16   --batch-size 1                               6 714           6 714   2405 µs   2585 µs
--clients 256  --batch-size 1                             112 493         112 493   2553 µs   2765 µs
--clients 1024 --batch-size 1                             436 968         436 968   2081 µs   3355 µs
--clients 64   --batch-size 64                             26 524       1 697 555   2449 µs   2633 µs
```

**Reading the numbers.** Per-call latency stays remarkably flat (~2.1–2.6 ms p50) across every concurrency level, which confirms the bottleneck is the per-RPC gRPC round-trip cost on loopback — *not* tsoracle's window allocator or server CPU. Throughput therefore scales nearly linearly with `--clients` until the round-trip ceiling is reached:

- 1 → 16 clients: throughput grows 16× (419 → 6 714), latency unchanged — pure concurrency scaling.
- 16 → 256 clients: throughput grows ~17× (matching the 16× ratio), still latency-bound.
- 256 → 1024 clients: another ~4× (matching the 4× ratio). p99 starts to tail out at 1024 clients (3.4 ms vs ~2.6 ms below) — first sign of queueing on the server side, but still well below 5 ms.
- Batching at `--batch-size 64`: ~26.5k batched RPCs/s → **~1.7M timestamps/s** with single-digit-ms latency. The latency per *RPC* matches the single-call latency (≈2.4 ms p50), so batching is essentially free amortization — you pay one round-trip and get 64 timestamps.

These are loopback numbers with the `InMemoryDriver` — no fsync, no real consensus. Production deployments will be slower in proportion to the cost of whatever `ConsensusDriver` they wire in.

## Profiling

Two optional Cargo features:

- `tokio-console`: enable the `console-subscriber` integration for inspecting tokio task state. Build with `cargo run --release -p bench-minimal --features tokio-console -- ...` and connect `tokio-console` to the process.
- `flamegraph`: enable `tracing-flame`. Same invocation pattern with `--features flamegraph`. Produces a `tracing.folded` file that `inferno-flamegraph` converts to SVG.

Neither feature is on by default — the release profile is debug-info-on (`debug = 2`, `split-debuginfo = "packed"`) so symbols survive into your profiler without an extra rebuild.