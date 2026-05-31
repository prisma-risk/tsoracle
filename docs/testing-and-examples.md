# Testing and Examples

Nine runnable example crates live under `examples/`, each illustrating a different layer of the stack, plus a survey of the test patterns the workspace uses. The examples are self-contained crates; build any of them with `cargo run -p example-<name>`. This page gives the orientation; each example README carries the copy-paste quickstart for that crate.

## Embedded-server example

`examples/embedded-server/` is the minimum library-use case: ~30 lines of `main.rs` that opens a `FileDriver`, builds a `Server`, and serves with Ctrl-C shutdown.

```bash
cargo run -p example-embedded-server
```

It listens on `127.0.0.1:50551` and persists state under `./tsoracle-embedded-data/`. Talk to it with any tsoracle client or `grpcurl`. The example is essentially the README snippet wired up — useful as a starting point when embedding tsoracle in your own binary.

What the example demonstrates:

- `FileDriver::open_or_init(dir)` is idempotent — it creates the state directory on first run and rehydrates from the existing record on subsequent runs.
- `Server::builder().consensus_driver(driver).build()` is the minimum configuration; `clock`, `window_ahead`, and `failover_advance` get their defaults.
- `serve_with_shutdown(addr, future)` drains in-flight RPCs when the shutdown future completes, then exits cleanly.

## Failover-demo example

`examples/failover-demo/` is in-process pedagogy: a single binary that builds a `Server` against the in-memory `InMemoryDriver` (from the `test-fakes` feature of `tsoracle-server`), connects a gRPC client, and scripts a leader → follower → new-leader sequence. The point is to make the [failover fence](key-subsystems.md#the-failover-fence) visible and to *assert* monotonicity holds across it.

```bash
cargo run -p example-failover-demo
```

What you'll see in the output: phase 1 issues 5 timestamps at epoch 1; phase 2 transitions the driver to follower and shows that `GetTs` now returns `FAILED_PRECONDITION`; phase 3 transitions back to leader at epoch 2 and issues 5 more timestamps. The example uses `assert!(packed_ts > prev)` to verify that every timestamp is strictly greater than the previous one — across the fence, the new leader's first timestamp is `> last_pre_fence_timestamp`.

No openraft, no real network, no real disk. The `InMemoryDriver` exposes `become_leader(Epoch)` and `become_follower(Option<String>)` as test affordances, which the example calls directly to script the sequence.

## openraft-standalone example

`examples/openraft-standalone/` is the worked HA setup: three independent processes, each running a tsoracle server backed by `tsoracle-driver-openraft` through the `tsoracle-standalone` helper crate. The example's `src/main.rs` is deliberately thin: parse CLI flags, call `tsoracle_standalone::build`, mount the resulting driver in `tsoracle_server::Server`, and wire SIGINT/SIGTERM shutdown.

See the [example's README](https://github.com/prisma-risk/tsoracle/tree/main/examples/openraft-standalone) for prerequisites, manual node startup, and design notes. Quickstart:

```bash
examples/openraft-standalone/scripts/run.sh
```

starts three node processes in the background, with logs under `examples/openraft-standalone/.data/n*.log`. Node 1 carries `--bootstrap` and `--members`; nodes 2 and 3 join from their replicated membership state. Issue a timestamp with `grpcurl -v` against any node — followers return `FAILED_PRECONDITION` with a `LeaderHint` trailer pointing at the current leader's advertised tsoracle address. The trailer is populated from the `service_endpoint` entries seeded by `--members`.

What this example demonstrates:

- The dedicated-cluster shape: persistent RocksDB-backed raft state, a tonic peer transport, and a `ConsensusDriver` built by `tsoracle-standalone`.
- Membership-seeded endpoint resolution: each member carries raft, tsoracle service, and admin endpoints; the driver resolves `LeaderHint` from the leader's replicated membership node.
- Graceful process shutdown: the standalone examples use the same `tsoracle_server::shutdown_signal()` helper as the stock binary, so SIGINT and SIGTERM both drain the server before process exit.

To observe failover: find the current leader in the logs (`grep "Leader" .data/n*.log`), kill that process, watch the survivors elect, then re-issue `GetTs`. Typical re-leader latency is 2–5 seconds (election + fence).

## openraft-piggyback example

`examples/openraft-piggyback/` is the single-binary, in-process demonstration of piggybacking TSO onto a host service's existing raft. When your service already runs openraft for something else, you don't need a second cluster — you wrap the driver crate's `HighWaterCommand` in your `AppData` enum and add a high-water field to your state machine.

```bash
cargo run -p example-openraft-piggyback
```

The demo boots a 3-node in-process cluster via `tsoracle_openraft_toolkit::test_fakes::MemNetwork`, runs a tsoracle server on each (each bound to an OS-assigned loopback port via `TcpListener::bind("127.0.0.1:0")`), and walks through: host KV writes that land in the host SM without touching the TSO field, GetTs bursts that are allocator-served (high-water does *not* advance per call), and a failover that asserts the freshness invariant survives (`new_high_water > old_high_water` AND `next_ts > last_pre_failover_ts`). Runs in roughly 3 seconds; the same `run_demo()` function backs `tests/smoke.rs`.

The envelope pattern at the heart of the example:

```rust
pub enum HostCommand {
    Kv(KvOp),
    Tso(tsoracle_driver_openraft::HighWaterCommand),
}
```

Your apply path enforces TSO monotonicity (`max(prev, target)`) in a field next to your KV map; your snapshot carries both halves; your `OpenraftHighWaterHost` impl wraps `HighWaterCommand::Advance(AdvancePayload { at_least })` in `HostCommand::Tso` when submitting. See [`docs/consensus-integration.md`](consensus-integration.md#openrafthighwaterhost-trait) for the trait contract and the [example's README](https://github.com/prisma-risk/tsoracle/tree/main/examples/openraft-piggyback) for the walkthrough.

## Paxos examples

`examples/paxos-standalone/` is the OmniPaxos counterpart to `openraft-standalone`: three processes, tonic peer transport through `tsoracle-standalone`, RocksDB-backed storage, and follower redirects via `LeaderHint`.

```bash
examples/paxos-standalone/scripts/run.sh
```

`examples/paxos-piggyback/` is the OmniPaxos counterpart to `openraft-piggyback`: a single-binary, in-process demo where host KV commands and TSO high-water commands share one Paxos log. The same `run_demo()` function backs the README quickstart and the example's smoke test.

```bash
cargo run -p example-paxos-piggyback
```

`examples/paxos-embedded/` shows the closest OmniPaxos shape to `embedded-server`. OmniPaxos needs a quorum, so the example runs all three Paxos nodes inside one process over the toolkit's `MemNetwork` and exposes three tsoracle gRPC endpoints.

```bash
cargo run -p example-paxos-embedded
```

## Metrics and TLS examples

`examples/metrics-prometheus/` installs `metrics-exporter-prometheus` before building the server, drives background load, and exposes `/metrics` on a separate scrape port.

```bash
cargo run -p example-metrics-prometheus
```

`examples/tls-mtls/` exercises plain TLS, mTLS, a custom connector, and an intentionally misconfigured mTLS client against fresh in-memory certificates.

```bash
cargo run -p example-tls-mtls
```

## Testing strategy

tsoracle's tests are organized along the same layering as the code:

- **`tsoracle-core/tests/monotonicity.rs`** — property tests via `proptest`. Generates randomized allocator request sequences (clock advances, leader transitions, request counts) and asserts that every issued timestamp is strictly greater than the previous one. Property-testing the core is cheap because it has no `await` and no I/O — many generated inputs run in seconds, and the `proptest-regressions` file pins seeds for failing cases.
- **`tsoracle-driver-file/tests/crash_recovery.rs`** — open a file driver, persist a high-water, drop without graceful shutdown, reopen, assert the loaded value is at least what was persisted before the drop. Verifies the fsync-before-Ok contract end-to-end against a real filesystem.
- **`tsoracle-server/tests/{e2e, leader_watch, embedded_router}.rs`** — integration tests against a real tonic server. They use [`InMemoryDriver`](#failover-demo-example) (exposed via the `test-fakes` Cargo feature) to script leader transitions deterministically without real consensus or disk. `embedded_router.rs` specifically tests mounting tsoracle alongside other services via `Server::into_router`.
- **`tsoracle-client/tests/e2e.rs`** — client integration against a real server with the in-memory driver.
- **`tsoracle-client/tests/freshness.rs`** — explicit test of the [freshness invariant](getting-started.md#the-freshness-invariant): concurrent waiters never receive timestamps allocated before they entered the driver.
- **`tsoracle-bin/tests/smoke.rs`** — black-box test of the `tsoracle` CLI: spawn the binary, send a `GetTs`, check the response, terminate.
- **`crates/{tsoracle-driver-file, tsoracle-server}/tests/failpoints.rs`** — fault-injection tests gated by each crate's `failpoints` Cargo feature. Inject panics, sleeps, and typed-error returns at named sites in `write_record`, the leader-watch fence, and the service path; assert the observable invariants on reopen / on the `JoinHandle` / on client-visible RPC behavior. See [Failpoint Testing](failpoint-testing.md) for the feature-gating model, the eight current sites, the wrapper macro shape, and contributor guidance for adding new sites.
- **`crates/tsoracle-driver-paxos/tests/standalone_shutdown.rs`** — async yield-point test gated by the `yieldpoints` Cargo feature. Pins the apply task at a named site in production code so the test can race `StandaloneHost::stop` against the apply loop's `select!` deterministically. The mechanism is a `tokio::sync::Notify` per yield point — async on purpose, because a sync `fail`-crate `pause` action would block a tokio worker thread and wedge the timer driver. See [Yield-point Testing](yieldpoint-testing.md) for the macro shape, the naming convention, and when to reach for yield points over failpoints.
- **`benchmarks/stress` (peer crate, not in `crates/`)** — the stress + chaos harness. Drives load against a tsoracle topology (mem, raft, or process) while a programmable nemesis injects faults; asserts global monotonicity, batch ordering, fence freshness, and liveness in real time. Run it locally with `cargo run -p stress -- run --topology mem --scenario killer-loop --duration 30s`. See [Stress testing](stress-testing.md) for the full reference.

The `test-fakes` feature on `tsoracle-server` is the linchpin: it exposes `InMemoryDriver` (which would otherwise be `pub(crate)`), letting integration tests in dependent crates exercise leader transitions on a real tonic-mounted server without needing a real consensus library. The [failover-demo example](#failover-demo-example) uses the same affordance as a pedagogy tool.

CI runs the full battery — `cargo test --workspace --all-features` — on every PR. See [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-checks-locally) for the local-run incantation.
