# Testing and Examples

Three runnable example crates under `examples/`, each illustrating a different layer of the stack, plus a survey of the test patterns the workspace uses. The examples are self-contained crates; build any of them with `cargo run -p example-<name>`.

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

## openraft-cluster example

`examples/openraft-cluster/` is the worked HA setup: three independent processes, each running a tsoracle server backed by a `ConsensusDriver` implemented over openraft. The `ConsensusDriver` impl lives in `src/driver.rs`; the rest is plumbing (file-backed log/state in `src/store/`, tonic peer transport in `src/network.rs`, leader-state watch in `src/leader_watch.rs`).

See the [example's README](https://github.com/prisma-risk/tsoracle/tree/main/examples/openraft-cluster) for prerequisites, manual node startup, and design notes. Quickstart:

```bash
examples/openraft-cluster/scripts/run.sh
```

starts three node processes in the background, with logs under `examples/openraft-cluster/.data/n*.log`. Node 1 carries `--bootstrap`; nodes 2 and 3 join. Issue a timestamp with `grpcurl` against any node — followers respond with a `LeaderHint` trailer pointing at the current leader's advertised tsoracle address.

What the example demonstrates beyond [Worked example: openraft](consensus-integration.md#worked-example-openraft):

- The state machine applies `TsoExtend` requests as `max(stored, req.at_least)` unconditionally — reordered or stale-epoch entries are absorbed monotonically rather than rejected. This is the on-driver realization of [Monotonic persistence](the-allocator.md#monotonic-persistence).
- Linearizable reads use `Raft::ensure_linearizable(ReadPolicy::ReadIndex)`, which commits a no-op heartbeat through the log before the read — the openraft 0.10 read-barrier API.
- openraft refuses non-leader `client_write` at the propose layer, so stale leaders fail with `ConsensusError::NotLeader` rather than the trait's `Fenced` variant. The `Fenced` variant exists for weaker drivers that can detect a stale epoch only post-write.
- `<raft-dir>/state.json` (tmp-file write + rename per apply) plus `<raft-dir>/log/` (one file per entry) form a readable tutorial storage layer. It is not a power-loss-hardened durability layer; production deployments should swap it for rocksdb, sled, fjall, or an equivalent store.

To observe failover: find the current leader in the logs (`grep "Leader" .data/n*.log`), kill that process, watch the survivors elect, then re-issue `GetTs`. Typical re-leader latency is 2–5 seconds (election + fence).

## Testing strategy

tsoracle's tests are organized along the same layering as the code:

- **`tsoracle-core/tests/monotonicity.rs`** — property tests via `proptest`. Generates randomized allocator request sequences (clock advances, leader transitions, request counts) and asserts that every issued timestamp is strictly greater than the previous one. Property-testing the core is cheap because it has no `await` and no I/O — many generated inputs run in seconds, and the `proptest-regressions` file pins seeds for failing cases.
- **`tsoracle-driver-file/tests/crash_recovery.rs`** — open a file driver, persist a high-water, drop without graceful shutdown, reopen, assert the loaded value is at least what was persisted before the drop. Verifies the fsync-before-Ok contract end-to-end against a real filesystem.
- **`tsoracle-server/tests/{e2e, leader_watch, embedded_router}.rs`** — integration tests against a real tonic server. They use [`InMemoryDriver`](#failover-demo-example) (exposed via the `test-fakes` Cargo feature) to script leader transitions deterministically without real consensus or disk. `embedded_router.rs` specifically tests mounting tsoracle alongside other services via `Server::into_router`.
- **`tsoracle-client/tests/e2e.rs`** — client integration against a real server with the in-memory driver.
- **`tsoracle-client/tests/freshness.rs`** — explicit test of the [freshness invariant](getting-started.md#the-freshness-invariant): concurrent waiters never receive timestamps allocated before they entered the driver.
- **`tsoracle-bin/tests/smoke.rs`** — black-box test of the `tsoracle` CLI: spawn the binary, send a `GetTs`, check the response, terminate.

The `test-fakes` feature on `tsoracle-server` is the linchpin: it exposes `InMemoryDriver` (which would otherwise be `pub(crate)`), letting integration tests in dependent crates exercise leader transitions on a real tonic-mounted server without needing a real consensus library. The [failover-demo example](#failover-demo-example) uses the same affordance as a pedagogy tool.

CI runs the full battery — `cargo test --workspace --all-features` — on every PR. See [CONTRIBUTING.md](../CONTRIBUTING.md#running-the-checks-locally) for the local-run incantation.
