# Stress testing

The stress harness (`benchmarks/stress`) drives load against a tsoracle topology while a programmable nemesis injects faults, and asserts four invariants in real time: global monotonicity, batch internal ordering, failover-fence freshness, and liveness. It is a peer of `benchmarks/minimal` — bench-minimal characterizes steady-state throughput and latency; stress checks that the system's safety invariants hold under chaos.

## Why a separate tool

tsoracle's existing tests cover narrow slices: `tsoracle-core/tests/monotonicity.rs` property-tests the allocator in isolation; the per-crate `failpoints.rs` suites inject one fault at a time against a static system; `bench-minimal` characterizes overhead with a fake driver. None of them exercise the regime where the high-value TSO bugs live — sustained load combined with concurrent chaos. Stress fills that gap.

The single most important invariant the stress harness checks is *global monotonicity*: every timestamp received by any client is strictly greater than every previously-received timestamp, across all clients, across all chaos windows. If this ever fails, tsoracle has shipped a duplicate or out-of-order timestamp — the one thing it promises not to do.

## Topologies

Three modes, behind `--topology={mem,raft,process}`:

- **mem** — single in-process tsoracle server backed by `InMemoryDriver`. Chaos via the driver's `become_leader`/`become_follower` affordances plus in-process failpoints. Fastest, cheapest, catches allocator + fence + leader-watch bugs. Use this for the bulk of development.
- **raft** — three-node openraft cluster on `MemNetwork`, all in-process. Chaos via raft network partition (drop the leader's messages, let the cluster elect a new one) plus in-process failpoints. Catches openraft-driver-side bugs that mem cannot see.
- **process** — spawned `tsoracle` binaries. Chaos via POSIX signals (SIGKILL/SIGSTOP/SIGCONT) plus `FAILPOINTS` env propagation. Unix-only. Catches process-level fault recovery, exit handling, and reconnection paths.

Each topology speaks the same `ChaosController` vocabulary (`kill_leader`, `pause_leader`, `arm_failpoint`, `disarm_failpoint`), so the same scenario runs against any of them.

## Scenarios

Five named scenarios (use `stress list-scenarios` for the live catalog):

- `steady` — pure load, no chaos. Sanity check that clean runs report clean.
- `burst` — load with a 5-second loadgen pause partway through; tests resumption.
- `killer-loop` — `KillLeader` every 2s. Continuous failover-fence stress.
- `fence-stress` — alternating `PauseLeader 500ms` and `KillLeader`, 1s apart. Fence interlock under heavy re-election.
- `failpoint-cycle` — arms each known failpoint for 5s, with 10s recovery between. Driver-level fault recovery. Requires the `stress-failpoints` feature.

Plus a seeded random mode: `--seed N` (any nonzero seed switches the scenario kind to `Random`, regardless of `--scenario`). The seed pins the entire generated schedule.

## Running

```bash
cargo build --release --bin tsoracle  # required for --topology process
cargo build --release -p stress       # the harness binary itself

cargo run --release -p stress -- list-scenarios

# Mem topology, killer-loop, 30s.
cargo run --release -p stress -- run --topology mem --scenario killer-loop --duration 30s --clients 32 --batch-size 4

# Raft topology, 5m soak with the failpoint-cycle scenario (requires stress-failpoints feature).
cargo run --release -p stress --features stress-failpoints -- \
  run --topology raft --scenario failpoint-cycle --duration 5m --nodes 3 --clients 16

# Process topology, with schedule dump for replay.
cargo run --release -p stress -- run --topology process --scenario killer-loop --duration 30s --nodes 3 --schedule-out /tmp/sched.json

# Replay the saved schedule against a fresh server.
cargo run --release -p stress -- replay /tmp/sched.json

# Positive control: should exit 1 with a recorded violation.
cargo run --release -p stress -- inject-violation --topology mem
```

The CI-smoke preset bundles short-duration, low-load arguments for use in workflows. It forces `--scenario killer-loop`, `--duration 20s`, `--clients 16`, `--batch-size 4`, and a 1000-call warmup, regardless of whatever else you pass:

```bash
cargo run --release -p stress -- run --topology mem --ci-smoke
```

## Reading the report

The text report ends with:

- `outcome=Ok | InvariantViolation | ProgrammerError | HarnessError | Interrupted` — the headline. Maps to the exit code; see below.
- `violations: N` — how many invariant breaks the supervisor recorded. `0` for a clean run.
- `chaos events: N` — how many nemesis ops were applied during the run.
- `latency per client call` — percentiles aggregated across all client tasks. (Currently reads zero; per-client histogram recording is a tracked follow-up. See `benchmarks/stress/README.md` § "Known gaps".)

`--json` produces a one-line JSON report; use this for CI parsing. `outcome` is the only field a workflow needs to key off; the rest is supporting evidence. (`--json-stream` is plumbed as a CLI flag for a future dashboard consumer but not yet honored by the report path.)

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Clean run; no invariant violations |
| 1 | At least one invariant violation (real tsoracle bug) |
| 2 | Configuration / programmer error |
| 3 | Harness or environment error |
| 130 | SIGINT |

CI gates should distinguish 1 from 3. The first is a real bug worth investigating; the second is a flake — re-run before opening an issue.

## Reproducing a failure

Every `stress run` writes its full nemesis schedule to `--schedule-out` (default `./stress-schedule.json`). To re-run a failure bit-for-bit:

```bash
stress replay <schedule.json>
```

The schedule pins both the named-or-random source and the materialized op sequence. Hand-written scenarios are deterministic on their own, but the schedule dump also captures topology-side non-determinism (port allocation, raft node IDs in raft mode) — which is what makes "replay" useful even for hand-written scenarios.

## CI surface

- **Per-PR**: `.github/workflows/ci.yml` contains a `stress-smoke` job that runs all three topologies with `--ci-smoke` plus the `inject-violation` positive control. Builds the tsoracle and stress binaries once, then exercises all four steps.
- **Nightly**: `.github/workflows/stress-nightly.yml` runs the full scenario menu against each topology at `--duration 5m` (matrix of 3 topologies × 7 scenarios = 21 jobs). Results are uploaded as artifacts; failures auto-file a deduplicated GitHub issue via `.github/actions/stress-auto-issue`.
- **Multi-hour soak**: not CI-gated. Run locally against a candidate release build:

  ```bash
  cargo run --release -p stress --features stress-failpoints -- \
    run --topology raft --scenario killer-loop --duration 4h --nodes 3
  ```

Both CI workflows can be re-triggered manually (`workflow_dispatch`); the nightly takes optional `topology`, `scenario`, and `duration` filters so you can re-run a single matrix cell without waiting on the cron.

## When to add a failpoint site

Stress scenarios consume the failpoint catalog documented in [`docs/failpoint-testing.md`](failpoint-testing.md). If a new fault becomes interesting to inject (e.g., a partial fsync, a hung RPC handler), add the site there first, following the contributor guidance in that doc. The `failpoint-cycle` scenario will pick it up automatically.

## Limitations

- Network partitions (`pfctl`/`iptables`) are not yet implemented. Out of V1 scope; tracked as a follow-up.
- Multi-host deployments are not yet supported — all topologies run on loopback.
- The process topology's `current_leader` is a best-effort round-robin; there is no protocol-level leader discovery against spawned binaries today.
- The process topology's `FAILPOINTS` env affects future-spawned children only, not live ones. Arming a failpoint and then waiting for the next chaos-induced respawn is the workflow.

## See also

- [`benchmarks/stress/README.md`](../benchmarks/stress/README.md) — quickstart for contributors.
- [`benchmarks/minimal/README.md`](../benchmarks/minimal/README.md) — the steady-state characterization sibling.
- [`docs/failpoint-testing.md`](failpoint-testing.md) — failpoint catalog and how to add new sites.
