# stress

The tsoracle stress + chaos harness. Drives load against a tsoracle topology while a programmable nemesis injects faults, and asserts four invariants in real time: global monotonicity, batch internal ordering, failover-fence freshness, and liveness.

This crate is a peer of `benchmarks/minimal`, not a replacement. `bench-minimal` characterizes steady-state throughput and latency against an in-memory driver. `stress` is the invariant checker under chaos. Different consumers, different outputs.

`publish = false`. Library code participates in `make coverage`; the CLI shim is filtered out via the Makefile's filename regex. Run it when you want to know whether tsoracle maintains its invariants under sustained chaos.

## Features

- `--topology mem`: single in-process `tsoracle-server` against `InMemoryDriver`.
- `--topology raft`: real in-process openraft cluster sharing a `MemNetwork`.
- `--topology process` (unix-only): spawned `tsoracle` binaries with POSIX-signal chaos and `FAILPOINTS` env propagation.
- All four invariants (monotonicity, batch ordering, fence freshness, liveness).
- Five named scenarios: `steady`, `burst`, `killer-loop`, `fence-stress`, `failpoint-cycle`.
- Seeded `random` scenario.
- `replay` subcommand.
- `inject-violation` self-test as a positive CI control.
- Smoke tests in `tests/smoke.rs` covering all three topologies (≤ 60 s total).
- Per-PR CI smoke (`stress-smoke` job in `.github/workflows/ci.yml`) and nightly long-run workflow (`.github/workflows/stress-nightly.yml`) covering the full topology × scenario matrix at `--duration 5m`, with failures auto-filed as deduplicated GitHub issues.

## Run

```bash
cargo run -p stress --release -- list-scenarios
cargo run -p stress --release -- run --topology mem --scenario killer-loop --duration 30s --clients 32 --batch-size 4
cargo run -p stress --release -- run --topology mem --scenario steady --duration 10s --schedule-out /tmp/sched.json
cargo run -p stress --release -- replay /tmp/sched.json
cargo run -p stress --release -- inject-violation --topology mem    # must exit 1
```

`--topology raft` runs a real openraft cluster (in-process, sharing a `MemNetwork`) so chaos exercises actual leader re-election rather than synthetic driver transitions.

```bash
# 3-node openraft cluster, in-process, sharing a MemNetwork.
cargo run -p stress --release -- run --topology raft --scenario killer-loop --duration 30s --nodes 3 --clients 32 --batch-size 4
```

`--topology process` (unix-only) launches one or more `tsoracle` child processes and exercises chaos via real POSIX signals (SIGKILL, SIGSTOP/SIGCONT) plus `FAILPOINTS=…` env propagation at spawn time. Unexpected child exits (those the nemesis did not initiate) are reaped by per-child supervisor tasks and surfaced as `LivenessIncident::UnexpectedServerExit` events. Build the binary first:

```bash
cargo build --release --bin tsoracle
cargo run -p stress --release -- run --topology process --scenario killer-loop --duration 30s --clients 16 --nodes 1
```

Process-mode caveats:

- Unix-only. SIGKILL/SIGSTOP/SIGCONT have no Windows analogue; `--topology process` is `cfg(unix)`-gated in `lib::run` and bails on Windows.
- The harness binds children to OS-assigned ports on the initial spawn (`--listen 127.0.0.1:0`); it then pins each node's port so respawns after SIGKILL rebind to the same address. Without this, single-node configurations would lose connectivity after every kill.
- "Current leader" is best-effort: the harness has no protocol-level handle to discover the actual Raft leader, so chaos rotates through children round-robin. Monotonicity and fence-freshness invariants stay sound regardless — they are global, not per-node.
- `arm_failpoint` / `disarm_failpoint` update an internal map but **do not affect already-running children**. Only the next respawn (e.g. after a `kill_leader`) inherits the new `FAILPOINTS=…` env. The `failpoint-cycle` scenario therefore needs at least one kill between arm and observation in process mode.

Build with the `stress-failpoints` feature to enable in-process failpoint chaos (the `failpoint-cycle` scenario only does useful work with this on):

```bash
cargo run -p stress --release --features stress-failpoints -- run --topology mem --scenario failpoint-cycle --duration 60s
```

## Reading the output

The text report ends with:

- `outcome=Ok | InvariantViolation | ...` — the headline result. Maps to the exit code below.
- `violations: N` — how many invariant breaks the supervisor recorded.
- `chaos events: N` — how many nemesis ops were applied.
- `latency per client call` — percentiles aggregated across all client tasks. (Latency stats currently read as zeros; per-client histogram recording is a known follow-up — see "Known gaps" below.)

`--json` produces a one-line JSON report on stdout. Use this for CI parsing. `outcome` is the only field a workflow needs to key off; the rest is supporting evidence.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Clean run; no invariant violations |
| 1 | Run completed; at least one invariant violation (real tsoracle bug) |
| 2 | Configuration / programmer error |
| 3 | Harness or environment error |
| 130 | SIGINT |

CI gates should distinguish 1 from 3 — the first is a real bug, the second is a flake.

## Known gaps

These are landed-but-incomplete. Neither affects any of the four invariants; they are presentation or budget polish that lands via small follow-ups:

- `--json-stream` is plumbed as a CLI flag but not honored by the report path. Honor it when a real dashboard consumer exists.
- `--ops`-bounded runs are accepted by CLI / validate() but `lib::run` only honors `--duration`. Adding an `--ops` budget needs a shared atomic + stop trigger; defer until needed.
- The topology controller emits `ChaosEvent`s to the supervisor, but they are not yet collected into the final `Report.chaos_events` field (currently hardcoded empty in `lib::run`). Plumbing those events end-to-end is a small follow-up.

## See also

- `../../docs/stress-testing.md` — operator + contributor reference (canonical).
- `../minimal/README.md` — the steady-state characterization sibling.
