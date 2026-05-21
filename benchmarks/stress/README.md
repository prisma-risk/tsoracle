# stress

The tsoracle stress + chaos harness. Drives load against a tsoracle topology while a programmable nemesis injects faults, and asserts four invariants in real time: global monotonicity, batch internal ordering, failover-fence freshness, and liveness.

This crate is a peer of `benchmarks/minimal`, not a replacement. `bench-minimal` characterizes steady-state throughput and latency against an in-memory driver. `stress` is the invariant checker under chaos. Different consumers, different outputs.

`publish = false`. Library code participates in `make coverage`; the CLI shim and the unimplemented topology stubs are filtered out via the Makefile's filename regex. Run it when you want to know whether tsoracle maintains its invariants under sustained chaos.

## Features

- `--topology mem`: single in-process `tsoracle-server` against `InMemoryDriver`.
- All four invariants (monotonicity, batch ordering, fence freshness, liveness).
- Five named scenarios: `steady`, `burst`, `killer-loop`, `fence-stress`, `failpoint-cycle`.
- Seeded `random` scenario.
- `replay` subcommand.
- `inject-violation` self-test as a positive CI control.
- Mem-topology smoke test in `tests/smoke.rs` (≤ 30 s).

`--topology raft` runs a real in-process openraft cluster sharing a `MemNetwork`. `--topology process` remains stubbed with `unimplemented!()` and will land in a follow-up change.

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

- `benchmarks/minimal/README.md` — the steady-state characterization sibling.
