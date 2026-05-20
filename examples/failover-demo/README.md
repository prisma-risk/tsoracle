# Failover fence demonstration

In-process pedagogy: a single binary that builds a tsoracle [`Server`](https://docs.rs/tsoracle-server) against the in-memory `InMemoryDriver` (from the `test-fakes` feature of `tsoracle-server`), connects a gRPC client, and scripts a leader → follower → new-leader sequence. The point is to make the failover fence visible and to *assert* monotonicity holds across it.

No openraft, no real network beyond a loopback tonic listener, no real disk — just enough scaffolding to show how the fence keeps timestamps strictly monotonic across a leadership change.

The full walkthrough lives in [Failover-demo example](../../docs/testing-and-examples.md#failover-demo-example); this README is the run-it-now quickstart.

## Run

```bash
cargo run -p example-failover-demo
```

Expected output (timestamps will differ, but the structure is fixed):

```
[serving] became leader at epoch=1
  ts = 1715200000000.0 (epoch=1)
  ts = 1715200000000.1 (epoch=1)
  ts = 1715200000000.2 (epoch=1)
  ts = 1715200000000.3 (epoch=1)
  ts = 1715200000000.4 (epoch=1)
[fenced] leadership lost, GetTs => Some(FailedPrecondition)
[serving] became leader at epoch=2
  ts = 1715200001000.0 (epoch=2)
  ts = 1715200001000.1 (epoch=2)
  ...
OK: 10 timestamps, all strictly monotonic across the fence.
```

The example uses `assert!(packed_ts > prev)` after every `GetTs` — if the fence ever fails to advance past the prior leader's last issued timestamp, the binary aborts and the assertion message shows which timestamp regressed.

## What to look at in `src/main.rs`

- **Phase 1** calls `driver.become_leader(Epoch(1))` and issues 5 timestamps. Note the epoch carried in each `GetTsResponse`.
- **Phase 2** calls `driver.become_follower(None)`. The next `GetTs` returns `FAILED_PRECONDITION` because the server has cleared `ServingState::Serving`.
- **Phase 3** calls `driver.become_leader(Epoch(2))`. The leader-watch task runs the fence (load → advance → persist → seed) before re-enabling serving. The first post-fence `GetTs` is required to be strictly greater than the last pre-fence timestamp.

The assertion that post-fence timestamps strictly dominate pre-fence ones is the safety property the [monotonicity proof](../../docs/the-allocator.md#monotonicity-proof) guarantees, made visible.

## When this example is *not* the right shape

- **You want to see failover under real consensus.** Use the [openraft-cluster example](../openraft-cluster/) for a real multi-process cluster with openraft-driven elections.
- **You want to embed tsoracle in your own binary.** Use the [embedded-server example](../embedded-server/) — this one isn't a server template, it's a behavior demo.
