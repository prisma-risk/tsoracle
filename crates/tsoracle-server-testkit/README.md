# tsoracle-server-testkit

**Internal — no public API stability.** Test-only harness for [tsoracle](https://github.com/prisma-risk/tsoracle). Not published to crates.io.

This crate hosts the deterministic-simulation-testing (DST) harness that runs a real `tsoracle-server` gRPC stack over [`turmoil`](https://docs.rs/turmoil)'s simulated network and clock, so timing-sensitive flows (window extension, fence retry/backoff, leadership churn) execute deterministically and instantly. It lives outside `tsoracle-server` so the published server crate's dependency graph never carries `turmoil` and the rest of the simulation stack.

## What's in here

A small set of helpers built on `tsoracle-server`'s public API:

- `into_sim_parts(server)` — capture a built `Server`'s `Routes`, leader-watch `JoinHandle`, and `ServingState` receiver.
- `serve(routes, port)` — serve those `Routes` over a turmoil listener inside a host closure.
- `client(host, port)` / `sim_channel(endpoint)` — lazy tonic client/channel that dials over the simulated network.

## Who uses it

`tsoracle-tests` depends on this crate behind its `dst` feature and drives all DST regression tests through it (`cargo test -p tsoracle-tests --features dst`). You almost certainly do not want to depend on this crate directly.
