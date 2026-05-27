# tsoracle-server

Embeddable gRPC server for the [tsoracle](https://github.com/prisma-risk/tsoracle) timestamp oracle.

Wires four pieces together: the sync window allocator from [`tsoracle-core`](https://crates.io/crates/tsoracle-core), a user-supplied [`ConsensusDriver`](https://crates.io/crates/tsoracle-consensus) (leadership state + durable high-water persistence), the tonic-generated `TsoService` from [`tsoracle-proto`](https://crates.io/crates/tsoracle-proto), and the internal leader-watch pipeline + failover fence that keep timestamps strictly monotonic across leader transitions.

## What's in the box

- `Server` + `ServerBuilder` — the embedding entry point. Provide a `ConsensusDriver` impl, optional TLS config, optional metrics recorder, and serve on a `SocketAddr` or pre-bound `TcpListener`.
- `BuildError` — surfaces invalid configurations at build time (missing driver, conflicting TLS settings, …).
- `ServerError` — runtime errors from the serving task.
- `ServingState` — observability snapshot for the leader-watch state machine.
- `Bt` — backtrace-on-error helper embedded in instrumented `ServerError` variants. ZST and no-op unless the `bt` feature is on; with it on, capture is still gated by `RUST_BACKTRACE` / `RUST_LIB_BACKTRACE`.

## Usage shape

```rust,ignore
use tsoracle_server::Server;
use tsoracle_driver_file::FileDriver;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let driver = FileDriver::open_or_init("./tsoracle-data")?;
    let server = Server::builder()
        .consensus_driver(driver)
        .build()?;

    server.serve("127.0.0.1:50051".parse()?).await?;
    Ok(())
}
```

See [`examples/embedded-server`](https://github.com/prisma-risk/tsoracle/tree/main/examples/embedded-server) for graceful Ctrl-C shutdown, and the HA examples (`openraft-standalone`, `openraft-piggyback`, `paxos-standalone`, `paxos-piggyback`, `paxos-embedded`) for swapping the file driver out for a replicated one.

## Follower behavior

Followers respond to RPCs with `FAILED_PRECONDITION` and a `tsoracle-leader-hint-bin` binary trailer. [`tsoracle-client`](https://crates.io/crates/tsoracle-client) consumes the hint to redirect transparently — no extra round trip.

## Feature flags

- `tls-rustls` (default) — TLS via rustls (`tonic/tls-aws-lc`).
- `tls-native` — TLS via the platform's native trust roots (`tonic/tls-native-roots`). Mutually exclusive with `tls-rustls` at the consumer level; pick one.
- `tracing` (default) — emit tracing spans/events through the `tracing` facade.
- `metrics` — emit allocator, leader, and request metrics through the `metrics` facade.
- `failpoints` — enables `fail` crate injection for chaos coverage.
- `yieldpoints` — enables `tsoracle-yieldpoint` injection sites (async sibling of failpoints; off by default since production carries zero overhead).
- `test-fakes` / `test-support` — test-only fixtures for downstream integration suites.
- `bt` — captures `std::backtrace::Backtrace` in instrumented `ServerError` variants. Off by default to keep cold paths free.

## Documentation

- [`docs/key-subsystems.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/key-subsystems.md) — the leader-watch + failover fence pipeline in depth.
- [`docs/operations.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/operations.md) — deployment guidance, TLS, metrics shape.
- [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md) — picking and implementing a `ConsensusDriver`.
