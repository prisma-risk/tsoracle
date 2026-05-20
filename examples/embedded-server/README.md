# Embedded tsoracle server

The minimum library-use case for tsoracle: opens a [`FileDriver`](https://docs.rs/tsoracle-driver-file), builds a [`Server`](https://docs.rs/tsoracle-server) with sensible defaults, and serves gRPC. This is the example to start from when you want tsoracle running inside your own binary rather than as a separate `tsoracle serve` process.

The full walkthrough lives in [Embedding the server](../../docs/getting-started.md#embedding-the-server); this README is the run-it-now quickstart.

## Run

```bash
cargo run -p example-embedded-server
```

The server binds `127.0.0.1:50551` and persists state under `./tsoracle-embedded-data/`. Ctrl-C drains in-flight RPCs and exits cleanly; any committed high-water extension was fsynced before the server handed out timestamps from that window.

Talk to it from another terminal with `grpcurl`:

```bash
grpcurl -plaintext -d '{"count":1}' 127.0.0.1:50551 tsoracle.v1.TsoService/GetTs
```

Or from Rust using the `tsoracle-client` crate — see [Calling tsoracle from Rust](../../docs/getting-started.md#calling-tsoracle-from-rust).

## What to look at in `src/main.rs`

- **`FileDriver::open_or_init(dir)`** is idempotent: the first run creates the state file, subsequent runs rehydrate from it.
- **`Server::builder().consensus_driver(driver).build()`** is the minimum required configuration. `clock`, `window_ahead`, and `failover_advance` get their defaults (3 s / 1 s).
- **`serve_with_shutdown(addr, future)`** drains in-flight RPCs when the shutdown future completes, then exits.

## When this example is *not* the right shape

- **You want HA.** This example uses `FileDriver`, which is single-node by design. Swap it for a `ConsensusDriver` over your replicated log — see the [openraft-cluster example](../openraft-cluster/) for a worked version.
- **You want to share a tonic listener with other services.** Use [`Server::into_router`](https://docs.rs/tsoracle-server/latest/tsoracle_server/struct.Server.html#method.into_router) instead of `serve_with_shutdown`; it returns a tonic `Routes` value plus a `JoinHandle<Result<(), ServerError>>` for the leader-watch task. Keep and observe that handle so watch failures are visible to your embedding process.
