# Getting Started

tsoracle can run as a standalone server, be called from Rust client code, or be embedded inside an existing binary. This chapter walks each path from zero, plus the vocabulary the rest of the docs assume and how to migrate off a prior timestamp source. Sections are independent — skip to the one that matches what you're trying to do.

## Basic concepts

The vocabulary used throughout the rest of these docs:

- **Timestamp.** A 64-bit value, packed as `physical_ms << 18 | logical`. The high 46 bits are milliseconds since Unix epoch; the low 18 bits are a counter that resets when `physical_ms` advances. See [Timestamp packing](architecture-deep-dive.md#timestamp-packing) for the field layout.
- **High-water `H`.** The durably-persisted upper bound on `physical_ms`. The allocator will never issue a timestamp with `physical_ms > H`. Every advance of `H` is durably committed (via the `ConsensusDriver`) before any timestamp under the new bound is handed out.
- **Window.** The range from which the allocator may currently issue, bounded above by `H`. The allocator *extends* its window by advancing `H` when serving approaches the current upper bound.
- **Epoch.** An opaque monotonic identifier identifying a leader's term, supplied by the `ConsensusDriver`. Used to fence stale-leader writes.
- **Leader / follower.** At any moment, exactly one node in a tsoracle deployment is the leader; only the leader serves `GetTs`. Followers respond with `NOT_LEADER` and a hint to the current leader's address. The single-node configuration is trivially "always leader."
- **Failover fence.** The mandatory sequence the new leader runs before serving on every transition into leadership — load `H`, advance `H` past anything the prior leader could have served, persist, then begin serving. The correctness argument is in [Monotonicity proof](the-allocator.md#monotonicity-proof); the orchestration is in [The failover fence](key-subsystems.md#the-failover-fence).

No code on this page — just the words. The pages that follow assume these terms.

## Installing and running

The standalone server ships as a CLI. Install it from crates.io:

```bash
cargo install tsoracle
```

Then start the server:

```bash
tsoracle serve
```

Defaults: listens on `127.0.0.1:50551` and persists state under `./tsoracle-data/`. The state directory is created on first start; the first server run against an empty directory begins at high-water 0 and increases from there.

Useful flags:

```bash
tsoracle serve \
    --listen 127.0.0.1:50551 \
    --state-dir /var/lib/tsoracle \
    --window-ahead 3s \
    --failover-advance 1s \
    --log info
```

- `--listen` — gRPC bind address. The default `127.0.0.1:50551` is appropriate for embedded sidecar deployments. Binding to a non-loopback address (e.g. `0.0.0.0:50551` to serve cluster peers or application clients) exposes the unauthenticated `GetTs` RPC to anything that can reach the socket; tsoracle ships no built-in authn, authz, TLS, or rate limiting, so only do this behind trusted network controls (private VPC/subnet, firewall, service mesh with mTLS, or an authorizing reverse proxy). A reachable caller can consume the ordering namespace and force window-extension work.
- `--state-dir` — where to keep the fsync'd state file.
- `--window-ahead` — how far ahead the allocator extends `H` on each extension. See [Sizing window_ahead](operations.md#sizing-window_ahead) for sizing guidance.
- `--failover-advance` — how far past `serving_floor = max(prior_max + 1, now_ms)` the failover fence advances `H` on every leadership gain. See [Sizing failover_advance](operations.md#sizing-failover_advance).

To seed the state directory at a non-zero high-water (for migrating off a prior system), jump to [Migrating from an existing timestamp source](#migrating-from-an-existing-timestamp-source).

## Calling tsoracle from Rust

The `tsoracle-client` crate is a thin async wrapper over the gRPC service. Use it when your application speaks Rust; for other languages, drive the protobuf service in `tsoracle-proto` directly.

Connect to one or more server endpoints, then call `get_ts` or `get_ts_batch`:

```rust
use tsoracle_client::Client;

let client = Client::connect(vec!["http://127.0.0.1:50551".into()]).await?;

let ts = client.get_ts().await?;             // a single strictly increasing Timestamp
let batch = client.get_ts_batch(64).await?;  // 64 contiguous strictly increasing timestamps
```

`Client::connect` is the minimum-overhead constructor. To tune the batch-flush interval (how long the client waits to coalesce concurrent waiters before issuing an outgoing `GetTs`), use the builder:

```rust
use std::time::Duration;
use tsoracle_client::ClientBuilder;

let client = ClientBuilder::endpoints(vec!["http://127.0.0.1:50551".into()])
    .batch_flush_interval(Duration::from_micros(500))
    .build()
    .await?;
```

### The freshness invariant

**The client never retains pre-fetched timestamps.** Every timestamp returned by `get_ts` or `get_ts_batch` was allocated by the server *after* the calling task entered the client driver — never from a prior RPC's leftover range. RPC efficiency comes from coalescing concurrent waiters into one outgoing `GetTs`, not from pre-fetching. This is the contract strict-consistency callers can rely on. The full argument — why this matters for cross-client real-time ordering, how the driver structurally enforces it, and how batches grow without configuration — is in [The Client Driver](the-client-driver.md).

### Leader discovery is automatic

You can pass multiple endpoints; the client tries them in order, caches whichever one accepts the RPC as the current leader, and follows `NOT_LEADER` redirects (using the `tsoracle-leader-hint-bin` trailer) without retry logic in your code. The worklist algorithm, error semantics, and what each `ClientError` variant means are covered in [Leader discovery and retries](client-api-and-usage.md#leader-discovery-and-retries).

## Embedding the server

When you need tsoracle inside your own binary instead of running it as a standalone process, build the server with `Server::builder`:

```rust
use tsoracle_server::Server;
use tsoracle_driver_file::FileDriver;

let driver = FileDriver::open_or_init("./tsoracle-data")?;
let server = Server::builder()
    .consensus_driver(driver)
    .build()?;

server
    .serve_with_shutdown("127.0.0.1:50551".parse()?, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
```

A complete, runnable version lives in [`examples/embedded-server`](https://github.com/prisma-risk/tsoracle/tree/main/examples/embedded-server); the walkthrough is in [Embedded-server example](testing-and-examples.md#embedded-server-example).

To mount tsoracle inside an existing tonic server (sharing a listener with your other gRPC services), use `Server::into_router`:

```rust
let server = Server::builder()
    .consensus_driver(driver)
    .build()?;
// Keep `watch_guard` alive for as long as the routes should serve; drop it
// (or call `watch_guard.shutdown().await`) at your own shutdown to stop the
// leader-watch task.
let (tsoracle_routes, watch_guard) = server.into_router()?;

tonic::transport::Server::builder()
    .add_routes(tsoracle_routes)
    .add_service(your_own_service)
    .serve(addr)
    .await?;
```

`into_router` returns a `Result` wrapping the tonic `Routes` plus a `WatchGuard` owning the spawned leader-watch task; with the `reflection` feature enabled it returns `Err(ServerError::ReflectionInit)` if the embedded descriptor set fails to decode, so propagate it (`?`) rather than unwrapping. Keep the guard alive for as long as the mounted routes should serve: it ties the watch task's lifetime to the guard's, so dropping the guard — or calling `watch_guard.shutdown().await` — cooperatively stops the task at your own shutdown. If the leadership watch fails on its own, the task poisons serving state to `NotServing` so subsequent RPCs fail fast; `watch_guard.shutdown().await` then surfaces the error so embedders can restart intentionally. For HA setups, swap `FileDriver` for a `ConsensusDriver` implementation backed by your replicated log — see [Consensus Integration](consensus-integration.md).

## Migrating from an existing timestamp source

`tsoracle serve` against an empty state directory starts at high-water 0. If you are migrating from any prior timestamp source (a previous TSO, snapshot of max-observed commit timestamps in your data, etc.), seed the state file once:

```bash
tsoracle init --seed-physical-ms <MAX_OBSERVED_MILLIS> --state-dir ./tsoracle-data
tsoracle serve --state-dir ./tsoracle-data
```

`init` refuses to overwrite an existing state file, so accidental rollback is prevented. Pick `MAX_OBSERVED_MILLIS` to be the largest `physical_ms` you have ever served from the prior system, plus a safety margin to account for any timestamps you may have issued but not yet checkpointed. The seed must fit the timestamp layout's 46-bit physical field (`<= PHYSICAL_MS_MAX`).
