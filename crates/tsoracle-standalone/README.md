# tsoracle-standalone

Driver selection, configuration, and peer transport for running a standalone [tsoracle](https://github.com/prisma-risk/tsoracle) node.

This is the layer the [`tsoracle`](https://crates.io/crates/tsoracle) CLI is built on: it turns a `DriverConfig` into a constructed, running [`ConsensusDriver`](https://crates.io/crates/tsoracle-consensus) plus its background peer-transport task. The caller owns the client-facing [`tsoracle_server::Server`](https://crates.io/crates/tsoracle-server); this crate owns only the driver and the peer transport that backs it.

Reach for it when you want the CLI's driver bootstrap — feature-gated file/openraft/paxos selection, peer mTLS, and graceful shutdown wiring — inside your own binary, without re-implementing the plumbing.

## What's in the box

- `build(DriverConfig) -> Standalone` — open storage, construct the selected driver, and bind + spawn its peer transport before returning, so a bind failure is a startup error rather than a background log line.
- `Standalone` — the running node: the `Arc<dyn ConsensusDriver>` to hand to `tsoracle_server::Server`, plus `take_drain()` (driver-specific pre-shutdown action, e.g. openraft leadership handoff) and `shutdown()` for the peer transport.
- `DriverConfig` + `FileConfig` / `OpenraftConfig` / `PaxosConfig` — the per-driver configuration types, with `MemberAddr`, `PeerTlsConfig`, `RaftTuning`, and `parse_peer_map` for parsing CLI-style peer lists.
- `StandaloneError` — startup failures (bad config, bind failure, storage open).

## Usage shape

```rust,ignore
use tsoracle_server::Server;
use tsoracle_standalone::{build, DriverConfig, FileConfig};

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let node = build(DriverConfig::File(FileConfig {
        state_dir: "./tsoracle-data".into(),
    }))
    .await?;

    let server = Server::builder().consensus_driver(node.driver.clone()).build()?;
    server.serve("127.0.0.1:50551".parse()?).await?;

    node.shutdown().await;
    Ok(())
}
```

## Feature flags

- `file` (default) — the single-node fsync-durable file driver.
- `openraft` (default) — the openraft HA driver and its peer transport.
- `paxos` (default) — the OmniPaxos HA driver and its peer transport.

Disable the drivers you don't deploy to trim the dependency tree; `build` only accepts a `DriverConfig` variant whose driver is compiled in.

## Documentation

- [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md) — picking and configuring a driver.
- The repository [README](https://github.com/prisma-risk/tsoracle) — architecture overview and the full crate map.
