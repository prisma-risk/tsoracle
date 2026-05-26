<div align="center">
    <picture>
        <source media="(prefers-color-scheme: dark)" srcset="site/static/tsoracle-dark.svg">
        <img alt="tsoracle" src="site/static/tsoracle-light.svg" width="360">
    </picture>
    <br /><br />

[![Crates.io](https://img.shields.io/crates/v/tsoracle.svg)](https://crates.io/crates/tsoracle)
[![DeepWiki](https://img.shields.io/badge/DeepWiki-tsoracle-blue.svg)](https://deepwiki.com/prisma-risk/tsoracle)
![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)
![Crates.io](https://img.shields.io/crates/d/tsoracle-core.svg)
<br/>
[![CI](https://github.com/prisma-risk/tsoracle/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/prisma-risk/tsoracle/actions/workflows/ci.yml)
[![stress](https://github.com/prisma-risk/tsoracle/actions/workflows/stress-nightly.yml/badge.svg?branch=main)](https://github.com/prisma-risk/tsoracle/actions/workflows/stress-nightly.yml)
[![Coverage Status](https://coveralls.io/repos/github/prisma-risk/tsoracle/badge.svg?branch=main)](https://coveralls.io/github/prisma-risk/tsoracle?branch=main)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/prisma-risk/tsoracle/badge)](https://scorecard.dev/viewer/?uri=github.com/prisma-risk/tsoracle)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12991/badge)](https://www.bestpractices.dev/projects/12991)

</div>

A distributed timestamp oracle for Rust — highly available and fault-tolerant, issuing strictly monotonic integer timestamps over gRPC, replicated via openraft or OmniPaxos (or your own log) with pluggable consensus.

## Features

- 🔢 **Strictly monotonic** — every issued timestamp is strictly greater than every previously issued one; no duplicates and no regression, ever. The packed integer space is not dense (logical resets when `physical_ms` advances, so some integer values are unused), but the issued sequence is total-ordered and unique.
- 🛡️ **Crash-safe** — window state is fsync'd before any timestamp in that window is handed out, so a restart never rewinds.
- 🔌 **Pluggable consensus, openraft + OmniPaxos included** — `tsoracle-driver-openraft` and `tsoracle-driver-paxos` ship production-ready replicated drivers; implement one trait (`ConsensusDriver`) to back tsoracle with raft-rs, etcd, or your own replicated log instead. See [`docs/consensus-integration.md`](docs/consensus-integration.md#choosing-a-driver) for picking between drivers.
- 📦 **gRPC client included** — `tsoracle-client` handles leader discovery, request coalescing, and reconnection for you.
- 📈 **Operational metrics** — enable the `metrics` feature on `tsoracle-server` to emit allocator, leader, and request metrics through the `metrics` facade.
- 🧪 **Hardened** — coverage-guided fuzzing on the postcard decoders, failpoint-driven crash tests, and a stress harness covering in-process and multi-process topologies across the openraft and OmniPaxos backends.
- 🧩 **Embeddable** — host the server inside your own binary with a few lines of Rust, or run the standalone CLI.

## Quickstart

Install the standalone binary and start the server:

```bash
cargo install tsoracle
tsoracle
```

Releases are signed with SLSA build provenance — see [Verifying release signatures](docs/release-signatures.md).

Bare `tsoracle` is shorthand for `tsoracle serve file`. The server listens on `127.0.0.1:50551` and persists state under `./tsoracle-data/`.

Then call it from Rust:

```rust
use tsoracle_client::Client;

let client = Client::connect(vec!["http://127.0.0.1:50551".into()]).await?;

let ts = client.get_ts().await?;             // a strictly increasing Timestamp
let batch = client.get_ts_batch(64).await?;  // amortize RPC cost across many IDs
```

## Use as a library

Embed the server in your own binary instead of running the CLI:

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

A complete, runnable version lives in [`examples/embedded-server`](examples/embedded-server). Want to mount tsoracle inside an existing tonic server? See [`Server::into_router`](https://docs.rs/tsoracle-server/latest/tsoracle_server/struct.Server.html#method.into_router).

## What's a TSO?

A *timestamp oracle* is a service that hands out strictly increasing integer IDs which order events across a distributed system. You reach for one when:

- You're building a database with snapshot isolation or MVCC (Spanner, CockroachDB, FoundationDB all use a TSO internally).
- You need to merge change-data from many shards into one globally ordered stream.
- You want audit logs with a real "happens-before" relation across machines.
- Per-host clocks aren't monotonic or accurate enough, and database sequences don't scale to your workload.

tsoracle is a small, embeddable Rust implementation. The consensus layer is left as a trait, so you can run it single-node behind one fsync (the default), or wire it into a replicated log of your choice.

## Documentation

- [DeepWiki](https://deepwiki.com/prisma-risk/tsoracle) — prose documentation covering the window allocator, the `ConsensusDriver` contract, and operational topics (fsync cost, leader handoff, deployment topologies).
- [docs.rs/tsoracle-server](https://docs.rs/tsoracle-server) — generated API reference.

## Examples

- [examples/embedded-server](examples/embedded-server) — embed `tsoracle-server` with the file driver in your own binary, with graceful shutdown.
- [examples/failover-demo](examples/failover-demo) — pedagogy: watch the failover fence keep timestamps strictly monotonic across simulated leadership changes, in-process, no openraft.
- [examples/openraft-standalone](examples/openraft-standalone) — HA: three-node multi-process cluster on a dedicated openraft, wired through [`tsoracle-driver-openraft`](crates/tsoracle-driver-openraft/) with a tonic peer transport and follower-redirect via `LeaderHint`.
- [examples/openraft-piggyback](examples/openraft-piggyback) — HA: in-process three-node demo of the envelope pattern, where your service's existing openraft carries both your `AppData` and the tsoracle `HighWaterCommand` on a single log, with one snapshot covering both halves.
- [examples/paxos-standalone](examples/paxos-standalone) — HA: three-node multi-process cluster on a dedicated OmniPaxos, wired through [`tsoracle-driver-paxos`](crates/tsoracle-driver-paxos/) with a tonic peer transport and follower-redirect via `LeaderHint`.
- [examples/paxos-piggyback](examples/paxos-piggyback) — HA: in-process three-node demo of the envelope pattern on OmniPaxos, where your service's existing paxos log carries both your `AppData` and the tsoracle `HighWaterCommand`, with one snapshot covering both halves.
- [examples/paxos-embedded](examples/paxos-embedded) — HA: single-process 3-node OmniPaxos cluster (OmniPaxos rejects single-node configs, so even "embedded" runs all three nodes in-memory). The closest paxos equivalent to `embedded-server`.
- [examples/metrics-prometheus](examples/metrics-prometheus) — embed `tsoracle-server` with `metrics-exporter-prometheus` installed before the server starts, exposing `/metrics` on a separate port for Prometheus to scrape; swap recorders with a one-line change.

Each example is its own crate. Build with `cargo run -p example-<name>`.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, the checks CI will run on your PR, commit message conventions, and the release process.

### Contributors

<a href="https://github.com/prisma-risk/tsoracle/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=prisma-risk/tsoracle"/>
</a>

Made with [contrib.rocks](https://contrib.rocks).

## License

Licensed under [Apache-2.0](LICENSE).
