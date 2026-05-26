# tsoracle

The standalone CLI for the [tsoracle](https://github.com/prisma-risk/tsoracle) timestamp oracle — a `tsoracle` binary that runs a node directly from the command line, no embedding required.

A *timestamp oracle* hands out strictly increasing integer IDs that order events across a distributed system. This crate is the ready-to-run server: pick a consensus driver, point it at a data directory, and serve gRPC. To embed the server in your own binary instead, use [`tsoracle-server`](https://crates.io/crates/tsoracle-server); to call a running node, use [`tsoracle-client`](https://crates.io/crates/tsoracle-client).

## Install

```bash
cargo install tsoracle
```

## Quickstart

```bash
# Single-node, fsync-durable file driver. Listens on 127.0.0.1:50551,
# persists window state under ./tsoracle-data.
tsoracle serve file

# Bare `tsoracle` is shorthand for `tsoracle serve file`.
tsoracle
```

Then call it from Rust with [`tsoracle-client`](https://crates.io/crates/tsoracle-client):

```rust,ignore
use tsoracle_client::Client;

let client = Client::connect(vec!["http://127.0.0.1:50551".into()]).await?;
let ts = client.get_ts().await?;             // a strictly increasing Timestamp
let batch = client.get_ts_batch(64).await?;  // amortize RPC cost across many IDs
```

## Commands

- `tsoracle serve file` *(default)* — single-node, fsync-durable [file driver](https://crates.io/crates/tsoracle-driver-file). Window state is fsync'd before any timestamp in that window is issued, so a restart never rewinds. `--state-dir` defaults to `./tsoracle-data`.
- `tsoracle serve openraft` — highly available via [openraft](https://crates.io/crates/tsoracle-driver-openraft) (3+ node cluster). Takes `--id`, `--raft-addr`, `--raft-dir`, and `--bootstrap`/`--members` on first boot.
- `tsoracle serve paxos` — highly available via [OmniPaxos](https://crates.io/crates/tsoracle-driver-paxos) (3+ node cluster). Takes `--node-id`, `--peer-listen`, `--peers`, `--tso-peers`, and `--data-dir`.
- `tsoracle init --seed-physical-ms <ms>` — initialize a fresh file-driver state directory seeded at a given high-water, without serving.

Every `serve` subcommand shares the common knobs `--listen` (default `127.0.0.1:50551`), `--window-ahead`, `--failover-advance`, `--log`, and TLS/mTLS for the client API (`--tls-cert`, `--tls-key`, `--tls-client-ca`). The HA drivers add their own peer-transport mTLS flags (`--peer-tls-*`). Run `tsoracle serve <driver> --help` for the full list.

## Feature flags

The three drivers are compiled in by default; trim the binary by disabling the ones you don't deploy.

- `file` (default) — the single-node file driver and the bare/`serve file` command.
- `openraft` (default) — the openraft HA driver and `serve openraft`.
- `paxos` (default) — the OmniPaxos HA driver and `serve paxos`.
- `reflection` — enable gRPC server reflection on the client API.
- `bt` — backtrace capture in error variants.

A driver that isn't compiled in produces a friendly "not included; rebuild with `--features <driver>`" message rather than a missing subcommand.

## Documentation

- [`docs/operations.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/operations.md) — deployment, TLS, and metrics.
- [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md) — choosing between the file, openraft, and paxos drivers.
- The repository [README](https://github.com/prisma-risk/tsoracle) — architecture overview and the full crate map.
