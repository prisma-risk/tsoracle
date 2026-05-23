# tsoracle-driver-openraft

`ConsensusDriver` implementation for [tsoracle](https://github.com/prisma-risk/tsoracle) backed by [openraft](https://github.com/databendlabs/openraft) via [`tsoracle-openraft-toolkit`](https://crates.io/crates/tsoracle-openraft-toolkit).

## What it provides

- `OpenraftDriver` — the `tsoracle_consensus::ConsensusDriver` impl. Wraps an `openraft::Raft` handle and the bundled `HighWaterStateMachine`; bridges them to tsoracle's `leadership_events`, `load_high_water`, and `persist_high_water` contract.
- `OpenraftHighWaterHost` — the single trait a host service implements to expose its openraft cluster to the driver. Lets a service share one raft log between its own state and the tsoracle `HighWaterCommand`.
- `StandaloneHost` — turnkey `OpenraftHighWaterHost` for services that don't already run openraft. Bring your `RaftLogStorage` and `RaftNetworkFactory`; this crate provides the state machine and the trait bridge.
- `HighWaterStateMachine` — the `RaftStateMachine` impl over an in-memory `u64` high-water counter. Snapshots use postcard via a pluggable `SnapshotStore` (in-memory default, plus an optional `RocksdbSnapshotStore` behind the `rocksdb-snapshot-store` feature).
- `HighWaterCommand` — the typed log entry the driver injects into the host's raft log.
- The `RaftTypeConfig` (via `tsoracle_openraft_toolkit::declare_raft_types_ext!`) — `TypeConfig` and friends.

## Patterns

### Standalone services

Greenfield path — wire `StandaloneHost::new(raft, state_machine)` with an `openraft::Raft` handle you constructed against `tsoracle_openraft_toolkit::RocksdbLogStore` plus your own `RaftNetworkFactory`, then hand the resulting `OpenraftDriver` to `Server::builder().consensus_driver(driver)`. See [`examples/openraft-standalone`](https://github.com/prisma-risk/tsoracle/tree/main/examples/openraft-standalone) for the full wiring with a tonic peer transport.

### Piggyback on an existing openraft

If your service already runs an openraft for its own state, implement `OpenraftHighWaterHost` directly against that handle. `HighWaterCommand` becomes a tagged variant of your application command type, decided on the same log alongside your own commands; one snapshot covers both halves. See [`examples/openraft-piggyback`](https://github.com/prisma-risk/tsoracle/tree/main/examples/openraft-piggyback) for the worked envelope pattern.

## Feature flags

- `rocksdb-snapshot-store` (default) — enables `RocksdbSnapshotStore`, keyed in a caller-managed RocksDB column family. Pairs naturally with `tsoracle_openraft_toolkit::RocksdbLogStore` so one `Arc<DB>` covers both the raft log and the persisted snapshot. Disable with `default-features = false` if you want a custom snapshot backend.

## Documentation

- [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md) — the `ConsensusDriver` trait reference, the "Choosing a driver" comparison (file vs openraft vs paxos), and the canonical worked example for the openraft side.
- [`tsoracle-openraft-toolkit`](https://crates.io/crates/tsoracle-openraft-toolkit) — the reusable openraft glue this crate is built on (RocksDB log store, `declare_raft_types_ext!` macro, lifecycle helpers).
