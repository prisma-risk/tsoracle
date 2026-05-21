# Three-node tsoracle cluster on openraft (standalone)

Multi-process tsoracle cluster backed by [openraft](https://github.com/databendlabs/openraft), wired together via [`tsoracle-driver-openraft`](../../crates/tsoracle-driver-openraft/). The driver crate provides the `ConsensusDriver` impl, the openraft `TypeConfig`, the `HighWaterStateMachine`, and the `StandaloneHost` that owns its own raft cluster. This example supplies the rest: a tonic raft peer transport (`src/network.rs`), the openraft `Config` + bootstrap glue (`src/main.rs`), and a small `StandaloneRouter` wrapper (`src/router.rs`) that adds host-specific `NodeId -> tsoracle-addr` resolution for `LeaderHint` follower-redirect.

If your service already runs openraft for other state and you want TSO to share it, see the [`openraft-piggyback`](../openraft-piggyback/) example instead.

> **Do not copy this example to production unchanged.** It is *correct enough* to teach the integration boundary, but several layers are deliberately simplified. See [Production caveats](#production-caveats).

## Prerequisites

- Rust 1.88+ (workspace toolchain).
- `protoc` installed (`brew install protobuf` on macOS).
- For `GetTs` smoke tests: `grpcurl` (optional but convenient).

## Run a 3-node cluster

Fastest path is the helper script — it starts three node processes in the background and tails their logs into `.data/n*.log`:

    scripts/run.sh

Alternatively start each node by hand in its own terminal. Node 1 carries `--bootstrap`:

    # node 1
    cargo run -p example-openraft-standalone -- \
      --id 1 \
      --raft-addr 127.0.0.1:51001 --tso-addr 127.0.0.1:50561 \
      --peers     "1=127.0.0.1:51001,2=127.0.0.1:51002,3=127.0.0.1:51003" \
      --tso-peers "1=127.0.0.1:50561,2=127.0.0.1:50562,3=127.0.0.1:50563" \
      --raft-dir ./.data/n1 --bootstrap

    # node 2 (same args, --id 2, ports …002/…562, dir n2, no --bootstrap)
    # node 3 (--id 3, ports …003/…563, dir n3, no --bootstrap)

## Issue a timestamp

Against any node:

    grpcurl -plaintext -d '{"count":1}' 127.0.0.1:50561 tsoracle.v1.TsoService/GetTs

A follower will respond with a `LeaderHint` trailer pointing at the current leader's tsoracle address (see `--tso-peers`). That trailer comes from `StandaloneRouter::leadership_events`, which resolves the leader's `NodeId` against the `--tso-peers` map.

## Observe failover

Find the current leader in the logs (`grep "Leader" .data/n*.log`), kill that process, watch the survivors elect a new leader. Subsequent `GetTs` calls succeed once the new leader has fenced — typically 2–5 seconds.

## What's in this example

- `src/main.rs` — CLI parse, openraft `Config`, one rocksdb instance with three CFs (`raft_log` / `raft_meta` for the log store, `raft_snapshot` for `RocksdbSnapshotStore`), `Raft::new`, optional `initialize`, and the three-binding driver wiring: `StandaloneHost::new` → `OpenraftDriver::new` → `StandaloneRouter::new`. About 150 lines including config and bootstrap.
- `src/router.rs` — `StandaloneRouter`, a `ConsensusDriver` that delegates `load_high_water` / `persist_high_water` to `OpenraftDriver<StandaloneHost>` but reimplements `leadership_events` to populate `LeaderState::Follower::leader_endpoint` from the `--tso-peers` map. About 40 lines. This is the "compose the driver, override what you need" pattern.
- `src/network.rs` — tonic raft peer transport (`AppendEntries`, `Vote`, chunked snapshot stream). The bulk of the example; ports across cleanly because the driver crate's `TypeConfig` is the only handle the network needs.
- `proto/raft.proto`, `build.rs` — peer-RPC service definition + tonic codegen.
- `scripts/run.sh` — 3-node bring-up.

## Production caveats

This example shows the **minimum** wiring to take `ConsensusDriver` end-to-end with openraft. Several layers are simplified for readability and **must** be replaced before any real deployment:

- **Snapshot transport.** `src/network.rs` streams snapshots in 1 MiB chunks over a client-streaming RPC, so frames stay well under gRPC's 4 MiB default. It does *not* implement resume-on-disconnect: a peer that fails mid-install starts the next attempt at chunk 0. For state machines that grow into the hundreds of MiB you'll want a resumable protocol with an explicit offset.

- **Membership operations.** This example bootstraps a fixed 3-node cluster via `--bootstrap`. There is no add-learner / promote / remove flow shown. Use openraft's `change_membership` API and gate it behind whatever authentication you require.

- **Authentication & TLS.** Every RPC (raft peer and tsoracle client) is plaintext. Wrap both servers with TLS and require client certs before exposing anything beyond a trusted control plane.

- **Leader-watch debounce.** The toolkit's `stream_from_receiver` emits a new state only when the role class changes. It does not coalesce bursts: if openraft flips Leader → Candidate → Leader within a single metrics tick the server will fence, un-fence, and fence again. Production wiring may want a short hold-off (50–100 ms) before propagating Unknown / Follower transitions.

## Cleaning up

    rm -rf examples/openraft-standalone/.data/
