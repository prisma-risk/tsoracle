# Three-node tsoracle cluster on openraft (standalone)

Multi-process tsoracle cluster backed by [openraft](https://github.com/databendlabs/openraft), wired together via [`tsoracle-driver-openraft`](../../crates/tsoracle-driver-openraft/). The driver crate provides the `ConsensusDriver` impl, the openraft `TypeConfig`, the `HighWaterStateMachine`, and the `StandaloneHost` that owns its own raft cluster. This example supplies the rest: a tonic raft peer transport (`src/network.rs`), the openraft `Config` + bootstrap glue (`src/main.rs`), and the `--tso-peers` map (`NodeId -> tsoracle-service-addr`), which seeds each member's `service_endpoint` in raft membership at bootstrap so the driver can resolve `LeaderHint` follower-redirects from the leader's membership node.

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

A follower will respond with a `LeaderHint` trailer pointing at the current leader's tsoracle address (see `--tso-peers`). That address is the leader's `service_endpoint` carried in raft membership (seeded from `--tso-peers` at bootstrap); the driver reads it from the leader's membership node.

## Observe failover

Find the current leader in the logs (`grep "Leader" .data/n*.log`), kill that process, watch the survivors elect a new leader. Subsequent `GetTs` calls succeed once the new leader has fenced — typically 2–5 seconds.

## What's in this example

- `src/main.rs` — CLI parse, openraft `Config`, one rocksdb instance with three CFs (`raft_log` / `raft_meta` for the log store, `raft_snapshot` for `RocksdbSnapshotStore`), `Raft::new`, optional `initialize`, and the driver wiring: `StandaloneHost::new` → `OpenraftDriver::new(host)`, with the `--tso-peers` addresses written into each member's `service_endpoint` in raft membership at bootstrap. About 150 lines including config and bootstrap.
- `src/network.rs` — tonic raft peer transport (`AppendEntries`, `Vote`, chunked snapshot stream). The bulk of the example; ports across cleanly because the driver crate's `TypeConfig` is the only handle the network needs.
- `proto/raft.proto`, `build.rs` — peer-RPC service definition + tonic codegen.
- `scripts/run.sh` — 3-node bring-up.

## Production caveats

This example shows the **minimum** wiring to take `ConsensusDriver` end-to-end with openraft. Several layers are simplified for readability and **must** be replaced before any real deployment:

- **Snapshot transport.** `src/network.rs` streams snapshots in 1 MiB chunks over a client-streaming RPC, so frames stay well under gRPC's 4 MiB default. The receiver bounds reassembly at `MAX_SNAPSHOT_BYTES` (64 MiB) and refuses anything larger with `ResourceExhausted`, caps each peer message at `MAX_PEER_MESSAGE_BYTES` (one chunk plus framing headroom), and times out a single install stream after `SNAPSHOT_STREAM_TIMEOUT` (60 s) — these are example-scale defaults; size them against your largest realistic state-machine snapshot. It does *not* implement resume-on-disconnect: a peer that fails mid-install starts the next attempt at chunk 0. For state machines that grow into the hundreds of MiB you'll want a resumable protocol with an explicit offset.

- **Membership operations.** This example bootstraps a fixed 3-node cluster via `--bootstrap`. There is no add-learner / promote / remove flow shown. Use openraft's `change_membership` API and gate it behind whatever authentication you require.

- **Authentication & TLS.** Every RPC (raft peer and tsoracle client) is plaintext, and the raft peer transport is **unauthenticated by design** — any client that can reach `--raft-addr` can drive replication (append-entries/vote) and stream snapshots into the node. The memory bounds above stop a reachable peer from OOMing the process, but they are not an access control: before exposing the raft port beyond a trusted control plane, do one of (a) bind loopback or a private subnet reachable only by cluster peers, (b) wrap the transport in mTLS with a client-cert allowlist (see the [`tls-mtls`](../tls-mtls/) example), or (c) front it with an authorizing proxy. Note that all bind addresses are operator-supplied — `--raft-addr` is a required flag with no default, so a bare `cargo run` fails rather than exposing anything by accident, and the bundled `scripts/run.sh` binds loopback.

- **Leader-watch debounce.** The toolkit's `leadership_events_from_metrics` emits a new state whenever the projected leadership state changes (role, term, or leader identity). It does not coalesce bursts: if openraft flips Leader → Candidate → Leader within a single metrics tick the server will fence, un-fence, and fence again. Production wiring may want a short hold-off (50–100 ms) before propagating Unknown / Follower transitions.

## Addressing and pod restarts

Peer addresses live in replicated raft membership, not in per-process config. Each member carries two addresses: `addr`, the raft transport endpoint as a scheme-less `host:port`, and `service_endpoint`, the tsoracle gRPC URL clients redirect to (`http://host:port`). Configure both with stable DNS names — run the cluster as a StatefulSet behind a headless Service so each pod has a durable name like `tso-0.tso.ns.svc.cluster.local`, never a raw pod IP. A pod that reschedules with a new IP keeps its name; the transport re-resolves it on the next dial (the pool evicts a failed channel), so no membership change is needed for an IP change. Production placement-driver/consensus stacks (Spanner, CockroachDB, FoundationDB) use this same stable-name model.

This example's peer RPCs are unframed postcard, and widening the membership node bumped the toolkit `SCHEMA_VERSION`, so this build requires a **fresh cluster**: a rolling/mixed-version upgrade across the change is unsupported.

## Cleaning up

    rm -rf examples/openraft-standalone/.data/
