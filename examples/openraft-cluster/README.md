# Three-node tsoracle cluster on openraft

A multi-process tsoracle cluster backed by [openraft](https://github.com/databendlabs/openraft), wired together as a **worked tutorial** for the `ConsensusDriver` integration boundary. The driver impl lives in `src/driver.rs`; everything else is the minimum plumbing needed to make it run end-to-end (file-backed storage in `src/store/`, tonic peer transport in `src/network.rs`, leader-state watch in `src/leader_watch.rs`).

The full walkthrough — including what this example demonstrates beyond [the worked-example sketch in docs](../../docs/consensus-integration.md#worked-example-openraft) — lives in [openraft-cluster example](../../docs/testing-and-examples.md#openraft-cluster-example); this README is the run-it-now quickstart and operational reference.

> **Do not copy this example to production.** It is *correct enough* to teach the integration boundary, but several layers are deliberately simplified. See [Production caveats](#production-caveats) for the list of things you must replace before shipping anything that looks like this.

## Prerequisites

- Rust 1.88+ (workspace toolchain).
- `protoc` installed (`brew install protobuf` on macOS).
- For `GetTs` smoke tests: `grpcurl` (optional but convenient).

## Run a 3-node cluster

The fastest path is the helper script — it starts three node processes in the background and tails their logs into `.data/n*.log`:

    scripts/run.sh

Alternatively start each node by hand in its own terminal. Node 1 carries `--bootstrap`:

    # node 1
    cargo run -p example-openraft-cluster -- \
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

A follower will respond with a `LeaderHint` trailer pointing at the current leader's tsoracle address (see `--tso-peers`).

## Observe failover

Find the current leader in the logs (`grep "Leader" .data/n*.log`), kill that process, and watch the survivors elect a new leader. Subsequent `GetTs` calls succeed once the new leader has fenced — typically 2–5 seconds.

## File-backed durability

The state machine writes `high_water` to `<raft-dir>/state.json` on every apply via `atomic_write_raw` in `src/store/io.rs`: tmp-file write → rename. The log is one file per entry under `<raft-dir>/log/`, written the same way. A full-cluster restart normally preserves the high-water as long as `<raft-dir>` persists, but this example does not fsync the temporary file or parent directory.

Production deployments should use a hardened KV store rather than this routine. See [Production caveats](#production-caveats).

## Production caveats

This example shows the **minimum** wiring to take `ConsensusDriver` end-to-end with openraft. Several layers are simplified for readability and **must** be replaced before any real deployment:

- **Storage layer.** `src/store/` is a one-file-per-log-entry filesystem store. It uses tmp-file write + rename for simple atomic replacement, but it does not fsync files or parent directories, and the layout (a growing log directory, JSON state, in-memory `BTreeMap` cache, no compaction beyond snapshot+purge) does not scale. Use an adapter over rocksdb / sled / fjall in production; the trait surface (`RaftLogStorage` + `RaftStateMachine`) is the same.

- **Snapshot transport.** `src/network.rs` streams snapshots in 1 MiB chunks over a client-streaming RPC, so frames stay well under gRPC's 4 MiB default. It does *not* implement resume-on-disconnect: a peer that fails mid-install starts the next attempt at chunk 0. For state machines that grow into the hundreds of MiB you'll want a resumable protocol with an explicit offset.

- **Leader-watch debounce.** `src/leader_watch.rs` emits a new `LeaderState` only when the *value* changes (not just the variant). It does not coalesce bursts: if openraft flips Leader → Candidate → Leader within a single metrics tick interval the server will fence, un-fence, and fence again. Production wiring may want to apply a short hold-off (e.g. 50–100 ms) before propagating Unknown / Follower transitions.

- **Membership operations.** This example bootstraps a fixed 3-node cluster via `--bootstrap`. There is no add-learner / promote / remove flow shown. Use openraft's `change_membership` API and gate it behind whatever authentication you require.

- **Authentication & TLS.** Every RPC (raft peer and tsoracle client) is plaintext. Wrap both servers with TLS and require client certs before exposing anything beyond a trusted control plane.

## Design notes

- **State-machine apply** does `max(stored, req.at_least)` unconditionally. Reordered or stale-epoch entries are absorbed monotonically rather than rejected, honoring `ConsensusDriver::persist_high_water`'s contract.
- **Defense-in-depth epoch check** logs a WARN when a stale-epoch entry is applied but never blocks. The state machine cannot regress the high-water by construction.
- **No `Fenced` errors.** openraft refuses non-leader `client_write` at the propose layer, mapped to `ConsensusError::NotLeader`. The trait's `Fenced` variant exists for weaker drivers.
- **Linearizable reads** use `Raft::ensure_linearizable(ReadPolicy::ReadIndex)` (openraft 0.10 read-barrier API). This commits a no-op heartbeat through the log before the read.

## Cleaning up

    rm -rf examples/openraft-cluster/.data/
