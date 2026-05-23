# Three-node tsoracle cluster on OmniPaxos (standalone)

Multi-process tsoracle cluster backed by [OmniPaxos](https://omnipaxos.com), wired together via [`tsoracle-driver-paxos`](../../crates/tsoracle-driver-paxos/). The driver crate provides the `ConsensusDriver` impl, the `HighWaterCommand` log entry, the apply-task state machine, the `Epoch ↔ Ballot` encoding, and `StandaloneHost` — a host that owns its own OmniPaxos cluster + apply pipeline. This example supplies the rest: a tonic peer transport (`src/network.rs`), the OmniPaxos `ClusterConfig` glue (`src/main.rs`), and CLI plumbing.

If your service already runs OmniPaxos for other state and you want TSO to share it, see the [`paxos-piggyback`](../paxos-piggyback/) example instead.

> **Do not copy this example to production unchanged.** It is *correct enough* to teach the integration boundary, but several layers are deliberately simplified. See [Production caveats](#production-caveats).

## Prerequisites

- Rust 1.88+ (workspace toolchain).
- `protoc` installed (`brew install protobuf` on macOS).
- For `GetTs` smoke tests: `grpcurl` (optional but convenient).

## Run a 3-node cluster

Fastest path is the helper script — it starts three node processes in the background and tails their logs into `.data/n*.log`:

    scripts/run.sh

Alternatively start each node by hand in its own terminal:

    # node 1
    cargo run -p example-paxos-standalone -- \
      --node-id 1 \
      --listen 127.0.0.1:53001 --tso-listen 127.0.0.1:50581 \
      --peers     "1=127.0.0.1:53001,2=127.0.0.1:53002,3=127.0.0.1:53003" \
      --tso-peers "1=127.0.0.1:50581,2=127.0.0.1:50582,3=127.0.0.1:50583" \
      --data-dir ./.data/n1

    # node 2 (same args, --node-id 2, ports …002/…582, dir n2)
    # node 3 (--node-id 3, ports …003/…583, dir n3)

Unlike the openraft variant, OmniPaxos does **not** need a one-time `--bootstrap` flag. Cluster membership is carried in the `ClusterConfig` that every node builds from `--peers`, and leader election runs as soon as quorum is reachable.

## Issue a timestamp

Against any node:

    grpcurl -plaintext -d '{"count":1}' 127.0.0.1:50581 tsoracle.v1.TsoService/GetTs

A follower will respond with a `LeaderHint` trailer pointing at the current leader's tsoracle address (see `--tso-peers`). That trailer comes from `PaxosDriver::leadership_events`, which delegates the per-state mapping to the toolkit's `LeadershipState::from_omnipaxos` over the `Peer` list passed to `StandaloneHost::builder().peers(...)`.

## Observe failover

Find the current leader in the logs (`grep "Leader" .data/n*.log`), kill that process, watch the survivors elect a new leader. Subsequent `GetTs` calls succeed once the new leader has fenced — typically 1–3 seconds with the default OmniPaxos timeouts.

## What's in this example

- `src/main.rs` — CLI parse, RocksDB open with one column family for the paxos log, `OmniPaxos::build`, the three-binding driver wiring: `StandaloneHost::builder` → `host.start(sink)` → `PaxosDriver::new`. About 110 lines including config and storage.
- `src/network.rs` — tonic peer transport. A single fire-and-forget unary RPC (`Send(PaxosMessage) → Ack`) carries postcard-encoded `omnipaxos::messages::Message<HighWaterCommand>` payloads. The `PeerSink` implements `MessageSink<HighWaterCommand>` (the contract `StandaloneHost::start` consumes); the `PaxosPeerService` server feeds inbound payloads to `OmniPaxos::handle_incoming`.
- `proto/paxos.proto`, `build.rs` — peer-RPC service definition + tonic codegen.
- `scripts/run.sh` — 3-node bring-up.

## Production caveats

This example shows the **minimum** wiring to take `ConsensusDriver` end-to-end with OmniPaxos. Several layers are simplified for readability and **must** be replaced before any real deployment:

- **Snapshot transport.** OmniPaxos's snapshot install path is in-band: snapshots ride as a regular `Message` over the peer RPC. Because `HighWaterSnapshot` is a single `u64`, it always fits inside a default-sized gRPC frame, so no chunking is needed. State machines that grow into the hundreds of MiB would need either a larger `max_decoding_message_size` setting on both ends or a dedicated streaming RPC.

- **Reconfiguration.** This example pins a fixed 3-node `ClusterConfig`. OmniPaxos supports reconfiguration via `StopSign`, but the membership change protocol (including the upgrade to a fresh `configuration_id`) is out of scope here.

- **Authentication & TLS.** Every RPC (paxos peer and tsoracle client) is plaintext. Wrap both servers with TLS and require client certs before exposing anything beyond a trusted control plane.

- **Graceful shutdown of the apply task.** Ctrl-C drains in-flight tsoracle RPCs, then drops the `PaxosDriver` (and the wrapped `StandaloneHost`). The runner tick task observes the runner's shutdown one-shot via its `Drop` impl and exits cleanly, but the apply task is parked on a `tokio::sync::Notify` whose source has gone away — the tokio runtime tears it down when the process exits. Production wiring should keep a separate handle on the host so `host.stop().await` can be awaited before the runtime shuts down.

## Cleaning up

    rm -rf examples/paxos-standalone/.data/
