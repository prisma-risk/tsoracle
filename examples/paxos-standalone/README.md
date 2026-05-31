# Three-node tsoracle cluster on OmniPaxos (standalone)

Multi-process tsoracle cluster backed by [OmniPaxos](https://omnipaxos.com), wired through [`tsoracle-standalone`](../../crates/tsoracle-standalone/) and [`tsoracle-driver-paxos`](../../crates/tsoracle-driver-paxos/). The standalone crate owns the OmniPaxos storage, peer transport, driver construction, and apply pipeline; this example is the thin operator-facing wrapper that parses CLI flags, starts the tsoracle gRPC server, and wires graceful shutdown.

If your service already runs OmniPaxos for other state and you want TSO to share it, see the [`paxos-piggyback`](../paxos-piggyback/) example instead.

> **Do not copy this example to production unchanged.** It is *correct enough* to teach the integration boundary, but several layers are deliberately simplified. See [Production caveats](#production-caveats).

> **⚠️ Warning — no authentication or encryption.** Every RPC here (the paxos peer transport *and* the tsoracle client API) is **unauthenticated plaintext**. The peer server feeds any deserialize-valid message straight into `OmniPaxos::handle_incoming` with no peer-identity or membership check, so a reachable peer port lets anyone disrupt elections, advance ballots, or get values decided. The `--listen`/`--tso-listen` defaults bind to **loopback only** (`127.0.0.1:0`); exposing the ports off-loopback requires an explicit override, and you must add TLS with client-cert auth first. See [Authentication & TLS](#production-caveats).

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

    grpcurl -v -plaintext -d '{"count":1}' 127.0.0.1:50581 tsoracle.v1.TsoService/GetTs

A follower will respond with a `LeaderHint` trailer pointing at the current leader's tsoracle address (see `--tso-peers`). That trailer comes from the standalone Paxos driver's leadership stream, which resolves the elected leader through the TSO peer map.

## Observe failover

Find the current leader in the logs (`grep "Leader" .data/n*.log`), kill that process, watch the survivors elect a new leader. Subsequent `GetTs` calls succeed once the new leader has fenced — typically 1–3 seconds with the default OmniPaxos timeouts.

## What's in this example

- `src/main.rs` — CLI parse, `DriverConfig::Paxos`, `tsoracle_standalone::build`, `Server::builder()`, and SIGINT/SIGTERM-aware shutdown via `tsoracle_server::shutdown_signal()`.
- `scripts/run.sh` — 3-node bring-up with a fresh `.data/` directory and reflection enabled so the `grpcurl` quickstart works.
- The OmniPaxos peer transport, RocksDB storage, driver construction, and apply pipeline live in [`tsoracle-standalone`](../../crates/tsoracle-standalone/) and [`tsoracle-driver-paxos`](../../crates/tsoracle-driver-paxos/).

## Production caveats

This example shows the **minimum** wiring to take `ConsensusDriver` end-to-end with OmniPaxos. Several layers are simplified for readability and **must** be replaced before any real deployment:

- **Snapshot transport.** OmniPaxos's snapshot install path is in-band: snapshots ride as a regular `Message` over the peer RPC. Because `HighWaterSnapshot` is a single `u64`, it always fits inside a default-sized gRPC frame, so no chunking is needed. State machines that grow into the hundreds of MiB would need either a larger `max_decoding_message_size` setting on both ends or a dedicated streaming RPC.

- **Reconfiguration.** This example pins a fixed 3-node `ClusterConfig`. OmniPaxos supports reconfiguration via `StopSign`, but the membership change protocol (including the upgrade to a fresh `configuration_id`) is out of scope here.

- **Authentication & TLS.** Every RPC (paxos peer and tsoracle client) is plaintext. Wrap both servers with TLS and require client certs before exposing anything beyond a trusted control plane.

- **Graceful shutdown of the apply task.** Ctrl-C drains in-flight tsoracle RPCs, then drops the `PaxosDriver` (and the wrapped `StandaloneHost`). The runner tick task observes the runner's shutdown one-shot via its `Drop` impl and exits cleanly, but the apply task is parked on a `tokio::sync::Notify` whose source has gone away — the tokio runtime tears it down when the process exits. Production wiring should keep a separate handle on the host so `host.stop().await` can be awaited before the runtime shuts down.

## Cleaning up

    rm -rf examples/paxos-standalone/.data/
