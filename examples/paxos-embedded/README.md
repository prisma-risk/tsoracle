# Embedded tsoracle/paxos cluster

Embed an OmniPaxos consensus driver alongside the tsoracle server in your own binary. Counterpart of [`embedded-server`](../embedded-server/) (which uses `FileDriver` for the single-node case) — this version uses [`tsoracle-driver-paxos`](../../crates/tsoracle-driver-paxos/) for HA.

> **About "single-node":** OmniPaxos has no single-node mode. Leader election (BLE) requires a majority of the configured nodes to be alive, so a 3-node `ClusterConfig` with two unstarted peers stays inert — no leader is ever elected, no proposals decide, the fence never fires. This example therefore runs **all three paxos nodes inside one process**, wired together via the toolkit's `MemNetwork`. If you need a single-node deployment, use `FileDriver` (see `embedded-server`).

## Run

```bash
cargo run -p example-paxos-embedded
```

The binary starts a 3-node OmniPaxos cluster in-process and binds three tsoracle gRPC endpoints:

- `http://127.0.0.1:50591` — node 1
- `http://127.0.0.1:50592` — node 2
- `http://127.0.0.1:50593` — node 3

Ctrl-C drains in-flight RPCs and exits.

Talk to it from another terminal with `grpcurl` against any node — followers respond with a `LeaderHint` trailer pointing at the leader's address:

```bash
grpcurl -plaintext -d '{"count":1}' 127.0.0.1:50591 tsoracle.v1.TsoService/GetTs
```

Or from Rust using the `tsoracle-client` crate; pass all three endpoints so the client honors `LeaderHint` redirects.

## What to look at in `src/main.rs`

- The cluster bring-up is a loop that builds one `StandaloneHost` per node, registers an inbox channel with `MemNetwork`, spawns the inbox pump, calls `host.start(sink)` with a `MeshSink` that delivers messages back through the same network, and constructs a `PaxosDriver` + `TsoServer` on top.
- All three nodes share the same `MemNetwork`, so outbound messages from one node arrive at another's inbox without any tonic / OS networking. Peer messages are not serialized; the network passes `Message<HighWaterCommand>` values directly.
- Each node's `StandaloneHost::builder().peers(...)` takes the OTHER nodes' tsoracle endpoints (not their paxos peer addresses, since `MemNetwork` is the paxos transport). Those tsoracle endpoints become `LeaderState::Follower::leader_endpoint` when leadership lands on a peer.

## When this example is *not* the right shape

- **You want a single-node deployment.** Use `FileDriver` (`embedded-server`). OmniPaxos cannot operate single-node.
- **You want three real processes.** Use `paxos-standalone` instead — it ports the same wiring to tonic.
- **You want TSO to share an existing OmniPaxos log.** Use `paxos-piggyback`.

## Production caveats

- **`MemNetwork`.** Replace with your real OmniPaxos peer transport (typically tonic; see `paxos-standalone/src/network.rs` for a worked version) before exposing this to anything beyond a single host.
- **`MemStorage`.** The cluster's paxos log is in-memory only — every restart loses state. Swap to the toolkit's `RocksdbStorage` for durable storage.
- **No graceful host shutdown.** Ctrl-C drains tsoracle servers but leaves the `StandaloneHost` runner + apply tasks to be torn down by the runtime. Production code should keep handles to call `host.stop().await` before returning from `main`.
