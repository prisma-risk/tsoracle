# Operations

How to run tsoracle in production — the parameters worth tuning (`window_ahead`, `failover_advance`), monitoring, deployment topologies, and the client's retry behavior. Each section is a focused reference; read in any order.

## Sizing window_ahead

Default is 3 seconds. Each window extension costs one `persist_high_water` round-trip — for the file driver that is `write + fsync + rename + dir-fsync`, roughly 1–5 ms on a modern SSD. At 3-second window-ahead, extension rate is well under 1/sec in steady state. Lower values trade more frequent fsyncs for tighter bounds on stale-window timestamps after a clock skip.

Do not run `window_ahead` below 100 ms with the file driver. The fsync rate dominates throughput at that point. If you need tighter window bounds, use a consensus driver with batched log appends instead.

## Sizing failover_advance

Default is 1 second. On leadership gain, the new leader first computes `serving_floor = max(prior_max + 1, now_ms)` and then persists `requested = serving_floor + failover_advance`. The `+1` is mandatory because `prior_max` is an inclusive high-water: the prior leader could have served `(prior_max, LOGICAL_MAX)`. Larger `failover_advance` values give more headroom against clock skew between old and new leaders; smaller values reduce timestamp "jumps" visible to clients. 1 second is appropriate for most deployments; consider 5–10 seconds if your nodes' clocks may differ by more than a second.

## Monitoring hooks

`tsoracle-server`, `tsoracle-client`, and the OmniPaxos backend (`tsoracle-driver-paxos` / `tsoracle-paxos-toolkit`) emit signals through the [`metrics`](https://docs.rs/metrics) crate facade. Emission is gated behind the `metrics` Cargo feature on each crate (off by default so the dependency stays opt-in for embedders who do not want it); enabling the feature on `tsoracle-driver-paxos` also turns it on for the toolkit it depends on. The client additionally emits structured events through the [`tracing`](https://docs.rs/tracing) crate; `tracing` is on by default for `tsoracle-client` (matching `tsoracle-server`).

**Server signals**

- `tsoracle.get_ts.total` — total GetTs RPCs handled (counter)
- `tsoracle.get_ts.timestamps_issued` — sum of `count` across all GetTs responses (counter)
- `tsoracle.window.extensions.total` — number of persist_high_water calls (counter)
- `tsoracle.window.extension_latency` — duration of persist_high_water (histogram, seconds)
- `tsoracle.leader_transition.total` — leader-watch saw a state change (counter)
- `tsoracle.leader_transition.fence_latency` — duration of the failover fence (histogram, seconds)
- `tsoracle.leader_transition.fence_transient_retries.total` — fence retried a transient consensus error during failover (counter)
- `tsoracle.not_leader.total` — RPCs rejected with `NOT_LEADER` (counter)

**Paxos consensus signals** (emitted by `tsoracle-driver-paxos` / `tsoracle-paxos-toolkit` when their `metrics` feature is enabled)

- `tsoracle.paxos.snapshot.total` — snapshot attempts triggered by the snapshot policy after a successful apply (counter)
- `tsoracle.paxos.snapshot.failures.total` — snapshot attempts the OmniPaxos handle rejected (counter). Snapshot failure is non-fatal for liveness, so the success rate is `total - failures`; a sustained non-zero rate points at a degrading snapshot/compaction path.
- `tsoracle.paxos.snapshot.last_index` — decided index of the last snapshot that succeeded (gauge). This should keep advancing under load; a stall while `snapshot.total` keeps climbing means snapshots are firing but failing.
- `tsoracle.paxos.storage.async_write_failures.total{op}` — non-synced RocksDB storage writes that failed (counter). `op` is one of `set_decided_idx`, `set_compacted_idx`, `trim`, `set_snapshot`, `set_stopsign`. These writes are recoverable after a crash, so a failure is not immediately fatal, but a recurring rate signals disk pressure or storage faults that would otherwise stay hidden in logs.

**Client signals**

- `tsoracle.client.not_leader.total` — `FAILED_PRECONDITION` responses observed by the client (counter)
- `tsoracle.client.leader_pivots.total` — times the retry loop accepted a leader-hint redirect and immediately retried the hinted endpoint (counter)
- `tsoracle.client.retries.total{reason}` — endpoints the retry loop advanced past without success (counter). `reason` is one of `connect_failure`, `not_leader`, `transport`, `decode_error`, `deadline_exceeded`.
- `tsoracle.client.leader_hint.decode_failures.total` — `FAILED_PRECONDITION` responses whose `tsoracle-leader-hint-bin` trailer was present but failed to decode as `LeaderHint` (counter). A non-zero rate indicates a misbehaving peer; absent trailers do not increment this.
- `tsoracle.client.connect.duration` — wall-clock duration of a fresh `Channel` dial on cache miss (histogram, seconds). Cache hits are not sampled.
- `tsoracle.client.connect.failures.total` — connect attempts that returned an error (counter)
- `tsoracle.client.driver.queue_depth` — waiter-queue size inside the coalescing driver task (gauge). Updated after every enqueue and after each dispatch drain.
- `tsoracle.client.driver.in_flight` — 0 or 1 indicating whether the driver currently has an outgoing batch in flight (gauge)

Both libraries are exporter-agnostic: embedders install whichever recorder they want (`metrics-exporter-prometheus`, `metrics-exporter-influx`, a custom sink) before constructing the [`Server`] or [`Client`]. The example below wires Prometheus over an HTTP listener for a process that hosts either side:

```toml
[dependencies]
tsoracle-server             = { version = "0.1", features = ["metrics"] }
tsoracle-client             = { version = "0.1", features = ["metrics"] }
metrics-exporter-prometheus = "0.16"
```

```rust,ignore
use metrics_exporter_prometheus::PrometheusBuilder;

PrometheusBuilder::new()
    .with_http_listener(([127, 0, 0, 1], 9100))
    .install()
    .expect("install Prometheus recorder");

// Build and serve `tsoracle_server::Server` as usual; emissions now flow
// through the installed recorder.
```

The exporter has no built-in authentication and the `/metrics` body discloses operational signals an attacker can use to fingerprint leader transitions, window-extension cadence, and load. Bind to loopback for same-host scrapers (Prometheus agent / node_exporter sidecar); when the scraper lives on a different host, prefer a unix-socket or use a non-loopback bind only behind trusted network controls (private subnet, firewall, mTLS at the scrape layer).

## Advertised endpoints in multi-node deployments

The consensus driver owns the mapping from consensus leader identity to tsoracle endpoint. The source of that mapping is the driver's choice — explicit configuration, consensus membership metadata, service discovery, or anything else. Drivers report the resolved endpoint to the server via `LeaderState::Follower { leader_endpoint }`; the server forwards it in `LeaderHint` trailers so clients can redirect. The library itself never sees the mapping and exposes no flag for it. Single-node deployments (`tsoracle-driver-file`) have no peers to advertise to.

## Deployment topologies

**Single-node:** one `tsoracle serve` process, `tsoracle-driver-file`. No HA. Good for dev, small services, deployments where TSO availability is not in the critical path.

**HA via your own consensus:** N nodes (typically 3 or 5), each running `tsoracle serve` embedded in a binary that supplies a custom `ConsensusDriver` over your consensus library. Clients configure all N endpoints. Leader handles `GetTs`; followers redirect.

**Sharded TSO domains:** for systems wanting separate monotonic sequences per keyspace, run one tsoracle cluster per shard. The library has no opinion on sharding.

## Client retry behavior

The client gives `FAILED_PRECONDITION` special handling: it parses the `tsoracle-leader-hint-bin` trailer (see [The leader-hint trailer](key-subsystems.md#the-leader-hint-trailer)) and moves the hinted leader to the front of the current retry worklist. Other gRPC errors, including `UNAVAILABLE` and `INTERNAL`, are recorded and the client continues through the configured endpoints once for that call. Configure `endpoints` with all known servers so cold-start works even when the cached leader is unreachable. Under `ClientBuilder::tls_config`, explicit `http://` leader-hint endpoints are dropped to prevent a contacted peer from downgrading the transport — emit hints with bare-host `host:port` (which the client rewrites to `https://` under TLS) or explicit `https://` only.

## TLS termination

`tsoracle-server` terminates TLS via `ServerBuilder::tls_config(tonic::transport::ServerTlsConfig)`. The configuration is applied inside `Server::serve`, `Server::serve_with_shutdown`, and `Server::serve_with_listener` — all three paths pass through tonic's server builder, and the TLS config is attached before `.add_routes(routes)`. The default feature `tls-rustls` (using `tonic/tls-aws-lc`) is on by default; opt into `tls-native` instead if you prefer the platform root store.

`Server::into_router` does **not** take a TLS config. It returns a `Routes` value for embedders mounting tsoracle alongside their own services on a shared tonic server; in that case the embedder configures TLS on their own `TonicServer::builder()` and we stay out of the way.

Cert and key shapes follow tonic: `ServerTlsConfig::new().identity(Identity::from_pem(cert, key))` for plain TLS; add `.client_ca_root(Certificate::from_pem(ca))` for mTLS.

The stock `tsoracle` CLI does not currently expose TLS flags. If you need TLS today, embed the library — see [`examples/tls-mtls`](../examples/tls-mtls/) for a runnable starting point.
