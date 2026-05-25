# Operating tsoracle

## Sizing window_ahead

Default is 3 seconds. Each window extension costs one `persist_high_water` round-trip — for the file driver that is `write + fsync + rename + dir-fsync`, roughly 1-5 ms on a modern SSD. At 3-second window-ahead, extension rate is well under 1/sec in steady state. Lower values trade more frequent fsyncs for tighter bounds on stale-window timestamps after a clock skip.

Do not run `window_ahead` below 100ms with the file driver. The fsync rate dominates throughput at that point. If you need tighter window bounds, use a consensus driver with batched log appends instead.

## Sizing failover_advance

Default is 1 second. On leadership gain, the new leader first computes `serving_floor = max(prior_max + 1, now_ms)` and then persists `requested = serving_floor + failover_advance`. The `+1` is mandatory because `prior_max` is an inclusive high-water: the prior leader could have served `(prior_max, LOGICAL_MAX)`. Larger `failover_advance` values give more headroom against clock skew between old and new leaders; smaller values reduce timestamp "jumps" visible to clients. 1 second is appropriate for most deployments; consider 5-10 seconds if your nodes' clocks may differ by more than a second.

## Migrating from a prior timestamp system

`tsoracle serve` against an empty state directory starts at high-water 0. If you are migrating from any prior timestamp source (a previous TSO, snapshot of max-observed commit timestamps in your data, etc.), seed the state file once:

```bash
tsoracle init --seed-physical-ms <MAX_OBSERVED_MILLIS> --state-dir ./tsoracle-data
tsoracle serve --state-dir ./tsoracle-data
```

`init` refuses to overwrite an existing state file, so accidental rollback is prevented. Pick `MAX_OBSERVED_MILLIS` to be the largest `physical_ms` you have ever served from the prior system, plus a safety margin to account for any timestamps you may have issued but not yet checkpointed. The seed must fit the timestamp layout's 46-bit physical field (`<= PHYSICAL_MS_MAX`).

## Monitoring hooks

The server emits the following signals through the [`metrics`](https://docs.rs/metrics) crate facade. Emission is gated behind the `metrics` Cargo feature on `tsoracle-server` (off by default so the dependency stays opt-in for embedders who do not want it):

- `tsoracle.get_ts.requests.total` — total well-formed GetTs RPCs offered, counted at entry before the NOT_LEADER gate; the honest offered load (counter)
- `tsoracle.get_ts.success.total` — GetTs RPCs that returned a grant; `requests.total - success.total` is the failure count (counter)
- `tsoracle.get_ts.timestamps_issued` — sum of `count` across all successful GetTs responses (counter)
- `tsoracle.window.extensions.total` — number of persist_high_water calls (counter)
- `tsoracle.window.extension_latency` — duration of persist_high_water (histogram, seconds)
- `tsoracle.leader_transition.total` — leader-watch saw a state change (counter)
- `tsoracle.leader_transition.fence_latency` — duration of the failover fence (histogram, seconds)
- `tsoracle.leader_transition.fence_transient_retries.total` — fence retried a transient consensus error during failover (counter)
- `tsoracle.not_leader.total` — RPCs rejected with `NOT_LEADER` (counter)

The library is exporter-agnostic: embedders install whichever recorder they want (`metrics-exporter-prometheus`, `metrics-exporter-influx`, a custom sink) before constructing the [`Server`]. The example below wires Prometheus over an HTTP listener:

```toml
[dependencies]
tsoracle-server             = { version = "0.1", features = ["metrics"] }
metrics-exporter-prometheus = "0.16"
```

```rust,ignore
use metrics_exporter_prometheus::PrometheusBuilder;

PrometheusBuilder::new()
    .with_http_listener(([0, 0, 0, 0], 9100))
    .install()
    .expect("install Prometheus recorder");

// Build and serve `tsoracle_server::Server` as usual; emissions now flow
// through the installed recorder.
```

## Client retry behavior

The client gives `FAILED_PRECONDITION` special handling: it parses the `tsoracle-leader-hint-bin` trailer and moves the hinted leader to the front of the current retry worklist. Other gRPC errors, including `UNAVAILABLE` and `INTERNAL`, are recorded and the client continues through the configured endpoints once for that call. Configure `endpoints` with all known servers so cold-start works even when the cached leader is unreachable.

## Advertised endpoints in multi-node deployments

The consensus driver owns the mapping from consensus leader identity to tsoracle endpoint. The source of that mapping is the driver's choice — explicit configuration, consensus membership metadata, service discovery, or anything else. Drivers report the resolved endpoint to the server via `LeaderState::Follower { leader_endpoint, leader_epoch }`; the server forwards it in `LeaderHint` trailers so clients can redirect. The driver also reports the leader's epoch (raft term) via `leader_epoch`; the server forwards it in the `LeaderHint` trailer so a client can reject a stale follower's lower-epoch redirect. The library itself never sees the mapping and exposes no flag for it. Single-node deployments (`tsoracle-driver-file`) have no peers to advertise to.

## Deployment topologies

**Single-node:** one `tsoracle serve` process, `tsoracle-driver-file`. No HA. Good for dev, small services, deployments where TSO availability is not in the critical path.

**HA via your own consensus:** N nodes (typically 3 or 5), each running `tsoracle serve` embedded in a binary that supplies a custom `ConsensusDriver` over your consensus library. Clients configure all N endpoints. Leader handles GetTs; followers redirect.

**Sharded TSO domains:** for systems wanting separate monotonic sequences per keyspace, run one tsoracle cluster per shard. The library has no opinion on sharding.
