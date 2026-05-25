# Prometheus metrics from an embedded tsoracle

How to use the [`metrics`](https://docs.rs/metrics) feature on [`tsoracle-server`](https://docs.rs/tsoracle-server) from your own binary: install [`metrics-exporter-prometheus`](https://docs.rs/metrics-exporter-prometheus) before the server starts, then expose `/metrics` on a separate HTTP port for Prometheus to scrape. A background loop calls `GetTs` continuously so a fresh scrape immediately shows non-zero counters.

If you just want metrics from the stock `tsoracle serve` binary, you don't need this example — the bin already installs a Prometheus exporter on `127.0.0.1:9551` by default. See [Operations → Monitoring hooks](../../docs/operations.md#monitoring-hooks). This example is for when you are *embedding* `tsoracle_server::Server` in your own binary and want to control the recorder yourself.

## Run

```bash
cargo run -p example-metrics-prometheus
```

The example binds the gRPC server on `127.0.0.1:50552` and the Prometheus scrape endpoint on `127.0.0.1:9552` (one above the stock bin's default `9551`, so the two can run side-by-side). Ctrl-C drains in-flight RPCs and exits cleanly.

Scrape it from another terminal:

```bash
curl -s http://127.0.0.1:9552/metrics | grep ^tsoracle_
```

After a few seconds the scrape body looks roughly like this (exact values move with load):

```
# TYPE tsoracle_get_ts_requests_total counter
tsoracle_get_ts_requests_total 49
# TYPE tsoracle_get_ts_success_total counter
tsoracle_get_ts_success_total 47
# TYPE tsoracle_get_ts_timestamps_issued counter
tsoracle_get_ts_timestamps_issued 235
# TYPE tsoracle_leader_transition_total counter
tsoracle_leader_transition_total 1
# TYPE tsoracle_window_extensions_total counter
tsoracle_window_extensions_total 1
# TYPE tsoracle_window_extension_latency summary
tsoracle_window_extension_latency{quantile="0.5"} 0.00031
tsoracle_window_extension_latency_sum 0.00031
tsoracle_window_extension_latency_count 1
```

The `metrics` crate translates `.` to `_` in the exposition output, so the documented `tsoracle.get_ts.requests.total` is scraped as `tsoracle_get_ts_requests_total`. Counters whose names end in `.total` keep that suffix (Prometheus tooling expects it). The gap between `requests_total` (offered load) and `success_total` (grants returned) is the failed-request count. The full signal catalog lives in [`docs/operations.md`](../../docs/operations.md#monitoring-hooks).

## What to look at in `src/main.rs`

- **`PrometheusBuilder::new().with_http_listener(...).install()` runs before anything else.** The `metrics` crate caches the global recorder per call site on first emission, so any tsoracle code path that emits before `install()` pins that call site to the noop recorder for the rest of the process — it will be silently absent from scrapes forever. Install the exporter first, then build `FileDriver` and `Server`.
- **`drive_load` is just there to make the demo visible.** It loops `get_ts_batch(5)` every 100 ms, retrying the initial `Client::connect` until the server is accepting. In a real embedding you would not synthesize load — your real workload is the load.
- **Shutdown is one `broadcast::channel`.** Ctrl-C fires the channel; the `serve_with_shutdown` future returns (draining the server) and the load loop's `select!` arm wakes and exits. Both stop cleanly before `main` returns.
- **Swapping exporters is a one-line change.** Replace the `metrics-exporter-prometheus` block with whichever recorder you want (`metrics-exporter-influx`, `metrics-exporter-statsd`, a custom `metrics::Recorder` impl). The `tsoracle-server` `metrics` feature only emits — it does not pick the sink.

## When this example is *not* the right shape

- **You are using the stock `tsoracle serve` binary.** It already installs a Prometheus exporter on `127.0.0.1:9551` and exposes `--metrics-listen` / `--no-metrics` flags. Just point your scraper at it.
- **You want HA.** This example uses [`FileDriver`](https://docs.rs/tsoracle-driver-file), which is single-node by design. The metrics wiring is independent of the driver — swap in a `ConsensusDriver` (see [`examples/openraft-standalone`](../openraft-standalone/) or [`examples/openraft-piggyback`](../openraft-piggyback/)) and keep the recorder install exactly as written here.
- **You want to expose metrics on the same listener as your gRPC traffic.** `PrometheusBuilder::with_http_listener` opens its own port. If you need a single port, use `PrometheusBuilder::build_recorder()` and serve the exposition payload from your own HTTP stack (the recorder's `handle()` returns a renderer).
