# Client API and Usage

The `tsoracle-client` crate end-to-end: constructing a `Client`, calling `get_ts` and `get_ts_batch`, how the client handles leader changes and `NOT_LEADER` rejections without surfacing them to your code, and the knobs you can turn.

For a minimum-overhead getting-started example, see [Calling tsoracle from Rust](getting-started.md#calling-tsoracle-from-rust). This chapter is the reference.

## The Client type

`tsoracle_client::Client` is the public type. It is constructed via the convenience constructor `Client::connect(endpoints)` or the builder `ClientBuilder::endpoints(endpoints).build()`. Both spawn a background coalescing task internally; the resulting `Client` is meant to be shared across your application (typically `Arc<Client>`) — there is no benefit to instantiating multiple clients per process.

Connections are managed by an internal channel pool (`crates/tsoracle-client/src/leader_resolved.rs`): one `tonic::transport::Channel` is built per endpoint on first use and cached. The pool tracks which endpoint last accepted a request as the leader, so steady-state RPCs skip the discovery dance. Code for the coalescing background task is in `crates/tsoracle-client/src/driver.rs`.

The client's lifecycle is bound to its task: dropping the `Client` drops the channel that drives the coalescing task and the task exits. There is no explicit `close` method.

## GetTs and GetTsBatch

There is **one wire RPC** — `GetTs { count }` — and two client-side methods that wrap it:

```rust
pub async fn get_ts(&self) -> Result<Timestamp, ClientError>;
pub async fn get_ts_batch(&self, count: u32) -> Result<Vec<Timestamp>, ClientError>;
```

`get_ts` calls `GetTs { count: 1 }`; `get_ts_batch(N)` calls `GetTs { count: N }`. The server responds with a single `GetTsResponse { physical_ms, logical_start, count, epoch_hi, epoch_lo }` describing a contiguous range of timestamps (the 128-bit leader epoch is carried as two 64-bit halves, `epoch_hi`/`epoch_lo`, which the client reassembles); the client validates the response fields and expands that range into `N` `Timestamp` values locally. A batch of 1000 is one RPC, one persist (if a window extension is triggered on the server), and 1000 local pack operations — never 1000 RPCs.

`get_ts_batch(0)` and `get_ts_batch(N)` where `N > LOGICAL_MAX + 1` are rejected as `ClientError::InvalidCount(N)` before any RPC is issued. The maximum explicit batch size is bounded by the per-millisecond logical capacity ([Timestamp packing](architecture-deep-dive.md#timestamp-packing)). Concurrent waiters that coalesce above that cap are split into multiple outgoing RPC chunks, each within the server's per-call limit.

Use `get_ts` for one-off cases and `get_ts_batch` whenever you can amortize. The coalescing layer makes single-call sites efficient even without explicit batching, but explicit batching still beats coalescing because it skips one client-side wait per batch.

## Leader discovery and retries

The client never asks "who's the leader" before issuing an RPC. It picks an endpoint from its pool, sends `GetTs`, and reacts to the response:

- **`Ok(response)`** — the endpoint is the leader. Cache it.
- **`Err(FAILED_PRECONDITION)` with a `tsoracle-leader-hint-bin` trailer pointing at an unvisited endpoint** — move the hinted endpoint to the front of the retry worklist and try it next.
- **`Err(FAILED_PRECONDITION)` without a usable hint** — clear the cached leader, fall back to the next endpoint in the worklist (round-robin across configured endpoints).
- **Any other error** — try the next endpoint in the worklist.

The implementation is in `crates/tsoracle-client/src/retry.rs::issue_rpc`. The worklist starts with the cached leader (if any) followed by the configured endpoints in round-robin order; each endpoint is tried at most once per RPC. If a `FAILED_PRECONDITION` response carries a usable leader hint, that endpoint is moved to the front of the current worklist. Other gRPC statuses are recorded and the client continues through the remaining endpoints. If the worklist is exhausted without success, the last error is returned (or `ClientError::NoReachableEndpoints` if nothing was tried).

The trailer's wire format is described in [The leader-hint trailer](key-subsystems.md#the-leader-hint-trailer). Strict-consistency callers can rely on the [freshness invariant](getting-started.md#the-freshness-invariant): even across a leader transition mid-call, no timestamp returned to the caller predates the call's entry into the client driver.

`ClientError` variants:

- `NoReachableEndpoints` — every configured endpoint failed to connect or returned an error.
- `Transport(_)` — tonic transport error wrapping the last attempt's failure.
- `Rpc(Status)` — the last attempt returned a tonic `Status` we couldn't recover from.
- `InvalidEndpoint(String)` — an endpoint string failed to parse as a URI.
- `InvalidCount(u32)` — `count == 0` or `count > LOGICAL_MAX + 1` was passed to `get_ts_batch`.
- `Connector(_)` — a user-supplied `channel_connector` closure returned an error. Built-in TLS failures continue to surface as `Transport(_)`.

## Configuration

The builder exposes three knobs:

```rust
ClientBuilder::endpoints(vec![
    "http://host1:50551".into(),
    "http://host2:50551".into(),
])
    .batch_flush_interval(Duration::from_millis(1))
    .retry_policy(tsoracle_client::RetryPolicy::default())
    .build()
    .await?;
```

**`endpoints`** is the list of candidate server addresses, tried in worklist order on first use. Order matters as a hint to the discovery algorithm — put the most-likely-leader first if you have one.

**`batch_flush_interval`** is the *cold-start* coalescing window — the time the background driver waits, after the first buffered call arrives into an idle driver, before issuing the outgoing `GetTs` (default: 1 ms). It does *not* set the steady-state batch size: once any RPC is in flight, every waiter arriving during its round-trip is automatically coalesced into the next batch regardless of this knob, so steady-state batch size is set by `arrival_rate × rpc_round_trip` instead. Lowering `batch_flush_interval` (down to `Duration::ZERO`) reduces the per-call latency floor for cold-start callers but loses the first-burst coalescing window; raising it widens that window at the cost of a fixed latency tax on every first-after-idle request. For workloads that already batch explicitly, or that sustain enough concurrency to keep at least one RPC in flight at all times, the value is largely irrelevant. The full discussion — including why steady-state low batch size is a caller-concurrency problem and not a flushing problem — is in [The Client Driver](the-client-driver.md).

**`retry_policy`** bounds the per-attempt and overall wall-clock cost of one `get_ts`/`get_ts_batch` call, and governs the backoff applied between retries. The struct is `RetryPolicy { max_attempts, per_attempt_deadline, overall_deadline, base_backoff, leader_ttl }`; defaults are `5`, `2 s`, `10 s`, `50 ms`, and `30 s` respectively. `per_attempt_deadline` is pushed down to `tonic::transport::Endpoint::connect_timeout` and `Endpoint::timeout` for the built-in default and TLS transport paths (along with `keep_alive_while_idle(true)` and a 30-second HTTP/2 keepalive interval), so a blackholed peer fails fast at the transport layer instead of parking on the OS-default TCP timeout. The retry loop also wraps each `(connect, get_ts)` pair in `tokio::time::timeout(per_attempt_deadline, ...)`, which is the only deadline that applies when a user-supplied [`channel_connector`](#custom-transport-tls-mtls-connectors) replaces the built-in path. `overall_deadline` short-circuits the loop across the full worklist; `max_attempts` caps iteration when leader-hint redirects expand the worklist beyond the configured list. Between attempts whose last error was `Unavailable`, `DeadlineExceeded`, or a transport-layer failure, the loop sleeps a jittered exponential backoff (full-jitter in `[0, base_backoff × 2^attempt]`, capped internally at 5 seconds). FAILED_PRECONDITION-with-hint redirects do not back off — the next endpoint is known and the redirect is part of normal discovery — and a hint whose leader epoch is strictly less than the cached leader's epoch is dropped (counted, traced) so a delayed NOT_LEADER from an old epoch cannot flap the cache backward. `leader_ttl` caps how long the cached leader endpoint may be retained without a successful RPC against it: the cache is touched on every successful RPC against it, so a steady-state leader is retained indefinitely, but once the leader falls quiet past the TTL the next RPC re-evaluates the configured endpoint list rather than pinning to a possibly-dead endpoint that has not been re-validated within the window. Callers wanting a per-call cap tighter than the policy can still wrap their `get_ts` call in `tokio::time::timeout`; the background coalescing task uses a bounded waiter queue (4096 entries), so callers also see backpressure once it is full.

## Custom transport (TLS, mTLS, connectors)

The default `Client` dials each endpoint over plaintext HTTP/2. Two builder methods change that. `.tls_config(ClientTlsConfig)` (feature `tls-rustls` or `tls-native`; the former is on by default) attaches a `tonic::transport::ClientTlsConfig` to every endpoint the client opens, including leader-hint redirects to endpoints that were not in the configured list. `.channel_connector(|endpoint| async { ... })` is the generic escape hatch: the caller's closure builds and returns a `tonic::transport::Channel` for any endpoint string the client needs to dial. Use `tls_config` when a `ClientTlsConfig` is enough; reach for `channel_connector` when you also need to configure keepalive, custom connectors, service-mesh interposers, or proxies.

Setting both `.tls_config(...)` and `.channel_connector(...)` is allowed; the last call wins (standard builder semantics).

### Scheme rule

The matrix below applies to *operator-supplied* endpoint strings — the entries passed to `ClientBuilder::endpoints`. The wire-input case (leader-hint trailers) is covered immediately after.

| Configured state | Bare `host:port` becomes | `http://...` | `https://...` |
| --- | --- | --- | --- |
| No transport config (default) | `http://host:port` | plaintext | TLS (requires a `tls-*` feature to actually connect) |
| `.tls_config(cfg)` set | `https://host:port` | plaintext (explicit beats configured) | TLS using `cfg` |
| `.channel_connector(closure)` set | passed verbatim to the closure | passed verbatim | passed verbatim |

"Explicit beats configured" applies to operator-supplied entries above: passing `http://host:port` to `endpoints` deliberately dials that one endpoint as plaintext even when `tls_config` is otherwise set, which is useful for mixed deployments (e.g., a loopback sidecar over plaintext alongside TLS-terminated peers).

### Leader-hint trailers (wire input)

`FAILED_PRECONDITION` responses can carry a `tsoracle-leader-hint-bin` trailer with a `leader_endpoint` string the client uses to redirect. That string is wire input from a contacted peer, not operator configuration, and the rule is tighter:

| Configured state | Bare `host:port` hint | `http://...` hint | `https://...` hint |
| --- | --- | --- | --- |
| No transport config (default) | rewritten to `http://host:port` and dialed | dialed as plaintext | dialed as TLS (requires a `tls-*` feature) |
| `.tls_config(cfg)` set | rewritten to `https://host:port` and dialed with `cfg` | **dropped** — logged at `warn` level (with the `tracing` feature); retry continues without redirecting | dialed as TLS with `cfg` |
| `.channel_connector(closure)` set | passed verbatim to the closure | passed verbatim to the closure | passed verbatim to the closure |

Under `tls_config`, an explicit `http://` hint from the wire is refused so a contacted peer cannot downgrade the transport. The `channel_connector` escape hatch owns its own scheme policy — the closure receives every endpoint string the retry loop produces and is responsible for whatever filtering it wants. If a caller wants TLS plus a custom connector with leader-hint-downgrade protection, set `.tls_config(...)` first, then build the connector inside the closure.

### Minimal TLS example

```rust
use tonic::transport::{Certificate, ClientTlsConfig};

let tls = ClientTlsConfig::new()
    .ca_certificate(Certificate::from_pem(&ca_pem))
    .domain_name("oracle.internal");

let client = tsoracle_client::ClientBuilder::endpoints(vec![
    "oracle-1.internal:50551".into(),
    "oracle-2.internal:50551".into(),
])
    .tls_config(tls)
    .build()
    .await?;
```

### Minimal `channel_connector` example

```rust
let tls = ClientTlsConfig::new()
    .ca_certificate(Certificate::from_pem(&ca_pem))
    .identity(Identity::from_pem(&client_cert_pem, &client_key_pem))
    .domain_name("oracle.internal");

let client = tsoracle_client::ClientBuilder::endpoints(endpoints)
    .channel_connector(move |endpoint: &str| {
        let tls = tls.clone();
        let uri = format!("https://{endpoint}");
        async move {
            let ep = tonic::transport::Endpoint::from_shared(uri)?
                .tls_config(tls)?
                .keep_alive_while_idle(true);
            Ok(ep.connect().await?)
        }
    })
    .build()
    .await?;
```

See [`examples/tls-mtls`](../examples/tls-mtls/) for a runnable demo covering plain TLS, mTLS, the connector escape hatch, and a misconfigured mTLS negative path.
