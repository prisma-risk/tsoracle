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

`get_ts` calls `GetTs { count: 1 }`; `get_ts_batch(N)` calls `GetTs { count: N }`. The server responds with a single `GetTsResponse { physical_ms, logical_start, count, epoch }` describing a contiguous range of timestamps; the client validates the response fields and expands that range into `N` `Timestamp` values locally. A batch of 1000 is one RPC, one persist (if a window extension is triggered on the server), and 1000 local pack operations — never 1000 RPCs.

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

## Configuration

The builder exposes two knobs:

```rust
ClientBuilder::endpoints(vec![
    "http://host1:50551".into(),
    "http://host2:50551".into(),
])
    .batch_flush_interval(Duration::from_millis(1))
    .build()
    .await?;
```

**`endpoints`** is the list of candidate server addresses, tried in worklist order on first use. Order matters as a hint to the discovery algorithm — put the most-likely-leader first if you have one.

**`batch_flush_interval`** is the *cold-start* coalescing window — the time the background driver waits, after the first buffered call arrives into an idle driver, before issuing the outgoing `GetTs` (default: 1 ms). It does *not* set the steady-state batch size: once any RPC is in flight, every waiter arriving during its round-trip is automatically coalesced into the next batch regardless of this knob, so steady-state batch size is set by `arrival_rate × rpc_round_trip` instead. Lowering `batch_flush_interval` (down to `Duration::ZERO`) reduces the per-call latency floor for cold-start callers but loses the first-burst coalescing window; raising it widens that window at the cost of a fixed latency tax on every first-after-idle request. For workloads that already batch explicitly, or that sustain enough concurrency to keep at least one RPC in flight at all times, the value is largely irrelevant. The full discussion — including why steady-state low batch size is a caller-concurrency problem and not a flushing problem — is in [The Client Driver](the-client-driver.md).

There is no per-call timeout knob — wrap your call in `tokio::time::timeout` if you need one. The background coalescing task uses a bounded waiter queue (4096 entries); once full, callers await queue capacity, which applies backpressure before memory can grow without bound.
