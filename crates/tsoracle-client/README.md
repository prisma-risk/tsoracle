# tsoracle-client

gRPC client driver for the [tsoracle](https://github.com/prisma-risk/tsoracle) timestamp oracle.

**The client never retains pre-fetched timestamps.** Every timestamp returned to a caller was allocated by the server *after* that caller's request entered the client driver. RPC efficiency comes from request coalescing (multiple concurrent waiters batch into one outgoing `GetTs`), not pre-fetching. This guarantees that no timestamp is ever handed out by the client unless the server actually issued it — there is no scenario where the client speculatively serves a timestamp that the server hasn't (or won't) commit.

## What's in the box

- `Client` — the gRPC client. Methods to issue single timestamps and batches; handles leader discovery, request coalescing, reconnection, and `LeaderHint` follower-redirect for you.
- `ClientBuilder` — configure endpoints, retry policy, TLS, optional metrics recorder.
- `RetryPolicy` — declarative retry config (max attempts, backoff parameters). Defaults are tuned for typical raft-like leader transitions.
- `ClientError` — error variants surface transport failures, leader-unavailable, and timeout cleanly so callers can distinguish them.

## Usage shape

```rust,no_run
use tsoracle_client::ClientBuilder;

let client = ClientBuilder::new()
    .endpoint("http://127.0.0.1:50051")
    .endpoint("http://127.0.0.1:50052")
    .endpoint("http://127.0.0.1:50053")
    .build().await?;

let ts = client.get_ts().await?;
println!("got timestamp: {ts:?}");
# Ok::<(), Box<dyn std::error::Error>>(())
```

Pass every cluster endpoint you can — the client cycles through them on transport failure and follows `LeaderHint` redirects on `FAILED_PRECONDITION` responses to land on whichever node is currently leader.

## TLS

Configured via `ClientBuilder::tls_config(ClientTlsConfig)`. The endpoint URL scheme determines whether TLS is engaged (`http://` = plaintext, `https://` = TLS). See [`docs/client-api-and-usage.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/client-api-and-usage.md) for the scheme rule and worked examples.

## Feature flags

- `tls-rustls` (default) — TLS via rustls.
- `tls-native` — TLS via the platform's native trust roots. Pick one.
- `tracing` (default) — emit tracing spans/events through the `tracing` facade.
- `metrics` — emit per-call metrics (latency, retry, leader-hint) through the `metrics` facade.
- `bt` — backtrace capture in `ClientError` variants.
