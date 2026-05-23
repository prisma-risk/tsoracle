# tsoracle-yieldpoint

Async yield points for [tsoracle](https://github.com/prisma-risk/tsoracle) — the structural analogue of [`fail-rs`](https://crates.io/crates/fail) failpoints, but driven by a `tokio::sync::Notify` so the production code yields its tokio worker while parked instead of blocking the thread.

## Why it exists

A fail-crate `pause` action uses `std::thread::park` / a condvar, which blocks the OS thread the failpoint fires on. Inside a tokio task that starves the runtime's timer driver — `tokio::time::sleep` stops returning for every task on that worker, and any race the test is trying to observe gets masked. Yield points exist for exactly the case where the call site is in an async path that must keep yielding to the runtime while parked.

## What's in the box

- `yieldpoint!(name)` — the macro consumers sprinkle into their async code paths. Inert with `yieldpoints` feature off; under the feature, suspends on a `tokio::sync::Notify` keyed by `name` until a test fires it.
- `registry` — runtime registry the macro hits (`arm`, `disarm`, `fire`). Used by chaos / integration tests to drive injection sites deterministically.

## Quick reference

Opt in by declaring a feature on the consumer crate that flips `tsoracle-yieldpoint/yieldpoints`:

```toml
# consumer Cargo.toml
[features]
yieldpoints = ["tsoracle-yieldpoint/yieldpoints"]

[dependencies]
tsoracle-yieldpoint = { workspace = true }
```

Then in code:

```rust,ignore
use tsoracle_yieldpoint::yieldpoint;

async fn some_path() {
    yieldpoint!("after-leader-promotion");
    // ...
}
```

With `yieldpoints` off (production), the macro expands to nothing. With it on (CI tests via `cargo test --features yieldpoints`), the macro suspends on a `Notify` until the test calls `registry::fire("after-leader-promotion")`.

## Feature flags

- `yieldpoints` (off by default) — enables the registry + macro body. Production builds carry zero overhead; CI tests opt in via the consumer crate's feature.

## Related

- [`fail`](https://crates.io/crates/fail) — synchronous failpoints. Use these for sites that can block their thread.
- `tsoracle-server` / `tsoracle-driver-paxos` / others — consumers that expose `yieldpoints` features wiring `tsoracle-yieldpoint/yieldpoints` for their async paths.
