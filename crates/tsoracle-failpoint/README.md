# tsoracle-failpoint

The one `failpoint!` macro shared across [tsoracle](https://github.com/prisma-risk/tsoracle) — a thin wrapper over [`fail-rs`](https://crates.io/crates/fail) that every crate re-uses instead of redefining its own copy.

## Why it exists

Four crates (`tsoracle-paxos-toolkit`, `tsoracle-openraft-toolkit`, `tsoracle-driver-file`, `tsoracle-server`) each carried a byte-identical wrapper macro that forwarded to `fail::fail_point!` under a `failpoints` feature and expanded to `()` otherwise. Every change to failpoint semantics had to be made N times, and the macro had even drifted in name (`fail_point!` vs `failpoint!`). This crate is the single home for that wrapper.

## What's in the box

- `failpoint!(name)` / `failpoint!(name, closure)` — the macro consumers sprinkle into synchronous code paths. Inert (`()`) with the `failpoints` feature off; under the feature, forwards to `fail::fail_point!` so a test can arm a `panic`, an error `return`, a `pause`, or a `sleep` at the named site.
- A `failpoints`-gated re-export of [`fail`](https://crates.io/crates/fail) (`tsoracle_failpoint::fail`), so test code arms the process-global registry through the same crate the macro lives in — consumers never name `fail` directly.

## Quick reference

Opt in by declaring a feature on the consumer crate that flips `tsoracle-failpoint/failpoints`:

```toml
# consumer Cargo.toml
[features]
failpoints = ["tsoracle-failpoint/failpoints"]

[dependencies]
tsoracle-failpoint = { workspace = true }
```

Then at the source site:

```rust,ignore
tsoracle_failpoint::failpoint!("file_driver::write_blocked");
```

And in a test (with the feature on):

```rust,ignore
let _scenario = tsoracle_failpoint::fail::FailScenario::setup();
tsoracle_failpoint::fail::cfg("file_driver::write_blocked", "panic").unwrap();
```

With `failpoints` off (production), the macro expands to nothing and `fail` is not linked.

## Feature flags

- `failpoints` (off by default) — enables the macro body, pulls in `fail`, and enables `fail/failpoints`. Production builds carry zero overhead.

## Related

- [`fail`](https://crates.io/crates/fail) — the synchronous failpoint library this wraps.
- `tsoracle-yieldpoint` — the async analogue, for sites that must keep yielding their tokio worker while parked instead of blocking the thread.
