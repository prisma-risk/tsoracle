# Performance-critical-path rules

A handful of files in this workspace sit on the request-handling hot path: every `GetTs` / `GetTsBatch` RPC, every window extension, every Raft propose/apply touches them. Regressions in these files surface as elevated end-to-end latency or reduced batch throughput, not as test failures, so we enforce a small set of source-level rules on top of the regular clippy/build checks. Files on the critical path carry this marker as the first line of the file:

    // #[PerformanceCriticalPath]

This is a comment marker, not a proc macro. Enforcement is by review plus a CI guard ([`scripts/check-critical-path.sh`](../scripts/check-critical-path.sh)). If you are editing a marked file, the rules below apply.

## Rules

1. **No synchronous I/O on the hot path.** Disk, gRPC, RocksDB, and other control-plane I/O must be behind a bounded async boundary. If a function in a marked file needs I/O, await a previously-started future or spawn the work onto a background task; never call a blocking op inline. This rule is not grep-enforceable — it depends on review.
2. **No `tracing::info!` or higher log levels.** Use `tracing::debug!` or lower. Hot-path logs at info-or-higher volume fill dashboards and force synchronous writes when the subscriber is configured to flush. The guard rejects `tracing::info!`/`warn!`/`error!`, the bare `info!`/`warn!`/`error!` shortcut (via `use tracing::info;`), and the matching `_span!` macros.
3. **No `println!`.** Same volume problem as info logs, plus it bypasses the `tracing` filter entirely.
4. **No long synchronous compute.** Anything measured in milliseconds belongs on a background worker. The hot path is for routing, packing, and enqueue — not compute.

Two related rules are intentionally not enforced by this guard — they live in other layers of the build:

- **No panics on recoverable paths.** Enforced at the workspace level by the [panic policy](../CONTRIBUTING.md#panic-policy-unwrap-and-expect) (`clippy::unwrap_used` + `clippy::expect_used` as `warn`, with `cargo clippy ... -- -D warnings` making the warning fatal).
- **No `std::sync::Mutex` held across an `.await`.** Planned: enable `clippy::await_holding_lock = "deny"` workspace-wide as a follow-up. The clippy lint is precise — it knows the difference between a sync mutex held across an await point (a real bug) and one held synchronously (fine) — whereas a grep-based "no `std::sync::Mutex::new` in this file" rule false-positives on legitimate non-async uses.

## CI guard

[`scripts/check-critical-path.sh`](../scripts/check-critical-path.sh) runs in CI as the `critical-path` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml). It:

- Finds every `crates/**/*.rs` whose first 10 lines contain `#[PerformanceCriticalPath]`.
- For each, greps for the banned patterns listed in the script (the `BANNED` array).
- Prints violations with line numbers.

**CI runs in strict mode** — the workflow exports `CRITICAL_PATH_STRICT=1`, so any violation fails the build. The script itself defaults to warn-only when `CRITICAL_PATH_STRICT` is unset, which is convenient for local iteration while preparing a new marker candidate. To mirror CI locally, run `CRITICAL_PATH_STRICT=1 ./scripts/check-critical-path.sh`.

New files added to the marker list must be compliant before marking — the guard does not accept pre-existing violations on newly-marked files, even in warn-only mode. "Warn-only" is a timing buffer for the initial rollout of a new banned pattern, not a license to ship non-compliant annotations.

To adjust the list of banned patterns or the strict-mode toggle, edit the script and update this doc in the same commit.

## Marker placement

Place the marker on line 1, above any module-level doc comment (`//!`), any inner attribute (`#![...]`), and any `use` statement. The guard only scans the first 10 lines, so the marker must sit at the top — pushing it below a long module doc silently disables enforcement. The marker is a plain `//` line comment, not Rust syntax; it does not interfere with the file's `//!` module doc (which still attaches to the module) or with any `#![cfg_attr(...)]` inner attribute (such as the [panic-policy attribute](../CONTRIBUTING.md#panic-policy-unwrap-and-expect) in each library crate's `lib.rs`).

The marker is per-file. If you split a marked module into child files, mark every child file that remains on the hot path — the guard does not inherit markers through `mod`.

After placing the marker, verify `CRITICAL_PATH_STRICT=1 ./scripts/check-critical-path.sh` is clean on that file and add the path to the list below in the same commit.

## Current critical-path files

Files in this list are compliant with the rules above — the guard is green against them today.

- [`crates/tsoracle-core/src/allocator.rs`](../crates/tsoracle-core/src/allocator.rs) — the window allocator state machine. Every `try_grant` and `would_grant` call goes through here.
- [`crates/tsoracle-driver-file/src/lib.rs`](../crates/tsoracle-driver-file/src/lib.rs) — single-node fsync-durable driver. The fsync on window extension is the durability boundary for non-replicated deployments.
- [`crates/tsoracle-driver-openraft/src/lib.rs`](../crates/tsoracle-driver-openraft/src/lib.rs) — openraft-backed driver. The propose/apply path for replicated deployments.
- [`crates/tsoracle-client/src/driver.rs`](../crates/tsoracle-client/src/driver.rs) — client-side coalescing driver. Every concurrent waiter passes through `driver_task`'s select loop.

## Files considered but intentionally unmarked

- [`crates/tsoracle-server/src/server.rs`](../crates/tsoracle-server/src/server.rs) — contains a one-shot `tracing::error!` on the leader-watch death path (line 177 at the time of this writing). The call fires at most once per process lifetime and is not on the per-request path, but the grep-based guard cannot distinguish it from per-request logging. Mark this file in a follow-up after splitting the request handlers into their own module or after deciding to downgrade the death-rattle log.
