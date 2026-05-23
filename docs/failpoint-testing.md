# Failpoint testing

Failpoint testing lets a test inject a panic, an error return, a pause, or a sleep at a named site in production code. The injection is gated by a Cargo feature so release builds physically cannot reach any failpoint. tsoracle uses the [`fail`](https://docs.rs/fail/0.5/) crate (the same library used by sled, etcd-rs, and openraft itself).

For sites in async code paths that need to park production code without blocking a tokio worker thread, see the async counterpart at [Yield-point Testing](yieldpoint-testing.md). A `fail`-crate `pause` action uses `std::thread::park` — fine for sync code, but blocking a tokio worker can starve the runtime's timer driver and wedge `tokio::time::sleep` for every task on that worker.

## When to add a failpoint

Add a failpoint when an invariant only manifests under a timing window or a partial-failure scenario that is otherwise impossible to provoke in a unit test. Crash-recovery, leader-transition correctness, and consensus apply-then-crash safety are the canonical examples. Don't add a failpoint when the same property is testable with the existing in-memory driver or via proptest.

## Feature gating

Three crates currently opt in via a per-crate `failpoints` Cargo feature: `tsoracle-driver-file`, `tsoracle-server`, and `tsoracle-openraft-toolkit`. The feature is off by default. Run the failpoint suite with:

    make test-failpoints

The Makefile target enables `tsoracle-driver-file/failpoints`, `tsoracle-server/failpoints`, `tsoracle-server/test-fakes` (needed for `InMemoryDriver`), `tsoracle-openraft-toolkit/failpoints`, and `tsoracle-openraft-toolkit/rocksdb-log-store`. The default CI test job runs `cargo test --workspace --all-features --locked`, which activates all of these features automatically, so the failpoint suite is part of the normal CI gate.

Release builds of the `tsoracle` binary do not include the `failpoints` feature on any crate. The `fail` crate is an optional dependency at the per-crate level (`fail = { workspace = true, optional = true }`) and the per-crate `failpoints` feature is what pulls it into the build graph; without the feature, `fail` is not linked.

## The wrapper macro

Each opting-in crate has a `failpoint` module with a small `failpoint!` macro. Source sites use the crate's macro via `crate::failpoint!(...)` and never reference `fail::` directly. The macro has two forms — the right form depends on whether the site needs to produce a typed return value:

```rust
// Single-arg form. Supports `panic`, `pause`, `sleep(ms)`, `print`, `off`.
// Configuring this site with `return` or `return(...)` panics at runtime
// with "Return is not supported for the fail point" — the bare macro has
// no way to know the enclosing function's return type and refuses to guess.
crate::failpoint!("file_driver::write_blocked");

// Closure form. The closure receives `Option<String>` (the action grammar's
// `return` passes `None`; `return(string)` passes `Some(string)`), and the
// closure must return the *exact* return type of the function the macro is
// called in. The macro `return`s the closure's value from the enclosing
// function. This is what makes failpoint returns type-safe.
crate::failpoint!("file_driver::before_write", |arg: Option<String>| -> Result<(), FileDriverError> {
    let _ = arg;  // multi-tag dispatch can match here in the future
    Err(FileDriverError::Io(std::io::Error::other("failpoint: file_driver::before_write")))
});
```

With `feature = "failpoints"` off, both forms expand to `()` — zero code, no dependency on `fail`.

## Naming convention

`{crate_short}::{module}::{temporal_phrase}` where `crate_short` is the crate name with the `tsoracle-` or `openraft-` prefix dropped and snake-cased, `module` is the file the site lives in, and `temporal_phrase` is one of `before_X`, `after_X`, or `after_X_before_Y`. The form `during_X` is banned because it is ambiguous about where inside `X` the point sits. Names are stable; renaming a failpoint is treated like renaming a public API symbol.

## Current sites

### `tsoracle-driver-file` — 4 sites in `crates/tsoracle-driver-file/src/driver.rs`

| Site name | Position | Useful actions | Test |
|---|---|---|---|
| `file_driver::before_write` | Top of `write_record`, before tmpfile open. | `return(io)` via the closure form; `panic`. | `reopen_after_crash_before_write_returns_prior_high_water` |
| `file_driver::after_tmp_fsync_before_rename` | After `file.sync_all()` on `state.tmp`, before `fs::rename`. | `panic`; `return(io)` via the closure form. | `reopen_after_crash_between_tmp_fsync_and_rename_returns_prior_high_water` |
| `file_driver::after_rename_before_dir_fsync` | After `fs::rename`, before `libc::fsync(fd)` on the directory. | `panic`. | `reopen_after_crash_between_rename_and_dir_fsync_is_monotonic` |
| `file_driver::write_blocked` | At the top of the `spawn_blocking` closure inside `persist_high_water`. | `pause` / `sleep(ms)` (this site is intended for timing tests, not error injection). | `load_is_not_blocked_by_in_flight_persist` — uses `fail::cfg_callback` for a deterministic entry signal. |

### `tsoracle-server` — 4 sites across `crates/tsoracle-server/src/{fence,service}.rs`

| Site name | Position | Useful actions | Test |
|---|---|---|---|
| `server::fence::after_load_before_persist` | In `run_leader_watch`, between `consensus.load_high_water()` and `consensus.persist_high_water()`. | `panic`, `sleep(ms)`, `return(transient)` via the closure form (closure produces `Err(ServerError::Consensus(ConsensusError::TransientDriver(_)))`). A single `1*return(transient)` is recovered by the fence's bounded retry; a `panic` still terminates the watch task. | `fence_recovers_after_transient_load_error` |
| `server::fence::after_persist_before_publish` | In `run_leader_watch`, after `persist_high_water` returns, before `try_on_leadership_gained` and the `state_tx.send(Serving)`. | `panic`. | `fence_panic_after_persist_advances_durable_but_not_serving` |
| `server::service::before_allocate` | In `Service::get_ts`, before the allocator lock is taken. | `sleep(ms)`, `pause`. Used for timing-shape tests only — closure-form `return` would produce a `Status` directly and bypass the production `ConsensusError → Status` classification path. | `before_allocate_sleep_delays_get_ts` |
| `server::service::extension_gate_held` | In `Service::extend_window`, immediately after the `extension_gate.read().await` guard is bound. | `pause`, `sleep(ms)`. | `extension_gate_held_sleep_delays_get_ts` |

### `tsoracle-openraft-toolkit` — 2 sites in `crates/tsoracle-openraft-toolkit/src/log_store/mod.rs`

| Site name | Position | Useful actions | Test |
|---|---|---|---|
| `tsoracle_openraft_toolkit::log_store::before_write_batch` | In `RocksdbLogStore::append`, immediately before `self.db.write_opt(batch, &wo)`. | `panic`. | `panic_at_before_write_batch_leaves_log_empty` |
| `tsoracle_openraft_toolkit::log_store::after_write_before_sync` | In `RocksdbLogStore::append`, between the rocksdb write and the `callback.io_completed(...)` notification. | `return` via the closure form (closure produces `Err(io::Error)`); `panic`. | `return_at_after_write_before_sync_persists_entry` |

## Writing a failpoint test

Tests live in `crates/<crate>/tests/failpoints.rs`. The file is `#![cfg(feature = "failpoints")]` at the top; a `[[test]]` entry in `Cargo.toml` declares the required features (so cargo silently skips the binary when the features are off rather than failing the compile).

Each test:

1. Acquires a process-global `FAILPOINT_TEST_SERIAL: tokio::sync::Mutex<()>` to serialize test bodies inside the binary. The `fail` registry is process-global; without body-level serialization, configured actions interleave across tests.
2. Sets up a `fail::FailScenario::setup()` RAII guard. The guard snapshots the registry on entry and restores it on drop — this is `fail`'s built-in protection against tests leaking configured actions into each other.
3. Calls `fail::cfg("name", "action")` to configure the failpoint.
4. Exercises the code path.
5. Calls `fail::cfg("name", "off")` to clear the action (the `FailScenario` drop would also clear it, but explicit teardown makes the test scope obvious).
6. Asserts the observable invariant.

For deterministic entry signaling (when the test needs to know the failpoint fired *before* taking some other action), use `fail::cfg_callback` — see `tsoracle-driver-file/tests/failpoints.rs::load_is_not_blocked_by_in_flight_persist` for the canonical pattern with the `entered_tx` / `release_rx` handshake.

Tests that need a real tonic listener and `tsoracle_client::Client` (the two `service::*` tests do) must wait deterministically for `ServingState::Serving` and for tonic's accept loop to be polled. `crates/tsoracle-server/tests/failpoints.rs` defines local `wait_until(state_rx, predicate)` and `wait_for_grpc_handshake(addr, budget)` helpers mirroring the pattern in `tests/e2e.rs`. Don't reach for `tokio::time::sleep(...)` as a warm-up — it is not deterministic across machine load.

## Action grammar crib sheet

- `off` — disable the failpoint. Always allowed.
- `panic` — panic the calling thread. Inside `spawn_blocking` or a `tokio::spawn` task, the panic surfaces as `JoinError` to whoever awaits the handle.
- `pause` — block the calling thread until `off` is configured for this failpoint. Note: `pause` gives the test no signal that the failpoint has been *reached*; use `cfg_callback` when a deterministic entry signal is needed.
- `sleep(N)` — sleep N milliseconds. The implementation is `std::thread::sleep`, which blocks the OS thread. Keep N modest (under a few hundred ms) to avoid tripping tonic's connection timeouts during request-path tests.
- `return` / `return(string)` — closure form only. With a single-arg site, configuring `return` panics at runtime.
- `print(text)` — log a message without altering control flow.

For probabilistic (`50%return(...)`) and call-counted (`5*panic`) actions, see `https://docs.rs/fail/latest/fail/`. We do not currently commit usage examples for them.

## Adding a new site

1. Pick a name following `{crate_short}::{module}::{temporal_phrase}`.
2. Decide single-arg vs. closure form. If the site needs to return a typed value, the closure form is required. If the action will only be `panic`, `pause`, `sleep`, or `print`, single-arg suffices.
3. Insert `crate::failpoint!(...)` at the source position. The crate must already have the `failpoints` feature, the `failpoint` module, and the `#[macro_use]` mount in `lib.rs` — see `tsoracle-driver-file` as the canonical reference.
4. Add a test in `crates/<crate>/tests/failpoints.rs`. Follow the pattern in this doc.
5. Run `make test-failpoints` locally and confirm everything still passes.
6. Document the new site in this file's "Current sites" table.

Renaming an existing site is a breaking change for any test or operator script that referenced it. Treat it like renaming a public API symbol — bundle the rename with the changes that motivate it, and mention it in the PR description.
