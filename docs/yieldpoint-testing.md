# Yield-point testing

Yield-point testing lets a test deterministically park an async task at a named site in production code and release it from outside. The mechanism is a `tokio::sync::Notify` keyed by a `&'static str` name — when the named yield point is armed, the production code awaits the registered `Notify`; when no test has armed it, the call site expands to nothing.

It is the async sibling of [failpoint testing](failpoint-testing.md). Failpoints (and the underlying [`fail`](https://docs.rs/fail/0.5/) crate) drive sync injection via `std::thread::park` / condvars, which is fine for sync code paths but blocks a tokio worker thread when invoked from inside an async task. A blocked worker can starve the runtime's timer driver — `tokio::time::sleep` stops returning, and any test that uses `drive_until` polling stalls. Yield points exist for exactly the case where the injection site is in an async path that must keep yielding to the runtime while parked.

## When to add a yield point

Add a yield point when:

- the race window is small enough that a `tokio::time::sleep`-based test is non-deterministic *and*
- the call site is in an async task that must not block its worker (anything ticking in a `tokio::select!`, anything sharing a runtime with timer-driven progress, anything inside a `tokio::spawn` body that other tasks need to make progress on).

Don't add a yield point when a failpoint would already serve — sync injection points should keep using `crate::failpoint!(...)`. Don't add one to paper over a flaky test; the bug it's masking is usually a missed-notification race like the one yield points exist to surface.

## Feature gating

Crates that opt in declare a `yieldpoints` Cargo feature. The feature is empty (it gates only macro expansion, not a dep). The macro lives in a `yieldpoint` module on each opting-in crate; the `yieldpoint!(...)` macro is exported at the crate root via `#[macro_export]`. With the feature off, the macro expands to `{}` — production builds carry zero overhead and the `tokio::sync::Notify` registry is never linked. With the feature on, the call site consults the per-process registry and `.await`s the registered `Notify` if armed.

Run the yield-point suite with:

    cargo test --workspace --all-features

`--all-features` activates `yieldpoints` on every opting-in crate, so the yield-point tests are part of the normal CI gate (same model as failpoints).

## The wrapper macro

Each opting-in crate has a `yieldpoint` module with a small `yieldpoint!` macro. Source sites use the crate's macro and never construct the `Notify` directly. The macro has a single form — yield points have no typed return; they only pause and resume:

```rust
crate::yieldpoint!("standalone_host::apply_task::between_iterations");
```

With `feature = "yieldpoints"` off, the call expands to `{}` — zero code, no `tokio::sync::Notify`, no registry lookup.

## Naming convention

`{module}::{site}::{temporal_phrase}` where `module` is the module the site lives in (typically named after the type), `site` is the function or task the yield point is inside, and `temporal_phrase` is one of `between_iterations`, `before_X`, `after_X`, or `after_X_before_Y`. The form `during_X` is banned because it is ambiguous about where inside `X` the point sits. Names are stable; renaming a yield point is treated like renaming a public API symbol.

The string is matched verbatim against the registry — typos at the call site or in `yieldpoint::cfg("name")` silently disable the gate (the test will then race the same way the production timing race used to). Pull the name through a `const &'static str` shared between the call site and the test when in doubt.

## Current sites

### `tsoracle-driver-paxos` — 3 sites in `crates/tsoracle-driver-paxos/src/standalone.rs`

| Site name | Position | Test |
|---|---|---|
| `standalone_host::apply_task::between_iterations` | End of the `apply_notify` branch in the apply task's `tokio::select!`, after `drain_decided_into` + `maybe_snapshot` and before the loop returns to the next `select!`. | `stop_delivers_shutdown_when_apply_task_is_mid_iteration` |
| `standalone_host::current_high_water::after_append_before_await` | In `PaxosHighWaterHost::current_high_water`, after the `Barrier` append and before the first `Notified::enable()` registers as an `apply_notifier` waiter. | `current_high_water_returns_when_apply_drained_before_register` |
| `standalone_host::submit_advance::after_append_before_await` | In `PaxosHighWaterHost::submit_advance`, after the `Advance` append and before the first `Notified::enable()` registers as an `apply_notifier` waiter. | `submit_advance_returns_when_apply_drained_before_register` |

## Writing a yield-point test

Tests live in `crates/<crate>/tests/<topic>.rs` with `#![cfg(feature = "yieldpoints")]` at the top so cargo silently skips the binary when the feature is off rather than failing the compile.

Each test:

1. Calls `yieldpoint::cfg("name")` to arm the gate. The returned `Arc<Notify>` is the release handle.
2. Drives production code into the yield point (start the host / spawn the task / fire the input that wakes the await).
3. Performs the test's side-effect (call `stop()`, or whatever shutdown / interleaving the bug requires).
4. Calls `handle.notify_one()` on the release handle to wake the production code.
5. Asserts the observable invariant — typically with `tokio::time::timeout` around the join, so the test fails with `Elapsed(())` rather than hanging if the bug is back.
6. Calls `yieldpoint::remove("name")` to clear the gate. (Tests that share the registry across iterations also need this; the registry is process-global, like `fail`'s.)

The release handle is an ordinary `Arc<Notify>`, so all of `notify_one`, `notify_waiters`, and `notified().await` are available — pick the method whose semantics the test wants. `notify_one` is the common case (single waiter, store-permit-if-no-waiter semantics).

## Adding a new site

1. Pick a name following `{module}::{site}::{temporal_phrase}`.
2. Insert `crate::yieldpoint!("name")` at the source position. The crate must already have a `yieldpoint` module and a `yieldpoints` cargo feature — see `tsoracle-driver-paxos` as the canonical reference.
3. If the call site is inside a critical section guarded by a `parking_lot` mutex (or any non-`Send`-across-`.await` guard), drop the guard before the macro invocation. The macro contains `.await`, so a guard held across it would make the enclosing future `!Send` and `tokio::spawn` would reject it.
4. Add a test in `crates/<crate>/tests/<topic>.rs`. Follow the pattern in this doc.
5. Run `cargo test -p <crate> --features yieldpoints` locally and confirm everything still passes. To confirm the test actually catches the bug it's claimed to: temporarily invert the fix, re-run, observe the `Elapsed(())` timeout. Revert the production code before committing.
6. Document the new site in this file's "Current sites" table.

Renaming an existing site is a breaking change for any test that referenced it. Treat it like renaming a public API symbol — bundle the rename with the changes that motivate it, and mention it in the PR description.

## Relationship to failpoints

| | Failpoint | Yield point |
|---|---|---|
| Mechanism | `std::thread::park` / condvar (sync) | `tokio::sync::Notify` (async) |
| Worker behavior while parked | OS thread blocked | Task yielded, worker free |
| Actions | `off`, `panic`, `pause`, `sleep(ms)`, `print(text)`, `return` / `return(...)` | Implicit single action: pause until released |
| Typed return injection | Yes (closure form) | No |
| Crate | [`fail`](https://docs.rs/fail/0.5/) | First-party, per opting-in crate |
| Cargo feature name | `failpoints` | `yieldpoints` |
| Source macro | `crate::failpoint!(...)` | `crate::yieldpoint!(...)` |
| Doc | [Failpoint Testing](failpoint-testing.md) | this file |

Reach for failpoints first if the site is sync or needs to inject a typed return. Reach for yield points when the site is async and the test needs to pause production code without blocking a tokio worker.
