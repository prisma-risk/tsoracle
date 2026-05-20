# The tsoracle window allocator

tsoracle issues 64-bit monotonic timestamps packed as `physical_ms << 18 | logical`. The high 46 bits are milliseconds since Unix epoch (good through year 4199); the low 18 bits are a logical counter that resets each time `physical_ms` advances. Maximum logical per physical_ms is 262,143; maximum timestamp issuance per ms is therefore 262,144 timestamps.

## Allocation model

The allocator tracks a *committed high-water* `H` — the persisted upper bound on `physical_ms` that the durable consensus layer has accepted. The allocator will not issue any timestamp with `physical_ms > H`. When approaching exhaustion (or right after a leader transition), the allocator computes `H' = max(H + 1, now_ms) + window_ahead_ms` and asks the driver to persist `H'`. The driver's monotonic-advance semantics return the actual durable value (`max(stored, H')`), which the allocator commits as the new in-memory bound. From that point, the allocator can issue timestamps with `physical_ms` up to the new `H`.

## Why two-phase extension

Window extension is split into `prepare_window_extension` (sync, no I/O, computes the target) and `commit_window_extension` (sync, applies the persisted value after the driver returns). This split keeps the core algorithm crate sync and runtime-neutral. The server crate is the only place async lives: between `prepare` and `commit` it `await`s `driver.persist_high_water` to durably commit the new bound. Folding the persist inside `try_grant` would drag tokio into the core for a single async call; the prepare/commit shape costs about twenty lines in server glue and keeps the core property-testable in microseconds.

## Why monotonic persist

`ConsensusDriver::persist_high_water(at_least, epoch)` is "advance to at least," never "absolute set." A stale or reordered call MUST be silently absorbed without regression. This is defense in depth: even with the allocator's internal monotonicity, a buggy caller, a clock-skew event, or a racing extension cannot regress the durable high-water. The driver returns the actual persisted value so the caller learns the true state.

## Leader-transition safety: the failover fence

A new leader at epoch `E_new` must not issue any timestamp at or below any timestamp the prior leader at epoch `E_prev < E_new` could have issued. tsoracle guarantees this through a mandatory failover fence run on every transition into `Leader`: clear serving, drain in-flight extensions, perform a linearized `load_high_water` (the driver MUST guarantee this read reflects all writes durably committed at any prior epoch), compute `serving_floor = max(prior_max + 1, now_ms)`, compute `requested = serving_floor + failover_advance`, durably persist `requested` at the new epoch, seed the allocator with `(serving_floor, actual_high_water, epoch)`, then begin serving. The library refuses `GetTs` with `NOT_LEADER` for the entire duration of the fence. This makes the safety argument direct: the new leader's first issued `physical_ms` is strictly above the prior persisted high-water, and its committed ceiling was durable before serving resumed.

## The Clock contract: advisory only

The `Clock` trait's `now_ms` is *advisory*. It tells the allocator how to advance physical time toward the wall clock. The allocator's monotonicity does not depend on clock correctness — a clock jumping backward cannot cause regression because the persisted high-water always wins. A clock pinned far in the past stalls new windows until wall time catches up past the persisted bound. Implementing a custom `Clock` (NTP-disciplined, HLC-extended, externally driven) does not require any changes to the allocator's correctness reasoning.

## What this is and is not

tsoracle is a TSO algorithm core plus a transport. It is not a clock-synchronization system. It does not implement consensus — it consumes one through `ConsensusDriver`. It does not implement multi-shard or partitioned TSO — one tsoracle instance issues one monotonic sequence. Users wanting Spanner-style true-time, partitioned timestamp domains, or HLC fan-in should layer those concerns over tsoracle rather than expecting them inside the library.
