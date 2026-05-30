+++
title = "How tsoracle's window allocator works"
description = "Inside tsoracle: the window allocator amortises one fsync into many IDs while keeping every issued timestamp strictly monotonic across crashes."
weight = 5
[taxonomies]
tags = ["distributed-systems", "tso", "tsoracle", "internals"]
+++

The headline number for a timestamp oracle is throughput, and the way every reasonable TSO hits a usable throughput number is the same: amortise the durability cost across many IDs. One fsync per ID gives you a TSO that does, on a modern NVMe SSD, somewhere between five and twenty thousand IDs per second. That's the floor. The ceiling, and the actual operating range you want, is millions per second, and the only way across that gap is a window allocator. This post is what tsoracle's allocator does, why the math behind window flips is the load-bearing part, and the proof that a crash mid-allocation cannot rewind any issued ID.

## What "amortise the fsync" actually means

Every distributed timestamp oracle has the same observable contract: when you ask for an ID, you get back an integer that is strictly greater than every integer ever returned, by anyone, by this service. The cheapest way to enforce that contract is to maintain a single counter on disk, increment it under fsync on every request, and return the new value. The contract is enforced. The throughput is bounded by `1 / fsync_latency`. On commodity NVMe the fsync latency is a few hundred microseconds, on tiered cloud storage it can be several milliseconds. Neither figure produces an interesting TSO.

The trick — old, well-known, and not invented by tsoracle — is that the durability story doesn't care about IDs, it cares about the fsync. So persist an *interval* of integers, fsync once, and hand them out from memory until the interval is exhausted. Persist the next interval before the current one runs out. The cost of the fsync is paid once per interval; if intervals are a thousand IDs each, you pay one fsync per thousand IDs. Throughput becomes `interval_size / fsync_latency`, which on the same NVMe hardware is comfortably millions of IDs per second.

That interval is the *window*. The piece of state on disk is the *high-water mark*. The allocator is the small piece of code that maintains the relationship between the in-memory window and the on-disk mark, and the invariant it enforces is the entire correctness story for tsoracle.

{{ sequence_flow(name="get-ts-flow", title="get_ts(batch=N) request path — one fsync amortises N IDs.") }}

## The high-water mark

The high-water mark is one integer. It lives on disk in a file under the data directory (`./tsoracle-data/` by default in the standalone binary), behind one fsync. Its meaning is short and load-bearing: *no ID at or below the high-water mark may ever be issued*. Phrased the other way, every ID the allocator will ever return is strictly greater than the mark. There is no other state. There is no list of issued IDs, no log of allocations, no in-flight bookkeeping that has to be reconciled with the mark at recovery time. The single integer is the proof.

The allocator's in-memory state is two integers: the *next ID to hand out* (call it `next`) and the *end of the currently allocated window* (call it `window_end`). The invariant is `next ≤ window_end ≤ high_water_mark`. The mark is always greater than or equal to the end of the in-memory window; in fact, it's exactly equal until the next window flip. Issuing an ID is `let id = next; next += 1; return id;` and that's the whole hot path. No I/O, no locking beyond the integer increment, no consensus. Just a number going up.

## The window flip

Sooner or later `next` reaches `window_end` and the in-memory window is exhausted. The allocator now needs more headroom, and the operation that gives it that is the *window flip*. The flip has four steps and the order matters.

1. Compute the candidate new end: `new_end = max(high_water_mark, next) + window_size`. The `max` is a safety against re-entry after recovery, where the in-memory `next` might be behind the on-disk mark; in steady state `next == high_water_mark` and the `max` is just `next`.
2. Write the new mark to disk and fsync it. After this fsync returns successfully, `new_end` is durable.
3. In a replicated topology, propose the new mark through the consensus driver and wait for it to commit. After commit, the new mark is also durable in the raft log on a quorum of replicas. (In single-node mode this step is skipped; the fsync alone is the durability proof.)
4. Update the in-memory `window_end` to `new_end`. Only now can `next` advance into the new range.

{{ window_timeline(name="window-flip", title="Window flip sequence. Click to step through the four states.") }}

The crucial part is the ordering between (2)/(3) and (4). The in-memory `window_end` is the cap that bounds issuance; until it advances, no ID in the new range can be returned. The fsync (and replication, when applicable) happens first, the in-memory advance happens second. If the process dies between (2) and (4), the on-disk mark covers a range that nothing has been issued from yet; on restart, recovery sees the mark and the allocator starts fresh with `next = mark`. No ID was ever handed out and the mark covers all of them.

If the process dies between (3) and (4), the raft log has committed the advance, the leader's on-disk mark reflects it, but no ID from the new window has been issued anywhere. On failover, the new leader's mark is at least the committed value, and the new leader starts handing out IDs above it. Again, no regression, no duplicate.

If the process dies between (1) and (2), nothing has been written or replicated. On restart, the mark is the old one, and the allocator picks up where it left off. The candidate `new_end` was a transient computation in memory; the world doesn't know it ever existed.

The full crash-safety argument is just the conjunction of those three cases, multiplied across leader handoff. There is no fourth case because the four steps are total-ordered and the durability gate is at step 2.

## Why the window size is a tuning knob, not a correctness knob

A larger window means fewer flips, which means a higher throughput ceiling and a larger amortisation of the fsync cost. It also means a larger range of IDs may go unused on a crash: if the allocator dies at step 4, the entire window between `high_water_mark` and `new_end` is committed to disk but nothing was issued from it; on recovery, `next` jumps to the mark and the gap is permanent. The trade-off is throughput against ID density.

In a TSO that uses IDs as opaque integers — Spanner-style commit timestamps, generic "ordered ID" use cases — the gaps don't matter at all; the sequence remains strictly monotonic. If your downstream consumer expects a dense sequence (uncommon for TSO use cases, but possible if you're using the TSO output as a sequence number in some other system), you'd pick a smaller window. tsoracle defaults to a moderate window size and exposes it as a configuration knob; the right value depends on your fsync latency and how badly you care about gap density.

## Wrap-around math

The integer space is finite. On a 64-bit unsigned counter, the integer space lasts somewhere around `2^64` IDs, which at a million IDs per second is roughly half a million years. In practice the integer space is partitioned: tsoracle's default ID format packs a physical-millisecond prefix into the top bits and a logical counter into the bottom bits. The packed format makes the integer space *not dense* — when `physical_ms` advances, the logical counter resets, and the integer values between the last logical value and the start of the next physical bucket are unused. The issued sequence remains strictly monotonic; just not all integer values in the range are issued.

The window flip needs to handle this correctly. When the in-memory window crosses a physical-millisecond boundary, the candidate `new_end` is computed by advancing the physical prefix and resetting the logical part — not just by adding `window_size`. The code that does this is the trickiest part of the allocator and is the densest comments in the codebase. The invariant it preserves is the same as ever: `new_end` must be strictly greater than every issued ID, and the on-disk mark must be fsync'd to that value before any ID in the new range leaves the process.

## Recovery

On process restart, the allocator reads the high-water mark from disk and sets `next = mark` and `window_end = mark`. The first request after restart triggers an immediate window flip (because `next` already equals `window_end`), which fsyncs a new mark and unblocks issuance. The hot path from there is unchanged.

In a replicated cluster, restart adds one more wrinkle. The freshly-restarted node may be a follower, in which case it doesn't hand out IDs at all and the allocator state isn't used in the hot path until it gets elected. When it does get elected, the in-memory state is reconciled with the latest mark from the committed raft log: `next = max(disk_mark, last_raft_committed_mark)`. The new leader cannot regress past the highest mark the previous leader replicated.

The reconciliation is what makes leader failover safe. A previous leader that fsync'd a mark to its own disk but failed to replicate it through raft will, after restart, see its private mark overwritten by the raft-committed value. Conversely, a new leader takes over with the raft-committed mark, which is always the durable upper bound across the cluster. No mark is ever lost; no mark is ever silently regressed.

## What this means for clients

The client-facing throughput model is straightforward. Most requests are served from the in-memory window in microseconds; they are essentially free. A small fraction of requests trigger a window flip and pay the fsync (and, in a replicated cluster, the consensus round-trip) — that's the tail. Batched allocations (`get_ts_batch(N)`) amortise this further: a single request can pull `N` IDs from the in-memory window, and as long as the window has space, no flip is needed. The client hands the IDs out locally for as long as `N` lasts.

The latency model has two distinct distributions. The fast path is bounded by the memory and network round-trip; on the same machine, that's tens of microseconds. The flip path is bounded by the slowest of: the disk fsync, the consensus replication, and the network round-trip back to the client. On modern NVMe + single-replica, that's a few hundred microseconds; on tiered cloud storage with three replicas across availability zones, that's a few milliseconds. The tail is where the operational story lives, and a TSO's observability — leader fsync latency, raft commit latency, per-window flip rate — is what oncall actually watches.

## Why this is a small story

A timestamp oracle reads like a complicated system because the failure modes are interesting. The crash safety is interesting. The leader failover is interesting. The math behind ID packing is interesting. But the *implementation* is small: one on-disk integer, two in-memory integers, a four-step flip, and a consensus driver that's a trait you implement once. The full window allocator in tsoracle is a few hundred lines of Rust. The replicated driver is a few thousand lines, almost all of which are the openraft integration and not the TSO logic. Most of the surface area is testing — the failpoint suite, the multi-process stress harness, the fuzz targets on the postcard decoders — because the algorithmic core is small enough that the safety comes from exercising the edge cases hard, not from a clever design.

If you want the contract from a different angle, the [how-it-works summary](/how-it-works/) frames the same architecture as a five-minute orientation; the next post in this series, [Wiring tsoracle into openraft](/posts/wiring-tsoracle-into-openraft/), shows how the consensus driver lands in practice. The two earlier posts — [Why distributed systems need a TSO](/posts/why-distributed-systems-need-a-tso/) and [When you need a TSO](/posts/when-you-need-a-tso/) — are the reason-from-first-principles framing.
