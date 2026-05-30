+++
title = "Gapless sequences: GetSeq alongside GetTs"
description = "tsoracle now hands out two kinds of strictly-ordered integer: monotonic timestamps and gapless, dense sequences. What gaplessness costs, why GetSeq is non-idempotent, and when to reach for it."
weight = 3
[taxonomies]
tags = ["distributed-systems", "tso", "tsoracle", "ids", "sequences"]
+++

For most of its life tsoracle has done exactly one thing: hand out strictly monotonic timestamps. You call `get_ts`, you get back an integer that is strictly greater than every integer the cluster has ever issued, and you use it to order events across machines. That primitive answers "what happened first," and it is the right tool for MVCC, snapshot isolation, and cross-shard event ordering. It is not, however, the right tool for every problem that looks like "I need increasing integers." Some workloads need a stronger property than monotonic: they need *gapless*. tsoracle now serves that too, through a second RPC, `get_seq`. This post is the honest account of what gaplessness is, what it costs, and when the cost is worth paying.

## Monotonic is not gapless

A tsoracle timestamp is a packed 64-bit value: a millisecond physical component in the high bits and a logical counter in the low bits. The logical counter resets every time the physical component advances. That packing is what makes timestamps cheap and meaningful — the value sorts by real time — but it also means the issued sequence has *holes*. Ask for three timestamps just before a millisecond boundary and you might get `…1004, …1005, …1006`; ask for three more just after and you get `…0000, …0001, …0002` at the next millisecond. The integers jumped. Nothing was issued in between. That is completely fine, because a timestamp's only job is to be **ordered**: for any two grants, the earlier one compares less than the later one. It never has to be **contiguous**.

Plenty of identifiers do have to be contiguous. A surrogate primary key that an operator reads off a dashboard. An invoice number a finance team expects to run `4001, 4002, 4003` with no missing 4002. A ledger line number an auditor will reconcile against a count. A partition offset a consumer walks one at a time. For all of these, "monotonic with gaps" is not good enough — a missing number is a missing row, a dropped invoice, a reconciliation failure. What these workloads want is a *dense* sequence: every integer in the run issued exactly once, nothing skipped. That is what `get_seq` provides, and it is a genuinely different guarantee from `get_ts`, not a reskin of the same call.

## The GetSeq contract

`get_seq(key, count)` reserves a contiguous block of `count` ordinals and returns its start:

```rust
let block = client.get_seq("invoices", 3).await?;
// block.start == 4001  →  the block is [4001, 4002, 4003]
// block.count == 3
```

The returned `start` is the durably-committed pre-advance value of the counter named `key`. The server advances that counter past `start + count`, persists and replicates the advance, and only then returns the block. Three properties fall out of that ordering, and they are the whole contract:

- **Gapless within a block.** The block is exactly `[start, start + count)` — every integer present, none skipped.
- **Contiguous across blocks.** Each successful grant for a key begins precisely where the previous one ended. Two callers hammering the same key see their blocks interleave, but the union of all delivered blocks is a dense run `[0, high)` with no holes.
- **Never reused.** The counter only moves forward. An ordinal handed out is gone — across leader transitions, across process restarts, across crashes. There is no scenario in which `4002` is issued twice.

Each `key` is its own counter. `get_seq("invoices", …)` and `get_seq("shipments", …)` advance independently, so a single tsoracle deployment backs as many named sequences as you like. A key is bounded UTF-8 (up to 128 bytes) and must be non-empty; the first call to a fresh key starts at `0`. And the batching trick that makes `get_ts` fast applies unchanged: ask for a block of a thousand, amortise the leader's fsync across all thousand ordinals, and hand them out locally.

## The price of gaplessness: non-idempotency

Here is the part that matters, and the part most worth understanding before you adopt `get_seq`. Gaplessness costs you idempotency, and there is no way around it.

Consider what happens when an RPC fails. With `get_ts`, a grant the client never received is simply *wasted*. The timestamps in it are burned, but burned timestamps are exactly the holes that the contract already permits — monotonicity is undisturbed. So the client can retry a failed `get_ts` freely: worst case, it leaves a gap nobody promised not to leave. `get_ts` is idempotent in the only sense that counts.

`get_seq` cannot do this. A gapless advance, by definition, *cannot be wasted* — if the counter moved from `4000` to `4003`, the block `[4001, 4003]` is spent whether or not the client ever saw it. Now suppose the request fails *after* it reached the server: a post-send timeout, a connection reset, an `INTERNAL` returned after the durable fsync. The client genuinely cannot tell whether the advance committed. Retrying is a coin flip between two bad outcomes — either the original commit didn't happen and the retry is fine, or it did and the retry double-spends, handing the same caller a *second*, *different* block while the first sits spent-but-undelivered.

tsoracle refuses to guess. An ambiguous post-send failure surfaces as a distinct outcome, `SeqUncertain`, rather than being silently retried. The contract to the caller is precise: *the advance may or may not have committed; you must not blindly retry.* What the caller does next is a deliberate choice, not a default:

- **Reconcile.** Read the current counter value back (or check whether the work keyed to that block landed) and resume from the truth. This keeps the sequence dense.
- **Tolerate a hole.** Some workloads are fine with a rare gap — a surrogate key only has to be unique and roughly increasing, not literally hole-free. For those, treating the block as lost and moving on is correct and cheap.

The one thing that is *never* correct is to retry an uncertain call as if it were idempotent. That is the trap `get_ts` users carry as muscle memory, and it is exactly wrong here. The honest summary: a gap in a `get_seq` sequence can only ever appear at a surfaced `SeqUncertain` the caller chose not to reconcile. Gaplessness is guaranteed for every block you accept; the failure mode is visible, named, and yours to handle.

(Pre-commit-certain rejections — an invalid argument, an over-cap `count`, an unsupported driver — are *not* uncertain. The request provably never advanced anything, so those return ordinary errors you can handle or retry like any other.)

## GetTs vs GetSeq, side by side

| Property | `get_ts` | `get_seq` |
|---|---|---|
| What it orders | events across machines | ordinals within a named key |
| Shape | packed physical + logical | dense per-key counter |
| Strictly monotonic | Yes | Yes |
| Gapless / contiguous | No (logical resets per ms) | Yes |
| Keyed | single global stream | many named keys |
| Idempotent (safe to retry) | Yes | **No** — ambiguous failure is `SeqUncertain` |
| Failure a caller must reason about | transport error, retry | `SeqUncertain`: reconcile or tolerate a gap |
| Reach for it when | you need *order* | you need *no holes* |

The row that decides the choice is the last data row. If your requirement is "A must compare less than B when A came first," you want `get_ts` and its gaps are irrelevant. If your requirement is "the numbers must run `n, n+1, n+2` with nothing missing," you want `get_seq` and you accept the non-idempotent failure mode as the price.

## Named sequences and the per-call ceiling

Because every sequence is identified by a key, one deployment can serve many independent counters without any per-sequence configuration — the counter for a key springs into existence on first use. That keeps the operational surface flat: there is no "create sequence" step, no schema, no migration. Just pick a key and call.

There is one knob worth knowing about. A single `get_seq` call is capped at a maximum `count` (default `65_536`, configurable on the server via `ServerBuilder::max_seq_count`). The cap is a soft anti-abuse guardrail, not a representational limit: because a dense block is permanently consumed the instant it commits, the ceiling bounds how much one call can irrevocably reserve. A request over the cap is rejected with an ordinary `INVALID_ARGUMENT` — a pre-commit-certain error, nothing spent, safe to split and retry.

## When to use which

```
Need increasing integers across machines?
├─ No  → a local counter or UUID is fine; you don't need an oracle.
├─ Need them ORDERED (earlier < later), gaps OK?
│  └─ Yes → get_ts. MVCC, snapshot isolation, audit ordering, CDC merge.
└─ Need them GAPLESS (n, n+1, n+2, nothing skipped)?
   └─ Yes → get_seq. Surrogate keys, invoice/order numbers, ledger lines,
            partition offsets — and accept the non-idempotent failure mode.
```

A useful gut check: if a missing number would be a *bug* (a skipped invoice, a hole in a ledger), you want `get_seq`. If a missing number is a *non-event* (who cares that timestamp 4002 was never issued), you want `get_ts`, and reaching for `get_seq` just buys you the `SeqUncertain` reconciliation burden for nothing.

## The same engine underneath

`get_seq` is not a separate system bolted on. It rides the exact machinery the [how-it-works summary](/how-it-works/) describes for `get_ts`: a single leader serves the call, the advance is fsync'd to disk before any ordinal leaves the process, and in a replicated cluster the advance is committed through the consensus log before the leader replies. The durability proof (the fsync) and the failover proof (the consensus commit) are the same two guarantees that keep timestamps from rewinding — they keep the per-key counter from rewinding too. A new leader after a failover only ever hands out ordinals above the last committed advance, which is precisely why an ordinal is never reused.

Sequence support is a property of the consensus driver. The file driver and the openraft (raft-replicated) driver implement it today; the OmniPaxos driver is on the roadmap. A driver that does not yet support dense sequences answers `get_seq` with `UNIMPLEMENTED` — an honest, diagnosable signal at the first call, never a timestamp-style error in disguise. If you are running the file or openraft driver, `get_seq` is ready; if you are on OmniPaxos, `get_ts` is unaffected and dense support is coming.

## Closing

tsoracle is now two oracles sharing one engine. `get_ts` gives you global order and tolerates gaps; `get_seq` gives you gapless density and tolerates a visible, non-idempotent failure mode. They are not interchangeable, and the whole value of having both is picking the one whose guarantee matches your requirement and whose cost you can pay. Pin the requirement first — *order* or *no holes* — and the choice makes itself.

If you want the architecture orientation, the [how-it-works summary](/how-it-works/) now covers both grant types. And if you are still deciding whether you need an oracle at all, [TSO vs UUIDv7](/posts/tso-vs-uuidv7/) draws the line between "k-sortable is enough" and "you need a real ordering authority."
