+++
title = "TSO vs UUIDv7"
description = "UUIDv7 and a timestamp oracle solve overlapping problems with very different tradeoffs. UUIDv7 is locally generated and k-sortable. A TSO is globally strictly monotonic. This post is an honest comparison — when each is correct, where UUIDv7 silently fails to meet a requirement, and a decision tree for picking."
date = 2026-05-22
[taxonomies]
tags = ["distributed-systems", "tso", "uuidv7", "ids"]
+++

UUIDv7 and a timestamp oracle look, at a glance, like they solve the same problem: both give you identifiers that sort roughly in the order they were created, and both show up on the same shortlist when an engineer goes hunting for "globally unique, time-ordered ID." The two are not interchangeable. The cost models are different, the guarantees are different, and the failure modes are different in ways that matter for correctness. This post is the honest comparison — when UUIDv7 is the right call, when a TSO is the right call, and what the common "I tried to bridge the gap" mistakes look like in practice.

## What UUIDv7 actually guarantees

The UUIDv7 format (RFC 9562) packs a 128-bit identifier as 48 bits of millisecond Unix timestamp in the top, followed by version and variant bits and roughly 74 bits split between optional sub-millisecond precision and random data. Because the timestamp lives in the top 48 bits, comparing two UUIDv7s byte-wise sorts them by their generating millisecond. That sort property is what people mean when they say UUIDv7 is "time-ordered." It has real value — a database index on UUIDv7 primary keys keeps recent rows clustered on disk, a substantial win over UUIDv4.

The precise guarantee is **k-sortability**, and the distinction between k-sortable and strictly monotonic is the whole game. K-sortable means that for any two identifiers `A` and `B`, if `A` was generated more than `k` units before `B` (for UUIDv7, `k = 1` millisecond), then `A < B`. Within a `k`-millisecond window the ordering is not defined by the spec; it depends on what the implementation chose to do with the lower bits. Strictly monotonic is stronger: for every pair, the one allocated first compares less than the one allocated later, with no window of ambiguity.

RFC 9562 offers a softer guarantee for the in-process case. Implementations **MAY** use the sub-millisecond bits as a counter to ensure that two UUIDv7s generated back-to-back inside the same process are strictly ordered. Good libraries do this. What the RFC explicitly does not promise — what no UUIDv7 implementation can promise without inventing a coordination layer — is monotonicity across processes. Two processes on the same machine, generating UUIDv7s in the same millisecond, will produce identifiers whose order is determined by the random tail. They will not collide, but the one allocated first might compare greater than the one allocated second. UUIDv7 is, by design, a coordination-free protocol; cross-process monotonicity is not in the contract because providing it would require coordination.

## What a TSO actually guarantees

A timestamp oracle is a service that hands out strictly monotonic integer IDs across every client that talks to it. If your call returns `N` and another client's call returns `M` and `N > M`, then your call entered the TSO after theirs — not "probably," not "in this process," but globally and unconditionally. The integer is opaque; the only contract is uniqueness plus strict global ordering. There are no random bits and no implementation-defined fallbacks. The TSO is the single source of truth for "what happened first," and the IDs it issues are the proof.

The cost of that guarantee is a network round-trip per allocation. Latency is bounded by the network plus a leader-side fsync to commit the new high-water mark, plus the consensus protocol's replication delay. Throughput is bounded by the leader's fsync and replication rate. Those numbers sound dire until you remember the standard trick: batched window allocation amortises the round-trip across thousands of IDs at once. A client asks for a window of one thousand IDs; the TSO allocates the next window, replicates it, returns the range; the client hands them out locally as fast as it wants. With batching, a TSO does millions of IDs per second per cluster on commodity hardware, and per-ID latency is microseconds for most calls (served from the local window) with occasional millisecond spikes (when the window refills).

The trust boundary matters. A TSO answer is only as trustworthy as the TSO's own ordering proof. If the TSO loses its high-water mark on a leader crash, or a stale leader hands out IDs after a partition, the strict-monotonicity contract is violated and downstream invariants break. A correctly built TSO closes both holes: durable storage of the high-water mark with an fsync before any ID is handed out, and consensus replication so a new leader cannot regress the counter. `tsoracle` does both — openraft replicates the watermark before the leader returns a window, and the watermark is fsync'd to disk before any ID from the new window leaves the process.

## Side-by-side

| Property | UUIDv7 | TSO |
|---|---|---|
| Generated locally | Yes | No (network round-trip) |
| Globally unique | Yes (probabilistically) | Yes (by construction) |
| Strictly monotonic across all producers | No | Yes |
| Sortable within a millisecond | Implementation-dependent | Always |
| Sortable across milliseconds | Yes | Yes |
| Requires a service | No | Yes |
| Failure mode if requirement misses | Silent correctness bug | Outage (no IDs available) |
| Typical latency | Microseconds | Single-digit milliseconds |
| Typical throughput | Millions/sec per process | Tens of thousands/sec per TSO (much higher with batching) |

The row that most often gets misread is **failure mode if the requirement misses**. UUIDv7 fails by silently producing the wrong answer when the workload needed strict monotonicity: the IDs sort in an order that does not match the order of allocation, and any logic that depends on that ordering — MVCC reads, snapshot isolation, audit trails — produces a result that looks plausible and is wrong. A TSO fails by becoming unavailable: if the leader is down and a new one has not been elected, no client can get an ID, and every caller sees an error. Outages are loud and get pages. Silent correctness bugs are quiet and get discovered when somebody runs a reconciliation report six months later. Choosing between them is choosing which one your system can survive.

## Where UUIDv7 is correct

Most ID generation in most systems is in the UUIDv7 bucket, and the post would be misleading if it didn't say so plainly. Database primary keys where the requirement is uniqueness and chronological clustering for index locality — UUIDv7 is the right call. The cluster-by-recent-time property is genuinely valuable and the lack of strict monotonicity is irrelevant because the key only needs to identify a row, not to prove ordering. Log identifiers and distributed trace IDs are the same shape: downstream consumers sort by timestamp, and "close enough within a millisecond" is the actual requirement. Object storage keys that benefit from a time prefix for lifecycle management. Anywhere the operative phrase is "k-sortable is enough," UUIDv7 is correct and a TSO is a wasted round-trip plus an operational dependency. Most systems do not need a TSO; reaching for one where UUIDv7 would do is a sign that the requirement was not pinned down.

## Where UUIDv7 silently fails

The dangerous cases are the ones where UUIDv7 looks like it works and quietly does not. The pattern is always the same: somebody assumed that "sorts by time" meant "is in the order things happened," and the assumption held for every test case until production traffic produced two events in the same millisecond from different processes.

**MVCC commit ordering.** A distributed database assigning commit timestamps with UUIDv7 will, eventually, commit two transactions in the same millisecond on different shards. Their UUIDv7 commit "timestamps" can sort in either order. A snapshot read that picks a timestamp between them will see one but not the other — except which one is "before" the snapshot depends on the random tail. The same snapshot, run twice, can return different rows. The bug is subtle, intermittent, and impossible to reproduce in a test that does not hit the millisecond collision.

**Snapshot isolation reads.** The application picks a "read as of UUID X" snapshot, intending to see every write committed before X. A write committed at the same millisecond as X, on a different node, can sort either before or after X. The snapshot becomes a non-deterministic view of the database state, and logic that depends on snapshot semantics — a financial report, a reconciliation job, an audit — produces a different answer every time you run it.

**Audit logs proving happens-before.** Compliance regimes that require provable ordering between events ("the consent was recorded before the data was processed") cannot be served by k-sortable IDs. The audit needs to prove that event `A` happened before event `B`, and k-sortable says "probably, within a millisecond, depending on the random tail." A regulator does not accept "probably" as a proof. A TSO does.

**CDC merge across producers.** Multiple shards each produce a stream of UUIDv7-stamped change events. A downstream consumer merges them and applies them in UUIDv7 order, expecting commit order. When two shards commit in the same millisecond, the consumer sees the events in the wrong order and applies them to a derived view that depends on commit order. The view drifts, and the bug shows up as data corruption weeks after the events that caused it.

The fix in every case is the same: pin the ordering requirement first, then pick the tool that meets it. If the requirement is "for correctness, event A must compare less than event B if A was allocated before B, across all producers, always," then k-sortable does not meet it and a TSO does.

## Decision tree

```
Need cross-machine ordering at all?
├─ No  → Use whatever (uuid, sequence, snowflake — doesn't matter).
├─ K-sortable is enough (rough chronological for sorting, not for correctness)?
│  └─ Yes → UUIDv7. Done.
└─ Need strictly monotonic for correctness (MVCC, snapshot isolation, audit)?
   └─ Yes → TSO (or HLC if you can tolerate the trade-offs). Don't try to bridge the gap with UUIDv7.
```

The most common "I tried to bridge" failure is wrapping UUIDv7 in a coordinator that "ensures monotonicity" — usually a singleton service that hands out a per-millisecond sequence to append to a UUIDv7. The pattern reinvents a TSO badly. The coordinator becomes a single point of failure because there is no consensus replication of its sequence state. The round-trip is no faster than a round-trip to a real TSO; both are network calls. The resulting infrastructure has the operational surface of a TSO without the durability guarantees, the failover behaviour, or the test coverage of an existing implementation like `tsoracle`, the FoundationDB resolver tier, or Spanner's TrueTime-backed TSO. The honest move is to drop the wrapper and use a real TSO.

## A pragmatic note

Most teams that *think* they need a TSO actually need UUIDv7 or something even simpler. The instinct that "we need global ordering" is often a vague restatement of "we want sorted IDs," and sorted-by-time is exactly what UUIDv7 gives you for free. Reaching for a TSO where UUIDv7 would do adds a network round-trip, a service to operate, and a new failure mode for no gain. Most teams that *think* UUIDv7 is enough have a correctness gap they have not hit yet — an MVCC interaction, a snapshot read, an audit requirement — that will surface as a quiet bug six months from now. The point of this post is not to push tsoracle; it is to make the distinction sharp enough that you pick correctly. K-sortable is enough until it isn't, and the cost of finding out which side of the line you are on after the fact is much higher than the cost of figuring it out now.

Two follow-ups that go deeper: [Why distributed systems need a TSO](/posts/why-distributed-systems-need-a-tso/) — the foundational post on why cross-machine ordering is hard — and [When you need a TSO](/posts/when-you-need-a-tso/) — the decision framework for picking the right ordering primitive for a given workload.
