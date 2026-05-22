+++
title = "When you need a TSO (and when you don't)"
description = "A decision framework for picking a distributed ID strategy. Most systems do not need a timestamp oracle. The ones that do can't substitute anything weaker. This post walks through the cases on both sides and shows where TSOs, HLCs, database sequences, UUIDv7, and Snowflake-style IDs each belong on the cost/correctness frontier."
date = 2026-05-23
[taxonomies]
tags = ["distributed-systems", "tso", "ids", "decisions"]
+++

The honest answer to "do I need a timestamp oracle?" is, for most readers, *probably not*. But the systems that do cannot substitute anything weaker without taking on a correctness gap that will not show up in testing. The cost of getting this wrong in one direction is operational overhead — a service to run, a failure mode to monitor. The cost in the other direction is silent data corruption that surfaces months later, when the original transactions are too old to repair cleanly. The first mistake is fixable on a Tuesday; the second is a project. This post is an attempt to pin the line.

## You do not need a TSO if

You are building a CRUD application on a single database. Primary keys come from `bigserial` or a UUIDv7 generator, and no second writer could disagree about what order things happened. The database's transaction log is the global order; adding a TSO adds a round-trip on every write for no property you care about.

Your "distributed" system is two or three application servers behind a load balancer, all talking to the same Postgres or MySQL. The application tier is horizontally scaled; the data tier is not. There is no cross-machine ordering problem because all writes funnel through one place that already serializes them.

You need globally-unique IDs but the order does not matter. Auth tokens, opaque resource identifiers in URLs, idempotency keys, S3 object names. UUIDv4 is fine; UUIDv7 is fine if you also want approximate time-bucketing. A TSO is overkill because the ordering guarantee it sells you is not a property anyone is going to read.

You need rough chronological ordering for a UI, but no correctness depends on exact order. A feed of recent activity, a list of notifications, an admin view of recent signups. If two events from the same millisecond display in either order, no user will notice. UUIDv7 or a Snowflake-style ID gives you "newest first" with no coordination overhead.

You are using a database that already provides snapshot isolation locally — Postgres with MVCC, MySQL with InnoDB — and you are not sharding it. The database is already running a TSO internally, in the form of its own transaction ID allocator, exposed as the snapshot semantics of its SQL interface. Reaching for an external TSO from application code that talks to a single such database is reaching past a perfectly good solution to build one yourself.

The cumulative cost of adopting a TSO when you do not need one is a replicated service in your request path: an extra hop per allocation (mitigated by batching, not eliminated), a component to monitor, a failure mode to learn. Do not do this.

## You do need a TSO if

You are building MVCC across shards. A read transaction picks a snapshot timestamp and needs every shard to return rows committed at or before that timestamp. Per-shard sequence numbers cannot be compared across shards; NTP-skewed wall clocks can put writes in the wrong order; UUIDv7 timestamps within a millisecond can sort either way. The failure is silent — the read returns a consistent-looking result that is not actually a consistent snapshot, and any business logic that depends on the snapshot being a real point in time produces a quietly wrong answer.

You need snapshot isolation across multiple independent databases — Postgres instances sharded for capacity, or a Postgres plus an OLAP store that need to agree on "as of when." Each database has its own transaction order but no shared notion of time with the others. A TSO sitting outside all of them gives you the common reference. Without one, you are reduced to picking wall-clock times and hoping the skew is small enough.

You are merging change-data from many shards into a single ordered downstream stream where the consumer depends on commit order — a search index keyed by commit timestamp, a derived view that joins rows from multiple shards. UUIDv7 silently puts events in the wrong order within a millisecond window; the consumer applies them in the wrong order; the derived state drifts; the bug surfaces weeks later as a row that should have been deleted but is still there.

You are building audit logs that need to *prove* happens-before across machines for compliance. The audit needs to defend "the consent was captured before the data was processed" in front of a regulator. K-sortable IDs say "probably, within a millisecond, depending on the random tail." That is not a proof. A TSO produces a strict order that can be presented as evidence: ID `N+1` was allocated after ID `N`, by construction.

You are building distributed transactions that need a single commit timestamp all participating shards agree on. The coordinator picks a commit time every shard stamps into its log, ordering this transaction correctly against every other. A TSO is the natural source. The alternatives are **Spanner**'s TrueTime (which requires the hardware) or HLCs (which require the skew assumption); both have their place, but neither is "free" in the way that "use the database sequence" is free for single-instance workloads.

## Alternatives

| Tool | Globally monotonic | Generated locally | Failure mode | When it's the right call |
|---|---|---|---|---|
| Database sequences | Within DB only | No (DB round-trip) | Doesn't scale across DBs | Single-DB systems |
| UUIDv7 | No (k-sortable) | Yes | Silent ordering bug | Rough sort, unique IDs |
| Snowflake / Sonyflake | No (k-sortable, per-node) | Yes | Silent ordering bug, clock-skew issues | Per-node monotonic, can tolerate skew |
| Logical clocks (Lamport) | Causally yes, real-time no | Yes | Requires acknowledgement chain | Causal-only ordering |
| HLCs | Bounded by wall-clock skew | Yes | Skew bounds violations | Causal + wall-clock approximation |
| TrueTime | Yes (within Spanner) | Sort of (GPS/atomic) | Requires specialized hardware | If you have Google's hardware |
| TSO | Yes | No (service round-trip) | Outage if TSO down | Need strict monotonicity, can tolerate one round-trip |

The row most often misread is **Snowflake / Sonyflake**. The construction is 41 bits of millisecond timestamp, 10 bits of node ID, 12 bits of per-node sequence. Within a single node, the sequence bits give strict monotonicity at up to 4096 IDs per millisecond. Across nodes it is k-sortable on the timestamp prefix and arbitrary on the node-ID prefix: if two nodes generate IDs in the same millisecond, the one with the lower node ID sorts first regardless of which event actually happened first.

Snowflake is the right call for many use cases — per-node monotonic identifiers for a sharded event store, where each shard's events sort correctly among themselves and cross-shard order does not matter. What Snowflake is not is a TSO. If two producers' clocks differ by ten milliseconds — normal on NTP-synced hardware — their IDs can be off by ten milliseconds in either direction. For applications where that is sort jitter, fine. For applications where that is a correctness violation, Snowflake is not the right tool, and dressing it up as one is the same mistake as wrapping UUIDv7 in a coordinator.

## The cost / correctness frontier

Every tool sits at some point on two axes. The cost axis is the operational overhead of running it plus the latency it adds per allocation. UUIDv7 and Snowflake are at the bottom: zero operational cost beyond a library, no round-trip. Database sequences are one step up: every allocation is a DB round-trip. HLCs are at a similar cost level to local generators, plus a gossip mechanism for skew bounds. TSOs are one step further: a replicated service in the request path, with batching keeping per-ID latency in microseconds. TrueTime is at the top: specialized hardware in every datacenter.

The correctness axis runs from k-sortable through causal to strictly monotonic. K-sortable is weakest: UUIDv7 and Snowflake sort by time bucket but not within a bucket. Causal is the middle: Lamport timestamps order causally-related events correctly but say nothing about events with no message between them; HLCs add a bounded wall-clock approximation on top. Strictly monotonic is strongest: a TSO or TrueTime produces a global total order with no skew assumption.

There is no free lunch. Every step up the correctness axis costs something on the cost axis. The decision is not "pick the strongest tool" — it is "pick the cheapest tool that actually meets the correctness requirement," and that requires writing the requirement down before shopping.

## How to make this decision

Write down the failure mode that would matter most for your system. The prompt that exposes the requirement quickly: *if event A happens at machine X and event B happens at machine Y, and the system thinks B happened first when A actually did, what breaks?*

If the answer is "nothing that anyone will notice," you do not need cross-machine ordering at all. Use whatever is simplest — a database sequence, a UUIDv4, an opaque token.

If the answer is "a UI sort looks slightly weird for one millisecond, then catches up on the next refresh," you need k-sortable ordering. UUIDv7 or Snowflake is the right call. Stronger tools are not justified by a UX shimmer.

If the answer is "we would compute a derived view that's wrong, commit a transaction that should have aborted, or fail an audit because we cannot prove the order of two events," you need strict monotonicity. A TSO is the right tool. HLCs are a credible alternative if your workload tolerates the maximum-skew assumption. TrueTime is the right tool if you happen to be Google. Wrapping UUIDv7 in a coordinator is not; that pattern reinvents a TSO without the durability or failover work.

The decision is the failure mode you are protecting against, not the abstract elegance of the model. "Strongest" is not the same as "correct for this workload."

## Closing

Most systems do not need a TSO. The ones that do cannot substitute anything weaker without taking on a silent correctness gap. That is the whole framework. Picking honestly is more important than picking sophisticatedly: reaching for a TSO when UUIDv7 is enough is a cost paid forever, and reaching for UUIDv7 when only a TSO will do is a debt paid all at once when the bug surfaces. Write down the failure mode, find the cheapest tool that closes it, stop.

Two posts that go deeper: [Why distributed systems need a TSO](/posts/why-distributed-systems-need-a-tso/) — the foundational post on why cross-machine ordering is hard — and [TSO vs UUIDv7](/posts/tso-vs-uuidv7/) — the head-to-head comparison with the most common alternative.
