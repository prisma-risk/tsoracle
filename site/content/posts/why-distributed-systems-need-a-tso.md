+++
title = "Why distributed systems need a timestamp oracle"
description = "Cross-machine event ordering is one of the foundational problems in distributed systems. Wall clocks don't solve it, database sequences don't scale, and consensus algorithms only get you partway. Here's what a timestamp oracle is, why Spanner / CockroachDB / FoundationDB all use one internally, and when you should reach for one yourself."
weight = 1
[taxonomies]
tags = ["distributed-systems", "tso", "ordering"]
+++

## The problem: ordering events across machines

Picture a write-heavy service sharded across thirty-two Postgres instances. Each shard owns a slice of customer accounts. The product team wants a change-data stream: every row mutation, in commit order, fed into a downstream warehouse and a search index. Within one shard the order is obvious — the database hands you a per-shard log sequence number and you read it monotonically. Across shards, the order is not obvious at all. Shard 14 just committed `update accounts set balance = balance - 10 where id = 7`. Shard 22 committed `insert into ledger ...` ten microseconds later. The downstream system needs to know which happened first, because the search index has a derived view that joins the two. If you replay them out of order, the view is wrong until the next full rebuild.

This is the ordering problem in distributed systems, and it is older than most of the systems that have it. The trouble is that "happened first" is only obvious within one process. Across processes — and especially across machines connected by a network — there is no shared clock you can trust, no shared register you can read, no oracle of physical time you can consult. Each shard has its own view of the world and is free to commit transactions at whatever rate it can sustain. Putting those transactions in a single global order requires a coordination protocol of some kind, and the choice of protocol determines what you can build on top.

"Ordering" is not one concept. It is at least three, and the distinctions matter for what follows. **Total order** is a single linear sequence of every event in the system; any two events `A` and `B` have a definite relationship, either `A` before `B` or `B` before `A`, never both, never neither. **Causal order** is weaker: it only orders events that could have influenced each other through some chain of message-passing. Two concurrent writes on separate shards with no shared client and no message between them are unordered under causal order, even if a wall clock would put one before the other. **Real-time order** is the strongest: it agrees with what an external observer with a perfect stopwatch would see, including events that happen on physically separate machines with no causal link.

The use cases that show up in practice need different flavors. MVCC and snapshot isolation in a distributed database need a total order on commits, because every transaction picks a read timestamp and needs to see a consistent snapshot of the data at that instant. Cross-shard transactions need a common commit timestamp that all participating shards agree on. Change-data merge from many shards into one downstream consumer needs a total order so the consumer can apply rows in a deterministic sequence. Audit logs that claim to record real-world happens-before — "the user clicked X before the system did Y" — need real-time order, not just causal order. None of these can be served correctly by per-shard sequence numbers alone.

## Why naive solutions don't work

### Wall clocks

The first instinct is to read the system clock on each shard and tag every event with the local time. The downstream consumer sorts by that timestamp and replays in order. This fails in two ways. First, NTP-synchronised clocks across a fleet of machines are typically accurate to about ten milliseconds, sometimes worse on virtualised hardware. Two events that occur on different shards within the same NTP-accuracy window can have timestamps in the wrong order. A write that commits at real time `t = 100ms` on a fast-clock host can get stamped earlier than a write that commits at `t = 95ms` on a slow-clock host. Second, two events can get exactly the same timestamp; clocks tick in discrete units and concurrent calls to `clock_gettime` on different machines collide often. The "take the larger timestamp" trick produces neither uniqueness nor a strict ordering.

```python
# Two processes on different hosts, NTP-synced to ~10ms.
# Both call time.time() at what appears, locally, to be the same instant.
host_a> time.time()   # 1716240000.123456
host_b> time.time()   # 1716240000.123456   # identical float
# Which one happened first? Wall clocks cannot tell you,
# and the NTP skew between A and B might be 8ms in either direction.
```

Spanner is the famous counterexample, and it deserves a careful treatment. Spanner uses TrueTime, an API that returns `[earliest, latest]` bounds on the current real time. The bounds are tight — single-digit milliseconds — because Google installs GPS receivers and atomic clocks in every datacenter and runs a time-master service that bounds the worst-case skew across the fleet. Spanner uses those bounds by waiting out the uncertainty interval at commit time. If you do not have GPS and atomic clocks in your racks, you cannot build TrueTime. Spanner's design works because of the hardware investment, not because they discovered a clever software trick.

### Per-host monotonic clocks

Linux gives you `CLOCK_MONOTONIC`, which is guaranteed to never go backward within a single boot. This is useful for measuring intervals on one machine. It tells you nothing across machines, because the zero point and the tick rate differ on every host. Two monotonic readings from different machines are not comparable in any meaningful way.

### Database sequences

A single Postgres or MySQL instance can serve a `nextval` sequence and you get strictly increasing integers, fast, with no client coordination. As soon as the workload outgrows one instance, the sequence stops working. You cannot extend a database sequence across shards without either funnelling every allocation through one node — which defeats the point of sharding — or introducing gaps and ranges that lose the strict total order. The practical ceiling on a single-DB sequence is somewhere in the low thousands of allocations per second before contention on the sequence row dominates everything else.

### Logical clocks

Lamport timestamps were the first principled answer. Each process maintains a counter; every send and every receive updates the counter to `max(local, received) + 1`. The result is a counter that respects causal order: if event `A` causally precedes event `B`, then `A`'s timestamp is less than `B`'s. The shortcoming is that Lamport clocks do not respect real-time order. Two events with no message between them can have timestamps in any order, and the system has no way to tell which actually happened first. For audit logs and CDC streams that need to reflect real-world ordering, Lamport timestamps are not enough.

### Hybrid logical clocks

Hybrid logical clocks combine a physical timestamp with a logical counter. The physical part is bounded by the wall-clock skew across the cluster; the logical part advances on every event to handle the case where wall clocks collide. CockroachDB uses HLCs and gets a useful property: within the bound of clock skew across the cluster (CockroachDB's default is 500ms), HLCs are consistent with real-time order. They also respect causal order, like Lamport clocks. This is a real improvement and is enough for many workloads. The trade-offs are: HLCs need gossip or heartbeats to keep clocks in sync, the real-time guarantee is bounded by the assumed maximum skew (and is violated if the assumption is wrong, with bad consequences for transactions that rely on it), and HLCs do not give you strictly monotonic global integer IDs in the form most downstream consumers want. You still need a coordination layer if you want each event to carry a unique, dense, strictly increasing ID.

## Enter: the timestamp oracle

A timestamp oracle is a service that hands out strictly increasing integer IDs on request. The contract is narrow and that is the point. You ask the TSO for an ID, you get back an integer. If your call returns `N` and someone else's call returns `M`, and `N > M`, then your call entered the TSO after theirs. The integer itself is opaque: it might encode physical time in the high bits and a logical counter in the low bits, or it might be a plain monotonic counter; clients are not supposed to care. What they care about is that the order of IDs reflects the order of allocations, globally, across every shard and every client.

The cost model is concrete and worth internalising. Each allocation is one round-trip from the client to the TSO leader. Throughput is bounded by the leader's ability to fsync the high-water mark to durable storage and the consensus layer's ability to replicate it; latency is bounded by the network round-trip plus that fsync. The standard trick is batched allocation: a client asks for a window of `1000` IDs at once, the TSO allocates the next window and replicates it, the client hands them out locally as fast as it likes. With batching, throughput stops being the bottleneck and the design is dominated by tail latency on the consensus path. A correctly built TSO on commodity hardware does millions of IDs per second per cluster without breaking a sweat.

Every distributed database you have heard of either has a TSO or has chosen a specific alternative and paid for that choice. **Google Spanner** combines TrueTime with a Paxos-replicated TSO; the TSO assigns the commit timestamp and the TrueTime wait at commit time ensures external consistency. The combination needs the hardware to be cost-effective. **CockroachDB** chose HLCs instead of a centralised TSO, paying for that with a maximum-skew assumption and getting back a TSO-free architecture where every node can stamp events locally. It is a different point on the design spectrum, and it has both costs and benefits depending on the workload. **FoundationDB** uses a small set of stateless proxies and a resolver tier; the proxies allocate read versions and commit versions in collaboration with the resolvers, which serves the same role a TSO serves in the others — a coordinated, monotonic stream of commit versions that orders the entire cluster.

## When you reach for a TSO

You reach for a TSO when you need a total order on events that real wall clocks cannot give you. The pattern that shows up over and over: MVCC with consistent snapshot reads across shards, where every reader picks a snapshot timestamp and needs to see exactly the state at that timestamp on every shard it touches. Snapshot isolation with the same shape: writes get a commit timestamp; readers pick a read timestamp; the invariant is that a read at time `T` sees every write committed at `T' < T` and no write committed at `T' > T`. Cross-shard transactions where multiple shards must agree on a single commit timestamp so that the transaction appears atomic to outside observers. CDC merge from many shards into one consumer, where the consumer applies events in TSO order and gets a deterministic, replayable stream. Audit logs that need real happens-before — "the password was rotated before the unauthorised login attempt" — and cannot tolerate the wall-clock-skew failure modes that would make the audit defensible only within a margin.

You do not reach for a TSO when something weaker is enough. Single-DB CRUD applications do not need one; the DB sequence is fine and you would only add latency and operational surface. Most analytics workloads, where the merge order does not need to be the real commit order and approximate ordering by event-time is enough, do not need one. Log identifiers that just need to be globally unique and roughly time-sortable do not need one; UUIDv7 or ULID is the right tool. Many event-driven systems get away with HLCs or simple per-shard sequences plus a deterministic tiebreaker. The decision framework — when the weaker tools really are not enough — is its own topic; the short version is that you need a TSO when you need a real total order with no skew assumption, and you do not when you do not.

## A small one in Rust

A TSO does not need to be a six-figure engineering effort. The core is small. You need a window allocator that fsyncs the high-water mark forward, a consensus layer that replicates the high-water mark so a leader crash does not regress IDs, and a client that knows how to find the current leader and batch-fetch IDs. That is the whole shape. The implementation work is mostly in the consensus integration, the leader-failover client behaviour, and the test surface for the failure modes.

`tsoracle` is one implementation of that shape. It is an embeddable Rust crate that ships a single binary, replicates via [openraft](https://github.com/databendlabs/openraft) by default, exposes a pluggable consensus trait for other backends, and includes a gRPC client driver with leader discovery and batched allocation built in. It is small enough to read in an afternoon and operate as a single replicated component next to the rest of your stack.

```bash
cargo install tsoracle
tsoracle serve
```

If you are at the point of reading this post, you probably already know whether you need a TSO. If you do, `tsoracle` is a small Rust one worth a few minutes of evaluation. Two follow-ups that readers usually want next: [tso-vs-uuidv7](/posts/tso-vs-uuidv7/) for the comparison with the most common alternative, and [When you need a TSO](/posts/when-you-need-a-tso/) for the decision framework — when the weaker tools are enough and when they are not.
