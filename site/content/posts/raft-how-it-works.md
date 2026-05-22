+++
title = "Raft, in pictures: elections, quorum, and tsoracle's high-water mark"
description = "How Raft elects a leader, replicates entries by quorum, and why tsoracle's high-water mark is just another entry on the same log. With animated diagrams you can play through."
weight = 6
[taxonomies]
tags = ["distributed-systems", "raft", "consensus", "tsoracle", "internals"]
+++

Raft is the consensus algorithm a working distributed system reaches for when it needs one machine's word to count for everyone — a single leader, an agreed log of decisions, durable across crashes and partitions. tsoracle's replicated mode uses it; so does etcd, CockroachDB, TiKV, and most of the storage layer beneath the modern infrastructure stack. The algorithm is older than this paragraph by about a decade and is well-explained at [raft.github.io](https://raft.github.io), which includes an interactive cluster you can drag nodes around in. This post is the same story told once more in the context of `tsoracle` specifically, with animations that show what the message flow actually looks like — leader elections in two different scenarios, and the quorum replication that carries each high-water-mark advance.

If you want the formal treatment, read [the original paper](https://raft.github.io/raft.pdf) — it's short and famously approachable. If you want to play, [raft.github.io's RaftScope demo](https://raft.github.io/) lets you take nodes offline and watch the cluster recover. This post is the middle path: enough motion to see the shape, enough prose to ground it in a concrete use case.

## The problem Raft solves

Pick a fact that has to be the same on every machine in a small cluster. The cluster runs through network partitions. The cluster runs through individual machine failures. The cluster keeps making progress whenever a majority of machines can talk to each other. Every machine should agree on the same fact regardless of which network it can see right now. Consensus is the name of this problem; Raft is one well-engineered solution.

For tsoracle the fact is the high-water mark — the integer that bounds which timestamps can be issued. If one node's high-water mark says `100` and another's says `200`, the cluster can issue `150` twice and the strictly-monotonic invariant is broken. The mark has to live somewhere that is the same across the cluster, that survives any single machine's death, and that advances on writes from a single coordinator. That place is the raft log; the coordinator is the raft leader.

## Leader election

A Raft cluster always has exactly one leader at a time (or zero, briefly, during transitions). All writes go through the leader; only the leader can propose new entries to the log. When the leader fails — or its heartbeats stop reaching the followers — the cluster elects a new one.

Each follower runs an *election timeout*: a randomized clock that starts whenever the last heartbeat from the leader arrived. The randomization is the magic. Different followers have different timeouts, so they don't all decide to start an election at the same moment. When a follower's timeout fires, it transitions to *candidate*, increments its term number, votes for itself, and asks every other node for a vote. If a quorum (majority) grants the vote, the candidate becomes the new leader and starts heartbeating.

The whole thing plays out in under a second on a healthy cluster. Click play below to watch it.

{{ raft_animation(name="election-normal", title="Normal election: one candidate wins quickly.") }}

The two key invariants are: (1) each follower can vote *once* per term, and (2) a candidate needs strictly more than half of the nodes to grant a vote. Together they guarantee that at most one leader is elected per term. A node that hears about a higher term during a vote immediately steps down to follower in that new term — so even a stale "leader" that has been partitioned away will recognize a fresh leader on rejoin and stop trying to lead.

### Split votes are self-correcting

The randomized timeout isn't just an optimization. It's the mechanism that breaks ties. Without randomization, every follower's timeout would fire at the same instant when the leader died, every follower would become a candidate, every follower would vote for itself, and no one would have quorum.

With randomization, two candidates can still time out close enough to collide on rare bad luck. Watch what happens:

{{ raft_animation(name="election-split-vote", title="Split vote: two candidates collide, neither reaches quorum, fresh timeouts resolve it.") }}

Neither candidate wins because each has voted for itself in term the next term and refuses to grant a second vote in the same term. The protocol's response: both fall back to follower, randomize a fresh timeout, and try again. The retry distribution is wide enough that the next round is overwhelmingly likely to produce a single candidate. After two or three rounds at most, the cluster has a leader.

For tsoracle this matters because *no IDs are issued during the gap*. The window allocator on the previous leader stopped issuing the moment its `propose_advance` calls started failing — so the cluster is in a degraded "no new timestamps" mode until the election completes. A typical election finishes in 150–500 ms on a healthy cluster; the gap is mostly invisible to clients, who see it as a brief latency spike on the next `get_ts` after the failover.

## Log replication and quorum

Once a leader is in place, work flows through it. The leader receives a request — for tsoracle, that's a high-water-mark advance — appends it to its local log, and replicates it to followers via `AppendEntries` RPCs. The replication is the load-bearing part for durability.

An entry isn't *committed* until a quorum of nodes (the leader + at least half of the others) has written it to their logs. The leader tracks how far each follower has caught up, and advances its commit index whenever the entry at position `N` has been replicated to a majority. Only after commit does the leader apply the entry to its state machine and acknowledge the original client request. Followers apply the entry to their own state machines once they observe the leader's commit index advance past it.

{{ raft_animation(name="quorum-replication", title="Replicating hwm=16 by quorum. The entry commits when N2 acks; N3 catches up after.") }}

Three things happen in that animation worth pausing on. First, the leader replicates *concurrently* — N2 and N3 are written to in parallel, not sequentially. Second, the commit happens as soon as a *quorum* exists (N1's own copy plus N2's ack is 2/3, which is the majority for a 3-node cluster). The leader doesn't wait for N3 before committing — if N3 is slow, the cluster doesn't stall on it. Third, the commit notice that propagates afterwards is the leader telling followers "you can apply entry 16 now"; it doesn't carry data, just an index.

This is how tsoracle's `HighWaterCommand` rides the log. Each advance is an `AppendEntries(HighWaterCommand { mark: N })` to the followers. The state machine on each replica, when it applies the entry, advances its own copy of the high-water mark. The leader's `propose_advance` call returns success as soon as quorum is reached — at that point the new mark is durable across the cluster and the leader can hand out IDs in the new range.

The fault tolerance arithmetic falls out of this: a cluster of `2f+1` nodes survives `f` failures. Three nodes survive one; five nodes survive two. The cost of going from three to five is more replication traffic per entry and a slightly higher per-write latency, in exchange for tolerating a second simultaneous failure. Five-node clusters are the default for production tsoracle deployments that need to ride through correlated-failure scenarios (an availability-zone outage, a host-level hardware fault that affects a rack).

## What this means for tsoracle specifically

The piggyback pattern from [the openraft post](/posts/wiring-tsoracle-into-openraft/) is the punchline this post is building up to. Your service already runs raft for its own state. The state machine apply step looks like:

```rust
fn apply(&mut self, entry: LogEntry) {
    match entry {
        LogEntry::App(cmd) => self.app_state.apply(cmd),
        LogEntry::Tso(cmd) => self.hwm.advance_to(cmd.mark),
    }
}
```

Two variants on the same log envelope. Both halves get the same election guarantees, the same quorum durability, the same commit semantics. There is no second raft cluster. There is no second consensus protocol to operate. The TSO's correctness story is now exactly your service's correctness story.

This is why narrow trait surfaces matter. tsoracle's `ConsensusDriver` doesn't know whether the log it's writing to also carries your application's commands; it just calls `propose_advance(mark)` and `await_commit(mark)`. The openraft driver wires those calls into the same `AppendEntries` flow you saw above. Everything else — the election, the log catch-up after a partition, the snapshot install — is openraft's job and your existing operational story.

## Failure modes worth knowing

Raft's safety story holds across the cases that distributed systems writers spend pages on. The summary, for tsoracle's purposes:

**A leader fails mid-write.** The proposed entry may or may not be replicated; it may or may not be committed. On election, the new leader's log contains everything committed in the old term, plus possibly some uncommitted suffixes from later terms. The new leader brings followers' logs back into sync by either overwriting uncommitted suffixes (if they conflict) or extending them. Once stable, replication resumes from the new commit index. tsoracle's invariant — no ID issued without a committed advance — holds because the previous leader's `propose_advance` call either returned success (entry committed, IDs safe) or returned an error (no IDs were ever issued from the would-be window).

**A network partition isolates the leader.** The leader's heartbeats stop reaching the majority side. The majority elects a new leader in a higher term. When the partition heals, the old leader sees the higher term, steps down to follower, and discards any uncommitted entries beyond the new leader's commit index. Any in-flight `propose_advance` on the old leader either races to finish before partition (in which case it succeeded normally) or returns an error (in which case the request retries against the new leader via `LeaderHint`).

**A follower lags behind for a long time.** Catch-up is incremental: the leader sends `AppendEntries` containing the missing range from the follower's last known index. If the follower lagged so far that the log has been snapshotted past its index, the leader sends a snapshot install instead. Either way the follower converges to the current log without affecting the cluster's hot path.

The interesting cases — the ones that fill the original Raft paper with diagrams — are about preserving safety while these transitions happen. Practically, for an operator running tsoracle, the questions to ask are: do my heartbeats fire often enough relative to network jitter (yes, by default); is my election timeout long enough that I don't elect leaders during transient packet loss (yes, by default); and do I have enough nodes that a single failure doesn't take the cluster offline (run three nodes, or five if you can spare them).

## When to dive deeper

This post is enough to read the [openraft post](/posts/wiring-tsoracle-into-openraft/) and the [window allocator post](/posts/how-the-window-allocator-works/) with the consensus layer demystified. It is not a substitute for the [Raft paper](https://raft.github.io/raft.pdf) (which formalizes the safety arguments) or the [raft.github.io interactive demo](https://raft.github.io/) (which lets you push the cluster harder than a scripted animation can). If you're operating a tsoracle deployment, those two are the right next reads.

The two posts above are the layers immediately above and below this one. [How tsoracle's window allocator works](/posts/how-the-window-allocator-works/) is the layer that runs on top — the algorithm that turns a quorum-committed high-water mark into a usable timestamp stream. [Wiring tsoracle into openraft](/posts/wiring-tsoracle-into-openraft/) is the integration story — how the `ConsensusDriver` trait connects to openraft's `AppendEntries`. The first three posts in the series — [Why distributed systems need a TSO](/posts/why-distributed-systems-need-a-tso/), [TSO vs UUIDv7](/posts/tso-vs-uuidv7/), and [When you need a TSO](/posts/when-you-need-a-tso/) — are the *whether* before the *how*.
