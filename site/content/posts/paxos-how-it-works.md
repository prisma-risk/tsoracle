+++
title = "Paxos, in pictures: ballots, promises, and the safety rule that makes it all work"
description = "How classic Paxos decides a single value, why dueling proposers livelock, how Multi-Paxos rescues throughput, and why tsoracle ships an omnipaxos driver next to the openraft one. With animated diagrams you can play through."
weight = 7
[taxonomies]
tags = ["distributed-systems", "paxos", "consensus", "tsoracle", "internals"]
+++

Paxos is the consensus algorithm Leslie Lamport published in 1989, formalized in 1998's "The Part-Time Parliament", and softened into the much shorter "Paxos Made Simple" in 2001 — and which has quietly been doing the actual work of keeping production distributed systems consistent for the better part of two decades. Google's Chubby uses it. Google Spanner runs it on every shard. Cassandra's lightweight transactions are Paxos under the hood. tsoracle's replicated mode runs on Raft via openraft or on Multi-Paxos via a second driver built on `omnipaxos`, a modern Rust implementation of Multi-Paxos. This post is the algorithm itself, told once more in tsoracle's context, with animations that show what the messages actually look like on the wire.

If you have already read [Raft, in pictures](/posts/raft-how-it-works/), most of the framing carries over: a quorum agreeing on a single fact, surviving partitions, surviving crashes. Paxos is what Raft is descended from, and the difference between the two is more about *how the safety argument is stated* than about what the algorithms ultimately compute. If you want the formal treatment, Lamport's [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) is short and famously the most approachable version. For a practical implementation guide most modern Paxos libraries trace their lineage to, van Renesse and Altinbuken's "Paxos Made Moderately Complex" (ACM Computing Surveys, 2015) is the canonical reference.

## The problem Paxos solves

A cluster of machines needs to agree on a single value. Some of those machines might be down. The network might drop or reorder messages. A majority of machines might still be talking to each other — but they don't know which majority — and the cluster must keep making progress whenever a majority does exist. Whatever value is finally chosen, every machine must agree on it forever.

For tsoracle the value, again, is the high-water mark. Whether the underlying log is openraft or omnipaxos, the question is the same: when the cluster agrees on `hwm=21`, the leader can hand out IDs up to and including `21`, and no two machines should ever disagree about that number. Paxos answers the question at the *single-decree* level — agree on one value — and Multi-Paxos extends that into the per-slot log structure that real systems run on top of.

## Single-decree Paxos: ballots, promises, accepts

Paxos uses two phases. A *proposer* picks a unique, monotonically increasing number called a *ballot* (sometimes "proposal number" or "round number"), and runs:

1. **Phase 1 — `prepare(b)`.** Send the ballot to a quorum of acceptors. Each acceptor that has not already promised a higher ballot promises this one, and replies with whatever value it has previously accepted at any lower ballot (or nothing, if it has accepted nothing yet).
2. **Phase 2 — `accept(b, v)`.** Once the proposer has promises from a quorum, it picks the value `v` to propose: if any promise reported a previously-accepted value, the proposer **must** propose the one with the highest ballot. Otherwise the proposer is free to propose its own value. It sends `accept(b, v)` to a quorum.

When a quorum returns `accepted(b)`, the value is decided. It will never change.

{{ paxos_animation(name="paxos-prepare-accept", title="One full Paxos round: prepare, promise, accept, accepted. P1 decides hwm=14.") }}

The two invariants are: (1) each acceptor only promises one ballot at a time, and once it promises ballot `b'` it refuses anything `< b'`; and (2) a proposer that runs phase 2 must adopt any previously-accepted value reported in phase 1. Together they guarantee that once a value is chosen, every future round either learns it or learns nothing — and a proposer that learns nothing about a slot is free to choose its own value, but if anything has been chosen, future rounds converge on it. This is the safety property the rest of the algorithm exists to defend.

The phrase "with no previously-accepted value" in step 2 is doing the heavy lifting. It means: even if a previous round failed mid-flight, even if the proposer crashed, even if half the cluster never saw the accept message, the next round's prepare phase will discover whatever might already be decided. Acceptors are the cluster's memory; the prepare phase is how a new proposer reads that memory.

## Dueling proposers

Single-decree Paxos as described above is safe — no two values are ever both decided — but it is not *live*. Two proposers can take turns leapfrogging each other's ballots and never let a value commit.

{{ paxos_animation(name="paxos-dueling-proposers", title="Two proposers leapfrog ballots. P1's accept arrives too late and is rejected. Classic Paxos livelock.") }}

The pathology is visible: P1 prepares, gets promises, picks a value, sends accepts; P2 prepares at a higher ballot before P1's accepts arrive; acceptors reject P1's accepts because they have promised P2; P1 gives up and retries at a higher ballot; the cycle repeats. Lamport's paper acknowledges this directly — the safety argument does not depend on liveness — and points to the practical fix: elect a distinguished proposer that does not have competition, and let only that proposer run rounds. The election does not have to be Paxos-strict; it only has to be *eventually* unique, since safety is preserved no matter how many proposers compete.

For tsoracle, this is the same reason Raft elects a single leader for the same role. If two leaders ran rounds simultaneously, both would still be safe — no two values ever get both decided — but throughput would collapse to zero. The fix is the same fix: pick one leader and let it work.

## Multi-Paxos: one phase 1, many phase 2s

A real system needs to decide a *sequence* of values, not just one. The naive "run a Paxos instance per slot" works for safety but is wasteful: every slot pays two round trips (prepare + accept), even though most slots have no contention.

Multi-Paxos collapses the steady-state cost to one round trip. A stable leader runs `prepare` *once*, with a high ballot, covering all future slots. The prepare phase tells the leader, for each outstanding slot, whether any value has been accepted at any lower ballot — and if so, the leader takes over those proposals and re-issues them at its own ballot. After that initial reconciliation, the leader proposes new values one slot at a time using only `accept` messages. Each new decision is one round trip from leader to quorum and back. The cost matches Raft's `AppendEntries`.

The interesting moment in Multi-Paxos is the *handoff* — when the leader fails and a new candidate runs its own phase 1 across the outstanding slots.

{{ paxos_animation(name="paxos-leader-takeover", title="Multi-Paxos handoff. L2 prepares at a higher ballot, discovers L1's partial accept, and must adopt it.") }}

Two things happen in the handoff that the safety rule from phase 1 makes load-bearing. First, L2 picks a ballot strictly higher than any L1 ever used — typically by stamping its node ID into the ballot's tiebreaker bits, so two concurrent takeovers can't accidentally collide on the same number. Second, L2's prepare collects every previously-accepted value from the quorum that promises, and for each slot it discovers a value at, L2 must propose that value at its own ballot before it can propose any new ones. The animation shows the simplest non-trivial case: a single slot where one acceptor saw L1's accept and two did not. L2 cannot know whether L1's value had already been decided across some quorum it did not see — so it re-proposes the value at `b=10` to remove the ambiguity. If L1 had decided the value, the cluster still agrees with that decision; if not, the cluster now commits to it. Either way the high-water mark does not regress.

This is also where Paxos and Raft diverge most visibly. In Raft, the new leader's log is brought into alignment by overwriting any uncommitted suffixes that conflict with the new leader's committed prefix. In Multi-Paxos, slots can be decided out of order — slot `N+5` can commit before slot `N+3` — and the leader-takeover phase is what fills in the gaps. Both algorithms reach the same safety guarantee; the bookkeeping is different.

## What this means for tsoracle specifically

tsoracle's `ConsensusDriver` trait was designed to make exactly this kind of swap easy. The trait has three operations — `persist_high_water`, `load_high_water`, and `leadership_events` — and an implementation of those operations is everything the allocator needs to talk to a replicated log. The trait is implemented by `tsoracle-driver-openraft` against an openraft cluster, by `tsoracle-driver-paxos` against an `omnipaxos` cluster, and by `tsoracle-driver-file` against a single local fsync. The `omnipaxos` driver sits on top of `tsoracle-paxos-toolkit` — a glue layer that provides a RocksDB-backed `Storage` implementation for the trait omnipaxos requires, lifecycle helpers for running the omnipaxos state machine inside a tokio runtime, codec wrappers for postcard serialization, and a test-fakes harness for spinning up a paxos cluster in-process.

The reason for shipping a second driver is straightforward: not every stack runs raft. Plenty of services already operate omnipaxos in production, plenty of teams prefer Multi-Paxos's out-of-order slot model for application reasons, and a TSO that requires you to also adopt openraft is not a TSO you can drop into an existing system. The piggyback story from [the openraft post](/posts/wiring-tsoracle-into-openraft/) — where the high-water-mark advance is just another log entry in your service's existing consensus — works identically on omnipaxos. One log, one snapshot, one fsync budget, one election. The variant lives in the envelope.

The omnipaxos driver layer above the toolkit has since landed as `tsoracle-driver-paxos`, with `paxos-standalone`, `paxos-piggyback`, and `paxos-embedded` examples mirroring the openraft ones. This post is the algorithmic background behind it.

## Failure modes worth knowing

The failure cases that matter operationally are the same ones the Raft post covers, restated in Paxos's vocabulary.

**A leader fails mid-decree.** Some acceptors may have received the accept and persisted it; others may not have. The new leader's prepare phase reads every acceptor's most-recent accepted value for the slot in question and adopts the one with the highest ballot. If a quorum had already accepted, the value is decided and the new leader propagates it; if not, the new leader could in principle propose freely — but for safety it always propagates whatever it saw, because it cannot distinguish "this almost made quorum" from "this did make quorum, and I just didn't see the proof".

**A network partition isolates a would-be leader.** The minority side cannot collect a quorum of promises, so its prepare phases fail and no new values are accepted. The majority side elects its own leader at a higher ballot. When the partition heals, the minority's would-be leader sees the higher ballot in any acceptor it talks to, gives up, and rejoins as a follower.

**A follower lags.** The leader's accepts include the slot index, and a follower that missed entries between its last-known slot and the leader's current slot simply requests catch-up. As in Raft, this happens through the same plumbing as normal replication — there is no separate recovery protocol — and the cluster's hot path is unaffected by one slow follower.

The interesting safety arguments — the ones that fill the paper — are about preserving the "no two values both decided" invariant while these transitions happen. Practically, for an operator running tsoracle on either driver, the operational story is the same: run an odd number of replicas, keep them on machines that don't share failure domains, and watch the same dashboards you already have for replication lag and leader stability.

## Paxos vs Raft, briefly

Multi-Paxos and Raft converge on the same shape: a stable leader, a replicated log, a quorum commit point, a snapshot-and-catch-up story for slow followers. The differences are real but smaller than the comparison usually makes them out to be.

Raft's log is a strict prefix — slot `N+1` cannot commit before slot `N`. Multi-Paxos's slots can commit out of order, which is occasionally useful (independent commands don't have to serialize behind a stalled earlier slot) and occasionally a bookkeeping burden (the snapshot has to handle gaps). Raft's election is designed for understandability — a vote per term, a randomized timeout, an explicit step-down rule — while Multi-Paxos's leader election is left more open, often layered as a separate Paxos-flavoured election or as a higher-level mechanism. omnipaxos in particular runs a Ballot/Leader Election (BLE) protocol on top of standard Paxos, and is engineered for graceful behaviour under partial connectivity in ways the original Multi-Paxos was not. In the Rust ecosystem today, openraft is the more mature option for general-purpose use; omnipaxos is catching up quickly and brings useful properties — particularly around partial-connectivity tolerance — that the original Multi-Paxos handled less gracefully.

The choice between them is rarely about correctness. It is about the rest of your stack.

## When to dive deeper

Read [Lamport's "Paxos Made Simple"](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) for the algorithm in its shortest correct form, then van Renesse and Altinbuken's "Paxos Made Moderately Complex" (2015) for the implementation-shaped version most modern libraries trace their lineage to. For the omnipaxos extensions specifically, the [omnipaxos project page](https://omnipaxos.com/) links to the paper and the reference implementation. If you want to keep going inside tsoracle, [How tsoracle's window allocator works](/posts/how-the-window-allocator-works/) is the layer above the consensus driver, and [Wiring tsoracle into openraft](/posts/wiring-tsoracle-into-openraft/) is the integration story for the openraft driver — most of which carries over verbatim to the omnipaxos driver. The earlier posts in the series — [Why distributed systems need a TSO](/posts/why-distributed-systems-need-a-tso/), [TSO vs UUIDv7](/posts/tso-vs-uuidv7/), and [When you need a TSO](/posts/when-you-need-a-tso/) — are the *whether* before any of this matters.
