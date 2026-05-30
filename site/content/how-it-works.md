+++
title = "How tsoracle works"
description = "A five-minute orientation to tsoracle's architecture: window allocation, consensus replication, and the crash-safety contract."
template = "how-it-works.html"
+++

tsoracle hands out two kinds of strictly-ordered integer: *timestamps* (`get_ts`), for ordering events across machines, and *gapless sequences* (`get_seq`), for handing out consecutive IDs with no waste. Both ride the same engine. The architecture has three moving parts: a *window allocator* that turns one disk-fsync into many IDs, a *consensus driver* that replicates the high-water mark so a leader crash never rewinds, and a *gRPC layer* that handles leader discovery and batched requests from clients. This page is the five-minute orientation; the [DeepWiki](https://deepwiki.com/prisma-risk/tsoracle) reference is the full prose documentation.

## The request path

{{ sequence_flow(name="get-ts-flow", title="get_ts(batch=N) request path") }}

A client asks the leader for a batch of `N` IDs. The leader picks the next range `[a, a+N)`, advances the on-disk high-water mark past `a+N`, fsyncs, replicates the advance via raft, and returns the range. The client hands out IDs from the range locally as fast as it likes. The next batch request triggers another fsync; the cost of the fsync is amortised across all `N` IDs.

## Window allocator

The allocator owns one piece of state: the high-water mark, an integer that no future ID may equal or fall below. A batch request asks the allocator for `N` IDs; the allocator picks the start `a` (`>= current high-water mark`), advances the high-water mark past `a+N`, persists the new high-water mark, and returns the range. Crash semantics are simple by construction: if the process dies before the fsync, no ID from the batch has been handed out; if it dies after, the new high-water mark is on disk and the batch is committed. There is no torn state to recover.

{{ window_timeline(name="window-allocator", title="Three consecutive window flips. Click to step through.") }}

## ConsensusDriver trait

In a replicated topology, the high-water mark must survive leader failover. The `ConsensusDriver` trait is the narrow interface tsoracle uses to talk to a replicated log; the production implementations back it with [openraft](https://github.com/databendlabs/openraft) and [omnipaxos](https://omnipaxos.com/), but a small trait surface lets you wire tsoracle into raft-rs, etcd's raft, or your service's own raft if you want to piggyback. Every high-water-mark advance is proposed through the trait; the new leader after a failover only hands out IDs above the last committed advance.

{{ cluster_state(name="cluster-overview", title="A 3-node tsoracle cluster.") }}

## Crash-safety contract

The contract is that no ID is ever issued without first persisting (and, in a replicated cluster, replicating) the advance that covers it. The single fsync per batch is the durability proof; the raft commit is the failover proof. Together they guarantee that, across crashes and leadership changes, the issued sequence stays strictly increasing.

## Gapless sequences

A timestamp can skip values — its packed `physical_ms`/`logical` form resets the logical counter every millisecond, so consecutive grants leave gaps, and that is fine because a timestamp only has to be *ordered*, never *contiguous*. Some workloads need the stronger property: a dense, gapless run of integers with nothing skipped. Surrogate primary keys, invoice and order numbers, ledger line numbers, partition offsets — anywhere a human or an auditor expects `…, 41, 42, 43, …` with no holes. That is what `get_seq` provides.

{{ sequence_flow(name="get-seq-flow", title="get_seq(key, N) request path") }}

`get_seq(key, N)` reserves a contiguous block `[start, start + N)` from a counter named by `key`, advances that counter past `start + N`, persists and replicates the advance, and returns the block. `start` is the durably-committed pre-advance value; ordinals are never reused, even across leader transitions or restarts. Each `key` is an independent counter — `orders` and `users` advance separately — so one deployment can back many sequences. The same window-allocator trick applies: ask for a block of a thousand and amortise the fsync across all thousand IDs.

The difference that matters is **idempotency**. `get_ts` is idempotent: a timestamp grant the client never received is simply wasted, and the client can safely retry. A gapless advance cannot be wasted — the counter has already moved — so `get_seq` is non-idempotent by construction. If a request fails *after* it may have committed (a post-send timeout, an ambiguous transport error), the client cannot know whether the block was spent, and retrying risks a double-spend. The client surfaces that as a distinct `SeqUncertain` outcome rather than silently retrying; the caller reconciles (read the counter back, or tolerate a one-block gap) instead of guessing. This is the honest cost of gaplessness, and it is the one place the `get_seq` contract is deliberately weaker than `get_ts`.

Sequence support is a property of the consensus driver. The file driver and the openraft (raft-replicated) driver implement it today; the OmniPaxos driver is on the roadmap. A driver that does not support dense sequences answers `get_seq` with `UNIMPLEMENTED` — never a disguised error — so the limitation is diagnosable at the first call.

## Read more

- [Why distributed systems need a TSO](/posts/why-distributed-systems-need-a-tso/) — the foundational post on why cross-machine ordering is hard.
- [When you need a TSO](/posts/when-you-need-a-tso/) — the decision framework.
- [TSO vs UUIDv7](/posts/tso-vs-uuidv7/) — head-to-head with the most common alternative.
- [Gapless sequences: GetSeq alongside GetTs](/posts/gapless-sequences-with-getseq/) — the dense, gapless ID primitive and when to reach for it.
- [How tsoracle's window allocator works](/posts/how-the-window-allocator-works/) — the algorithm in depth.
- [Wiring tsoracle into openraft](/posts/wiring-tsoracle-into-openraft/) — the integration story.
- [DeepWiki](https://deepwiki.com/prisma-risk/tsoracle) — long prose reference.
