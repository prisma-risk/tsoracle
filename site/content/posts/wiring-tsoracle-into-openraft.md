+++
title = "Wiring tsoracle into openraft"
description = "A timestamp oracle needs a replicated log under it. Inside tsoracle's ConsensusDriver trait, the openraft driver, and the envelope pattern for piggybacking on your service's existing raft."
weight = 5
[taxonomies]
tags = ["distributed-systems", "tso", "tsoracle", "raft", "internals"]
+++

A single-node timestamp oracle is a useful pedagogical artefact and an unacceptable production component. The single point of failure means one disk loss takes down everyone reading IDs from it, and replicated databases that need ordered timestamps cannot serve traffic when the TSO is unavailable. The fix is consensus: replicate the high-water mark across a quorum so any one machine's failure does not unmake the ordering proof. tsoracle ships an `openraft` integration as its production-ready replicated driver, and the surface area you have to understand to use it is small: one trait, three deployment patterns, one log envelope.

## The ConsensusDriver trait

The interface between tsoracle's allocator and the replicated log is narrow by design. The `ConsensusDriver` trait has three concerns: durably advance the high-water mark, read it back, and stream the leadership transitions that tell the gRPC layer when this node is the leader. That is the whole contract on the consensus side. The allocator does not care what state machine the log feeds, how snapshots are taken, how membership changes work, or what wire protocol the replicas use to talk to each other. All of that is implementation detail of the driver.

This narrowness is deliberate. A TSO that is tightly coupled to one consensus library is hard to operate inside a stack that already runs a different one. By keeping `ConsensusDriver` to a small trait, tsoracle ships an opinionated default (the openraft driver) without forcing you to adopt openraft if your stack already has raft-rs, etcd's raft, or a homegrown replicated log. You implement one trait, you wire it into the allocator, and the TSO works against your existing consensus story.

The trait, in full (doc comments elided):

```rust
#[async_trait::async_trait]
pub trait ConsensusDriver: Send + Sync + 'static {
    /// Stream of leadership transitions; the first item is the current state.
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>>;

    /// Read the durably-committed high-water mark.
    async fn load_high_water(&self) -> Result<u64, ConsensusError>;

    /// Advance the mark to *at least* `at_least`; returns the stored value.
    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError>;
}
```

`persist_high_water` proposes the advance and returns only once the log has committed it on a quorum — propose-and-await collapsed into one idempotent call that advances the mark to *at least* the requested value and returns whatever is now durably stored. `load_high_water` reads that committed mark back. `leadership_events` is a stream of leadership transitions the gRPC layer watches so it knows when to serve locally and when to redirect non-leader requests via `LeaderHint`. The `epoch` carries the leadership term the caller believed it held, so a stale leader's write is fenced rather than silently applied. That is the whole trait; the driver behind it — openraft here — is where the membership, snapshot, and wire-protocol machinery lives.

## Three deployment patterns

tsoracle ships with three integration patterns, each useful in a different situation.

**Standalone.** The TSO runs as its own service with its own raft cluster. You deploy three (or five) tsoracle replicas, they elect a leader among themselves, the leader serves get-timestamp requests, and the raft cluster replicates the high-water mark. This is the simplest mental model and the one to reach for when the TSO is genuinely a separate concern from the rest of your system. The example at [`examples/openraft-standalone`](https://github.com/prisma-risk/tsoracle/tree/main/examples/openraft-standalone) is a complete three-node multi-process deployment with a tonic peer transport and follower-redirect via `LeaderHint`.

**Piggyback.** Your service already runs openraft for its own state. Adding a separate raft cluster for the TSO doubles the operational surface for the same fault tolerance, which is wasteful. The piggyback pattern reuses your existing raft: tsoracle's `HighWaterCommand` becomes part of your log's `AppData` envelope, and your single raft log carries both your application's commands and the TSO's mark advances. One log, one snapshot, one fsync budget, one leader election, one membership story. The example at [`examples/openraft-piggyback`](https://github.com/prisma-risk/tsoracle/tree/main/examples/openraft-piggyback) is a three-node in-process demo of the pattern.

**File driver.** No raft at all — single node, durable to one disk. The `tsoracle-driver-file` crate ships this as a minimal `ConsensusDriver` implementation that's actually just an fsync wrapper, not a consensus protocol. It's appropriate when you want strict monotonicity within one process and accept that a host failure means the TSO is unavailable until a new one is started. This is the default for the standalone CLI (`tsoracle serve`), which is intentionally a development/single-node tool.

## The envelope pattern

The piggyback pattern is the more interesting one to look at because it's where the trait surface earns its narrowness. Your application's existing openraft instance has an `AppData` type — the application-specific commands the log carries. Adding tsoracle means wrapping `AppData` in an envelope:

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum LogEntry {
    App(MyAppData),
    Tso(tsoracle_driver_openraft::HighWaterCommand),
}
```

Your state machine pattern-matches on the variant and applies the appropriate side. The application side updates application state; the TSO side advances the high-water mark. One snapshot — also an enum — covers both halves. The replication path, the leader election, the membership management are all unchanged from your existing openraft setup; the only new component is the variant in the log envelope and the small `apply_tso` branch in your state machine.

This is the right design for services that already run replicated state. A search engine that uses raft to replicate inverted indexes can carry its own TSO on the same log. A distributed key-value store that wants snapshot isolation can issue its read timestamps from a TSO that's piggybacking on the same raft. The cost is one variant in the log envelope; the saving is everything you'd otherwise need to run as a second replicated service.

## Snapshotting

A raft log eventually compacts via a snapshot, and the snapshot has to cover the full state machine. In the standalone pattern this is straightforward — the snapshot contains the high-water mark, full stop. In the piggyback pattern the snapshot is the union of your application state and the high-water mark:

```rust
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LogSnapshot {
    pub app: MyAppSnapshot,
    pub tso_mark: u64,
}
```

The serialization format is your choice (postcard, bincode, JSON — whatever your existing state machine uses). The snapshot is written through openraft's normal snapshot installation flow; tsoracle does not introduce a separate snapshot lifecycle. The piggyback example wires this end-to-end.

## Leader handoff

The semantics of leader change are the most subtle part. The invariant tsoracle defends is that no ID is ever issued without first being covered by a durably-committed advance to the mark. A leader change must preserve that across the gap between the old leader's last commit and the new leader's first one.

{{ raft_animation(name="cluster-leader-handoff", title="Leader handoff. Step through the failure, election, and recovery — and watch hwm cannot regress.") }}

The mechanism is two-sided.

On the leader-losing side, the old leader stops issuing new IDs the moment it discovers it has lost leadership (typically when its heartbeat fails or it sees a higher term). It may have IDs in flight from in-memory windows, but no further advances can be proposed — its `persist_high_water` calls return errors. In practice, the open-but-in-memory window from the old leader is effectively abandoned; the gap between the last issued ID and the end of that window is left unused.

On the leader-gaining side, the new leader takes over with the raft-committed mark, which is always the durable upper bound across the cluster. Its first action is to read its own on-disk mark (which is `>=` the raft-committed mark in steady state, but may be stale if the new leader was a follower that lagged briefly), reconcile it with the committed log, and then start its first window flip from that mark. The first IDs issued by the new leader are strictly greater than every ID issued by every previous leader. The invariant holds.

This is also where the `LeaderHint` returned by the gRPC layer earns its keep. A client that calls `get_ts` against a now-non-leader replica gets back a hint pointing to the current leader and retries against the right place. The gRPC client driver (`tsoracle-client`) handles this transparently; from the caller's perspective, a leadership change appears as a brief latency spike on the affected requests and nothing else.

## What you get operationally

Wiring tsoracle into openraft — either standalone or piggyback — means your TSO inherits everything openraft provides operationally: log compaction on a configurable schedule, snapshot install during catch-up, membership changes via the joint-consensus pattern, observability of replication lag and election cycles. There's no separate set of dashboards to build for tsoracle; the metrics you already export for raft commit rate and leader stability cover the TSO too.

The one operational concern you do still have to attend to is the fsync at the leader. In the piggyback pattern, the leader's fsync covers your application's state machine plus the TSO. The throughput of the combined log is bounded by that single fsync, just as it would be for a pure application log. In practice, a typical NVMe leader sustains tens to hundreds of thousands of log entries per second, and the TSO's allocation rate is rarely the bottleneck unless your application's own log throughput is exceptionally low. The piggyback pattern wins on operational surface and loses nothing on hot-path performance.

## When to reach for which driver

The standalone driver is the right call when the TSO is a separate concern — when the rest of your stack does not run openraft, when the operational team would prefer the TSO to be visible as its own service, or when the TSO's failure domain should be independent from the application's. The piggyback driver is the right call when your service already runs openraft for its own state and you want one log to cover everything. The file driver is appropriate only for single-node development and for embedded scenarios where you accept that host failure means TSO unavailability.

If you want the broader architecture in a single page, [how it works](/how-it-works/) summarises the moving parts. If you want the window-allocator algorithm in depth — the layer below `ConsensusDriver` — [how the window allocator works](/posts/how-the-window-allocator-works/) is the companion to this post. The two earlier posts in the series, [Why distributed systems need a TSO](/posts/why-distributed-systems-need-a-tso/) and [When you need a TSO](/posts/when-you-need-a-tso/), are the framing for *whether* you should reach for a TSO at all.
