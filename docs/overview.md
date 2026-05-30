# Overview

tsoracle is a distributed timestamp **and sequence** oracle (TSO) for Rust — a highly available, fault-tolerant service that hands out two kinds of strictly-ordered integer over gRPC. Each issued **timestamp** (`get_ts`) is strictly greater than every previously issued one — no duplicates and no regression, even across failover. The packed integer space is not dense: the logical counter resets each time `physical_ms` advances (see [The Allocator](the-allocator.md)), so some integer values in the 64-bit space are skipped; the issued sequence itself is still total-ordered and unique. When you need *contiguity* rather than just *order*, tsoracle also issues **gapless dense sequences** (`get_seq`): per-named-key counters that hand out contiguous `[start, start + count)` blocks with nothing skipped, never reused across crashes or failover (see [Client API & Usage](client-api-and-usage.md#getseq--dense-gapless-sequences)). The crate is layered: a sync algorithm core (`tsoracle-core`), an async server (`tsoracle-server`) that wires the core to the network, and a pluggable consensus surface (`ConsensusDriver`) that lets you run it single-node behind one fsync or replicated on top of a consensus library of your choice.

```mermaid
flowchart TB
    callers["Clients · CLI · embedded host"]
    grpc["gRPC service<br/>(tsoracle-proto)"]
    server["tsoracle-server<br/>tonic · leader-watch · failover fence"]
    core["tsoracle-core<br/>window allocator (sync)"]
    trait_node["ConsensusDriver trait<br/>(tsoracle-consensus)"]
    file["FileDriver<br/>single-node, fsync"]
    custom["Your driver<br/>openraft · etcd · …"]

    callers --> grpc
    grpc --> server
    server --> core
    server --> trait_node
    trait_node --> file
    trait_node --> custom
```

## What this is, and is not

tsoracle is a TSO algorithm core plus a transport. It is not a clock-synchronization system. It does not implement consensus — it consumes one through `ConsensusDriver`. It does not implement multi-shard or partitioned TSO — one tsoracle instance issues a single monotonic timestamp stream (alongside any number of independent named dense sequences), all served from one leader. Users wanting Spanner-style true-time, partitioned timestamp domains, or HLC fan-in should layer those concerns over tsoracle rather than expecting them inside the library.

## Where to go next

- New to TSOs or to tsoracle — start with [Getting Started](getting-started.md).
- Curious how it works internally — [Architecture Deep Dive](architecture-deep-dive.md) and [The Allocator](the-allocator.md).
- Plugging tsoracle into your own consensus — [Consensus Integration](consensus-integration.md).
- Running it in production — [Operations](operations.md).
