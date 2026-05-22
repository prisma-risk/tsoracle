# tsoracle guide

Long-form reference for [tsoracle][repo] — a distributed, highly available timestamp oracle issuing strictly monotonic, gap-free integer timestamps over gRPC.

This guide complements the crate-level API documentation. For auto-indexed, "ask the wiki" style lookups, see [DeepWiki](https://deepwiki.com/prisma-risk/tsoracle).

## Chapters

- [`algorithm`] — window allocator, 46/18 bit packing, monotonicity proof, the "clock is advisory" invariant. (Lives in `tsoracle-core`.)
- [`consensus_integration`] — implementing `ConsensusDriver` against openraft, raft-rs, etcd, or any other consensus layer. (Lives in `tsoracle-consensus`.)
- [`operations`] — tuning, fsync cost, leader handoff, deployment topologies, monitoring hook points.

[`algorithm`]: tsoracle_core::docs::algorithm
[`consensus_integration`]: tsoracle_consensus::docs::consensus_integration
[repo]: https://github.com/prisma-risk/tsoracle
