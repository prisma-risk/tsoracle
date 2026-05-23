# tsoracle-consensus

The `ConsensusDriver` trait — the single pluggable extension point for HA and durable persistence in [tsoracle](https://github.com/prisma-risk/tsoracle).

Implement this trait against your preferred mechanism (openraft, raft-rs, etcd, a single-node file, …) and you can host the tsoracle server on it. The library itself does not run consensus; it consumes whatever you wire into the trait.

## What's in the box

- `ConsensusDriver` — the three-method trait the server calls into: `leadership_events`, `load_high_water`, `persist_high_water`. About fifty lines of trait surface.
- `LeaderState` — the role-class enum the driver's `leadership_events` stream carries (Leader / Follower / Candidate / NoLeader).
- `ConsensusError` — the error type all three trait methods return.

## Documentation

- [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md) — the trait reference: contract for each method, per-driver implementation recipes, a worked example (openraft), the "Choosing a driver" comparison, and the single-leader requirement explained.

## Existing impls in this workspace

If you want a ready-made driver rather than implementing one yourself:

- [`tsoracle-driver-file`](https://crates.io/crates/tsoracle-driver-file) — single-node, fsync-durable.
- [`tsoracle-driver-openraft`](https://crates.io/crates/tsoracle-driver-openraft) — HA via openraft.
- [`tsoracle-driver-paxos`](https://crates.io/crates/tsoracle-driver-paxos) — HA via OmniPaxos.

## Feature flags

- `serde` — derives `Serialize` / `Deserialize` on the public types so they cross wire and storage boundaries cleanly. Propagates to `tsoracle-core`.
- `bt` — enables backtrace capture in error variants (propagates to `tsoracle-core`).
