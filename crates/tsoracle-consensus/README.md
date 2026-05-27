# tsoracle-consensus

The `ConsensusDriver` trait — the single pluggable extension point for HA and durable persistence in [tsoracle](https://github.com/prisma-risk/tsoracle).

Implement this trait against your preferred mechanism (openraft, raft-rs, etcd, a single-node file, …) and you can host the tsoracle server on it. The library itself does not run consensus; it consumes whatever you wire into the trait.

## What's in the box

- `ConsensusDriver` — the three-method trait the server calls into: `leadership_events`, `load_high_water`, `persist_high_water`. About fifty lines of trait surface.
- `LeaderState` — the role-class enum the driver's `leadership_events` stream carries: `Leader { epoch }`, `Follower { leader_endpoint, leader_epoch }`, `Unknown`.
- `ConsensusError` — the error type all three trait methods return.
- `AdvancePayload` + `reject_out_of_range_advance` — the shared "advance the high-water to at least N" command payload and the range guard every driver MUST call before persisting it. See [Required pre-persist guard](#required-pre-persist-guard).

## Required pre-persist guard

Every `ConsensusDriver::persist_high_water` implementation MUST call `reject_out_of_range_advance(at_least)` before durably appending the advance. The check rejects any value above `tsoracle_core::PHYSICAL_MS_MAX` (the 46-bit physical-millisecond cap) as `ConsensusError::PermanentDriver`.

This is a poison-write guard, not a styling preference. The high-water only ratchets up, so once an out-of-range value is durably committed every subsequent leadership gain reloads it, `PhysicalMs::try_new` rejects it (`CoreError::PhysicalMsOutOfRange`), and the new leader can never serve — there is no path to self-heal. The single-node `FileDriver` already guards at its persist site; consensus-backed drivers append through a replicated log and apply with an unchecked `max`, so this shared check is what keeps them aligned with the same contract.

## Documentation

- [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md) — the trait reference: contract for each method, per-driver implementation recipes, a worked example (openraft), the "Choosing a driver" comparison, and the single-leader requirement explained.

## Existing impls in this workspace

If you want a ready-made driver rather than implementing one yourself:

- [`tsoracle-driver-file`](https://crates.io/crates/tsoracle-driver-file) — single-node, fsync-durable.
- [`tsoracle-driver-openraft`](https://crates.io/crates/tsoracle-driver-openraft) — HA via openraft.
- [`tsoracle-driver-paxos`](https://crates.io/crates/tsoracle-driver-paxos) — HA via OmniPaxos.

## Feature flags

- `serde` — derives `Serialize` / `Deserialize` on the public types so they cross wire and storage boundaries cleanly. Propagates to `tsoracle-core`.
