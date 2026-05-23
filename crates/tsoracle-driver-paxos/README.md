# tsoracle-driver-paxos

`ConsensusDriver` implementation for [tsoracle](https://github.com/prisma-risk/tsoracle) backed by [OmniPaxos](https://github.com/haraldng/omnipaxos) via [`tsoracle-paxos-toolkit`](../tsoracle-paxos-toolkit/).

## What it provides

- `PaxosDriver` — the `tsoracle_consensus::ConsensusDriver` impl. Holds a `PaxosHighWaterHost` and a leader-event stream; bridges them to tsoracle's `leadership_events`, `load_high_water`, and `persist_high_water` contract.
- `PaxosHighWaterHost` — the single trait a host service implements to expose its OmniPaxos handle to the driver. Lets a service share one paxos log between its own state and the tsoracle `HighWaterCommand`.
- `StandaloneHost` — turnkey `PaxosHighWaterHost` impl for services that don't already run their own OmniPaxos. Build via `StandaloneHost::builder()` with the toolkit's storage + your network sink + your peer list.
- `SnapshotPolicy` — controls when snapshots are taken: `disabled`, every-N-decisions, or callback-driven.
- `HighWaterCommand` / `HighWaterSnapshot` — the typed log entry and snapshot payload the driver injects into the host's OmniPaxos log.
- `encode_epoch` / `decode_epoch` — codec for the `Epoch ↔ Ballot` mapping (see below).

## Patterns

### Standalone services

If your service is greenfield — no existing OmniPaxos — wire `StandaloneHost::builder()` with `tsoracle_paxos_toolkit::storage::RocksdbStorage`, your network sink (implementing `MessageSink<HighWaterCommand>` from the toolkit), and the list of peer endpoints. Hand the resulting `PaxosDriver` to `Server::builder().consensus_driver(driver)`. See [`examples/paxos-standalone`](../../examples/paxos-standalone/) for the full wiring with a tonic peer transport.

### Piggyback on an existing OmniPaxos

If your service already runs an OmniPaxos for its own state, implement `PaxosHighWaterHost` directly against that handle. `HighWaterCommand` becomes a tagged variant of your application command type and is decided on the same log alongside your own commands; one snapshot covers both halves. See [`examples/paxos-piggyback`](../../examples/paxos-piggyback/) for the worked envelope pattern.

### Linearizable reads

The driver's read path consults a `Barrier` synchronized with the local OmniPaxos handle's `accepted_idx`. A read that lands on the leader waits at most the configured grace window for the barrier to clear, then returns a fresh high water mark; a read that lands on a follower returns a `LeaderHint` redirect so the client retries against the current leader.

### Epoch ↔ Ballot encoding

tsoracle's `Epoch` (the fence-watermark identifier persisted with each `HighWaterCommand`) is a 128-bit value that packs the round-changing fields of an OmniPaxos `Ballot` — `config_id` (bits 96..128), `n`, the round counter (bits 64..96), and `pid`, the proposer id (bits 0..64) — via `encode_epoch` / `decode_epoch`. The full ballot identity fits exactly, so the encoding is lossless and total: distinct ballots never collide and a later ballot always encodes to a strictly greater epoch. This gives the driver a stable, monotonic per-leader epoch that the failover fence uses to reject stale writes after a leader change — every advance carries the proposer's ballot, and a write that races a re-election will fence against the new leader's epoch. `priority` is not encoded: it is a static per-node tiebreaker fully determined by `pid`.

## Documentation

- [`docs/consensus-integration.md`](../../docs/consensus-integration.md) — the `ConsensusDriver` trait reference, including the **Choosing a driver** comparison (file vs openraft vs paxos) and a worked example for the openraft side.
- [`tsoracle-paxos-toolkit`](../tsoracle-paxos-toolkit/) — the reusable OmniPaxos glue this crate is built on.
