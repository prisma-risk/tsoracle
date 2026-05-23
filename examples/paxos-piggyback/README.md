# Piggyback TSO onto an existing OmniPaxos cluster

A single-binary, in-process 3-node demonstration of the envelope pattern: a service that already runs OmniPaxos for its own state (a tiny KV in this example) adds tsoracle high-water replication to the **same** OmniPaxos log, instead of bringing up a second cluster.

For the "I want a dedicated TSO OmniPaxos" case, see [`paxos-standalone`](../paxos-standalone/).

## Run

    cargo run -p example-paxos-piggyback

The demo runs in roughly 3 seconds and prints a scripted walk-through. Output is also asserted by `tests/smoke.rs`.

## What the demo shows

1. **Boot.** Three OmniPaxos nodes connected by the toolkit's `MemNetwork` (no tonic, no peer discovery). Each runs a `tsoracle::Server` bound to an OS-assigned loopback port, so the demo and its smoke test can run repeatedly without port-conflict flake.
2. **Post-fence high-water.** Once the tsoracle-server leader-watch task fires on the new leader, the fence persists `serving_floor + failover_advance`. The demo prints this value.
3. **Host KV writes ride the same paxos log.** Appending `MyAppCommand::Kv(Put { ... })` directly on the leader's OmniPaxos handle lands an entry in the shared log. The apply pump folds the KV variant into the host's in-memory map; the TSO field stays at its current value.
4. **Steady-state TSO is allocator-served.** A burst of `GetTs` calls returns strictly monotonic timestamps **without** moving the durable high-water — high-water advances on fences and on window-extension; steady-state `GetTs` hits the allocator.
5. **Failover preserves monotonicity.** The leader is shut down; a survivor elects; its fence runs; the demo asserts both `new_high_water > old_high_water` AND `next_ts > last_pre_failover_ts` (the freshness invariant).

## The envelope pattern, in three concepts

**`MyAppCommand::{Kv(KvOp), HighWater(HighWaterCommand)}`.** Your existing entry type becomes an enum. TSO commands ride as one variant; your own commands ride as the others. See `src/host_service.rs`.

**Apply-path responsibility.** TSO monotonicity (`max(prev, target)`) lives in *your* apply pump, in a field next to the rest of your state. The driver crate doesn't enforce it for you in the piggyback case — that's why the trait is split: `PaxosHighWaterHost` is about storage and submission, not the apply contract.

**Snapshot responsibility.** Your snapshot now carries both halves. `MyAppSnap { kv, high_water }` in `src/host_service.rs` shows the shape; `impl Snapshot<MyAppCommand>` folds both at compaction time.

## Who provides what

| Concern | Provider |
| --- | --- |
| `HighWaterCommand` (the log entry payload for TSO) | `tsoracle-driver-paxos` |
| `MyAppCommand` envelope enum + apply pump | **You** (`host_service.rs::MyAppCommand` + `drain_decided_into`) |
| `MyAppSnap` snapshot type + fold | **You** (`host_service.rs::MyAppSnap`) |
| `PaxosHighWaterHost` impl wrapping the host | **You** (`host_service.rs::PiggybackHost`) |
| `ConsensusDriver` glue + leadership-events stream | `tsoracle-driver-paxos` (via `PaxosDriver`) |
| Linearizable reads | **You** (`current_high_water` appends a `HighWater(Barrier)` and polls `decided_idx`) |
| Peer transport | **You** (the demo uses `MemNetwork`; production uses your service's existing transport) |

## Production caveats

- **In-process MemNetwork.** Replace with your real OmniPaxos peer transport. Nothing in the envelope pattern requires `MemNetwork`; the demo uses it to stay single-binary.
- **In-memory `MemStorage`.** This demo uses the toolkit's `MemStorage<MyAppCommand>` test fake. Real services already have a persisted Storage; substitute it (e.g., the toolkit's `RocksdbStorage` if you're starting fresh, or your own existing storage impl).
- **Snapshotting disabled.** This demo never triggers `OmniPaxos::snapshot`; the apply pump still handles `LogEntry::Snapshotted` so a piggyback host that does enable snapshotting in production gets correct behavior. Re-enable in your own code with a snapshot policy.
- **Endpoint resolution for `LeaderHint`.** The demo's client is constructed with all three loopback endpoints and rotates across them; the driver returns `LeaderState::Follower { leader_endpoint: None }`. Production piggyback hosts that want `LeaderHint` populated populate the `Peer` list passed to whatever toolkit machinery they use to produce the leader stream.
- **No add-learner / reconfiguration demo.** Out of scope.
