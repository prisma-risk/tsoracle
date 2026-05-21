# Piggyback TSO onto an existing openraft cluster

A single binary, in-process 3-node demonstration of the envelope pattern: a service that already runs openraft for its own state (a tiny KV in this example) adds tsoracle high-water replication to the **same** raft log, instead of bringing up a second cluster.

For the "I want a dedicated TSO raft" case, see [`openraft-standalone`](../openraft-standalone/).

## Run

    cargo run -p example-openraft-piggyback

The demo runs in roughly 3 seconds and prints a scripted walk-through of the integration. Output is also asserted by `tests/smoke.rs`.

## What the demo shows

1. **Boot.** Three openraft nodes connected by `openraft_toolkit::MemNetwork` (no tonic, no peer discovery). Each runs a `tsoracle::Server` bound to an OS-assigned loopback port (`TcpListener::bind("127.0.0.1:0")`), so the demo and its smoke test can run repeatedly without port-conflict flake.
2. **Post-fence high-water.** Once the tsoracle-server leader-watch task fires on the new leader, the fence in `tsoracle-server/src/fence.rs` persists `serving_floor + failover_advance`. The demo prints this value and labels it "post-fence; this is when consensus actually persisted."
3. **Host KV writes ride the same raft.** Directly calling `leader_raft.client_write(HostCommand::Kv(Put { ... }))` lands an entry in the host's log. The demo asserts that the apply result's `tso` field is `None` (KV writes do not touch the TSO half) and prints a KV dump.
4. **Steady-state TSO is allocator-served.** A burst of `GetTs` calls through `tsoracle_client::Client` returns strictly monotonic timestamps, **without** the durable high-water moving. That's not a bug: high-water advances on fences and on window-extension; steady-state `GetTs` hits the allocator. See [`docs/key-subsystems.md`](../../docs/key-subsystems.md) ("Steady-state window extension") for the rationale.
5. **Failover preserves monotonicity.** The leader's raft is shut down; a survivor elects; its fence runs; the demo asserts both `new_high_water > old_high_water` AND `next_ts > last_pre_failover_ts` (the freshness invariant).

## The envelope pattern, in three concepts

**`AppData = HostCommand::{Kv(KvOp), Tso(HighWaterCommand)}`.** Your existing `AppData` becomes an enum. TSO commands ride as one variant; your own commands ride as the others. See `src/host_service.rs`.

**Apply-path responsibility.** TSO monotonicity (`max(prev, target)`) lives in *your* state machine's apply path, in a field next to the rest of your state. The driver crate doesn't enforce it for you in the piggyback case — that's why the trait is split: `OpenraftHighWaterHost` is about storage and submission, not the apply contract.

**Snapshot responsibility.** Your snapshot now carries both halves. `HostSnapshot { kv, high_water, last_applied, last_membership }` in `src/host_service.rs` shows the shape.

## Who provides what

| Concern | Provider |
| --- | --- |
| `HighWaterCommand` (the log entry type) | `tsoracle-driver-openraft` |
| Log entry envelope (`AppData` enum + apply path) | **You** (`host_service.rs::HostCommand`) |
| State machine that applies both halves | **You** (`host_service.rs::HostStateMachine`) |
| `OpenraftHighWaterHost` impl wrapping the host | **You** (`host_service.rs::PiggybackHost`) |
| `ConsensusDriver` glue + leadership-events stream | `tsoracle-driver-openraft` (via `OpenraftDriver`) |
| Linearizable reads | **You** (the demo issues `ensure_linearizable` in `current_high_water`) |
| Peer transport | **You** (the demo uses `MemNetwork`; production uses your service's existing transport) |

## Production caveats

- **In-process MemNetwork.** Replace with your real openraft transport. Nothing in the envelope pattern requires `MemNetwork`; the demo uses it to stay single-binary.
- **In-memory `HostStateMachine`.** This demo serializes its state via postcard but only keeps the most-recent snapshot in memory. Real services that already run openraft already have a persisted state machine; reuse it and add the high-water field alongside.
- **`SnapshotPolicy::Never`.** The driver crate's `HighWaterStateMachine` persists snapshots through a pluggable `SnapshotStore` (and the `openraft-standalone` example does enable the default snapshot policy on top of it), but this example's own `HostStateMachine` still keeps state and snapshots in memory only. Re-enable snapshots here once that SM gains persisted snapshot install/load — the [`SnapshotStore`](https://docs.rs/tsoracle-driver-openraft/latest/tsoracle_driver_openraft/trait.SnapshotStore.html) trait shipped for `HighWaterStateMachine` is the same shape you can use.
- **Endpoint resolution for `LeaderHint`.** The demo's client is constructed with all three loopback endpoints and rotates across them; the driver returns `LeaderState::Follower { leader_endpoint: None }`. Production piggyback hosts that want `LeaderHint` populated wrap the driver themselves (see `openraft-standalone/src/router.rs` for the pattern).
- **No add-learner / membership-change demo.** Out of scope.
