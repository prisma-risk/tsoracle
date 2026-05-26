# Driver Comparison: openraft vs. paxos vs. file

This chapter is the capability reference for tsoracle's three first-party `ConsensusDriver` implementations. It exists to answer two questions:

- **Operator**: which driver should I pick, and what do I give up by picking it?
- **Contributor**: how is feature *X* implemented per driver, and where do I look in the code?

For *how to deploy* a chosen driver (Helm values, container images, peer mTLS setup) see [Deployment](deployment.md). For the trait contract itself see [Consensus Integration](consensus-integration.md). For the on-disk/on-wire evolution story see [Format Migration & Upgrade](format-migration-upgrade.md).

Legend used in the matrices below: ✅ supported · ❌ not supported · ➖ not applicable · ⚠️ partial / has a known gap.

---

## Part 1 — Operator-facing comparison

### Capability matrix

| Capability | `file` | `openraft` | `paxos` |
|---|---|---|---|
| Multi-node HA | ❌ (single-node only) | ✅ | ✅ |
| Tolerates `f` failures with `2f+1` nodes | ➖ | ✅ | ✅ |
| Persistence backend | local filesystem (atomic rename + fsync) | RocksDB (log + pluggable snapshot store) | RocksDB (via `tsoracle-paxos-toolkit`) |
| Peer transport | ➖ | tonic gRPC, unary + client-streaming snapshots | tonic gRPC, unary |
| Peer mTLS | ➖ | ✅ (`PeerTlsConfig`) | ✅ (`PeerTlsConfig`) |
| Secure-by-default Helm render (peer TLS required for HA) | ➖ | ✅ ([#452](https://github.com/prisma-risk/tsoracle/pull/452)) | ✅ ([#452](https://github.com/prisma-risk/tsoracle/pull/452)) |
| Runtime dynamic membership (add/remove nodes online) | ❌ | ✅ ([#453](https://github.com/prisma-risk/tsoracle/pull/453)) | ❌ — peers fixed at startup, restart required |
| Admin gRPC service + `tsoracle admin` CLI | ❌ | ✅ (own `--admin-listen` port, optional admin mTLS) | ❌ (returns `AdminError::Unsupported`) |
| Admin mTLS | ➖ | ✅ (`AdminTlsConfig`) | ➖ (no admin service) |
| Graceful leader handoff on SIGTERM | ➖ | ✅ — transfers leadership to most-caught-up voter, then drains ([#423](https://github.com/prisma-risk/tsoracle/pull/423)) | ❌ — process exits without handoff; followers re-elect |
| Cooperative shutdown with grace period | ✅ | ✅ | ✅ |
| `SIGTERM` honored (k8s drain) | ✅ ([#406](https://github.com/prisma-risk/tsoracle/pull/406)) | ✅ | ✅ |
| Log compaction / snapshots | ➖ (no log) | ✅ (`SnapshotPolicy::LogsSinceLast(N)`, pluggable store: in-memory or RocksDB) | ✅ (OmniPaxos `snapshot(decided_idx)`; off by default) |
| Snapshot transport | ➖ | gRPC client-streaming | embedded (no separate transport) |
| Zero-downtime on-disk + on-wire format evolution | ➖ | ✅ ([#454+](https://github.com/prisma-risk/tsoracle/pull/454) family, see [Format Migration](format-migration-upgrade.md)) | ❌ — format fixed per release; bump requires coordinated rolling restart |
| Per-RPC deadlines on peer transport | ➖ | ✅ ([#443](https://github.com/prisma-risk/tsoracle/pull/443)) | ✅ |
| Helm + container images (fat + lean) | ✅ | ✅ | ✅ |
| Kubernetes e2e lane (cold-start + rolling-restart soak) | ➖ | ✅ | ✅ |

### What you get from each driver, in one paragraph

**`file`** — a single fsynced state file on local disk, guarded by an OS `flock`. No peers, no log, no replication. The simplest possible deployment: one pod, one PVC, replicas pinned to 1 (the Helm chart enforces this). If the node loses its disk you lose the oracle's high-water mark and must rebuild from a backup. Suitable for development, single-tenant deployments, and anywhere the underlying disk durability story is acceptable.

**`openraft`** — production HA on top of [openraft](https://github.com/databendlabs/openraft). The richest feature set: dynamic membership over a dedicated admin port, graceful leader handoff on SIGTERM, online format evolution, snapshot streaming, peer + admin mTLS. The default choice for HA deployments; the Helm chart treats `driver=openraft` as the recommended default.

**`paxos`** — production HA on top of [OmniPaxos](https://omnipaxos.com/). Feature-light relative to openraft: no dynamic membership, no graceful handoff, no online format evolution, no admin gRPC. What it brings is a different consensus algorithm (paxos lineage rather than raft) and a per-leader *single-active-stream lease* that openraft does not currently implement (see contributor notes). Use it when you specifically want paxos semantics or want the second consensus implementation as a hedge.

### Choosing a driver

**Pick `openraft` unless you have a specific reason not to.** It is the recommended HA default in the Helm chart and the only driver with the full operational toolkit — dynamic membership, graceful handoff, zero-downtime format evolution, admin gRPC, admin mTLS. Every feature this project ships first for "drivers" generally ships for openraft first; paxos catches up later (and sometimes never, where the underlying algorithm makes parity hard).

**Pick `paxos` only when one of these applies:**

- You specifically want the OmniPaxos algorithm in production (compliance, prior operator experience, hedging on a second consensus implementation, etc.).
- Your deployment is small, long-lived, and doesn't need online membership changes — you can plan a coordinated restart when peers change or when you upgrade across a format-version boundary.
- You're willing to accept noticeably more `NOT_LEADER` responses during rolling restarts than openraft produces, because paxos has no graceful handoff (followers re-elect via OmniPaxos ballot bumps after the leader exits).

If you can't say yes to one of those, openraft is the better default — the missing capabilities in paxos are real operational burdens, not paper gaps.

**Pick `file` when single-node is genuinely sufficient.** Legitimate uses include development clusters, an embedded oracle inside a larger system that already provides HA at another layer (e.g. a per-tenant sidecar where the parent service is what gets replicated), and small single-tenant deployments where you control the disk durability story end-to-end (backup cadence, restore drill, blast radius of the underlying volume). The Helm chart enforces `replicas=1` for `driver=file`; do not try to evade that guard — the file driver has no peer protocol and the second pod will simply fail to acquire the `flock` and exit.

**Things worth knowing before you pick:**

- **Clients ride out elections without an external load balancer.** The leader-hint trailer mechanism means clients re-target after a redirect on their own. You do not need a smart proxy in front of the cluster — point clients at all replicas and let the client handle leader discovery.
- **`tsoracle` is single-shard.** All three drivers issue one monotonic sequence per cluster. If you need partitioned timestamp domains, deploy multiple clusters; don't try to shard at the driver layer.
- **Brief `NotServing` windows during leader transitions are normal.** All three drivers publish `NotServing` while the failover fence runs (load high-water, reseat the allocator). Clients tolerate this; alerts that page on every `NotServing` will be noisy.
- **Peer mTLS is on by default in the Helm chart for HA drivers.** Disabling it requires the explicit `tls.allowInsecurePeer=true` opt-out. Don't treat this as ceremony — a plaintext peer port with the default headless Service is unauthenticated consensus on the pod network.

---

## Part 2 — Contributor-facing deep dive

### The `ConsensusDriver` trait surface

All three drivers implement [`ConsensusDriver`](consensus-integration.md), defined at `crates/tsoracle-consensus/src/driver.rs`. The trait is three methods:

- `leadership_events() -> Stream<LeaderState>` — emits `Leader { epoch }` / `Follower { leader_endpoint, leader_epoch }` / `NotServing` transitions.
- `load_high_water() -> Result<u64>` — read the durable high-water mark.
- `persist_high_water(at_least, epoch) -> Result<u64>` — write the high-water mark; result is the actual persisted value (monotonic).

The trait is location-agnostic: it does not specify who names peers or who opens peer sockets. Each driver makes those decisions independently.

### Per-feature deep dive

#### Dynamic membership and the admin surface

Definitions live in `crates/tsoracle-standalone/src/admin/`. The `MembershipAdmin` trait (mod.rs:97-104) is implemented by:

- `OpenraftMembershipAdmin` (admin/openraft.rs:40-260) — wires `list_members`, `add_learner`, `promote`, `remove` directly to `Raft::add_learner` / `Raft::change_membership`. Guards against removing the last voter; allows the leader to remove itself (steps down after commit). Maps openraft errors to admin-domain errors (`ForwardToLeader → NotLeader`, `LearnerNotFound → NotMember`, …).
- `UnsupportedAdmin` (admin/mod.rs:109-133) — used by `paxos` and `file`. Every mutating call returns `AdminError::Unsupported`. `list_members` returns a fixed view passed at construction.

The admin proto (`proto/admin.proto`) is the same for all drivers; what differs is the impl behind the trait. For openraft, the admin service runs on its own `--admin-listen` port with an `AdminInsecureRoutable` guard at `drivers/openraft/mod.rs:131-141` that rejects plaintext admin on a routable address (loopback is fine).

Paxos blockers that prevented including it in the [#453](https://github.com/prisma-risk/tsoracle/pull/453) scope: state-transfer unwired, persisted-stopsign restart incoherence, leader-hint resolves the data-plane port not the admin port, `announce` partial-update, `tso_peers` lives in the toolkit runner. These are real blockers, not just "not yet wired."

#### Graceful leader handoff

Openraft only. The handoff lives in `crates/tsoracle-standalone/src/drivers/openraft/handoff.rs:96` and is invoked via `Standalone::take_drain()` (`lib.rs:99`). The flow:

1. SIGTERM observed via `tsoracle_server::shutdown_signal()` (server/signal.rs).
2. Driver picks the most-caught-up voter from raft metrics, calls `raft.trigger().transfer_leader(target)`.
3. `transfer_leader` is wired into both the wire protocol (`drivers/openraft/network.rs:415` client, `:600` server) and the underlying `Raft::handle_transfer_leader`.
4. Outgoing leader drains in-flight extensions, then exits.

Soak measurement (rolling restart on staging EKS, [`project-openraft-graceful-handoff`](https://github.com/prisma-risk/tsoracle/pull/423)): `NOT_LEADER` responses dropped from 361/46108 to 3/39045 (~100×).

Paxos has no equivalent. There is no `drivers/paxos/handoff.rs`, no `transfer_leader`, no `take_drain`. On SIGTERM the paxos process exits cooperatively but without handing off; surviving nodes re-elect via OmniPaxos ballot bumps.

#### On-disk + on-wire format evolution

Openraft only. The version contract is four constants in `crates/tsoracle-openraft-toolkit/src/codec.rs:41-80`:

- `MIN_READABLE_VERSION` — read-floor (today: 4).
- `MAX_READABLE_VERSION` — read-ceiling (today: 4; bumps to 5 under the `e2e-max-readable-next` feature for soak testing).
- `BASELINE_WRITE_VERSION` — fallback write version (today: 4).
- `ActiveWriteVersion(Arc<AtomicU8>)` — the runtime-mutable active write version, seeded from the log at recovery and *only* mutated by a committed `SetFormatVersion` raft entry.

Activation is gated by `all_members_can_read(target, capabilities)` (`drivers/openraft/capabilities.rs:73-95`), which uses the additive `Capabilities` peer RPC to gather every member's `[min_readable, max_readable, active_write]` triple. The flip itself happens at apply via `ApplyOutcome::FormatActivated`. Wire payloads carry an additive proto `format_version uint32` field that defaults to 0 → `BASELINE_WRITE_VERSION`.

Paxos has none of this. The on-disk schema is fixed per `tsoracle-paxos-toolkit` release; a version bump requires a coordinated rolling restart with operator-managed compatibility. See [Format Migration](format-migration-upgrade.md) for the full story.

#### Single-active leadership-stream lease

Paxos only. The lease lives in `crates/tsoracle-driver-paxos/src/driver.rs:52-83` and is the answer to a specific paxos hazard: two `Server`s constructed from one `Arc<PaxosDriver>` share *one* epoch, so the failover-fence's epoch comparison cannot distinguish them — meaning the leadership stream is the *sole* duplicate-timestamp guard. If a leaked-but-quiescent stream coexisted with a live one, both could mint timestamps.

The lease uses an `active_generation: AtomicU64` slot plus a `next_generation` counter that *skips zero on wrap* ([#442](https://github.com/prisma-risk/tsoracle/pull/442) — zero is the "slot free" sentinel; raw `fetch_add` would wrap to zero once per 2^64 and allow a CAS(0,0) no-op to defeat the guard). A second concurrent `leadership_events()` call returns `stream::empty()` (fail-closed → `NotServing`) and bumps the `tsoracle.leadership_stream.rejected.total` counter. The `StreamLease` RAII guard releases the slot on drop with an ABA-safe CAS that only frees if it still holds its own generation.

Openraft does not implement this. The lease is a noted follow-up — openraft's failover fence relies on the raft term, which differs per leader epoch in a way that makes the paxos hazard less acute, but the asymmetry is intentional documentation, not a "fixed" property.

#### Persistence and snapshots

- **`file`** (`crates/tsoracle-driver-file/src/driver.rs`) — one POSIX file `state` holds a fixed-width big-endian `u64`. Write-to-temp + fsync + atomic rename + directory fsync (POSIX) or re-open + sync (Windows). OS-level `flock` on a sentinel `LOCK` inode prevents two processes from opening the same directory; kernel releases on process exit. Three failpoint hooks (`file_driver::before_write`, `…::after_tmp_fsync_before_rename`, `…::after_rename_before_dir_fsync`) cover the rename window.
- **`openraft`** — RocksDB log store from `tsoracle-openraft-toolkit`, framed with the version byte described above. Snapshot store is *pluggable* via the `SnapshotStore` trait (`crates/tsoracle-driver-openraft/src/snapshot_store.rs:52-96`): `InMemorySnapshotStore` by default, `RocksdbSnapshotStore` under the `rocksdb-snapshot-store` feature. Snapshots are version-framed (`[active_write_version | postcard(...)]`) and decoded over the `[MIN_READABLE, MAX_READABLE]` range.
- **`paxos`** — RocksDB via `tsoracle_paxos_toolkit::storage::RocksdbStorage<T>`. Snapshots are owned by OmniPaxos and exposed via `SnapshotPolicy::every_n_decided()` (`crates/tsoracle-driver-paxos/src/snapshot_policy.rs:31-69`); the policy is `0` (disabled) by default.

A snapshot-publish TOCTOU hazard ([#426](https://github.com/prisma-risk/tsoracle/pull/426)) is guarded at the openraft toolkit layer by routing build + install through a single `commit_snapshot` gated by `supersedes_published`. The guard exists because the durable disk write must be serialized with the install — CAS on the in-memory publish alone is insufficient (restart recovers from disk, so a stale build can land its write *after* a newer install regresses both).

#### Peer transport and TLS

Both HA drivers use tonic gRPC. The peer protos live alongside the standalone crate:

- `crates/tsoracle-standalone/proto/raft_peer.proto` — openraft.
- `crates/tsoracle-standalone/proto/paxos_peer.proto` — paxos.

Openraft uses unary RPCs plus a client-streaming snapshot RPC (`SnapshotChunk` body after a header message; chunk size = `SNAPSHOT_CHUNK_SIZE`). Paxos is unary-only. Both pool tonic channels per peer.

Peer mTLS is `PeerTlsConfig { cert, key, ca }` on the per-driver config struct (`crates/tsoracle-standalone/src/config.rs:68-72`). Admin mTLS is `AdminTlsConfig` (line 88) and lives on `OpenraftConfig` only (line 111). The Helm chart fails openraft/paxos HA renders without `tls.enabled=true` unless `tls.allowInsecurePeer=true` is explicitly set ([#452](https://github.com/prisma-risk/tsoracle/pull/452)).

Per-RPC deadline handling on the openraft peer transport ([#443](https://github.com/prisma-risk/tsoracle/pull/443)): the shared `unary_call` helper applies `timeout(option.hard_ttl(), …)` and evicts the cached channel on transport-error or deadline. Important detail: openraft's `append_entries` does *not* self-wrap in `C::timeout` (only `vote` and `transfer_leader` do), so a transport that ignores `RPCOption` silently black-holes replication until TCP keepalive notices.

#### Peer addressing

- **`openraft`** — addresses live in raft membership (`OpenraftPeer { raft_addr, service_endpoint, admin_endpoint }`). Membership is read from persistent state on recovery or from the bootstrap config. Online membership changes update the addresses without a restart. `service_endpoint` is scheme-less `host:port` (the TLS client rejects explicit `http://`).
- **`paxos`** — addresses are a static `peers: BTreeMap<u64, String>` plus a separate `tso_peers` map keyed by node ID, both required at every startup and frozen until next restart. Changing peer addresses requires a config rewrite and a coordinated restart.

#### Bootstrap / standalone host

- **`openraft`** — `tsoracle_driver_openraft::standalone::StandaloneHost` owns `Raft<TypeConfig, HighWaterStateMachine>` and the state machine clone. Bootstrap is `OpenraftConfig { bootstrap: true, initial_membership: Some(...) }` → `StandaloneHost::from_config()`. Recovery merges membership config from raft metrics.
- **`paxos`** — `tsoracle_driver_paxos::standalone::StandaloneHost<S: Storage>` owns the `OmniPaxos<HighWaterCommand, S>` handle (generic storage). Recovery replays decided entries from RocksDB.
- **`file`** — no standalone host concept; the file driver *is* always standalone.

Both HA drivers also expose a piggyback trait (`OpenraftHighWaterHost`, `PaxosHighWaterHost`) for embedding the high-water state machine in an existing cluster you already own.

### Forward-looking gaps and follow-ups

These are known gaps rather than bugs — they shape what's safe to deploy where:

- **Paxos dynamic membership** — five real blockers documented in [project-dynamic-membership](https://github.com/prisma-risk/tsoracle/pull/453); not a "just wire it" item.
- **Paxos format evolution** — no `MIN/MAX/ActiveWrite` triple, no `SetFormatVersion`, no `Capabilities` RPC. Version bumps are operator-coordinated rolling restarts.
- **Paxos graceful handoff** — no `handoff.rs` analogue. `SIGTERM` exits cooperatively but without transferring leadership.
- **Paxos admin TLS** — `AdminTlsConfig` exists only on `OpenraftConfig`. Paxos has no admin service to TLS-protect.
- **Openraft single-active leadership-stream lease** — openraft mints streams freely. Documented intentional asymmetry; revisit if the same hazard becomes reachable.
- **Openraft snapshot-publish TOCTOU** — guarded at the toolkit layer; the driver does not need to re-guard.

---

## Where to look next

- [Consensus Integration](consensus-integration.md) — the `ConsensusDriver` trait contract and per-method recipes.
- [Format Migration & Upgrade](format-migration-upgrade.md) — the openraft format-evolution story end-to-end.
- [Deployment](deployment.md) — Helm values, container images, peer TLS setup.
- [Operations](operations.md) — sizing, monitoring, client retry behaviour.
