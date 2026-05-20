# openraft-toolkit

Reusable glue for building services on top of [openraft](https://github.com/databendlabs/openraft).

## What's in the box

- `declare_raft_types_ext!` — wraps the upstream `RaftTypeConfig` declaration with multi-leader-per-term, `OneshotResponder`, and the other defaults databend-meta and our PD raft already use. Consumers supply only the slots that actually vary (`Node`, `AppData`, `AppDataResponse`, `SnapshotData`).
- `RocksdbLogStore<C, K>` — generic `RaftLogStorage` + `RaftLogReader` implementation backed by RocksDB. The `K: KeySpace` parameter chooses between `Flat` (single-group: one raft instance per process) and `GroupPrefixed` (multi-group: N raft instances sharing column families, keyed by group id). Passes openraft's bundled storage conformance suite.
- Lifecycle helpers: `bootstrap` (Fresh / Reopen / Join), `change_membership`, `add_learner`, and `leadership_events` — a deduped stream of role-class transitions derived from `Raft::metrics()`.
- Wire codec: `encode` / `decode` helpers using the `[version_byte | bincode(payload)]` framing that both PD and the multi-raft runtime use today for raft RPCs and storage records.

## Feature flags

- `rocksdb-log-store` (default) — pulls in `rocksdb` and exposes `RocksdbLogStore`. Disable if you bring your own storage backend.
- `test-fakes` — exposes in-memory test fixtures for downstream conformance suites. Off by default.

## Out of scope (today)

The toolkit deliberately stops short of shipping:

- A `RaftStateMachine` adapter. Both consumers' state machines have different broadcast/responder shapes, and the abstraction isn't load-bearing yet.
- A `RaftNetworkV2` implementation. Single-group consumers can roll their own with the wire codec; multi-group routing belongs in the host runtime.
- A snapshot-streaming transport. The filesystem-backed sidecar in the multi-raft host is opinionated enough that lifting it would constrain its caller.
- Multi-group host orchestration. The multi-raft runtime's per-host lifecycle is a calc-graph-shaped concern.

These may move into the toolkit when a second consumer appears.
