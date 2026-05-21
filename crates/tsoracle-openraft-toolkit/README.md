# tsoracle-openraft-toolkit

Reusable glue for building services on top of [openraft](https://github.com/databendlabs/openraft).

## What's in the box

- `declare_raft_types_ext!` — wraps the upstream `RaftTypeConfig` declaration with multi-leader-per-term, `OneshotResponder`, and a curated set of other defaults. Consumers supply only the slots that actually vary (`Node`, `AppData`, `AppDataResponse`, `SnapshotData`).
- `RocksdbLogStore<C, K>` — generic `RaftLogStorage` + `RaftLogReader` implementation backed by RocksDB. The `K: KeySpace` parameter chooses between `Flat` (single-group: one raft instance per process) and `GroupPrefixed` (multi-group: N raft instances sharing column families, keyed by group id). Passes openraft's bundled storage conformance suite.
- Lifecycle helpers: `bootstrap` (Fresh / Reopen / Join), `change_membership`, `add_learner`, and `leadership_events` — a deduped stream of role-class transitions derived from `Raft::metrics()`.
- Wire codec: `encode` / `decode` helpers using the `[version_byte | bincode(payload)]` framing common to raft RPCs and storage records.

## Feature flags

- `rocksdb-log-store` (default) — pulls in `rocksdb` and exposes `RocksdbLogStore`. Disable if you bring your own storage backend.
- `test-fakes` — exposes in-memory test fixtures for downstream conformance suites. Off by default.

## Out of scope (today)

The toolkit deliberately stops short of shipping:

- A `RaftStateMachine` adapter. State machines' broadcast and responder shapes vary widely between consumers, and the abstraction isn't load-bearing yet.
- A `RaftNetworkV2` implementation. Single-group services can roll their own with the wire codec; multi-group routing belongs in the host runtime that owns the group lifecycle.
- A snapshot-streaming transport. Filesystem-backed sidecar transports are opinionated enough that lifting one would constrain its callers.
- Multi-group host orchestration. Hosting N raft instances per process is a deployment-shaped concern, not a library concern.

These may move into the toolkit when a clear shared shape emerges.
