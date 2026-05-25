# tsoracle-paxos-toolkit

Reusable glue for building services on top of [OmniPaxos](https://github.com/haraldng/omnipaxos).

## What's in the box

- `RocksdbStorage<T>` — generic `omnipaxos::storage::Storage` implementation backed by RocksDB. Passes OmniPaxos's bundled storage conformance suite; `T: omnipaxos::storage::Entry` is the host's log entry type.
- Lifecycle helpers: `PaxosRunner<T, S>` (the tick task + apply task lifecycle wrapper), the `MessageSink<T>` outbound trait, `LeadershipState`, the leader-event stream (`LeaderEventStream` + `LeaderEventSender`), and the `Peer` struct used for follower-redirect endpoints.
- Test fakes (feature-gated): `MemNetwork<T>` for in-process clusters, `PartitionController` for chaos coverage, `MemStorage<T>` for storage-free smoke tests.
- Wire codec: `encode` / `decode` helpers using a `[version_byte | postcard(payload)]` framing, suitable for both paxos RPCs and storage records.

## Feature flags

- `rocksdb-storage` (default) — pulls in `rocksdb` and exposes `RocksdbStorage`. Disable if you bring your own storage backend.
- `test-fakes` — exposes in-memory test fixtures (`MemNetwork`, `MemStorage`, `PartitionController`) for downstream conformance suites and chaos harnesses. Off by default.
- `failpoints` — enables `fail` crate integration so the toolkit's instrumented sites can be driven from chaos tests. Off by default.

## Out of scope (today)

The toolkit deliberately stops short of shipping:

- A `Network` implementation. Single-group services can roll their own with the codec helpers; multi-group routing belongs in the host runtime that owns the cluster lifecycle.
- A snapshot-streaming transport. Filesystem-backed sidecar transports are opinionated enough that lifting one would constrain its callers.
- Multi-group host orchestration. Hosting N omnipaxos instances per process is a deployment-shaped concern, not a library concern.

These may move into the toolkit when a clear shared shape emerges.
