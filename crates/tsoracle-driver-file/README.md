# tsoracle-driver-file

Single-node, fsync-durable `ConsensusDriver` implementation for [tsoracle](https://github.com/prisma-risk/tsoracle).

If you don't need HA — local demo, embedded use, "downtime is acceptable" — this is the driver to wire. Zero operational overhead, one process, one disk, no cluster to deploy.

## What it provides

- `FileDriver` — the `ConsensusDriver` impl. Persists the committed high-water as a 17-byte CRC-checked record (`"TSOR"` magic + version + `u64` high-water + crc32c) and `fsync`s the file before any timestamp in that window is handed out. The fsync is the durability boundary — a crash and restart can never rewind the issued timestamp sequence.
- `FileDriver::open_or_init(dir)` — opens an existing state directory or initializes a fresh one. Acquires an exclusive OS-level lock on a `LOCK` sentinel file so two concurrent opens against the same directory cannot silently produce diverging in-memory caches.
- `FileDriver::init_seeded(dir, seed)` — one-shot migration entry point for callers seeding from a prior sequence.
- `FileDriverError` — the driver-specific error type. `AlreadyLocked` surfaces the concurrent-open guard; the rest covers I/O, codec, and integrity-check failures.

## Stopped-state export

`StoppedFileState::open(dir)` takes the existing native lock read-only without initialization. `guard.receipt()` validates and exports reservation, complete retained leases and dense counters with a versioned SHA-256 inventory. Keep the guard alive through copying and hash verification. Missing required files, unknown or temporary entries, corruption, replacement of the lock and lock contention refuse. Absent lazy dense and lease files mean empty sets and are explicitly recorded in the inventory. The additive library API is unconditional and first ships in version 2.3.0. The CLI exposes `tsoracle export file --state-dir <dir> --compact` only with `single-node` and `file`. See the [capture and recovery contract](https://github.com/prisma-risk/tsoracle/blob/main/docs/stopped-file-export.md).

## When to use this vs HA drivers

This driver is the right choice when:
- A single process is acceptable (demo, embedded, test fixture).
- An outage of the timestamp oracle is acceptable (no replication = no automatic failover).
- The fsync-per-advance cost fits your throughput budget.

For HA, see [`tsoracle-driver-openraft`](https://crates.io/crates/tsoracle-driver-openraft) or [`tsoracle-driver-paxos`](https://crates.io/crates/tsoracle-driver-paxos). The [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md#choosing-a-driver) "Choosing a driver" section covers the tradeoffs.

## Feature flags

- `single-node` enables shared semantic validation at startup and locks seeded initialization. Off by default. Public types and the additive receipt API remain unconditional.
- `failpoints` — enables `fail` crate injection for chaos-test coverage of the storage path. Off by default.
- `bt` — enables backtrace capture in error variants.

## Examples

- [`examples/embedded-server`](https://github.com/prisma-risk/tsoracle/tree/main/examples/embedded-server) — host the server in your own binary with `FileDriver` + graceful Ctrl-C shutdown.
- The standalone `tsoracle serve` CLI in [`tsoracle-bin`](https://crates.io/crates/tsoracle-bin) wires `FileDriver` up automatically.
