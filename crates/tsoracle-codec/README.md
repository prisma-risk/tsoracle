# tsoracle-codec

The one version-prefixed `postcard` codec shared across [tsoracle](https://github.com/prisma-risk/tsoracle) — every persisted blob is framed as `[version_byte | postcard(value)]` so an on-disk layout change fails loudly instead of misdecoding.

## Why it exists

Both consensus toolkits (`tsoracle-paxos-toolkit`, `tsoracle-openraft-toolkit`) carried a near-identical `encode`/`decode`/`CodecError` copy of this framing. Every persisted struct — a `Ballot`, a `HighWaterCommand` log entry, a snapshot, a Raft log record — is stamped with a leading schema-version byte; a node upgraded mid-flight that reads prior-version bytes hits a structured version mismatch rather than silently parsing them against a new struct layout. This crate is the single home for that framing.

## What's in the box

- `encode(version, value)` — serializes `value` with `postcard` and prepends `version`, yielding `[version | postcard(value)]`.
- `decode(expected_version, bytes)` — checks the leading byte against `expected_version` (returning `CodecError::Version` on mismatch) before deserializing the body, and rejects any bytes left unconsumed past a valid body with `CodecError::TrailingBytes` rather than silently discarding them.
- `CodecError` — `Empty`, `Version { expected, actual }`, `Encode(postcard::Error)`, `Decode(postcard::Error)`, `TrailingBytes { extra }`. Encode and decode failures are kept distinct, and the underlying `postcard::Error` is carried as the error source (no `From` conversion) so a stray `?` never silently becomes a `CodecError`. `TrailingBytes` flags surplus bytes after an otherwise-valid body — for a format that exists to catch drift, garbage appended by a partial overwrite is a corruption signal, not noise to drop.

The schema-version *number* is deliberately **not** owned here. `version` is a parameter, so each consumer keeps its own `SCHEMA_VERSION` constant and evolves its on-disk format independently of the others.

## Quick reference

```toml
# consumer Cargo.toml
[dependencies]
tsoracle-codec = { workspace = true }
```

```rust,ignore
// Each consumer owns its own schema version.
pub const SCHEMA_VERSION: u8 = 1;

let framed = tsoracle_codec::encode(SCHEMA_VERSION, &value)?;
let value: MyType = tsoracle_codec::decode(SCHEMA_VERSION, &framed)?;
```

Bump the consumer's `SCHEMA_VERSION` when a persisted struct's `postcard` layout changes in a backward-incompatible way (field reorder/insert/remove); a stale reader then fails with `CodecError::Version` instead of misdecoding.

## Related

- [`postcard`](https://crates.io/crates/postcard) — the compact `serde` wire format this frames.
- `tsoracle-failpoint`, `tsoracle-yieldpoint` — the other shared micro-crates that exist so a single behavior is not duplicated across the workspace.
