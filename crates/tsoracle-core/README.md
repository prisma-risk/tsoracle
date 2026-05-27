# tsoracle-core

Sync algorithm core for the [tsoracle](https://github.com/prisma-risk/tsoracle) timestamp oracle.

No I/O, no async, no tokio. Runtime-neutral, property-testable in microseconds. Anything you need to reason about tsoracle's timestamp generation lives here — the higher layers (`tsoracle-server`, `tsoracle-client`, the consensus drivers) are wiring around this core.

## What's in the box

- `Allocator` — the window allocator. Hands out 46/18-bit-packed `Timestamp`s within a granted window, tracks the leader's epoch, and exposes the `WindowGrant` lifecycle used by the server's leader-watch + fence pipeline.
- `Timestamp` — the 64-bit packed representation. `PHYSICAL_MS_MAX` and `LOGICAL_MAX` document the bit budget; `TimestampError` covers the narrow space of invalid encodings.
- `Epoch` — the leader-watermark identifier the failover fence checks. Persisted alongside the high water by every `ConsensusDriver` impl.
- `CoreError` — the algorithm-level error type. Server, client, and driver crates wrap it in their own variants.

## Feature flags

- `std` (default) — enables `std`-dependent surface (most of it). Disabling gives a `no_std` core; the public surface shrinks but the allocator + timestamp math still work.
- `serde` — derives `Serialize` / `Deserialize` on the public types so they cross wire and storage boundaries cleanly.

## Documentation

- [`docs/architecture-deep-dive.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/architecture-deep-dive.md) — the algorithm and bit-packing contract this crate implements.
- [`docs/consensus-integration.md`](https://github.com/prisma-risk/tsoracle/blob/main/docs/consensus-integration.md) — how the `Allocator` interacts with a `ConsensusDriver` impl.
