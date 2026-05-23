# tsoracle-proto

Generated protobuf / gRPC wire types for the [tsoracle](https://github.com/prisma-risk/tsoracle) timestamp oracle.

This crate is the stable wire contract shared between [`tsoracle-server`](https://crates.io/crates/tsoracle-server) and [`tsoracle-client`](https://crates.io/crates/tsoracle-client). Breaking changes are enforced via `buf breaking` in CI; new fields must be backwards-compatible.

## What's in the box

- `v1` module — all generated types from `proto/tsoracle/v1/tso.proto`. Includes the `TsoService` gRPC service definition, the `GetTs` / `GetTsBatch` request/response types, and the `LeaderHint` binary-trailer payload type the server emits on follower-redirect.

You probably don't depend on this crate directly. `tsoracle-server` and `tsoracle-client` re-export what consumers need at their respective seams.

## Feature flags

- `serde` — derives `Serialize` / `Deserialize` on the generated types so they cross storage and logging boundaries without a manual converter.
- `reflection` — bundles a binary descriptor set and enables `tonic-reflection`-based server reflection. Pulls in `tonic-reflection`; off by default to keep dependency closure small for non-reflection consumers.

## Regenerating

The codegen runs at build time via `build.rs`. Modify `proto/tsoracle/v1/tso.proto`, rebuild, and the generated module updates. `buf breaking` runs in CI against `main` to catch incompatible changes before merge.
