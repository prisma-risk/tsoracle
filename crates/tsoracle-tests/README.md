# tsoracle-tests

**Internal — no public API.** Cross-crate integration tests for [tsoracle](https://github.com/prisma-risk/tsoracle).

This crate exists so the end-to-end tests that exercise `tsoracle-client` against `tsoracle-server` (and vice versa) don't need a dev-dep cycle between the two libs. Releasing one no longer requires the other to already be on crates.io. You almost certainly do not want to depend on this crate.

## What's in here

All real test code lives under `tests/`. Run with `cargo test -p tsoracle-tests`. Some tests are gated behind features:

- `failpoints` — mirrors `tsoracle-server/failpoints` for failpoint-driven crash-recovery tests. `cargo test -p tsoracle-tests --features failpoints`.
- `metrics` — mirrors `tsoracle-client/metrics` for the client-recorder fake test. `cargo test -p tsoracle-tests --features metrics`.
- `tls-rustls` (default) / `tls-native` — pick the TLS backend that matches your environment's lockfile.

## Why published?

The workspace doesn't gate any crate via `publish = false`. This crate's release to crates.io is harmless (no public API to depend on) and lets the workspace's release pipeline stay uniform.
