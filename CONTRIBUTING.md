# Contributing

Thanks for your interest in tsoracle. This guide covers the local setup, the checks CI will run on your PR, and the conventions we follow.

## Setup

This is a Rust project. The toolchain channel is pinned in [`rust-toolchain.toml`](rust-toolchain.toml); running any `cargo` command will install the matching version for you.

You'll also need:

- **`protoc`** — the Protocol Buffers compiler. `tonic-prost-build` invokes it during `cargo build`. On macOS: `brew install protobuf`. On Debian/Ubuntu: `sudo apt-get install -y protobuf-compiler`.
- **LLVM/Clang (`libclang`)** — `librocksdb-sys` invokes `bindgen` during `cargo build`, which needs `libclang` to generate FFI bindings. The default features of `tsoracle-openraft-toolkit` (`rocksdb-log-store`) and `tsoracle-driver-openraft` (`rocksdb-snapshot-store`) enable `rocksdb`, so any workspace build (`cargo build --workspace [--all-features]`) triggers it. On macOS, the Xcode Command Line Tools (`xcode-select --install`) include it; if `bindgen` still can't find it, `brew install llvm` and `export LIBCLANG_PATH="$(brew --prefix llvm)/lib"` (optionally `LLVM_CONFIG_PATH="$(brew --prefix llvm)/bin/llvm-config"`). On Debian/Ubuntu: `sudo apt-get install -y clang libclang-dev` (same as CI).
- **[`buf`](https://buf.build/docs/installation)** (optional, only needed if you touch `.proto` files) — CI runs `buf lint`, `buf format`, and `buf breaking` against `crates/tsoracle-proto/proto`.
- **[`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny)** (optional) — CI runs `cargo deny check` against [`deny.toml`](deny.toml) to enforce the license allow-list and advisory policy.
- **[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov)** (optional, only needed to reproduce coverage locally) — CI runs `cargo llvm-cov` and uploads the resulting `lcov.info` to [Coveralls](https://coveralls.io/github/prisma-risk/tsoracle). Install with `cargo install cargo-llvm-cov`. Coverage is reported only — the build does not fail on a coverage drop.

## Workspace layout

The repo is a Cargo workspace. The crates under `crates/` are:


| Crate                          | Purpose                                                              |
| ------------------------------ | -------------------------------------------------------------------- |
| `tsoracle-proto`               | gRPC service & message definitions                                   |
| `tsoracle-core`                | window allocator, epoch, monotonicity invariants                     |
| `tsoracle-openraft-toolkit`    | reusable openraft glue: TypeConfig macro, RocksDB log store, helpers |
| `tsoracle-consensus`           | the `ConsensusDriver` trait and shared types                         |
| `tsoracle-driver-file`         | single-node, fsync-backed driver                                     |
| `tsoracle-driver-openraft`     | openraft-backed `ConsensusDriver` for multi-node deployments         |
| `tsoracle-server`              | the tonic service and leader handoff                                 |
| `tsoracle-client`              | gRPC client with leader discovery and coalescing                     |
| `tsoracle-bin`                 | the `tsoracle` CLI                                                   |


Runnable examples live under `examples/` (`embedded-server`, `failover-demo`, `openraft-standalone`, `openraft-piggyback`) and are part of the default workspace members, so `cargo check` covers them too.

## Documentation

tsoracle's prose documentation lives in two trees with different audiences:

1. **Root [`docs/`](docs/) — the deep dive.** Covering everything from getting started through architecture, allocator internals, consensus integration patterns, operations, and per-example walkthroughs. Browsable on GitHub; indexed by [DeepWiki](https://deepwiki.com/prisma-risk/tsoracle). See [`docs/README.md`](docs/README.md) for the table of contents.
2. **In-crate `docs` modules — the docs.rs subset.** Three chapters, each pulled into a crate's `docs` module via `#[doc = include_str!(...)]` so it renders on docs.rs alongside the API reference:

   | Chapter | Source | Rendered at |
   | --- | --- | --- |
   | `algorithm` | [`crates/tsoracle-core/src/docs/algorithm.md`](crates/tsoracle-core/src/docs/algorithm.md) | `tsoracle_core::docs::algorithm` |
   | `consensus_integration` | [`crates/tsoracle-consensus/src/docs/consensus_integration.md`](crates/tsoracle-consensus/src/docs/consensus_integration.md) | `tsoracle_consensus::docs::consensus_integration` |
   | `operations` | [`crates/tsoracle-server/src/docs/operations.md`](crates/tsoracle-server/src/docs/operations.md) | `tsoracle_server::docs::operations` |

   The "guide" badge in the README points at [`tsoracle-server`'s `docs` index](https://docs.rs/tsoracle-server/latest/tsoracle_server/docs/index.html), which cross-links into the other two crates' `docs` modules.

### Which file do I update?

- **Algorithm, `ConsensusDriver` contract, or `operations` content** that belongs on docs.rs → update the in-crate file. The matching root `docs/` chapter (`the-allocator.md`, `consensus-integration.md`, `operations.md`) should mirror or link to it; keep them consistent in the same PR.
- **Anything else** — getting started, sub-topics like the failover fence or monotonicity proof, deployment topologies, example walkthroughs — lives only in root `docs/`. Edit the chapter directly.
- **Protocol-visible behavior or allocator changes** — update the relevant chapter(s) (in-crate **and** root deep-dive, where both apply) in the same PR.

Preview the in-crate chapters locally with:

```bash
cargo doc -p tsoracle-server --no-deps --open
```

Then navigate to the `docs` module on the generated page.

## Running the checks locally

CI runs the commands below — match them before pushing and your PR will be green on the first try:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --all-features
cargo test  --workspace --all-features
```

The `--all-features` flag activates the `failpoints` Cargo feature on each opting-in crate, so the failpoint suite (see [`docs/failpoint-testing.md`](docs/failpoint-testing.md)) is part of the normal `cargo test` run. `--all-features` also activates the `yieldpoints` Cargo feature, which gates async yield-point tests in `tsoracle-driver-paxos` (see [`docs/yieldpoint-testing.md`](docs/yieldpoint-testing.md) — the async counterpart of failpoints, for tests that need to park production code in an async path without blocking a tokio worker). To run just the failpoint suite:

```bash
make test-failpoints
```

### Pre-commit hook

A tracked pre-commit hook in [`.husky/pre-commit`](.husky/pre-commit) runs the first two of those checks (`cargo fmt --check` and clippy) and blocks the commit on failure.

It auto-installs on your first `cargo test` (any flavor — workspace-wide, or `-p tsoracle-core`) via [`husky-rs`](https://crates.io/crates/husky-rs), a dev-dependency on `tsoracle-core` that sets `core.hooksPath = .husky` for this clone. No manual setup is needed as long as you run cargo before your first commit. If you want the hook active before any cargo invocation, run `make install-hooks` to set `core.hooksPath = .husky` directly.

Bypass with `git commit --no-verify` when you know what you're doing — CI runs the same checks regardless, so a bypassed commit will still fail upstream.

If you touched anything under `crates/tsoracle-proto/proto/`:

```bash
buf lint     crates/tsoracle-proto/proto
buf format   --diff --exit-code crates/tsoracle-proto/proto
buf breaking crates/tsoracle-proto/proto \
  --against ".git#branch=main,subdir=crates/tsoracle-proto/proto"
```

Supply-chain check:

```bash
cargo deny check
```

Coverage (library crates only — `tsoracle-bin` and `examples/` are excluded):

```bash
make coverage   # writes lcov.info at the workspace root
```

## Panic policy: `unwrap` and `expect`

Library and binary crates avoid `.unwrap()` and `.expect(...)` in non-test code. Each library/binary crate root carries:

```rust
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]
```

This is an inner attribute, scoped to that crate's compilation unit. Two consequences worth understanding before editing it:

- **The lint is off during `cargo test --lib`.** `cfg(not(test))` is false when the lib is compiled under `--test`, so `#[cfg(test)] mod tests { ... }` blocks inside `src/` are exempt by construction. This is more reliable than `clippy.toml`'s `allow-unwrap-in-tests` setting, which has known bugs for helper functions inside test modules ([rust-clippy #9612](https://github.com/rust-lang/rust-clippy/issues/9612)).
- **Integration tests, examples, and benches are separate compilation units.** They don't inherit the lib's inner attributes. They aren't linted, full stop — no per-file `#![allow]` needed, even for module-scope helpers in `tests/foo.rs`. This sidesteps [rust-clippy #13981](https://github.com/rust-lang/rust-clippy/issues/13981), where `allow-unwrap-in-tests` fails to cover `tests/`, `examples/`, and `benches/`.

`tsoracle-proto` is excluded — it's `tonic-prost-build`-generated wire code with `#![allow(clippy::all)]`. Example and benchmark crates don't carry the attribute either; demonstration code is allowed to be terse.

Because CI runs `cargo clippy ... -- -D warnings`, an unannotated `.unwrap()` or `.expect()` in runtime code is a hard build failure.

When you genuinely need `.unwrap()` or `.expect(...)` in runtime code — typically because the invariant is statically guaranteed by a `const`, by surrounding control flow, or by a build-time artifact — annotate the callsite with `#[expect(clippy::expect_used, reason = "...")]` (or the `unwrap_used` variant). The `reason` field is required by convention:

1. **Explain the invariant.** What makes this call unreachable in practice? Name the const, the cfg, or the upstream check that holds.
2. **Link to a tracking issue.** If a follow-up to replace the panic with typed-error propagation exists, write `Tracked by #N.` so the marker stays connected to ongoing work.

Place the `#[expect]` on the smallest enclosing item: prefer a `let`-statement attribute, fall back to the enclosing function when the call isn't bound to a `let`. `#[expect]` is preferred over `#[allow]` because it warns if the expected lint stops firing — the marker self-clears when the panic path is removed.

Example:

```rust
#[expect(
    clippy::expect_used,
    reason = "`KEY` is a `const &'static str` of valid ASCII; `MetadataKey::from_bytes` cannot fail here. Tracked by #5."
)]
let key = MetadataKey::from_bytes(KEY.as_bytes()).expect("valid key");
```

## Performance-critical-path rules

A handful of files sit on the request-handling hot path. They carry a `// #[PerformanceCriticalPath]` marker as their first line, and CI's `critical-path` job ([`scripts/check-critical-path.sh`](scripts/check-critical-path.sh)) enforces a small set of source-level rules against them — no `tracing::info!`/`warn!`/`error!`, no `println!`, no synchronous I/O, no long synchronous compute. See [`docs/performance-critical-path.md`](docs/performance-critical-path.md) for the full rule set, the marker-placement contract, and the current marked-file list. If your edit touches a marked file, run `CRITICAL_PATH_STRICT=1 ./scripts/check-critical-path.sh` locally before pushing.

## Working with git

- **Write commit messages like an email to your teammates.** This repo follows [Conventional Commits](https://www.conventionalcommits.org/) prefixes (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`), optionally with a scope (`feat(server): ...`). See [How to Write a Git Commit Message](https://cbea.ms/git-commit/) for guidance on the body itself — explain *why*, not *what*.
- Do **rebase** and **squash** onto the latest `main` before opening a PR.
- Do **NOT** rebase after publishing a PR. Push fixup commits on top so reviewers can see what changed between rounds; squash happens at merge.

## Releases

Releases run on [release-plz](https://release-plz.dev/). Each crate is versioned **independently** — a change to one crate bumps only that crate (and any dependents whose `version = "..."` pin release-plz updates), not the whole workspace. The flow is:

1. Land commits on `main` using [Conventional Commits](https://www.conventionalcommits.org/) prefixes (`feat:`, `fix:`, `chore:`, etc.). The prefix determines the semver bump, and `release_commits = "^(feat|fix|perf)"` in [`release-plz.toml`](release-plz.toml) means only `feat:`/`fix:`/`perf:` commits trigger a release for the crate they touch — `chore:`, `docs:`, `refactor:`, `style:`, `test:`, and `build:` do not.
2. The `release-plz PR` workflow opens (or updates) a "Release PR" with the version bump and per-crate `CHANGELOG.md` diffs.
3. Reviewing and merging that PR triggers the `release-plz release` job: it tags each crate (e.g. `tsoracle-core-v0.2.0`) and runs `cargo publish` in dependency order. A GitHub Release is created per tag.

> **Adding public API to a library crate (e.g. `tsoracle-proto`) must use `feat:` or `fix:`, never `refactor:`.** The API surface is user-visible to *dependent crates* even when the gRPC wire contract (the `v1` proto package) is unchanged — a new `pub` item, re-export, or function is a release-worthy change. Because crates version independently and `refactor:` does not trigger a release, labelling such a change `refactor:` leaves the published crate behind: a dependent that already uses the new symbol then fails `cargo publish --verify` because it resolves the dependency to the stale, still-published version. Keep the proto package version (`tsoracle.v1`) and the crate version (`tsoracle-proto` on crates.io) distinct in your head — the former is the wire contract, the latter is the Rust packaging semver that dependents compile against.

### Before the first publish (one-time bootstrap)

release-plz can only manage crates that already exist on crates.io, so the first publish has to happen out-of-band:

1. Generate a crates.io API token with `publish-new` + `publish-update` scopes and add it as the `CARGO_REGISTRY_TOKEN` secret in the GitHub repo settings.
2. Run `make release-dry-run` locally to confirm every crate packages cleanly.
3. Publish each crate manually in dependency order with `cargo publish -p <crate>`. The order is in `RELEASE_CRATES` in the [Makefile](Makefile); crates.io rejects publishes whose path-resolved deps aren't yet on the registry, so order matters. If a publish fails mid-list, fix the issue and resume from that crate.
4. From the next merge onward, release-plz takes over.

### Pre-flight check for manifest changes

Before opening a PR that touches `Cargo.toml` metadata (license, readme, keywords, the per-dep `version = "..."` pins, etc.), run:

```bash
make release-dry-run
```

It iterates `cargo publish --dry-run -p <crate>` over every publishable crate in dependency order. Catches packaging issues (missing readme, license-allow-list violations, broken `include` lists) before they reach the release PR.

