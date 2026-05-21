# Contributing

Thanks for your interest in tsoracle. This guide covers the local setup, the checks CI will run on your PR, and the conventions we follow.

## Setup

This is a Rust project. The toolchain channel is pinned in [`rust-toolchain.toml`](rust-toolchain.toml); running any `cargo` command will install the matching version for you.

You'll also need:

- **`protoc`** — the Protocol Buffers compiler. `tonic-prost-build` invokes it during `cargo build`. On macOS: `brew install protobuf`. On Debian/Ubuntu: `sudo apt-get install -y protobuf-compiler`.
- **[`buf`](https://buf.build/docs/installation)** (optional, only needed if you touch `.proto` files) — CI runs `buf lint`, `buf format`, and `buf breaking` against `crates/tsoracle-proto/proto`.
- **[`cargo-deny`](https://github.com/EmbarkStudios/cargo-deny)** (optional) — CI runs `cargo deny check` against [`deny.toml`](deny.toml) to enforce the license allow-list and advisory policy.
- **[`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov)** (optional, only needed to reproduce coverage locally) — CI runs `cargo llvm-cov` and uploads the resulting `lcov.info` to [Coveralls](https://coveralls.io/github/prisma-risk/tsoracle). Install with `cargo install cargo-llvm-cov`. Coverage is reported only — the build does not fail on a coverage drop.
- **[`cargo-edit`](https://github.com/killercup/cargo-edit)** (optional, only needed for releases) — provides `cargo set-version`, used by the Makefile's `release-bump` target to bump `workspace.package.version` and rewrite every intra-workspace dep version ref in one shot. Install with `cargo install cargo-edit`.

## Workspace layout

The repo is a Cargo workspace. The crates under `crates/` are:


| Crate                  | Purpose                                          |
| ---------------------- | ------------------------------------------------ |
| `tsoracle-proto`       | gRPC service & message definitions               |
| `tsoracle-core`        | window allocator, epoch, monotonicity invariants |
| `tsoracle-consensus`   | the `ConsensusDriver` trait and shared types     |
| `tsoracle-driver-file` | single-node, fsync-backed driver                 |
| `tsoracle-server`      | the tonic service and leader handoff             |
| `tsoracle-client`      | gRPC client with leader discovery and coalescing |
| `tsoracle-bin`         | the `tsoracle` CLI                               |


Runnable examples live under `examples/` (`embedded-server`, `failover-demo`, `openraft-cluster`) and are part of the default workspace members, so `cargo check` covers them too.

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

### Pre-commit hook

A tracked pre-commit hook in [`.githooks/pre-commit`](.githooks/pre-commit) runs the first two of those checks (`cargo fmt --check` and clippy) and blocks the commit on failure. Enable it once per clone:

```bash
make install-hooks
```

That writes `core.hooksPath = .githooks` into your local `.git/config`. Bypass with `git commit --no-verify` when you know what you're doing — CI runs the same checks regardless, so a bypassed commit will still fail upstream.

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

## Working with git

- **Write commit messages like an email to your teammates.** This repo follows [Conventional Commits](https://www.conventionalcommits.org/) prefixes (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`), optionally with a scope (`feat(server): ...`). See [How to Write a Git Commit Message](https://cbea.ms/git-commit/) for guidance on the body itself — explain *why*, not *what*.
- Do **rebase** and **squash** onto the latest `main` before opening a PR.
- Do **NOT** rebase after publishing a PR. Push fixup commits on top so reviewers can see what changed between rounds; squash happens at merge.

## Release checklist

All crates are released together at the single version in `workspace.package.version`. Bumping that one field bumps every crate.

1. **Pre-flight.** All of the following pass locally:
  ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   cargo test  --workspace --all-features
  ```
2. **Docs.** Any user-visible change is reflected in the relevant file under `docs/`. If the API or CLI surface changed, the README is updated too.
3. **Version bump.** Update `workspace.package.version` in the root `Cargo.toml`. Commit on `main` as `chore: release vX.Y.Z`.
4. **Publish to crates.io in dependency order.** Each crate must be on the registry before the crate that depends on it. Use `cargo publish -p <crate>` for each, in this order. If a publish fails, fix and resume from that crate — do not skip ahead.
5. **Tag and announce.** Tag the release commit as `vX.Y.Z`, push the tag, then draft the release notes on the GitHub Releases page.

