# tsoracle Makefile
#
# These targets mirror the checks CI runs — see .github/workflows/ci.yml and
# CONTRIBUTING.md. Run `make` (or `make ci`) before pushing and your PR will
# match CI on the first try.

CARGO      ?= cargo
PROTO_DIR  := crates/tsoracle-proto/proto

# `buf breaking` base. Defaults to local `main`; override with the CI-equivalent
# remote ref when you want the exact check CI will run:
#   make proto-breaking PROTO_BASE='.git#branch=origin/main,subdir=crates/tsoracle-proto/proto'
# Note: `#` must be escaped in Makefile values, otherwise it starts a comment.
PROTO_BASE ?= .git\#branch=main,subdir=crates/tsoracle-proto/proto

# Publish order for the release-* targets — must match the dependency order
# documented in CONTRIBUTING.md (each crate depends only on those before it).
RELEASE_CRATES := \
    tsoracle-proto \
    tsoracle-core \
    tsoracle-consensus \
    tsoracle-driver-file \
    tsoracle-server \
    tsoracle-client \
    tsoracle-bin

# Workspace version read once from the root Cargo.toml. Used by release-tag to
# enforce that the tag, HEAD's commit message, and the bumped version all agree.
WORKSPACE_VERSION := $(shell grep -E '^version[[:space:]]*=' Cargo.toml | head -1 | sed -E 's/.*"([^"]+)".*/\1/')

.PHONY: all ci check fmt fmt-check lint fix build test test-failpoints doc \
        proto proto-lint proto-fmt proto-fmt-check proto-breaking \
        deny coverage clean help install-hooks \
        bench bench-throughput-sweep bench-latency \
        release-bump release-dry-run release-publish release-tag

# Default target: full CI parity.
all: ci

# Composite targets ----------------------------------------------------------

ci: fmt-check lint build test proto deny

check: fmt-check lint build test

# Rust workspace -------------------------------------------------------------

fmt:
	$(CARGO) fmt --all

fmt-check:
	$(CARGO) fmt --all -- --check

lint:
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings

fix:
	$(CARGO) clippy --workspace --all-targets --all-features \
	  --fix --allow-dirty --allow-staged
	$(CARGO) fmt --all

build:
	$(CARGO) build --workspace --all-features

test:
	$(CARGO) test --workspace --all-features

# Run just the failpoint suite (fault-injection tests gated by the
# `failpoints` Cargo feature on each opting-in crate). See
# docs/failpoint-testing.md for the model.
test-failpoints:
	$(CARGO) test --workspace \
	  --features tsoracle-driver-file/failpoints \
	  --features tsoracle-server/failpoints,tsoracle-server/test-fakes \
	  --features openraft-toolkit/failpoints,openraft-toolkit/rocksdb-log-store

doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps --all-features

clean:
	$(CARGO) clean

# Point this clone's git at the tracked .githooks/ directory so the pre-commit
# hook runs `cargo fmt --check` and clippy before every commit. One-time setup
# per clone; the config write is local to .git/config and not tracked.
install-hooks:
	git config core.hooksPath .githooks
	@echo "core.hooksPath -> .githooks (bypass with 'git commit --no-verify')"

# Protobuf -------------------------------------------------------------------
# Mirrors the `buf` job in .github/workflows/ci.yml.

proto: proto-lint proto-fmt-check proto-breaking

proto-lint:
	buf lint $(PROTO_DIR)

proto-fmt:
	buf format -w $(PROTO_DIR)

proto-fmt-check:
	buf format --diff --exit-code $(PROTO_DIR)

proto-breaking:
	buf breaking $(PROTO_DIR) --against "$(PROTO_BASE)"

# Supply chain ---------------------------------------------------------------

deny:
	$(CARGO) deny check

# Coverage -------------------------------------------------------------------
# Produces lcov.info at the workspace root; the `coverage` CI job uploads it
# to Coveralls. Library coverage only — the tsoracle CLI shim and the example
# crates are excluded because their behavior is exercised transitively by
# integration tests on tsoracle-server, and including them would dilute the
# signal. Requires cargo-llvm-cov: cargo install cargo-llvm-cov.
#
# `openraft-toolkit/src/lifecycle/{bootstrap,membership}.rs` are excluded by
# `--ignore-filename-regex`: they are thin async wrappers around real
# `Raft<C, SM>` calls and need a live raft to execute, which the toolkit's own
# tests deliberately don't stand up (see `tests/lifecycle.rs` header). Coverage
# for those wrappers is earned downstream by the openraft consumer that uses
# them; the compile-time signature shims in `tests/lifecycle.rs` catch API drift.
#
# `benchmarks/stress/src/bin/stress.rs` is excluded for the same reason as the
# `tsoracle` CLI shim: it is a `clap` argument-parsing wrapper around
# `stress::run` / `stress::run_inject_violation`, which the `tests/smoke.rs`
# integration tests exercise end-to-end. `benchmarks/stress/src/topology/raft.rs`
# and `process.rs` are `unimplemented!()` placeholders for future topology
# variants and intentionally do not execute under any current test.

coverage:
	$(CARGO) llvm-cov \
	  --workspace --all-features \
	  --exclude tsoracle \
	  --exclude example-embedded-server \
	  --exclude example-failover-demo \
	  --exclude example-openraft-piggyback \
	  --exclude example-openraft-standalone \
	  --exclude bench-minimal \
	  --ignore-filename-regex '(crates/openraft-toolkit/src/lifecycle/(bootstrap|membership)|benchmarks/stress/src/bin/stress|benchmarks/stress/src/topology/(raft|process))\.rs$$' \
	  --lcov --output-path lcov.info

# Release --------------------------------------------------------------------
# The release flow has four steps, mapped to four explicit targets. There is
# deliberately no `release` umbrella target: a partial publish should never be
# the consequence of a single `make` invocation.
#
#   1) make release-bump VERSION=X.Y.Z   # bump workspace + intra-workspace deps, commit
#   2) make release-dry-run              # package every crate in publish order, fail fast
#   3) make release-publish              # the real, irreversible publishes
#   4) make release-tag                  # annotated tag from workspace.package.version, push
#
# Step 1 uses `cargo set-version` (from cargo-edit). Install with:
#   cargo install cargo-edit
# Without it, the per-crate `tsoracle-X = { ..., version = "..." }` refs would
# be left at the old version and the published metadata would be inconsistent.

release-bump:
	@: $${VERSION:?VERSION is required. Usage: make release-bump VERSION=X.Y.Z}
	@echo "$(VERSION)" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$$' \
	  || { echo "error: VERSION='$(VERSION)' is not X.Y.Z[-pre]"; exit 1; }
	@test -z "$$(git status --porcelain)" \
	  || { echo "error: working tree has uncommitted changes — commit or stash first"; exit 1; }
	@branch=$$(git symbolic-ref --short HEAD); \
	  test "$$branch" = "main" \
	  || { echo "error: not on main (current branch: $$branch)"; exit 1; }
	@command -v cargo-set-version >/dev/null 2>&1 \
	  || { echo "error: cargo-set-version not found. Install with: cargo install cargo-edit"; exit 1; }
	$(CARGO) set-version --workspace $(VERSION)
	$(CARGO) update --workspace
	git add Cargo.toml crates/*/Cargo.toml Cargo.lock
	git commit -m "chore: release v$(VERSION)"
	@echo
	@echo "Bumped workspace to v$(VERSION). Next: make release-dry-run"

# Dry-run packaging for every crate, in publish order. Catches packaging,
# metadata, and license-allow-list issues before any irreversible upload.
release-dry-run:
	@for crate in $(RELEASE_CRATES); do \
	  echo "==> cargo publish --dry-run -p $$crate"; \
	  $(CARGO) publish --dry-run -p $$crate || exit 1; \
	done
	@echo
	@echo "All crates packaged cleanly. Next: make release-publish"

# Real publish loop. If a publish fails mid-list, the upstream crates are
# already on crates.io permanently — fix the failed crate and resume by
# re-invoking this target (already-published versions are idempotent no-ops).
release-publish:
	@test -z "$$(git status --porcelain)" \
	  || { echo "error: working tree has uncommitted changes — release commit must be HEAD"; exit 1; }
	@branch=$$(git symbolic-ref --short HEAD); \
	  test "$$branch" = "main" \
	  || { echo "error: not on main (current branch: $$branch)"; exit 1; }
	@expected="chore: release v$(WORKSPACE_VERSION)"; \
	  actual=$$(git log -1 --pretty=%s); \
	  test "$$actual" = "$$expected" \
	    || { echo "error: HEAD subject is '$$actual', expected '$$expected'. Did you run release-bump?"; exit 1; }
	@for crate in $(RELEASE_CRATES); do \
	  echo "==> cargo publish -p $$crate"; \
	  $(CARGO) publish -p $$crate \
	    || { echo; echo "publish failed at $$crate. Fix the issue and re-run: make release-publish"; exit 1; }; \
	done
	@echo
	@echo "All crates published at v$(WORKSPACE_VERSION). Next: make release-tag"

# Tag the release commit and push the tag. Reads the version from the root
# Cargo.toml so the tag can never disagree with what was actually published.
release-tag:
	@test -n "$(WORKSPACE_VERSION)" \
	  || { echo "error: could not parse workspace.package.version from Cargo.toml"; exit 1; }
	@test -z "$$(git status --porcelain)" \
	  || { echo "error: working tree has uncommitted changes"; exit 1; }
	@branch=$$(git symbolic-ref --short HEAD); \
	  test "$$branch" = "main" \
	  || { echo "error: not on main (current branch: $$branch)"; exit 1; }
	@expected="chore: release v$(WORKSPACE_VERSION)"; \
	  actual=$$(git log -1 --pretty=%s); \
	  test "$$actual" = "$$expected" \
	    || { echo "error: HEAD subject is '$$actual', expected '$$expected'. Did you run release-bump?"; exit 1; }
	@git rev-parse "v$(WORKSPACE_VERSION)" >/dev/null 2>&1 \
	  && { echo "error: tag v$(WORKSPACE_VERSION) already exists locally"; exit 1; } \
	  || true
	git tag -a "v$(WORKSPACE_VERSION)" -m "Release v$(WORKSPACE_VERSION)"
	git push origin "v$(WORKSPACE_VERSION)"
	@echo
	@echo "Tagged and pushed v$(WORKSPACE_VERSION). Draft the release notes on GitHub."

# Benchmarks ----------------------------------------------------------------
# `bench-minimal` is a characterization tool, not a CI gate. None of these
# targets are wired into `ci:` or `check:`.

bench:
	$(CARGO) run --release -p bench-minimal --bin bench -- \
	  --clients 64 --ops 1m --batch-size 4

bench-throughput-sweep:
	@for c in 1 4 16 64 256 1024; do \
	  echo "==> --clients $$c"; \
	  $(CARGO) run --release -p bench-minimal --bin bench -- \
	    --clients $$c --ops 1m --batch-size 4 --json > bench-$$c.json || exit 1; \
	done

bench-latency:
	$(CARGO) run --release -p bench-minimal --bin bench -- \
	  --clients 1 --ops 200k --batch-size 1

# Help -----------------------------------------------------------------------

help:
	@echo "tsoracle Makefile targets:"
	@echo ""
	@echo "  all / ci         Full CI parity (default): fmt-check, lint, build,"
	@echo "                   test, proto checks, cargo-deny."
	@echo "  check            Rust-only loop: fmt-check + lint + build + test."
	@echo ""
	@echo "  fmt              cargo fmt --all"
	@echo "  fmt-check        cargo fmt --all -- --check"
	@echo "  lint             clippy --workspace --all-targets --all-features -D warnings"
	@echo "  fix              clippy --fix, then cargo fmt"
	@echo "  build            cargo build --workspace --all-features"
	@echo "  test             cargo test  --workspace --all-features"
	@echo "  doc              cargo doc with RUSTDOCFLAGS=-D warnings"
	@echo "  clean            cargo clean"
	@echo ""
	@echo "  proto            buf: lint + format-check + breaking"
	@echo "  proto-lint       buf lint $(PROTO_DIR)"
	@echo "  proto-fmt        buf format -w (writes changes)"
	@echo "  proto-fmt-check  buf format --diff --exit-code"
	@echo "  proto-breaking   buf breaking --against PROTO_BASE (override to use origin/main)"
	@echo ""
	@echo "  deny             cargo deny check"
	@echo ""
	@echo "  coverage         cargo llvm-cov on library crates -> lcov.info"
	@echo "                   (excludes tsoracle-bin and examples/)"
	@echo ""
	@echo "  bench            Run the bench-minimal characterization workload."
	@echo "  bench-throughput-sweep  Run bench across clients=1..1024 (--json files)."
	@echo "  bench-latency    Run bench at clients=1 for latency-focused output."
	@echo ""
	@echo "  install-hooks    Point this clone's git at .githooks/ (pre-commit fmt+lint)."
	@echo ""
	@echo "Release flow (run in order; see CONTRIBUTING.md):"
	@echo "  release-bump     1) Bump workspace + intra-workspace dep refs, commit."
	@echo "                      Usage: make release-bump VERSION=X.Y.Z"
	@echo "  release-dry-run  2) cargo publish --dry-run for every crate, in order."
	@echo "  release-publish  3) Real publishes to crates.io, in order."
	@echo "  release-tag      4) Annotated tag from workspace version, push to origin."
	@echo ""
	@echo "  help             this message"
