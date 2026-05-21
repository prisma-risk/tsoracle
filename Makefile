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

# Publish order for `release-dry-run` — must match the dependency order
# documented in CONTRIBUTING.md (each crate depends only on those before it).
RELEASE_CRATES := \
    tsoracle-proto \
    tsoracle-core \
    tsoracle-openraft-toolkit \
    tsoracle-consensus \
    tsoracle-driver-file \
    tsoracle-driver-openraft \
    tsoracle-server \
    tsoracle-client \
    tsoracle

.PHONY: all ci check fmt fmt-check lint fix build test test-failpoints doc \
        proto proto-lint proto-fmt proto-fmt-check proto-breaking \
        deny coverage coverage-html clean help install-hooks \
        bench bench-throughput-sweep bench-latency \
        release-dry-run

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
	  --features tsoracle-tests/failpoints \
	  --features tsoracle-openraft-toolkit/failpoints,tsoracle-openraft-toolkit/rocksdb-log-store

doc:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --workspace --no-deps --all-features

clean:
	$(CARGO) clean

# Point this clone's git at the tracked .husky/ directory so the pre-commit
# hook runs `cargo fmt --check` and clippy before every commit. Normally
# installed automatically by husky-rs (a dev-dependency of tsoracle-core) on
# the first `cargo test`; this target is the manual fallback for clones that
# haven't run cargo yet. The config write is local to .git/config and not
# tracked.
install-hooks:
	git config core.hooksPath .husky
	@echo "core.hooksPath -> .husky (bypass with 'git commit --no-verify')"

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
# `cargo llvm-cov` accepts only a single `--ignore-filename-regex`, so the
# per-file exclusions below are built up as separate Make variables (one per
# logical reason) and joined into a single regex at the end.

# Thin async wrappers around real `Raft<C, SM>` calls — they need a live raft
# to execute, which the toolkit's own tests deliberately don't stand up (see
# `tests/lifecycle.rs` header). Coverage is earned downstream by the openraft
# consumer; the compile-time signature shims catch API drift.
COV_IGNORE_OPENRAFT_LIFECYCLE := crates/tsoracle-openraft-toolkit/src/lifecycle/(bootstrap|membership)

# `clap` argument-parsing wrapper around `stress::run` /
# `stress::run_inject_violation`. The library entry points are exercised
# end-to-end by `benchmarks/stress/tests/smoke.rs`.
COV_IGNORE_STRESS_BIN := benchmarks/stress/src/bin/stress

# Shared integration-test bootstrap helper. Gated by `test-support` (and
# `cfg(test)`); never ships in the published library. Lives in `src/` only
# because Cargo doesn't let multiple crates share `tests/common/mod.rs`.
# Exercised implicitly by every test that calls `boot_server` /
# `wait_for_grpc_handshake`; measuring it as production source would punish
# legitimate "rare race" retry paths that local CI never trips.
COV_IGNORE_TEST_SUPPORT := crates/tsoracle-server/src/test_support

COV_IGNORE := ($(COV_IGNORE_OPENRAFT_LIFECYCLE)|$(COV_IGNORE_STRESS_BIN)|$(COV_IGNORE_TEST_SUPPORT))\.rs

# Shared exclude flags so `coverage` (lcov for CI) and `coverage-html` (local
# browsable report) cannot drift apart on which crates participate.
COV_EXCLUDES := \
    --exclude tsoracle \
    --exclude example-embedded-server \
    --exclude example-failover-demo \
    --exclude example-openraft-piggyback \
    --exclude example-openraft-standalone \
    --exclude bench-minimal

# The process-topology smoke tests in `benchmarks/stress/tests/smoke.rs` shell
# out to the `tsoracle` binary, but `--exclude tsoracle` above means
# `cargo llvm-cov` never builds it into `target/llvm-cov-target/`. Build it
# into the regular `target/debug/` first and hand the absolute path to the
# harness via `TSORACLE_BIN`; the smoke tests check that env var before the
# walk-up fallback.
coverage:
	$(CARGO) build --bin tsoracle
	TSORACLE_BIN="$$(pwd)/target/debug/tsoracle" $(CARGO) llvm-cov \
	  --workspace --all-features \
	  $(COV_EXCLUDES) \
	  --ignore-filename-regex '$(COV_IGNORE)$$' \
	  --lcov --output-path lcov.info

# Local HTML report. Output at target/llvm-cov/html/index.html; `--open` opens
# it in the default browser. Re-runs the test suite, same as `coverage`.
coverage-html:
	$(CARGO) build --bin tsoracle
	TSORACLE_BIN="$$(pwd)/target/debug/tsoracle" $(CARGO) llvm-cov \
	  --workspace --all-features \
	  $(COV_EXCLUDES) \
	  --ignore-filename-regex '$(COV_IGNORE)$$' \
	  --html --open

# Release --------------------------------------------------------------------
# Releases are driven by release-plz (see release-plz.toml at the repo root
# and .github/workflows/release-plz.yml). Conventional-commit-driven bumps open
# a release PR; merging it tags + publishes from CI. This target is the
# local-validator complement: `cargo publish --dry-run` over every publishable
# crate in dep order. Useful before merging anything that touches manifest
# metadata, and required for the very first bootstrap publish (which release-plz
# can't perform — every crate must already exist on crates.io before the bot
# can manage it).

release-dry-run:
	@for crate in $(RELEASE_CRATES); do \
	  echo "==> cargo publish --dry-run -p $$crate"; \
	  $(CARGO) publish --dry-run -p $$crate || exit 1; \
	done
	@echo
	@echo "All crates packaged cleanly."

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
	@echo "  coverage-html    Same as coverage; renders HTML at target/llvm-cov/html"
	@echo "                   and opens it in the default browser."
	@echo ""
	@echo "  bench            Run the bench-minimal characterization workload."
	@echo "  bench-throughput-sweep  Run bench across clients=1..1024 (--json files)."
	@echo "  bench-latency    Run bench at clients=1 for latency-focused output."
	@echo ""
	@echo "  install-hooks    Point this clone's git at .husky/ (pre-commit fmt+lint)."
	@echo ""
	@echo "  release-dry-run  cargo publish --dry-run over every publishable crate, in dep order."
	@echo "                   Real releases run via release-plz (.github/workflows/release-plz.yml)."
	@echo ""
	@echo "  help             this message"
