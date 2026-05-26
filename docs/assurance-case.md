<!-- SPDX-License-Identifier: Apache-2.0 -->
# Assurance case

This document presents the project's safety and security claims with their supporting arguments and evidence. It supplements the architectural documentation ([`docs/architecture-deep-dive.md`](architecture-deep-dive.md)) and is structured as a claims → arguments → evidence tree.

It is also the canonical answer to the OpenSSF Best Practices `assurance_case` criterion.

## Claim 1 (Safety): Strict timestamp monotonicity under fault

**Claim**: tsoracle preserves strict monotonicity of allocated timestamps under

- process crashes (any node, any time),
- network partitions,
- leader handoff (planned, via the openraft graceful-handoff path; unplanned, via election after failure),
- rolling restarts (every-pod-restart soak in Kubernetes),
- mixed-version operation (during in-place format upgrade).

### Argument 1.1: Deterministic in-process integration tests

**Sub-claim**: The monotonicity property holds across all consensus drivers under deterministic test conditions.

**Evidence**:

- [`crates/tsoracle-driver-openraft/tests/`](../crates/tsoracle-driver-openraft/tests/) — single_node, restart_replay, partition_churn, snapshot_restart, and additional scenarios under the same directory.
- [`crates/tsoracle-server/tests/`](../crates/tsoracle-server/tests/) — server-level integration including `serve_shutdown.rs`, `leader_watch.rs`, `leadership_churn.rs`, `fence_yieldpoint.rs`, and the `e2e.rs` end-to-end harness.
- [`crates/tsoracle-tests/tests/`](../crates/tsoracle-tests/tests/) — cross-driver integration tests including leadership_churn and client interaction patterns.
- These run on every PR via the `test` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) with `cargo test --workspace --all-features --locked`.

### Argument 1.2: Real-cluster acceptance + soak with measured error budget

**Sub-claim**: The monotonicity property holds under real Kubernetes failure modes — cold-start, rolling-restart, leader-SIGKILL, add-learner / promote / remove during steady state, mixed-version upgrade.

**Evidence**:

- [`.github/workflows/kube-e2e.yml`](../.github/workflows/kube-e2e.yml) — cold-start and rolling-restart soak against a real `kind` cluster.
- [`.github/workflows/kube-e2e-mixed-version.yml`](../.github/workflows/kube-e2e-mixed-version.yml) — mixed-version-soak exercising in-place format upgrade under steady-state load.
- [`e2e/kube/run-assertions.sh`](../e2e/kube/run-assertions.sh) — soak error budget enforcement at 0.05%.
- The `kube-e2e-driver` crate (in-cluster Job topology) runs the driver from inside the cluster so it can follow leader-hint redirects to pod DNS.

### Argument 1.3: Stress lane with positive-control detector validation

**Sub-claim**: The monotonicity detector itself is verified to fire on a known violation. Absence-of-failure in stress runs therefore implies absence-of-violation, not absence-of-detection.

**Evidence**:

- `stress-smoke` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) — mem, raft, paxos, and process topologies.
- The `inject-violation` positive-control step in the stress lane must exit `1` to prove the supervisor still catches monotonicity breaks; the CI step fails the job if the detector does not fire.
- [`.github/workflows/stress-nightly.yml`](../.github/workflows/stress-nightly.yml) — extended stress runs nightly.

### Argument 1.4: Software-fault injection at safety-critical paths

**Sub-claim**: The monotonicity property holds when faults are deliberately injected at identified critical points (lock acquisition order, snapshot publish, fence enter, leader watch, etc.).

**Evidence**:

- [`docs/failpoint-testing.md`](failpoint-testing.md) — the failpoint feature suite, run as `cargo test --workspace --features failpoints`.
- [`docs/yieldpoint-testing.md`](yieldpoint-testing.md) — the yieldpoint feature suite for deterministic interleaving.
- Recent regression tests anchored on this scaffolding: barrier-seq durable seed, snapshot publish TOCTOU, WatchGuard drop sync step-down, openraft graceful handoff, paxos lease generation wrap.

### Argument 1.5: Decoder-path fuzzing

**Sub-claim**: Malformed wire input or malformed on-disk records cannot induce a safety-relevant state.

**Evidence**:

- [`fuzz/fuzz_targets/`](../fuzz/fuzz_targets/) — 15 libFuzzer harnesses covering codec roundtrip, log entry decode, openraft entry/meta decode, paxos codec/log/meta-ballot/snapshot decode, proto request/response/leader-hint decode, record decode/roundtrip, snapshot payload decode, toolkit codec decode.
- [`.github/workflows/fuzz-pr.yml`](../.github/workflows/fuzz-pr.yml) — per-PR fuzz lane.
- [`.github/workflows/fuzz-nightly.yml`](../.github/workflows/fuzz-nightly.yml) — nightly fuzz lane (30 min per target).
- Crashes auto-file as GitHub issues with the offending input.

## Claim 2 (Security): No unauthenticated peer/admin RPCs in secure-by-default deployment

**Claim**: tsoracle does not admit unauthenticated peer or admin RPC traffic in its secure-by-default deployment posture.

### Argument 2.1: Helm chart secure-by-default render guard

**Sub-claim**: An operator who installs the default chart cannot accidentally deploy an HA cluster (openraft / paxos) with plaintext peer traffic.

**Evidence**:

- [`deploy/charts/tsoracle/templates/_helpers.tpl`](../deploy/charts/tsoracle/templates/_helpers.tpl) contains a render-guard `fail` block that aborts `helm install` when `.Values.driver` is `openraft` or `paxos`, `tls.enabled` is `false`, and `tls.allowInsecurePeer` is unset.
- [`deploy/charts/tsoracle/values.yaml`](../deploy/charts/tsoracle/values.yaml) — `tls.allowInsecurePeer` defaults to `false`.
- The chart's `NOTES.txt` warns operators if `tls.allowInsecurePeer` is explicitly set to `true`.
- The chart test suite under `deploy/charts/tsoracle/tests/` exercises both the secure-by-default render and the `allowInsecurePeer` opt-out paths.

### Argument 2.2: Admin port `AdminInsecureRoutable` guard

**Sub-claim**: The admin gRPC service refuses to bind to a routable address without TLS.

**Evidence**:

- [`crates/tsoracle-standalone/src/drivers/openraft/mod.rs`](../crates/tsoracle-standalone/src/drivers/openraft/mod.rs) contains the `AdminInsecureRoutable` check (line ~140) that returns an error when the admin listener is configured to bind a non-loopback address without TLS.
- [`crates/tsoracle-standalone/tests/activation_admin_rpc.rs`](../crates/tsoracle-standalone/tests/activation_admin_rpc.rs) exercises the admin RPC path under mTLS.

### Argument 2.3: Client TLS by default with full verification

**Sub-claim**: The client uses TLS 1.3 with full chain + hostname verification.

**Evidence**:

- `ClientTlsConfig` in the `tsoracle-client` crate accepts operator-supplied certificates and CA roots; it does not provide a hook to disable verification.
- The project uses rustls's default `ServerCertVerifier` (no custom override); rustls performs full chain validation including SAN / CN hostname match.
- [`crates/tsoracle-tests/tests/client_tls.rs`](../crates/tsoracle-tests/tests/client_tls.rs) — client TLS integration test.

### Argument 2.4: Open gap — peer-listener secure-by-default in the binary

**Sub-claim (open)**: The Helm chart fails closed, but the binary itself does not yet refuse to bind a plaintext peer listener on a routable interface.

**Evidence and action**:

- Tracked at issue [#481](https://github.com/prisma-risk/tsoracle/issues/481).
- Action: mirror the admin `AdminInsecureRoutable` guard for openraft `raft_addr` and paxos `peer_listen`, gated by an `--allow-insecure-peer` flag matching the chart's `tls.allowInsecurePeer` opt-out.
- Until this lands, defense-in-depth is provided by the chart guard (Argument 2.1) and operator NetworkPolicy.

## How this document is maintained

- Every PR that touches a safety- or security-critical path updates the relevant Argument section with the new evidence link.
- The Claim list is not modified without an issue at <https://github.com/prisma-risk/tsoracle/issues> first to discuss the change.
- This document is reviewed at every major release (≥0.X feature releases) to confirm evidence links still resolve.
