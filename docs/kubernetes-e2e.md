# Kubernetes end-to-end testing (kind)

> Status: wired to CI as a manual `workflow_dispatch` job. The cluster is deployed via the Helm chart at `deploy/charts/tsoracle`; assertion Jobs live under `e2e/kube/driver/`.

## Why a kind lane at all

The existing test suite already covers consensus correctness far better than a real cluster ever could. Turmoil [deterministic simulation](testing-and-examples.md) drives the real gRPC server and client over a simulated network and clock, so leader churn, fence retries, and window-extension races replay byte-for-byte from a seed. The [stress harness](stress-testing.md) runs four topologies — `mem`, `raft`, `paxos`, `process` — under sustained load while a programmable nemesis injects faults and four invariants (global monotonicity, batch ordering, failover-fence freshness, liveness) are checked in real time.

What none of those reach is the **deployment envelope**: the layer between "the process is correct" and "N real processes come up, find each other over a real network, and survive Kubernetes lifecycle events." Concretely, the gaps are:

- The `process` topology is **single-node**. Its doc comment is explicit: `tsoracle serve` is single-node, and `StressConfig::validate` rejects `nodes != 1`. It SIGKILLs and respawns one child to exercise the file driver's persisted high-water across crashes. It never runs a multi-process cluster.
- The `raft` and `paxos` topologies are multi-node but share an in-process `MemNetwork`. There are no sockets, no TCP, no DNS, no real peer transport between separate OS processes.
- The multi-node **example** binaries lag the stock binary on **SIGTERM**. `tsoracle serve` already drives graceful drain on SIGTERM via its `shutdown_signal()` helper (`crates/tsoracle-bin/src/main.rs`, added in #245), but `examples/openraft-standalone` and `examples/paxos-standalone` wire only `tokio::signal::ctrl_c()` (SIGINT) into `serve_with_shutdown`. Those examples are exactly what a cluster runs (the stock binary is single-node), so under Kubernetes — which terminates pods with SIGTERM — the cooperative-cancel and `WatchGuard` shutdown machinery never runs.

A kind ("Kubernetes IN Docker") lane fills exactly this envelope and nothing below it.

## What this lane is allowed to test (and what it is not)

This lane validates deployment-level properties only. It must not re-derive protocol correctness — that belongs in the deterministic harnesses, where failures are reproducible.

In scope:

- **Image + manifest correctness.** The Dockerfile builds, the container starts with the real entrypoint, the headless Service DNS names resolve, config and volumes mount.
- **Cluster formation over a real network.** Three separate pods, each a real `openraft-standalone` process, discover peers via StatefulSet DNS and elect a leader over real gRPC.
- **StatefulSet semantics.** Stable ordinal identity → stable node ID, PVC reattach on reschedule, ordered rollout.
- **Real network faults.** True partitions and asymmetric splits via `NetworkPolicy` (and latency/jitter via a `tc netem` sidecar), rather than the `process` topology's `SIGSTOP`.
- **Kubernetes lifecycle.** SIGTERM + `terminationGracePeriodSeconds` graceful shutdown, readiness-gated rolling restarts, `PodDisruptionBudget` enforcement, leader-pod deletion and re-election.

Out of scope (already covered, cheaper and deterministic elsewhere):

- Allocator monotonicity, fence freshness, consensus safety/liveness at the protocol level.
- Single-node crash/respawn against the file driver (that is the `process` topology's job).
- Leader-churn and partition-heal cycles at the protocol level (turmoil + `raft`/`paxos` topologies).

## Concrete defects only this lane can catch

These are the findings that justify the lane. Each is invisible to every current harness.

1. **SIGTERM is ignored by the example binaries.** Pods would ride out the full grace period and get SIGKILLed on every ordinary rollout or scale-down, never running cooperative cancel. The stock `tsoracle serve` already handles this (#245); the standalone examples a cluster actually runs do not — see "Prerequisites" below.
2. **PVC reattach correctness.** When a pod reschedules onto a new node, does the RocksDB raft log/snapshot survive and recover to the right cursor? The [snapshot-policy restart](consensus-integration.md) logic is exercised only against tempdirs today.
3. **DNS-based peer discovery.** The standalone example takes static `id=host:port` peer maps. Mapping StatefulSet ordinals to node IDs and headless-Service FQDNs to peer addresses is new wiring with its own failure modes (resolution timing on cold start, pod IP churn).
4. **Readiness semantics.** A follower returning `FAILED_PRECONDITION` with a leader hint is healthy and must stay in Service rotation. A naive `GetTs`-based readiness probe would wrongly evict every follower; this lane pins the correct TCP-based contract.
5. **Bootstrap-once under reschedule.** `--bootstrap` is idempotent, but the lane confirms that a rescheduled ordinal-0 pod re-running with the flag does not disturb an already-formed cluster.

## Architecture

```
                    ┌────────────────────────── kind cluster ───────────────────────────┐
                    │                                                                   │
   client ─────────►│  Service: tsoracle (ClusterIP, round-robin)                       │
   (round-robin,    │     │   followers redirect via LeaderHint trailer                 │
    redirected to   │     ▼                                                             │
    leader)         │  StatefulSet: tsoracle  (replicas: 3, anti-affinity per node)     │
                    │     ├── tsoracle-0  ── PVC data-tsoracle-0  (raft log + snapshot) │
                    │     ├── tsoracle-1  ── PVC data-tsoracle-1                        │
                    │     └── tsoracle-2  ── PVC data-tsoracle-2                        │
                    │            ▲                                                      │
                    │            │ raft peer RPCs (5100) over headless DNS              │
                    │  Service: tsoracle-peer (headless, clusterIP: None)               │
                    │     tsoracle-{0,1,2}.tsoracle-peer.<ns>.svc.cluster.local         │
                    │                                                                   │
                    │  PodDisruptionBudget: minAvailable: 2  (quorum-safe drains)       │
                    └───────────────────────────────────────────────────────────────────┘
```

### Image and chart

The cluster is deployed from `deploy/charts/tsoracle` (Helm chart) using the image built from `deploy/Dockerfile`. The chart is parameterised for the kind lane with `--set driver=openraft,replicas=3,ports.client=5051,ports.peer=5100` so the pod DNS names (`tsoracle-{n}.tsoracle-peer`) and ports match what the assertion driver expects.

The entrypoint (`deploy/entrypoint.sh`) derives the node ID and peer maps from `$HOSTNAME` (`tsoracle-${ORDINAL}`) and environment variables injected by the chart, so the StatefulSet ships one spec for all pods.

### Services

- **`tsoracle-peer`** — headless (`clusterIP: None`), the `serviceName` of the StatefulSet. Publishes the stable per-pod DNS the peer transport dials. Created by the chart when `driver != file`.
- **`tsoracle`** — ordinary ClusterIP, round-robins clients across all pods. Followers redirect to the leader via the `LeaderHint` trailer, so a dumb round-robin Service is correct and needs no leader-aware routing. Created by the chart's `service-client.yaml`.

### Probes

- **Readiness:** `tcpSocket` on the tso port. A follower is ready — it serves redirects. Do **not** use a `GetTs` exec probe; it would evict every non-leader.
- **Liveness:** `tcpSocket` on the tso port, proving the gRPC listener is up.

True "is this node serving as leader" is observable in-process via `Server::subscribe()` → `ServingState`, but it is intentionally not a readiness signal here.

## How it relates to the stress harness

The `ChaosController` trait (`benchmarks/stress/src/topology/mod.rs`) — `kill_leader`, `pause_leader`, `arm_failpoint`, `disarm_failpoint`, `endpoints`, `current_leader` — is a tempting seam: a `KubeController` could be a fifth topology, inheriting the shared supervisor and all four invariant checks for free.

Recommendation: **do not** make it a stress topology. The stress harness's whole value is fast, seed-reproducible replay; bolting multi-minute, wall-clock-nondeterministic cluster spin-up onto it couples slow flaky infra to a binary designed for the opposite. Instead, build a **separate `e2e/kube/` lane** with its own workflow (manual + nightly trigger) that reuses only the client library and the invariant-checking code, not the chaos scheduler. `arm_failpoint`/`disarm_failpoint` also do not map cleanly: `FAILPOINTS` is read from the environment at process start, so in-place arming would require a pod restart or an admin endpoint that does not exist — the kube lane should rely on pod/network chaos instead.

## Tradeoffs

- **Nondeterminism.** This lane trades reproducibility for realism. It belongs in nightly/manual, never the per-PR smoke path, and every assertion needs generous retry/backoff against wall-clock timing.
- **Cost.** Docker-in-Docker or a privileged runner, image builds, and multi-minute cluster startup. Budget 5–15 minutes per scenario versus seconds-to-minutes for stress cells.
- **Layer.** Everything below the gRPC socket is already better-covered. Keep the scenario set small: cold-start formation, rolling restart, leader-pod delete, network partition, PVC reattach.

## Prerequisites this lane surfaces

1. **A Dockerfile and Helm chart.** Both live in `deploy/`: `deploy/Dockerfile` builds the multi-driver image and `deploy/charts/tsoracle` is the chart used by CI and production.
2. **SIGTERM handling in the example binaries.** Done (#406): the standalone examples now feed `serve_with_shutdown` the shared `tsoracle_server::shutdown_signal()` future, which resolves on SIGTERM and SIGINT. The graceful rolling-restart assertion in the kube lane exercises this path.
3. **Optional: `grpc.health.v1.Health`.** Adding the standard gRPC health service would let probes use `grpc_health_probe` and report leader/serving state precisely. Not required for the TCP-based contract above, but a clean follow-up.

## TLS cell

The workflow runs the assertions twice against two helm releases in two namespaces of the same kind cluster — once with plaintext consensus (`tls.allowInsecurePeer=true`, the legacy "insecure cell") and once with `tls.enabled=true` plus minted certs (the "TLS cell"). The TLS cell exists because the chart's secure-by-default render guard from PR #452 forces HA deployments to either opt out explicitly or supply TLS material, and only the latter exercises the peer mTLS path that PR #445 introduced. See issue #483.

The two cells share one kind cluster because `podAntiAffinity` in the chart's StatefulSet keys on `app.kubernetes.io/instance: <Release.Name>` — so an insecure-cell pod and a TLS-cell pod can coexist on the same worker without violating the anti-affinity rule. They live in separate namespaces (`default` and `e2e-tls`) so the same Helm release name isn't needed (and the second `helm install` doesn't trample the first's state).

The chart mounts ONE Secret cluster-wide, so each pod must use the same leaf cert. Per-pod identity (Vault- or cert-manager-style) is out of scope for this fixture. Instead, `cargo run -p kube-e2e-driver --bin gen-certs` (on the GitHub runner, before `helm install`) signs a single leaf cert whose SANs cover *every* pod FQDN (`tsoracle-tls-{0..N-1}.tsoracle-tls-peer.e2e-tls.svc.cluster.local`) plus both Service DNS names. That cert is sufficient because PR #445's peer authorization is CA-based: any cert chaining to the configured peer CA is accepted, and the SNI match on each peer dial is satisfied by the per-pod SAN. The CA private key is discarded after signing (one-shot fixture mint).

The assertion driver (`kube-e2e-driver`) speaks TLS via a `--tls-ca` flag that points at the mounted Secret's `ca.crt`. Bare `host:port` endpoints are rewritten by `tsoracle-client` to `https://host:port` when a `ClientTlsConfig` is set (see `crates/tsoracle-client/src/transport.rs`), so SNI defaults to the host component of each endpoint URL. Job manifests in `e2e/kube/driver/tls/` use the full FQDN form in `--endpoints` (not the shorter `pod.peer-svc:port`) so SNI matches a SAN on the chart's leaf.

What the TLS cell proves that the insecure cell does not:

1. The chart's render guard accepts the happy path (`tls.enabled=true` with `tls.secretName`) without `tls.allowInsecurePeer`.
2. Peer mTLS handshakes survive rolling restarts and SIGKILL-leader recovery: kube DNS / Pod-IP rotation does not invalidate the per-pod SNI against the fan-SAN leaf.
3. The driver's TLS client (configured with the cluster's CA) interoperates with the chart's server-auth TLS on the client API, including under leader-redirect across cells.