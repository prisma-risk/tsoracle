# Kubernetes end-to-end testing (kind)

> Status: design sketch / proposal. The manifests referenced here live under `e2e/kube/`. Nothing in this lane runs in CI yet; it is opt-in and nightly/manual by intent.

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

### Binary

The cluster runs the `openraft-standalone` example binary (package `example-openraft-standalone`, bin `openraft-standalone`), not `tsoracle serve` — the stock binary is single-node by design. The paxos variant (`paxos-standalone`) is a drop-in alternative with the same shape; the lane should parametrize on backend so both get coverage.

Relevant flags (`examples/openraft-standalone/src/main.rs`):

| Flag | Source in k8s |
| --- | --- |
| `--id <u64>` | StatefulSet ordinal + 1 |
| `--raft-addr 0.0.0.0:5100` | fixed per pod (own netns) |
| `--tso-addr 0.0.0.0:5051` | fixed per pod |
| `--peers id=fqdn:5100,…` | built from replica count + headless DNS |
| `--tso-peers id=fqdn:5051,…` | same, tso port |
| `--raft-dir /data/raft` | PVC mount |
| `--bootstrap` | ordinal 0 only (idempotent) |

An entrypoint script derives the node ID and peer maps from `$HOSTNAME` (`tsoracle-${ORDINAL}`) and a `REPLICAS` env var, so the StatefulSet ships one spec for all pods. See `e2e/kube/entrypoint.sh`.

### Services

- **`tsoracle-peer`** — headless (`clusterIP: None`), the `serviceName` of the StatefulSet. Publishes the stable per-pod DNS the raft transport dials.
- **`tsoracle`** — ordinary ClusterIP, round-robins clients across all pods. Followers redirect to the leader via the `LeaderHint` trailer, so a dumb round-robin Service is correct and needs no leader-aware routing.

### Probes

- **Readiness:** `tcpSocket` on the tso port (5051). A follower is ready — it serves redirects. Do **not** use a `GetTs` exec probe; it would evict every non-leader.
- **Liveness:** `tcpSocket` on the raft port (5100), proving the peer transport is up.

True "is this node serving as leader" is observable in-process via `Server::subscribe()` → `ServingState`, but it is intentionally not a readiness signal here.

## How it relates to the stress harness

The `ChaosController` trait (`benchmarks/stress/src/topology/mod.rs`) — `kill_leader`, `pause_leader`, `arm_failpoint`, `disarm_failpoint`, `endpoints`, `current_leader` — is a tempting seam: a `KubeController` could be a fifth topology, inheriting the shared supervisor and all four invariant checks for free.

Recommendation: **do not** make it a stress topology. The stress harness's whole value is fast, seed-reproducible replay; bolting multi-minute, wall-clock-nondeterministic cluster spin-up onto it couples slow flaky infra to a binary designed for the opposite. Instead, build a **separate `e2e/kube/` lane** with its own workflow (manual + nightly trigger) that reuses only the client library and the invariant-checking code, not the chaos scheduler. `arm_failpoint`/`disarm_failpoint` also do not map cleanly: `FAILPOINTS` is read from the environment at process start, so in-place arming would require a pod restart or an admin endpoint that does not exist — the kube lane should rely on pod/network chaos instead.

## Tradeoffs

- **Nondeterminism.** This lane trades reproducibility for realism. It belongs in nightly/manual, never the per-PR smoke path, and every assertion needs generous retry/backoff against wall-clock timing.
- **Cost.** Docker-in-Docker or a privileged runner, image builds, and multi-minute cluster startup. Budget 5–15 minutes per scenario versus seconds-to-minutes for stress cells.
- **Layer.** Everything below the gRPC socket is already better-covered. Keep the scenario set small: cold-start formation, rolling restart, leader-pod delete, network partition, PVC reattach.

## Prerequisites this lane surfaces

1. **A Dockerfile.** None exists in the repo today. `e2e/kube/Dockerfile` is part of this sketch.
2. **SIGTERM handling in the example binaries.** The standalone examples should feed `serve_with_shutdown` a future that resolves on SIGTERM (and SIGINT), not `ctrl_c()` alone. The stock binary's `shutdown_signal()` helper (`crates/tsoracle-bin/src/main.rs`) is the pattern to copy. Until that lands, set a long `terminationGracePeriodSeconds` and treat SIGKILL-on-drain as a known gap the lane documents rather than asserts against.
3. **Optional: `grpc.health.v1.Health`.** Adding the standard gRPC health service would let probes use `grpc_health_probe` and report leader/serving state precisely. Not required for the TCP-based contract above, but a clean follow-up.