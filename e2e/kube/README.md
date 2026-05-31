# Kubernetes (kind) e2e cluster

A 3-node `openraft-standalone` tsoracle cluster on a local [kind](https://kind.sigs.k8s.io/) cluster. This validates the deployment envelope — cluster formation over a real network, StatefulSet identity, PVC reattach, lifecycle events — that the in-process harnesses cannot reach. See [`docs/kubernetes-e2e.md`](../../docs/kubernetes-e2e.md) for the design and scope.

This is opt-in tooling, not a CI gate. It runs the example binary, not `tsoracle serve` (which is single-node by design).

## Layout

- `Dockerfile` — multi-stage build of the `openraft-standalone` binary.
- `entrypoint.sh` — derives `--id` and peer maps from the StatefulSet ordinal.
- `kind-config.yaml` — 1 control-plane + 3 workers (one per replica, for anti-affinity).
- `manifests/` — headless + client Services, StatefulSet, PodDisruptionBudget.

## Run

```sh
# From the repo root.
kind create cluster --config e2e/kube/kind-config.yaml

# Build the node + driver images and load them into kind (no registry needed).
docker build -f e2e/kube/Dockerfile        -t tsoracle-e2e:latest .
docker build -f e2e/kube/driver/Dockerfile -t tsoracle-e2e-driver:latest .
kind load docker-image tsoracle-e2e:latest        --name tsoracle-e2e
kind load docker-image tsoracle-e2e-driver:latest --name tsoracle-e2e

kubectl apply -f e2e/kube/manifests/
kubectl rollout status statefulset/tsoracle --timeout=180s

# Assertions run as in-cluster Jobs (so leader-hint redirects to pod DNS
# resolve): cold-start probes each ordinal; soak checks zero failed calls +
# monotonicity across a graceful rolling restart.
./e2e/kube/run-assertions.sh

kind delete cluster --name tsoracle-e2e
```

## Scope

This lane currently covers cold-start formation, graceful rolling-restart, SIGKILL-leader recovery, and dynamic-membership churn (add-learner / promote / remove). The `openraft-standalone` example handles SIGTERM (via `tsoracle_server::shutdown_signal()`, #406), so the graceful-rollout assertion exercises the real cooperative-shutdown path. The dynamic-membership cell (issue #487) is the first end-to-end exercise of the three-address membership model shipped in #453; it drives all admin calls through `kubectl exec LEADER_POD -- tsoracle admin ...` against the loopback `127.0.0.1:51002` admin port the chart binds. Network partition + PVC reattach and a nightly schedule remain follow-ups.

## Cells: insecure + TLS

The CI workflow runs the same assertion lane twice against two helm releases in two namespaces of the same kind cluster (issue #483):

- **Insecure cell** (`tsoracle` in `default`) — `tls.allowInsecurePeer=true`. Exercises the deployment envelope (DNS / Pod-IP rotation, lifecycle events) under plaintext consensus. This is the legacy cell that has always been here.
- **TLS cell** (`tsoracle-tls` in `e2e-tls`) — `tls.enabled=true` plus a Secret minted on the runner by `cargo run -p kube-e2e-driver --bin gen-certs` (a fan-SAN leaf cert covering every pod FQDN, signed by a one-shot test CA). Exercises the chart's secure-by-default render guard's *happy path* (PR #452), peer mTLS under realistic kube DNS rotation, and cross-pod trust via the cluster-dedicated peer CA model (PR #445).

Both cells share `run-assertions.sh`, which reads `RELEASE` and `JOB_DIR` env vars to know which release to drive and which Job manifest set to apply. The TLS cell's Job manifests live in `driver/tls/` and mount the cluster's TLS Secret so the driver client trusts the same CA the cluster's server cert chains to.

After each cell's assertion lane, the workflow runs a NetworkPolicy enforcement probe (`probe/run-netpol-probe.sh`) from a separate namespace with non-matching labels to confirm the chart's NetworkPolicy actually denies the peer port and allows the `tso` port from outside the StatefulSet. See `probe/README.md` and issue #486.

To run only the TLS cell locally, after the kind cluster is up:

```sh
kubectl create namespace e2e-tls
kubectl config set-context --current --namespace=e2e-tls
mkdir -p /tmp/tsoracle-tls
cargo run -p kube-e2e-driver --bin gen-certs -- --out /tmp/tsoracle-tls --release tsoracle-tls --namespace e2e-tls --replicas 3
kubectl -n e2e-tls create secret generic tsoracle-tls-certs --from-file=tls.crt=/tmp/tsoracle-tls/tls.crt --from-file=tls.key=/tmp/tsoracle-tls/tls.key --from-file=ca.crt=/tmp/tsoracle-tls/ca.crt
helm install tsoracle-tls deploy/charts/tsoracle -n e2e-tls --set image.repository=tsoracle,image.tag=e2e --set driver=openraft,replicas=3 --set ports.client=5051,ports.peer=5100 --set tls.enabled=true --set tls.secretName=tsoracle-tls-certs --wait --timeout 5m
RELEASE=tsoracle-tls JOB_DIR=$(pwd)/e2e/kube/driver/tls ./e2e/kube/run-assertions.sh
```

## Mixed-version lane (`kube-e2e-mixed-version.yml`)

The `workflow_dispatch`-only mixed-version lane runs a separate kind cluster with two image tags: `tsoracle:e2e-baseline` built from the pinned `tsoracle-v2.0.0` commit (v4-only, `MAX_READABLE_VERSION=4`, no GetSeq) and `tsoracle:e2e-next` built from the PR head (`FEATURES=openraft`, v5-capable). It soaks `GetTs` monotonicity and `GetSeq` gaplessness across a partition=1 rolling restart, asserts the all-members gate rejects activation while the v4 member is present, then activates `DENSE_WRITE_VERSION=5` once all members are v5-capable.

### Old-reader-rejects-too-new (version gate)

The "an old-only reader rejects a too-new record at the version gate rather than misdecoding" property is covered deterministically, not in this e2e lane:

- Unit: `snapshot_codec_accepts_full_readable_range` in `crates/tsoracle-driver-openraft/src/state_machine.rs` and `decode_entry_record_rejects_out_of_range_version` in the openraft toolkit log store assert a version above `MAX_READABLE_VERSION` is rejected, not misdecoded.
- Fuzz: `openraft_dense_command_decode` drives decode at a fuzzed version byte and asserts out-of-range versions are rejected.

This lane covers the complementary system-level guarantee: the all-members gate ensures a v4-only member is never *exposed* to a v5 record (the `MEMBERS_BELOW_TARGET` rejection in step 3).

The lane also activates write version 6 after version 5 and asserts `GetSeqBatch` gaplessness post-activation: the driver probes `get_seq_batch` over two independent keys (`orders-a`, `orders-b`) on every loop iteration, recording pre-activation `FailedPrecondition` / `Unimplemented` responses as not-serving (not errors) and verifying zero gaplessness violations once version 6 is active.

## Deploying to EKS (staging)

The kind flow above is fully local. To deploy the same `openraft-standalone` cluster onto the real **staging** EKS cluster (arm64, real EBS) for a manual smoke test, the Kubernetes manifests live in the **infra** repo at `k8s/tsoracle-e2e/`; this repo only builds and pushes the images.

Prerequisites: AWS credentials, the `staging` kube context, and the ECR repositories created once via `terraform apply` in `infra/terraform/shared-artifacts`.

```sh
# build + push the arm64 node and driver images to ECR
./e2e/kube/push-to-ecr.sh
```

Then follow `infra/k8s/tsoracle-e2e/README.md` for apply, assertions, and teardown. Unlike the kind lane (side-loaded images, `imagePullPolicy: IfNotPresent`), the EKS overlay uses `imagePullPolicy: Always` against ECR.
