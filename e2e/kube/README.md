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

This lane currently covers cold-start formation and graceful rolling-restart (rollout steps 2–3). Network partition + PVC reattach (step 4) and a nightly schedule (step 5) are follow-ups. The `openraft-standalone` example now handles SIGTERM (via `tsoracle_server::shutdown_signal()`, #406), so the graceful-rollout assertion exercises the real cooperative-shutdown path.

## Deploying to EKS (staging)

The kind flow above is fully local. To deploy the same `openraft-standalone` cluster onto the real **staging** EKS cluster (arm64, real EBS) for a manual smoke test, the Kubernetes manifests live in the **infra** repo at `k8s/tsoracle-e2e/`; this repo only builds and pushes the images.

Prerequisites: AWS credentials, the `staging` kube context, and the ECR repositories created once via `terraform apply` in `infra/terraform/shared-artifacts`.

```sh
# build + push the arm64 node and driver images to ECR
./e2e/kube/push-to-ecr.sh
```

Then follow `infra/k8s/tsoracle-e2e/README.md` for apply, assertions, and teardown. Unlike the kind lane (side-loaded images, `imagePullPolicy: IfNotPresent`), the EKS overlay uses `imagePullPolicy: Always` against ECR.
