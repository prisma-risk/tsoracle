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

# Build the node image and load it into the kind nodes (no registry needed).
docker build -f e2e/kube/Dockerfile -t tsoracle-e2e:latest .
kind load docker-image tsoracle-e2e:latest --name tsoracle-e2e

kubectl apply -f e2e/kube/manifests/
kubectl rollout status statefulset/tsoracle --timeout=120s

# Smoke a GetTs through the client Service.
kubectl port-forward svc/tsoracle 5051:5051 &
# ... drive a tsoracle-client against 127.0.0.1:5051 ...

kind delete cluster --name tsoracle-e2e
```

## Known gap

The `openraft-standalone` example handles only SIGINT, not SIGTERM, so pod drains currently ride out `terminationGracePeriodSeconds` and end in SIGKILL — the cooperative-shutdown path does not run. (The stock `tsoracle serve` binary already handles SIGTERM via its `shutdown_signal()` helper, added in #245; the example simply never got the same treatment.) `terminationGracePeriodSeconds` is set generously so this does not flap the lane. Wiring SIGTERM into the example's shutdown future is the first follow-up.
