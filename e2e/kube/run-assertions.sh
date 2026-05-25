#!/usr/bin/env bash
# Orchestrates the kind e2e assertions against the current kubectl context:
#   1. cold-start: every ordinal serves or redirects (proves 3-node formation)
#   2. soak: zero final client errors + monotonicity across a graceful rollout
# Assumes the chart has been installed (helm install tsoracle deploy/charts/tsoracle)
# and the StatefulSet has reached readiness.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

# Poll a Job to terminal state. Succeeds on .status.succeeded=1, fails on any
# .status.failed, dumping the Job's logs so the assertion summary is visible.
wait_job() {
    local job="$1" timeout="$2" elapsed=0
    while true; do
        local succeeded failed
        succeeded="$(kubectl get job "$job" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
        failed="$(kubectl get job "$job" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
        if [ "$succeeded" = "1" ]; then
            echo "$job: succeeded"
            kubectl logs "job/$job" || true
            return 0
        fi
        if [ -n "$failed" ] && [ "$failed" != "0" ]; then
            echo "$job: FAILED"
            kubectl logs "job/$job" || true
            return 1
        fi
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "$job: timed out after ${timeout}s"
            kubectl logs "job/$job" || true
            return 1
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
}

# Block until the soak Job has logged its first-success sentinel, so the cluster
# is only perturbed once load is provably live.
wait_soak_live() {
    local job="$1" timeout="$2" elapsed=0
    while true; do
        if kubectl logs "job/$job" 2>/dev/null | grep -q "soak: first GetTs ok"; then
            echo "$job: load is live"
            return 0
        fi
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "$job: never became live"
            kubectl logs "job/$job" || true
            return 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
}

echo "== step 2: cold-start (each ordinal serves or redirects) =="
kubectl apply -f "$here/driver/job-cold-start.yaml"
wait_job tsoracle-e2e-cold-start 120

echo "== step 3: soak across a graceful rolling restart =="
kubectl apply -f "$here/driver/job-soak.yaml"
wait_soak_live tsoracle-e2e-soak 60
kubectl rollout restart statefulset/tsoracle
kubectl rollout status statefulset/tsoracle --timeout=150s
wait_job tsoracle-e2e-soak 180

echo "== all assertions passed =="
