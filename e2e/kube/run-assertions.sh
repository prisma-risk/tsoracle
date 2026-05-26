#!/usr/bin/env bash
# Orchestrates the kind e2e assertions against the current kubectl context:
#   1. cold-start: every ordinal serves or redirects (proves 3-node formation)
#   2. soak: zero final client errors + monotonicity across a graceful rollout
# Assumes the chart has been installed (helm install tsoracle deploy/charts/tsoracle)
# and the StatefulSet has reached readiness.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

# shellcheck source=./_assertions_lib.sh
source "$here/_assertions_lib.sh"

echo "== step 2: cold-start (each ordinal serves or redirects) =="
kubectl apply -f "$here/driver/job-cold-start.yaml"
wait_job tsoracle-e2e-cold-start 120

echo "== step 3: soak across a graceful rolling restart =="
kubectl apply -f "$here/driver/job-soak.yaml"
wait_soak_live tsoracle-e2e-soak "soak: first GetTs ok" 60
kubectl rollout restart statefulset/tsoracle
kubectl rollout status statefulset/tsoracle --timeout=150s
wait_job tsoracle-e2e-soak 180

echo "== all assertions passed =="
