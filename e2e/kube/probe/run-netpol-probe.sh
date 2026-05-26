#!/usr/bin/env bash
# Apply the NetworkPolicy enforcement probe (issue #486) against a tsoracle
# chart deployment. Confirms that:
#
#   - the `peer` port is DENIED from a pod that lives outside the StatefulSet
#     (different namespace, different labels).
#   - the `tso` port is ALLOWED from that same outside pod.
#
# Used by the kube-e2e workflow once per cell (insecure + TLS). The probe Job
# is applied to a *separate* namespace from the chart's, so the chart's
# NetworkPolicy `from:` rule (sibling-pod match) cannot match it — that is the
# negative test we want. The chart-side NetworkPolicy is rendered for any HA
# driver (openraft|paxos) with `networkPolicy.enabled=true` (the default;
# introduced by PR #452).
#
# Required env vars:
#   TARGET_RELEASE   — Helm release name to probe (e.g. `tsoracle`).
#   TARGET_NAMESPACE — Namespace the chart was installed into (e.g. `default`).
#   PROBE_NAMESPACE  — Namespace into which the probe Job is applied
#                      (created if missing). Must differ from TARGET_NAMESPACE
#                      so the probe pod's labels can't satisfy the chart's
#                      sibling-pod `from:` clause.
#
# Optional:
#   PROBE_TIMEOUT    — Seconds to wait for the Job to reach a terminal state.
#                      Default 120 (image pull + two 8s nc probes + slack).
set -euo pipefail

: "${TARGET_RELEASE:?TARGET_RELEASE must be set (chart release name)}"
: "${TARGET_NAMESPACE:?TARGET_NAMESPACE must be set (chart namespace)}"
: "${PROBE_NAMESPACE:?PROBE_NAMESPACE must be set (separate namespace for the probe)}"
PROBE_TIMEOUT="${PROBE_TIMEOUT:-120}"

if [ "$PROBE_NAMESPACE" = "$TARGET_NAMESPACE" ]; then
    echo "PROBE_NAMESPACE must differ from TARGET_NAMESPACE so the probe pod" >&2
    echo "is outside the chart's namespace; got both='${PROBE_NAMESPACE}'" >&2
    exit 1
fi

here="$(cd "$(dirname "$0")" && pwd)"

# Idempotent namespace bootstrap so the workflow can re-run a failed cell
# without manual cleanup. `--dry-run=client -o yaml | kubectl apply -f -` is
# the standard "ensure exists" pattern; plain `kubectl create namespace`
# errors when the namespace already exists.
kubectl create namespace "$PROBE_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Render the static manifest with selective envsubst. Without the explicit
# allow-list, envsubst would clobber `${TARGET}`/`${PEER_PORT}`/`${TSO_PORT}`
# in the *shell* script inside the Job's `args:` — those should reach the
# container untouched and be resolved by sh at runtime. Naming the two host-
# side substitutions explicitly leaves every other `${...}` alone.
# shellcheck disable=SC2016 # single quotes are intentional: envsubst reads
# them as a literal allow-list of variable names, not shell-expanded values.
envsubst '${TARGET_RELEASE} ${TARGET_NAMESPACE}' < "$here/job-netpol-probe.yaml" \
    | kubectl apply -n "$PROBE_NAMESPACE" -f -

# Poll for terminal state. Two conditions matter: Complete (probe passed) or
# Failed (one of the assertions did not hold). `kubectl wait` can only race
# one condition at a time, so we poll both manually.
elapsed=0
while [ "$elapsed" -lt "$PROBE_TIMEOUT" ]; do
    succeeded="$(kubectl -n "$PROBE_NAMESPACE" get job tsoracle-e2e-netpol-probe \
        -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
    failed="$(kubectl -n "$PROBE_NAMESPACE" get job tsoracle-e2e-netpol-probe \
        -o jsonpath='{.status.failed}' 2>/dev/null || true)"
    if [ "$succeeded" = "1" ]; then
        echo "netpol probe: succeeded (release=${TARGET_RELEASE} ns=${TARGET_NAMESPACE})"
        kubectl -n "$PROBE_NAMESPACE" logs job/tsoracle-e2e-netpol-probe || true
        exit 0
    fi
    if [ -n "$failed" ] && [ "$failed" != "0" ]; then
        echo "netpol probe: FAILED (release=${TARGET_RELEASE} ns=${TARGET_NAMESPACE})" >&2
        kubectl -n "$PROBE_NAMESPACE" logs job/tsoracle-e2e-netpol-probe || true
        exit 1
    fi
    sleep 3
    elapsed=$((elapsed + 3))
done

echo "netpol probe: timed out after ${PROBE_TIMEOUT}s (release=${TARGET_RELEASE})" >&2
kubectl -n "$PROBE_NAMESPACE" logs job/tsoracle-e2e-netpol-probe || true
kubectl -n "$PROBE_NAMESPACE" describe job tsoracle-e2e-netpol-probe || true
exit 1
