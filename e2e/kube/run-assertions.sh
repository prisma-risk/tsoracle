#!/usr/bin/env bash
# Orchestrates the kind e2e assertions against the current kubectl context:
#   1. cold-start: every ordinal serves or redirects (proves 3-node formation)
#   2. soak: zero final client errors + monotonicity across a graceful rollout
#   3. sigkill-soak: monotonicity holds across a force-deleted leader pod
#      (no graceful shutdown, no transfer_leader, no draining) — exercises the
#      crash-recovery paths that #427 (barrier-seq durable seed) and #426
#      (snapshot-publish TOCTOU) fixed, currently covered only by failpoint
#      unit tests. See issue #485.
# Assumes the chart has been installed (helm install tsoracle deploy/charts/tsoracle)
# and the StatefulSet has reached readiness. Step 4 additionally assumes the
# chart's openraft serve invocation binds the admin listener (see the
# entrypoint.sh `--admin-listen 127.0.0.1:${ADMIN_PORT}` wire-up); without it
# `tsoracle admin members` inside the pod cannot reach the admin gRPC server.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

# shellcheck source=./_assertions_lib.sh
source "$here/_assertions_lib.sh"

# Query `tsoracle admin members` via the loopback admin port inside a tsoracle
# pod and print the pod name of the current leader (e.g. "tsoracle-1"). Retries
# until the responding node reports a known leader or the timeout elapses —
# briefly returning "leader: none" mid-election is normal.
#
# The query targets tsoracle-0 unconditionally. ListMembers is locally
# answerable on any node, so the responder need not BE the leader; if the
# leader happens to be tsoracle-0, we still get its id back and the subsequent
# delete proceeds normally. The admin gRPC server binds 127.0.0.1:${ADMIN_PORT}
# (default 51002) per the chart's entrypoint.sh — kubectl exec reaches it via
# loopback without any Service/NetworkPolicy plumbing.
find_leader_pod() {
    local timeout="$1" elapsed=0 admin_port="${ADMIN_PORT:-51002}"
    while true; do
        local out leader_id raft_field pod_name
        # `|| true` so the loop survives a transient "Error from server" during
        # admin server startup; the next iteration re-queries.
        out="$(kubectl exec tsoracle-0 -c tsoracle -- \
            tsoracle admin members --endpoint "http://127.0.0.1:${admin_port}" \
            2>/dev/null || true)"
        leader_id="$(printf '%s\n' "$out" | awk '/^leader: / { print $2 }')"
        if [ -n "$leader_id" ] && [ "$leader_id" != "none" ]; then
            # Each member line is `  id=N role=... raft=HOST:PORT service=... admin=...`.
            # Pick the raft field for the leader's row and strip everything from
            # the first dot onward — the pod name is the bare hostname label.
            raft_field="$(printf '%s\n' "$out" | awk -v id="id=${leader_id}" '
                $1 == id {
                    for (i=2; i<=NF; i++) if (substr($i,1,5) == "raft=") {
                        print substr($i,6); exit
                    }
                }')"
            pod_name="${raft_field%%.*}"
            if [ -n "$pod_name" ]; then
                echo "$pod_name"
                return 0
            fi
        fi
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "find_leader_pod: no leader within ${timeout}s" >&2
            echo "last admin members output:" >&2
            printf '%s\n' "$out" >&2
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
wait_soak_live tsoracle-e2e-soak "soak: first GetTs ok" 60
kubectl rollout restart statefulset/tsoracle
kubectl rollout status statefulset/tsoracle --timeout=150s
wait_job tsoracle-e2e-soak 180

echo "== step 4: sigkill-soak (monotonicity across a force-deleted leader) =="
kubectl apply -f "$here/driver/job-sigkill-soak.yaml"
wait_soak_live tsoracle-e2e-sigkill-soak "sigkill-soak: first GetTs ok" 60
leader_pod="$(find_leader_pod 60)"
echo "step 4: killing leader pod $leader_pod"
kubectl delete pod "$leader_pod" --grace-period=0 --force
# The StatefulSet must bring the deleted pod back and reach all-ready before
# we trust the soak's final verdict; otherwise a flaky pod-recreate would
# masquerade as a real monotonicity regression. 180s covers the kubelet's
# pod-recreate latency + image pull (IfNotPresent in CI) + cluster rejoin.
kubectl rollout status statefulset/tsoracle --timeout=180s
wait_job tsoracle-e2e-sigkill-soak 300

echo "== all assertions passed =="
