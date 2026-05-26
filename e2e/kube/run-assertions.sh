#!/usr/bin/env bash
# Orchestrates the kind e2e assertions against the current kubectl context:
#   1. cold-start: every ordinal serves or redirects (proves 3-node formation)
#   2. soak: zero final client errors + monotonicity across a graceful rollout
#   3. sigkill-soak: monotonicity holds across a force-deleted leader pod
#      (no graceful shutdown, no transfer_leader, no draining) — exercises the
#      crash-recovery paths that #427 (barrier-seq durable seed) and #426
#      (snapshot-publish TOCTOU) fixed, currently covered only by failpoint
#      unit tests. See issue #485.
#   4. membership-soak: monotonicity + sub-0.5% error rate hold across a
#      dynamic-membership churn (scale +1, add-learner, promote, remove a
#      non-leader voter) driven by `tsoracle admin` over the loopback admin
#      port. First end-to-end exercise of the three-address membership model
#      shipped in #453. openraft-only. See issue #487.
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

echo "== step 5: dynamic-membership soak (add-learner / promote / remove) =="
# Default ports come from the chart's values.yaml: peer=51001 admin=51002,
# client=50551. The kube-e2e workflow overrides client→5051 and peer→5100;
# admin stays loopback-only on 51002. Allow env overrides so the same
# script can drive both the kind lane and operator-tuned deployments.
peer_port="${PEER_PORT:-5100}"
tso_port="${TSO_PORT:-5051}"
admin_port="${ADMIN_PORT:-51002}"
kubectl apply -f "$here/driver/job-membership-soak.yaml"
wait_soak_live tsoracle-e2e-membership-soak "membership-soak: first GetTs ok" 60

# Scale by one (3 → 4); the existing replicas keep serving while tsoracle-3
# starts. The pod template was rendered with REPLICAS=3 at helm-install
# time, so tsoracle-3's entrypoint emits no `--bootstrap` and no
# `--members` — the new node starts as an empty openraft follower waiting
# to be add-learner'd. Once Ready (TCP-readiness on the tso port implies
# the raft listener is also up, both bind in the same process), the leader
# can dial it at $raft_addr below.
echo "step 5: scaling StatefulSet to 4 (adding tsoracle-3 as a future learner)"
kubectl scale statefulset/tsoracle --replicas=4
kubectl rollout status statefulset/tsoracle --timeout=180s

leader_pod="$(find_leader_pod 60)"
echo "step 5: current leader = $leader_pod"

# Three-address membership: raft (peer transport), service (tsoracle client
# port for leader-hint redirect), admin (operator gRPC for future ops).
# Short DNS resolves within the cluster's search path; matches the soak
# endpoints' form. The admin address is unreachable from the headless
# Service (admin is not in its port list) but membership stores it for
# completeness — production deployments that bind admin on 0.0.0.0 with
# TLS would gain redirect-follow without script changes.
new_id=4
new_raft="tsoracle-3.tsoracle-peer:${peer_port}"
new_svc="tsoracle-3.tsoracle-peer:${tso_port}"
new_admin="tsoracle-3.tsoracle-peer:${admin_port}"
echo "step 5: add-learner id=$new_id raft=$new_raft service=$new_svc admin=$new_admin"

# `tsoracle admin add-learner` calls openraft's `add_learner(_, _, blocking=true)`
# which returns only after the learner has caught up with the leader's
# committed index (crates/tsoracle-standalone/src/admin/openraft.rs:200).
# So the issue #487 "poll members until caught-up" step is implicit in this
# single exec; no separate catch-up wait is needed before the subsequent
# `promote`. The exec runs INSIDE the leader pod so the loopback admin port
# is reachable and no leader_admin_endpoint redirect is required.
kubectl exec "$leader_pod" -c tsoracle -- \
    tsoracle admin add-learner \
        --endpoint "http://127.0.0.1:${admin_port}" \
        --id "$new_id" \
        --raft-addr "$new_raft" \
        --service-endpoint "$new_svc" \
        --admin-endpoint "$new_admin"

echo "step 5: promote id=$new_id to Voter"
kubectl exec "$leader_pod" -c tsoracle -- \
    tsoracle admin promote \
        --endpoint "http://127.0.0.1:${admin_port}" \
        --id "$new_id"

# Pick a non-leader voter to remove (leaving the leader in place avoids an
# election storm; the sigkill-soak step above already covers leader-loss
# recovery). Exclude $new_id so we don't immediately remove the node we
# just promoted.
echo "step 5: selecting a non-leader voter to remove (exclude=$new_id)"
members_out="$(kubectl exec "$leader_pod" -c tsoracle -- \
    tsoracle admin members --endpoint "http://127.0.0.1:${admin_port}")"
remove_id="$(printf '%s\n' "$members_out" | pick_remove_target "$new_id")" || {
    echo "step 5: pick_remove_target found no eligible voter (exclude=$new_id)"
    echo "step 5: admin members output:"
    printf '%s\n' "$members_out"
    exit 1
}
echo "step 5: remove id=$remove_id"
kubectl exec "$leader_pod" -c tsoracle -- \
    tsoracle admin remove \
        --endpoint "http://127.0.0.1:${admin_port}" \
        --id "$remove_id"

# Cooldown: wait for the membership-soak Job's full duration to elapse.
# The 180s soak duration leaves ~120s of headroom after the orchestrator's
# ~30-60s of churn — the tracker must observe monotonicity HOLDING in the
# post-transition steady state, not just up to the moment of the last
# operation. 240s timeout = soak duration + slack for the in-flight RPC at
# deadline.
wait_job tsoracle-e2e-membership-soak 240

echo "== all assertions passed =="
