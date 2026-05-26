#!/usr/bin/env bash
# Mixed-version soak orchestrator: drives a partial-then-full image rollout
# against a chart-installed openraft cluster, asserting that:
#   1. activate-format mid-partial-rollout is rejected by the all-members gate
#   2. activate-format post-full-rollout succeeds
#   3. the underlying soak Job reports zero monotonicity violations + <0.5% err
# Assumes the chart has been installed at image.tag=e2e-baseline and the
# StatefulSet has reached readiness.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=./_assertions_lib.sh
source "$here/_assertions_lib.sh"

# Activation target. BASELINE_WRITE_VERSION is 4 today; e2e-max-readable-next
# raises MAX_READABLE_VERSION to BASELINE+1 = 5. If BASELINE bumps, this
# constant trips a visible mismatch on the next run rather than silently
# activating to an unintended version.
ACTIVATION_TARGET=5
# Loopback admin port; matches deploy/entrypoint.sh:12 ADMIN_PORT default
# and the chart values.yaml ports.admin entry added in Task 7.
ADMIN_PORT=51002
# The kube-e2e-mixed-version.yml workflow tags images as :e2e-baseline and
# :e2e-next. The Helm install sets the baseline tag.
NEXT_IMAGE=tsoracle:e2e-next

# Try `tsoracle admin activate-format` against each tsoracle pod's loopback
# admin in turn. Exit codes (CLI contract, see crates/tsoracle-bin/src/main.rs
# ActivationOutcome):
#   0 = success
#   2 = MEMBERS_BELOW_TARGET (all-members gate rejected — :next leader saw baseline peers)
#   3 = NOT_LEADER (follower; skip and try next pod)
#   4 = TARGET_OUT_OF_RANGE (local-range rejected — baseline leader can't read target)
#   1 = anything else (hard fail)
# For `expect=REJECTED` we accept EITHER exit 2 OR exit 4 — both are "the
# activation control plane refused this unsafe state." See spec Component 2
# for why the e2e doesn't try to force a specific rejection shape (would
# require a tsoracle admin transfer-leader RPC, out of scope here).
#
# Args: TARGET EXPECT (EXPECT in {OK, REJECTED})
try_activate_format() {
    local target="$1" expect="$2"
    local pods
    pods=$(kubectl get pods -l app.kubernetes.io/name=tsoracle \
        -o jsonpath='{.items[*].metadata.name}')
    local last_rc=0
    for pod in $pods; do
        local rc=0
        timeout 30s kubectl exec "$pod" -c tsoracle -- \
            tsoracle admin activate-format \
            --endpoint "http://127.0.0.1:${ADMIN_PORT}" \
            --target "$target" \
            >&2 || rc=$?
        case "$rc" in
            0)
                if [ "$expect" = OK ]; then
                    echo "activate-format(target=$target) succeeded on $pod"
                    return 0
                fi
                echo "FAIL: activate-format succeeded on $pod but expected $expect"
                return 1
                ;;
            2|4)
                if [ "$expect" = REJECTED ]; then
                    local shape
                    if [ "$rc" = 2 ]; then
                        shape="MEMBERS_BELOW_TARGET (gate)"
                    else
                        shape="TARGET_OUT_OF_RANGE (local-range)"
                    fi
                    echo "activate-format(target=$target) safely rejected ($shape) on $pod"
                    return 0
                fi
                echo "FAIL: activate-format rejected on $pod (rc=$rc) but expected $expect"
                return 1
                ;;
            3)
                last_rc=3
                continue
                ;;
            *)
                echo "FAIL: activate-format on $pod: rc=$rc"
                return 1
                ;;
        esac
    done
    echo "FAIL: no pod accepted activate-format (last_rc=$last_rc)"
    return 1
}

echo "== step 1: start mixed-version soak (load is live before any perturbation) =="
kubectl apply -f "$here/driver/job-mixed-version-soak.yaml"
wait_soak_live tsoracle-e2e-mixed-version-soak \
    "mixed-version-soak: first GetTs ok" 60

echo "== step 2: partition=2; roll only ordinal 2 to :e2e-next =="
kubectl patch sts/tsoracle --type=strategic \
    -p '{"spec":{"updateStrategy":{"rollingUpdate":{"partition":2}}}}'
kubectl set image sts/tsoracle "tsoracle=${NEXT_IMAGE}"
wait_pod_image_and_ready tsoracle-2 "${NEXT_IMAGE}" 120

echo "== step 3: activation MUST be safely rejected (mixed-version state) =="
try_activate_format "$ACTIVATION_TARGET" REJECTED

echo "== step 4: partition=0; complete the rollout =="
kubectl patch sts/tsoracle --type=strategic \
    -p '{"spec":{"updateStrategy":{"rollingUpdate":{"partition":0}}}}'
kubectl rollout status sts/tsoracle --timeout=180s

echo "== step 5: activation MUST succeed (all members on :e2e-next) =="
try_activate_format "$ACTIVATION_TARGET" OK

echo "== step 6: drain the soak (zero monotonicity violations + <0.5% errors) =="
wait_job tsoracle-e2e-mixed-version-soak 180

echo "== all mixed-version assertions passed =="
