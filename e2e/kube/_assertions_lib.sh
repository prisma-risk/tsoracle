# Shared helpers for the kube e2e assertion lanes. Sourced by both
# run-assertions.sh and run-mixed-version-assertions.sh. POSIX-friendly
# (functions, no arrays).

# Poll a Job to terminal state. Succeeds on .status.succeeded=1, fails on
# any .status.failed, dumping the Job's logs so the assertion summary is
# visible.
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

# Block until a soak-style Job has logged its specific first-success
# sentinel. The sentinel is explicit (not a substring guess) so a future
# rename in one mode doesn't silently match the other's log.
#
# Args: JOB SENTINEL TIMEOUT_S
wait_soak_live() {
    local job="$1" sentinel="$2" timeout="$3" elapsed=0
    while true; do
        if kubectl logs "job/$job" 2>/dev/null | grep -qF "$sentinel"; then
            echo "$job: load is live (sentinel: $sentinel)"
            return 0
        fi
        # Short-circuit on Job failure (image pull / driver panic / cluster
        # unreachable). Without this check, an immediately-failing Job sits
        # for the full TIMEOUT_S before surfacing as "never became live",
        # delaying diagnosis by up to a minute.
        local failed
        failed="$(kubectl get job "$job" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
        if [ -n "$failed" ] && [ "$failed" != "0" ]; then
            echo "$job: FAILED before sentinel '$sentinel' appeared"
            kubectl logs "job/$job" || true
            return 1
        fi
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "$job: never became live (sentinel: $sentinel)"
            kubectl logs "job/$job" || true
            return 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
}

# Block until POD's tsoracle container reports a specific IMAGE in
# .status.containerStatuses AND .status.containerStatuses[*].ready=true.
# Without the image check, an already-Ready baseline pod falsely satisfies
# a "did the new image land" wait.
#
# Args: POD IMAGE TIMEOUT_S
wait_pod_image_and_ready() {
    local pod="$1" image="$2" timeout="$3" elapsed=0
    while true; do
        local actual_image ready
        actual_image="$(kubectl get pod "$pod" \
            -o jsonpath='{.status.containerStatuses[?(@.name=="tsoracle")].image}' \
            2>/dev/null || true)"
        ready="$(kubectl get pod "$pod" \
            -o jsonpath='{.status.containerStatuses[?(@.name=="tsoracle")].ready}' \
            2>/dev/null || true)"
        if [ "$actual_image" = "$image" ] && [ "$ready" = "true" ]; then
            echo "$pod: on image=$image and Ready"
            return 0
        fi
        if [ "$elapsed" -ge "$timeout" ]; then
            echo "$pod: timed out (image=$actual_image ready=$ready, want image=$image ready=true)"
            kubectl describe pod "$pod" || true
            return 1
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
}
