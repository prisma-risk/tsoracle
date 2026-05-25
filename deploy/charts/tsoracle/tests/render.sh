#!/usr/bin/env sh
# Render-assertion smoke for the tsoracle chart: verifies each driver produces
# the right workload shapes and that invariants fire. Run by CI (PR job).
set -eu
chart="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# openraft => StatefulSet + headless svc + PDB
out=$(helm template t "$chart" --set driver=openraft)
echo "$out" | grep -q "kind: StatefulSet" || { echo "openraft: missing StatefulSet"; exit 1; }
echo "$out" | grep -q "kind: PodDisruptionBudget" || { echo "openraft: missing PDB"; exit 1; }
echo "$out" | grep -q "clusterIP: None" || { echo "openraft: missing headless Service"; exit 1; }

# paxos => same HA shape
helm template t "$chart" --set driver=paxos | grep -q "kind: StatefulSet" || { echo "paxos: missing StatefulSet"; exit 1; }

# file => Deployment + PVC, no StatefulSet, no headless svc (replicas=1 to pass the guard)
out=$(helm template t "$chart" --set driver=file,replicas=1)
echo "$out" | grep -q "kind: Deployment" || { echo "file: missing Deployment"; exit 1; }
echo "$out" | grep -q "kind: PersistentVolumeClaim" || { echo "file: missing PVC"; exit 1; }
if echo "$out" | grep -q "kind: StatefulSet"; then echo "file: must NOT have StatefulSet"; exit 1; fi

# tls on => secret volume mount path present
helm template t "$chart" --set driver=openraft,tls.enabled=true,tls.secretName=certs | grep -q "/etc/tsoracle/tls" \
    || { echo "tls: missing mount"; exit 1; }

# invariant: file + replicas>1 must FAIL
if helm template t "$chart" --set driver=file,replicas=3 >/dev/null 2>&1; then
    echo "file+replicas guard missing"; exit 1
fi

# invariant: tls.enabled without secretName must FAIL
if helm template t "$chart" --set driver=openraft,tls.enabled=true >/dev/null 2>&1; then
    echo "tls-without-secret guard missing"; exit 1
fi

# storageClassName must NOT be emitted as empty string (would bind the "" class)
if helm template t "$chart" --set driver=openraft | grep -q 'storageClassName: ""'; then
    echo "empty storageClassName emitted"; exit 1
fi

echo "chart render assertions passed"
