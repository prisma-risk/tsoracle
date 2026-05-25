#!/usr/bin/env sh
# Render-assertion smoke for the tsoracle chart: verifies each driver produces
# the right workload shapes and that invariants fire. Run by CI (PR job).
set -eu
chart="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

# openraft => StatefulSet + headless svc + PDB + NetworkPolicy
out=$(helm template t "$chart" --set driver=openraft,tls.allowInsecurePeer=true)
echo "$out" | grep -q "kind: StatefulSet" || { echo "openraft: missing StatefulSet"; exit 1; }
echo "$out" | grep -q "kind: PodDisruptionBudget" || { echo "openraft: missing PDB"; exit 1; }
echo "$out" | grep -q "clusterIP: None" || { echo "openraft: missing headless Service"; exit 1; }
echo "$out" | grep -q "kind: NetworkPolicy" || { echo "openraft: missing NetworkPolicy"; exit 1; }

# paxos => same HA shape
helm template t "$chart" --set driver=paxos,tls.allowInsecurePeer=true | grep -q "kind: StatefulSet" || { echo "paxos: missing StatefulSet"; exit 1; }

# file => Deployment + PVC, no StatefulSet, no headless svc, no NetworkPolicy (replicas=1 to pass the guard)
out=$(helm template t "$chart" --set driver=file,replicas=1)
echo "$out" | grep -q "kind: Deployment" || { echo "file: missing Deployment"; exit 1; }
echo "$out" | grep -q "kind: PersistentVolumeClaim" || { echo "file: missing PVC"; exit 1; }
if echo "$out" | grep -q "kind: StatefulSet"; then echo "file: must NOT have StatefulSet"; exit 1; fi
if echo "$out" | grep -q "kind: NetworkPolicy"; then echo "file: must NOT have NetworkPolicy"; exit 1; fi

# tls on => secret volume mount path present, and no allowInsecurePeer needed
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

# invariant: HA driver with TLS off and no opt-out must FAIL (no unauthenticated peer port by default)
if helm template t "$chart" --set driver=openraft >/dev/null 2>&1; then
    echo "insecure-peer guard missing (openraft)"; exit 1
fi
if helm template t "$chart" --set driver=paxos >/dev/null 2>&1; then
    echo "insecure-peer guard missing (paxos)"; exit 1
fi

# opt-out: allowInsecurePeer renders but still ships a NetworkPolicy by default
helm template t "$chart" --set driver=openraft,tls.allowInsecurePeer=true | grep -q "kind: NetworkPolicy" \
    || { echo "allowInsecurePeer: NetworkPolicy not emitted"; exit 1; }

# networkPolicy.enabled=false suppresses the policy
if helm template t "$chart" --set driver=openraft,tls.allowInsecurePeer=true,networkPolicy.enabled=false | grep -q "kind: NetworkPolicy"; then
    echo "networkPolicy.enabled=false still emitted a NetworkPolicy"; exit 1
fi

# storageClassName must NOT be emitted as empty string (would bind the "" class)
if helm template t "$chart" --set driver=openraft,tls.allowInsecurePeer=true | grep -q 'storageClassName: ""'; then
    echo "empty storageClassName emitted"; exit 1
fi

echo "chart render assertions passed"
