#!/usr/bin/env sh
# Derive this node's raft identity and membership map from the StatefulSet ordinal.
#
# A StatefulSet ships one pod spec for every replica, so each pod must work
# out its own --id and the full --members map at start time.
# Pod hostnames are "<statefulset>-<ordinal>" (e.g. tsoracle-0); node IDs are
# 1-based, so id = ordinal + 1. Peer FQDNs come from the headless service.
set -eu

: "${REPLICAS:?REPLICAS must be set}"
: "${STATEFULSET_NAME:?STATEFULSET_NAME must be set}"
: "${PEER_SERVICE:?PEER_SERVICE must be set}"
: "${POD_NAMESPACE:?POD_NAMESPACE must be set}"
: "${RAFT_PORT:=5100}"
: "${TSO_PORT:=5051}"
: "${DATA_DIR:=/data}"

ordinal="${HOSTNAME##*-}"
node_id=$((ordinal + 1))

members=""
i=0
while [ "$i" -lt "$REPLICAS" ]; do
    id=$((i + 1))
    fqdn="${STATEFULSET_NAME}-${i}.${PEER_SERVICE}.${POD_NAMESPACE}.svc.cluster.local"
    members="${members}${members:+,}${id}=${fqdn}:${RAFT_PORT}/${fqdn}:${TSO_PORT}"
    i=$((i + 1))
done

# Initialize the cluster from exactly one node. --bootstrap is idempotent
# (a re-run on an already-formed cluster logs and continues), so pinning it
# to ordinal 0 is safe across reschedules.
bootstrap=""
if [ "$node_id" -eq 1 ]; then
    bootstrap="--bootstrap"
fi

exec tsoracle serve openraft \
    --id "$node_id" \
    --raft-addr "0.0.0.0:${RAFT_PORT}" \
    --listen "0.0.0.0:${TSO_PORT}" \
    --raft-dir "${DATA_DIR}/raft" \
    ${bootstrap:+$bootstrap --members "$members"}
