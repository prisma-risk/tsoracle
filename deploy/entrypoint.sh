#!/usr/bin/env sh
# Compute this pod's `tsoracle serve <driver>` invocation from the StatefulSet
# ordinal (HA drivers) and chart-provided env. Pod hostnames are
# "<statefulset>-<ordinal>"; node ids are 1-based (id = ordinal + 1). Peer FQDNs
# come from the headless service. POSIX sh (`/bin/sh` is dash on debian-slim — no
# `pipefail`).
set -eu

: "${DRIVER:?DRIVER must be set (file|openraft|paxos)}"
: "${TSO_PORT:=50551}"
: "${PEER_PORT:=51001}"
: "${DATA_DIR:=/data}"

# Optional TLS material (chart mounts a Secret at $TLS_DIR with tls.crt/tls.key/ca.crt).
client_tls=""
peer_tls=""
if [ -n "${TLS_DIR:-}" ] && [ -f "${TLS_DIR}/tls.crt" ]; then
    client_tls="--tls-cert ${TLS_DIR}/tls.crt --tls-key ${TLS_DIR}/tls.key"
    if [ "${CLIENT_MTLS:-false}" = "true" ]; then
        client_tls="${client_tls} --tls-client-ca ${TLS_DIR}/ca.crt"
    fi
    peer_tls="--peer-tls-cert ${TLS_DIR}/tls.crt --peer-tls-key ${TLS_DIR}/tls.key --peer-tls-ca ${TLS_DIR}/ca.crt"
fi

# Server knobs common to every serve subcommand (the bin reads --log as its
# tracing filter; RUST_LOG is not consulted).
: "${WINDOW_AHEAD:=3s}"
: "${FAILOVER_ADVANCE:=1s}"
: "${LOG_LEVEL:=info}"
common="--window-ahead ${WINDOW_AHEAD} --failover-advance ${FAILOVER_ADVANCE} --log ${LOG_LEVEL}"

case "$DRIVER" in
file)
    # shellcheck disable=SC2086
    exec tsoracle serve file \
        --listen "0.0.0.0:${TSO_PORT}" \
        --state-dir "${DATA_DIR}" \
        $client_tls $common
    ;;
openraft|paxos)
    : "${REPLICAS:?REPLICAS must be set}"
    : "${STATEFULSET_NAME:?STATEFULSET_NAME must be set}"
    : "${PEER_SERVICE:?PEER_SERVICE must be set}"
    : "${POD_NAMESPACE:?POD_NAMESPACE must be set}"
    ordinal="$(hostname | sed 's/.*-//')"
    node_id=$((ordinal + 1))

    members=""
    peers=""
    tso_peers=""
    i=0
    while [ "$i" -lt "$REPLICAS" ]; do
        id=$((i + 1))
        fqdn="${STATEFULSET_NAME}-${i}.${PEER_SERVICE}.${POD_NAMESPACE}.svc.cluster.local"
        members="${members}${members:+,}${id}=${fqdn}:${PEER_PORT}/${fqdn}:${TSO_PORT}"
        peers="${peers}${peers:+,}${id}=${fqdn}:${PEER_PORT}"
        tso_peers="${tso_peers}${tso_peers:+,}${id}=${fqdn}:${TSO_PORT}"
        i=$((i + 1))
    done

    if [ "$DRIVER" = "openraft" ]; then
        bootstrap=""
        if [ "$node_id" -eq 1 ]; then
            bootstrap="--bootstrap --members $members"
        fi
        # shellcheck disable=SC2086
        exec tsoracle serve openraft \
            --id "$node_id" \
            --raft-addr "0.0.0.0:${PEER_PORT}" \
            --listen "0.0.0.0:${TSO_PORT}" \
            --raft-dir "${DATA_DIR}/raft" \
            $peer_tls $client_tls $common $bootstrap
    else
        # shellcheck disable=SC2086
        exec tsoracle serve paxos \
            --node-id "$node_id" \
            --peer-listen "0.0.0.0:${PEER_PORT}" \
            --listen "0.0.0.0:${TSO_PORT}" \
            --peers "$peers" \
            --tso-peers "$tso_peers" \
            --data-dir "${DATA_DIR}" \
            $peer_tls $client_tls $common
    fi
    ;;
*)
    echo "unknown DRIVER: $DRIVER (want file|openraft|paxos)" >&2
    exit 1
    ;;
esac
