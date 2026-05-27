#!/usr/bin/env bash
#
# Smoke-test staging TSOracle over Tailscale with the real tsoracle-client.

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/get-ts-staging.sh [count]

Environment overrides:
  AWS_PROFILE     AWS profile for kubectl auth (default: prisma-risk)
  KUBE_CONTEXT    kubectl context (default: prisma-risk-staging)
  NAMESPACE       Kubernetes namespace (default: tsoracle-staging)
  SECRET_NAME     mTLS secret name (default: tsoracle-tls)
  TSO_ENDPOINTS   Comma-separated Tailscale pod endpoints
                  (default: tsoracle-staging-{0,1,2}.taildd5193.ts.net:50551)
  TSO_TLS_DOMAIN  TLS authority/SAN (default: tsoracle-staging)
  TSO_PORT        TSO gRPC port (default: 50551)
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

COUNT="${1:-1}"
AWS_PROFILE="${AWS_PROFILE:-prisma-risk}"
KUBE_CONTEXT="${KUBE_CONTEXT:-prisma-risk-staging}"
NAMESPACE="${NAMESPACE:-tsoracle-staging}"
SECRET_NAME="${SECRET_NAME:-tsoracle-tls}"
TSO_TLS_DOMAIN="${TSO_TLS_DOMAIN:-tsoracle-staging}"
TSO_PORT="${TSO_PORT:-50551}"
TSO_ENDPOINTS="${TSO_ENDPOINTS:-tsoracle-staging-0.taildd5193.ts.net:${TSO_PORT},tsoracle-staging-1.taildd5193.ts.net:${TSO_PORT},tsoracle-staging-2.taildd5193.ts.net:${TSO_PORT}}"
HINT_INTERNAL_SUFFIX="${HINT_INTERNAL_SUFFIX:-.tsoracle-peer.tsoracle-staging.svc.cluster.local}"
HINT_EXTERNAL_PREFIX="${HINT_EXTERNAL_PREFIX:-tsoracle-staging}"
HINT_EXTERNAL_DOMAIN="${HINT_EXTERNAL_DOMAIN:-taildd5193.ts.net}"

for tool in cargo kubectl base64 git; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "error: required tool not found: $tool" >&2
    exit 1
  fi
done

if ! [[ "$COUNT" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: count must be a positive integer" >&2
  exit 1
fi

REPO_ROOT=$(git -C "$(dirname "$0")/.." rev-parse --show-toplevel)
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

fetch_secret_file() {
  local field="$1"
  local output="$2"

  AWS_PROFILE="$AWS_PROFILE" kubectl --context "$KUBE_CONTEXT" \
    get secret "$SECRET_NAME" \
    -n "$NAMESPACE" \
    -o "jsonpath={.data.${field}}" \
    | base64 -d > "$output"
}

fetch_secret_file 'ca\.crt' "$TMPDIR/ca.crt"
fetch_secret_file 'tls\.crt' "$TMPDIR/tls.crt"
fetch_secret_file 'tls\.key' "$TMPDIR/tls.key"

cargo run --quiet --manifest-path "$REPO_ROOT/Cargo.toml" -p tsoracle-client --example staging-get-ts -- \
  --endpoints "$TSO_ENDPOINTS" \
  --count "$COUNT" \
  --tls-ca "$TMPDIR/ca.crt" \
  --tls-cert "$TMPDIR/tls.crt" \
  --tls-key "$TMPDIR/tls.key" \
  --tls-domain "$TSO_TLS_DOMAIN" \
  --hint-internal-suffix "$HINT_INTERNAL_SUFFIX" \
  --hint-external-prefix "$HINT_EXTERNAL_PREFIX" \
  --hint-external-domain "$HINT_EXTERNAL_DOMAIN"
