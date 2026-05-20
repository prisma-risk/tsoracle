#!/usr/bin/env bash
# Start a 3-node tsoracle/openraft cluster locally for demo purposes.
# Logs go to .data/n<id>.log. Press Ctrl-C to terminate all nodes.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p .data
rm -rf .data/n1 .data/n2 .data/n3

PEERS="1=127.0.0.1:51001,2=127.0.0.1:51002,3=127.0.0.1:51003"
TSO_PEERS="1=127.0.0.1:50561,2=127.0.0.1:50562,3=127.0.0.1:50563"

pids=()
trap 'echo "shutting down..."; kill "${pids[@]}" 2>/dev/null || true; wait' INT TERM

cargo build -p example-openraft-cluster
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')"
BIN="$TARGET_DIR/debug/openraft-cluster"

run_node() {
  local id="$1"; local raft="$2"; local tso="$3"; local bootstrap="${4:-}"
  "$BIN" \
    --id "$id" --raft-addr "$raft" --tso-addr "$tso" \
    --peers "$PEERS" --tso-peers "$TSO_PEERS" \
    --raft-dir ".data/n$id" $bootstrap \
    > ".data/n$id.log" 2>&1 &
  pids+=("$!")
}

run_node 1 127.0.0.1:51001 127.0.0.1:50561 --bootstrap
run_node 2 127.0.0.1:51002 127.0.0.1:50562
run_node 3 127.0.0.1:51003 127.0.0.1:50563

echo "3-node tsoracle/openraft cluster running."
echo "  node 1 logs: .data/n1.log  (tsoracle: http://127.0.0.1:50561)"
echo "  node 2 logs: .data/n2.log  (tsoracle: http://127.0.0.1:50562)"
echo "  node 3 logs: .data/n3.log  (tsoracle: http://127.0.0.1:50563)"
echo "Ctrl-C to stop."
wait
