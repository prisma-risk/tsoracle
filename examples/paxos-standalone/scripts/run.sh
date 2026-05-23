#!/usr/bin/env bash
# Start a 3-node tsoracle/paxos cluster locally for demo purposes.
# Logs go to .data/n<id>.log. Press Ctrl-C to terminate all nodes.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p .data
rm -rf .data/n1 .data/n2 .data/n3

PAXOS_PEERS="1=127.0.0.1:53001,2=127.0.0.1:53002,3=127.0.0.1:53003"
TSO_PEERS="1=127.0.0.1:50581,2=127.0.0.1:50582,3=127.0.0.1:50583"

pids=()
trap 'echo "shutting down..."; kill "${pids[@]}" 2>/dev/null || true; wait' INT TERM

cargo build -p example-paxos-standalone --features reflection
TARGET_DIR="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')"
BIN="$TARGET_DIR/debug/paxos-standalone"

run_node() {
  local id="$1"; local listen="$2"; local tso="$3"
  "$BIN" \
    --node-id "$id" --listen "$listen" --tso-listen "$tso" \
    --peers "$PAXOS_PEERS" --tso-peers "$TSO_PEERS" \
    --data-dir ".data/n$id" \
    > ".data/n$id.log" 2>&1 &
  pids+=("$!")
}

run_node 1 127.0.0.1:53001 127.0.0.1:50581
run_node 2 127.0.0.1:53002 127.0.0.1:50582
run_node 3 127.0.0.1:53003 127.0.0.1:50583

echo "3-node tsoracle/paxos cluster running."
echo "  node 1 logs: .data/n1.log  (tsoracle: http://127.0.0.1:50581)"
echo "  node 2 logs: .data/n2.log  (tsoracle: http://127.0.0.1:50582)"
echo "  node 3 logs: .data/n3.log  (tsoracle: http://127.0.0.1:50583)"
echo "Ctrl-C to stop."
wait
