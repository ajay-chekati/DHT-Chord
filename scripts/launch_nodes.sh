#!/usr/bin/env bash
# launch_nodes.sh
# Usage: ./scripts/launch_nodes.sh 8 6000 127.0.0.1

set -euo pipefail

N=${1:-8}
START=${2:-6000}
HOST=${3:-127.0.0.1}
LOGDIR=logs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI_PATH="$PROJECT_ROOT/cli.py"
PYTHON_BIN=${PYTHON:-python}

mkdir -p "$LOGDIR"

for i in $(seq 0 $((N - 1))); do
  PORT=$((START + i))
  LOG_FILE="$LOGDIR/node-$PORT.log"
  if [ $i -eq 0 ]; then
    "$PYTHON_BIN" "$CLI_PATH" start --host "$HOST" --port "$PORT" >"$LOG_FILE" 2>&1 &
  else
    "$PYTHON_BIN" "$CLI_PATH" start --host "$HOST" --port "$PORT" --bootstrap "$HOST:$START" >"$LOG_FILE" 2>&1 &
  fi
  echo "Started node on port $PORT (logs: $LOG_FILE)"
  sleep 0.2
done

echo "Launched $N nodes starting at port $START (host=$HOST)"