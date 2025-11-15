#!/usr/bin/env bash
# launch_nodes.sh
# Usage: ./launch_nodes.sh 8 6000
N=${1:-8}
START=${2:-6000}
LOGDIR=logs
mkdir -p $LOGDIR
for i in $(seq 0 $((N-1))); do
  PORT=$((START + i))
  if [ $i -eq 0 ]; then
    # bootstrap node
    python ../cli.py start --host 127.0.0.1 --port $PORT > $LOGDIR/node-$PORT.log 2>&1 &
  else
    python ../cli.py start --host 127.0.0.1 --port $PORT --bootstrap 127.0.0.1:$START > $LOGDIR/node-$PORT.log 2>&1 &
  fi
  echo "Started node on port $PORT (logs: $LOGDIR/node-$PORT.log)"
  sleep 0.2
done
echo "Launched $N nodes starting at port $START"