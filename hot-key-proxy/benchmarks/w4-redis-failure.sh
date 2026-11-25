#!/usr/bin/env bash
# Hot traffic through the gateway, with Redis stopped mid-run and started again.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p benchmarks/results

docker run --rm -v "$PWD:/scripts" grafana/k6:0.54.0 run /scripts/benchmarks/k6/w4-redis-failure.js \
  --summary-export /scripts/benchmarks/results/w4-protected.json \
  -e BASE_URL=http://host.docker.internal:8080 \
  -e DURATION=70s \
  -e VUS="${VUS:-100}" \
  -e PRODUCTS="${PRODUCTS:-100000}" &
k6_pid=$!

sleep 20
echo "Stopping Redis nodes"
docker compose stop redis-1 redis-2 redis-3
sleep 20
echo "Starting Redis nodes"
docker compose start redis-1 redis-2 redis-3
wait "$k6_pid"
echo "Expect X-Cache: BYPASS while Redis is down, and no 5xx responses."
