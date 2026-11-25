#!/usr/bin/env bash
# Runs W1, W2, W3 and W5 against baseline and protected paths.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p benchmarks/results

DURATION="${DURATION:-30s}"
SPIKE_VUS="${SPIKE_VUS:-1000}"
PRODUCTS="${PRODUCTS:-100000}"

run_k6() {
  local script="$1"
  local url="$2"
  local export="$3"
  shift 3
  if command -v k6 >/dev/null 2>&1; then
    k6 run "$script" --summary-export "$export" -e "BASE_URL=$url" -e "DURATION=$DURATION" -e "PRODUCTS=$PRODUCTS" "$@"
  else
    docker run --rm -v "$PWD:/scripts" grafana/k6:0.54.0 run "/scripts/$script" \
      --summary-export "/scripts/$export" \
      -e "BASE_URL=$url" -e "DURATION=$DURATION" -e "PRODUCTS=$PRODUCTS" "$@"
  fi
}

for mode in baseline protected; do
  if [ "$mode" = "baseline" ]; then url="http://host.docker.internal:8081"; else url="http://host.docker.internal:8080"; fi
  run_k6 benchmarks/k6/w1-uniform.js "$url" "benchmarks/results/w1-uniform-$mode.json"
  run_k6 benchmarks/k6/w2-hotkey.js "$url" "benchmarks/results/w2-hotkey-$mode.json" -e "SPIKE_VUS=$SPIKE_VUS"
  run_k6 benchmarks/k6/w3-moving-hotspot.js "$url" "benchmarks/results/w3-moving-$mode.json"
  run_k6 benchmarks/k6/w5-overload.js "$url" "benchmarks/results/w5-overload-$mode.json"
done

echo "Summaries written to benchmarks/results"
