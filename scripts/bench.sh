#!/usr/bin/env bash
# Runs a benchmark against a fresh Compose stack of its own and writes the
# results to bench/results/.
#
#   scripts/bench.sh loadtest [loadtest flags]
#
# The stack is a separate Compose project (llm-gateway-bench) on other ports,
# with its own volumes, so it never touches the development stack. It starts
# empty and is removed afterwards; KEEP=1 leaves it running.
set -euo pipefail
cd "$(dirname "$0")/.."

project=${BENCH_PROJECT:-llm-gateway-bench}
out=bench/results
mkdir -p "$out"

export PG_PORT=${BENCH_PG_PORT:-15432} REDIS_PORT=${BENCH_REDIS_PORT:-16379} MOCK_PORT=${BENCH_MOCK_PORT:-19000}
export GATEWAY_PORT=${BENCH_GATEWAY_PORT:-18080} GATEWAY_ADMIN_PORT=${BENCH_ADMIN_PORT:-18081}
export ADMIN_TOKEN=${BENCH_ADMIN_TOKEN:-dev-admin-token}
# Settings from the caller's shell would leak into the stack; each benchmark
# sets the ones it needs.
unset REQUEST_TIMEOUT ATTEMPT_TTFT_TIMEOUT RECONCILE_GRACE RECONCILE_INTERVAL LOG_LEVEL

commit=$(git rev-parse --short HEAD)
git diff --quiet HEAD -- . ':(exclude)bench' || commit+="-dirty"
export BENCH_COMMIT=$commit
cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)
export BENCH_HOST="$cpu, $(uname -s); Docker VM: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}}' |
  awk '{printf "%s %s %.1f GiB memory", $1, $2, $3/1073741824}')"

dc() { docker compose -p "$project" -f compose.yaml -f compose.bench.yaml "$@"; }

cleanup() {
  if [[ -z "${KEEP:-}" ]]; then
    dc down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# up starts a fresh stack: an empty ledger and no mock usage records.
up() {
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  dc up -d --build --wait gateway
}

# run_in_network runs a benchmark program inside the Compose network and
# keeps its JSON output only if it succeeds.
run_in_network() {
  local service=$1 json=$2
  shift 2
  dc build "$service"
  dc run --rm -T "$service" "$@" >"$json.tmp"
  mv "$json.tmp" "$json"
}

case ${1:-} in
loadtest)
  shift
  up
  run_in_network loadtest "$out/loadtest.json" "$@"
  go run ./scripts/loadtest -render "$out/loadtest.json" >"$out/loadtest.md"
  echo "wrote $out/loadtest.md"
  ;;
*)
  echo "usage: $0 loadtest [flags]" >&2
  exit 2
  ;;
esac
