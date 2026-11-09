#!/usr/bin/env bash
# Runs a benchmark against a fresh Compose stack of its own and writes the
# results to bench/results/.
#
#   scripts/bench.sh loadtest [loadtest flags]
#   scripts/bench.sh failover [failover_test flags]
#   scripts/bench.sh chaos    [ledger_chaos flags]
#   scripts/bench.sh postgres
#
# The stack is a separate Compose project (llm-gateway-bench) on other ports,
# with its own volumes, so it never touches the development stack. It starts
# empty and is removed afterwards; KEEP=1 leaves it running.
set -euo pipefail
cd "$(dirname "$0")/.."

# A Mac that idle-sleeps mid-run freezes the Docker VM, and the run's numbers
# are then meaningless, so keep it awake for the duration.
if [[ "$(uname -s)" == Darwin && -z "${BENCH_CAFFEINATED:-}" ]] && command -v caffeinate >/dev/null; then
  BENCH_CAFFEINATED=1 exec caffeinate -ims "$0" "$@"
fi

project=${BENCH_PROJECT:-llm-gateway-bench}
out=bench/results
mkdir -p "$out"

export PG_PORT=${BENCH_PG_PORT:-15432} REDIS_PORT=${BENCH_REDIS_PORT:-16379} MOCK_PORT=${BENCH_MOCK_PORT:-19000}
export GATEWAY_PORT=${BENCH_GATEWAY_PORT:-18080} GATEWAY_ADMIN_PORT=${BENCH_ADMIN_PORT:-18081}
export ADMIN_TOKEN=${BENCH_ADMIN_TOKEN:-dev-admin-token}
# Settings from the caller's shell would leak into the stack; each benchmark
# sets the ones it needs.
unset REQUEST_TIMEOUT ATTEMPT_TTFT_TIMEOUT RECONCILE_GRACE RECONCILE_INTERVAL LOG_LEVEL
export BENCH_PG_SYNCHRONOUS_COMMIT=${BENCH_PG_SYNCHRONOUS_COMMIT:-on}

# A run is marked dirty if code or config differ from the commit; edits to
# documentation and to the results themselves do not count.
commit=$(git rev-parse --short HEAD)
git diff --quiet HEAD -- . ':(exclude)bench' ':(exclude)docs' ':(exclude)*.md' || commit+="-dirty"
export BENCH_COMMIT=$commit
cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)
export BENCH_HOST="$cpu, $(uname -s); Docker VM: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}}' |
  awk '{printf "%s %s %.1f GiB memory", $1, $2, $3/1073741824}')"

dc() { docker compose -p "$project" -f compose.yaml -f compose.bench.yaml "$@"; }

# Docker's disk lives on the host's (Docker Desktop's disk image grows into
# it), and when the host's disk fills, the Docker engine crashes. So a run
# refuses to start, and stops, when less than BENCH_MIN_FREE_MB is free on
# BENCH_DISK, which must be on the same filesystem as Docker's data.
min_free_mb=${BENCH_MIN_FREE_MB:-1024}
disk=${BENCH_DISK:-$HOME}
check_disk() {
  local free
  free=$(df -Pm "$disk" | awk 'NR == 2 { print $4 }')
  if ((free < min_free_mb)); then
    echo "bench: only $free MB free on $disk, less than BENCH_MIN_FREE_MB=$min_free_mb" >&2
    return 1
  fi
}

# watch_disk takes the stack down if the disk runs low during a run. The
# benchmark then fails, and its results are not kept.
watch_disk() {
  while sleep 5; do
    if ! check_disk; then
      echo "bench: stopping the stack before the disk fills" >&2
      dc down -v --remove-orphans --timeout 1 >/dev/null 2>&1 || true
      return
    fi
  done
}

cleanup() {
  if [[ -n "${watcher:-}" ]]; then
    kill "$watcher" 2>/dev/null || true
  fi
  # A failed run's partial output.
  rm -f "$out"/*.tmp
  if [[ -z "${KEEP:-}" ]]; then
    dc down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

check_disk || exit 1
watch_disk &
watcher=$!

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
  # BENCH_PG_SYNCHRONOUS_COMMIT=off measures how much of the latency is the
  # disk; that run's report gets a name of its own.
  name=loadtest
  if [[ "${BENCH_PG_SYNCHRONOUS_COMMIT:-on}" != on ]]; then
    name=loadtest-synchronous-commit-$BENCH_PG_SYNCHRONOUS_COMMIT
  fi
  up
  run_in_network loadtest "$out/$name.json" "$@"
  go run ./scripts/loadtest -render "$out/$name.json" >"$out/$name.md"
  echo "wrote $out/$name.md"
  ;;
failover)
  shift
  up
  run_in_network failover_test "$out/failover.json" "$@"
  go run ./scripts/failover_test -render "$out/failover.json" >"$out/failover.md"
  echo "wrote $out/failover.md"
  ;;
chaos)
  shift
  # Short timeouts, so orphaned reservations become eligible for the
  # reconciler within a minute of the last kill.
  export REQUEST_TIMEOUT=20s ATTEMPT_TTFT_TIMEOUT=5s RECONCILE_GRACE=10s RECONCILE_INTERVAL=5s
  up
  container=$(dc ps -q gateway)
  go run ./scripts/ledger_chaos \
    -gateway "http://127.0.0.1:$GATEWAY_PORT" -admin "http://127.0.0.1:$GATEWAY_ADMIN_PORT" \
    -mock "http://127.0.0.1:$MOCK_PORT" \
    -database-url "postgres://gateway:gateway@127.0.0.1:$PG_PORT/gateway?sslmode=disable" \
    -container "$container" -request-timeout "$REQUEST_TIMEOUT" -reconcile-grace "$RECONCILE_GRACE" \
    "$@" >"$out/ledger_chaos.json.tmp"
  mv "$out/ledger_chaos.json.tmp" "$out/ledger_chaos.json"
  go run ./scripts/ledger_chaos -render "$out/ledger_chaos.json" >"$out/ledger_chaos.md"
  echo "wrote $out/ledger_chaos.md"
  ;;
postgres)
  dc down -v --remove-orphans >/dev/null 2>&1 || true
  dc up -d --wait postgres
  dc exec -T -e BENCH_COMMIT -e BENCH_HOST postgres sh -s <scripts/pg_commit_latency.sh >"$out/postgres_commit.md.tmp"
  mv "$out/postgres_commit.md.tmp" "$out/postgres_commit.md"
  echo "wrote $out/postgres_commit.md"
  ;;
*)
  echo "usage: $0 loadtest|failover|chaos|postgres [flags]" >&2
  exit 2
  ;;
esac
