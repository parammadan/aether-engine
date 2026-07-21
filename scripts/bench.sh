#!/usr/bin/env bash
# Throughput/latency scaling benchmark: run the load generator against clusters of 1, 2 and
# 4 shards (synthetic source) and print a scaling table. Release build; localhost, so the
# numbers are relative (scaling behavior on one host) rather than production figures.
#
#   ./scripts/bench.sh [duration_secs=10] [concurrency=32] [mode=search|count]
set -uo pipefail
cd "$(dirname "$0")/.."

DURATION="${1:-10}"
CONCURRENCY="${2:-32}"
MODE="${3:-search}"

echo "building (release)…"
cargo build --release -q -p coordinator -p shard-node       # the cluster binaries
cargo build --release -q -p coordinator --example loadgen   # the load generator

cleanup() { pkill -f 'target/release/shard-node' 2>/dev/null || true; pkill -f 'target/release/coordinator' 2>/dev/null || true; }
trap cleanup EXIT
cleanup; sleep 2

# Wait until a "serving on <addr>" line appears in a log, or time out.
wait_for() { local log="$1" pat="$2"; for _ in $(seq 1 40); do grep -q "$pat" "$log" 2>/dev/null && return 0; sleep 0.5; done; return 1; }

echo
echo "shards | requests | QPS | p50 ms | p95 ms | p99 ms"
echo "-------|----------|-----|--------|--------|-------"
for N in 1 2 4; do
  # Distinct ports per iteration so a prior coordinator's TIME_WAIT can't block the rebind.
  COORD_PORT=$((50050 + N * 100))
  AETHER_COORDINATOR_ADDR=127.0.0.1:$COORD_PORT AETHER_SHARD_COUNT=$N \
    ./target/release/coordinator >/tmp/bench-coord.log 2>&1 &
  wait_for /tmp/bench-coord.log "serving on 127.0.0.1:$COORD_PORT" || { echo "coordinator failed to start"; cleanup; continue; }

  for i in $(seq 0 $((N-1))); do
    AETHER_NODE_ID=s$i AETHER_SHARD_ADDR=127.0.0.1:$((COORD_PORT+10+i)) AETHER_SHARD_INDEX=$i AETHER_SHARD_COUNT=$N \
      AETHER_COORDINATOR_ADDR=127.0.0.1:$COORD_PORT AETHER_SOURCE=synthetic AETHER_POLL_SECS=1 \
      ./target/release/shard-node >/tmp/bench-s$i.log 2>&1 &
    wait_for /tmp/bench-s$i.log "serving on" || echo "  (shard $i slow to start)"
  done
  sleep 8  # let ingestion build a corpus across the shards

  OUT=$(AETHER_COORDINATOR_ADDR=127.0.0.1:$COORD_PORT ./target/release/examples/loadgen "$MODE" "$DURATION" "$CONCURRENCY" 2>&1)
  REQ=$(echo "$OUT" | awk -F': ' '/requests:/{print $2}')
  QPS=$(echo "$OUT" | awk -F': ' '/throughput:/{print $2}' | awk '{print $1}')
  P50=$(echo "$OUT" | grep -oE 'p50=[0-9.]+' | cut -d= -f2)
  P95=$(echo "$OUT" | grep -oE 'p95=[0-9.]+' | cut -d= -f2)
  P99=$(echo "$OUT" | grep -oE 'p99=[0-9.]+' | cut -d= -f2)
  printf "%6s | %8s | %4s | %6s | %6s | %6s\n" "$N" "${REQ:-?}" "${QPS:-?}" "${P50:-?}" "${P95:-?}" "${P99:-?}"
  cleanup; sleep 2
done
