#!/usr/bin/env bash
# A narrated terminal demo of Aether — clean enough to record with asciinema:
#
#   asciinema rec aether.cast -c ./scripts/demo.sh      # record
#   asciinema upload aether.cast                         # share (embeds in the README)
#
# It brings up a 2-shard cluster (leader + follower each), runs real queries with
# provenance, then does the signature chaos move: kill a shard leader, watch coverage go
# partial, the follower get promoted, and coverage recover — with zero failed queries.
set -uo pipefail
cd "$(dirname "$0")/.."

say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$1"; sleep 1; }
run() { printf '\033[2m$ %s\033[0m\n' "$*"; "$@"; sleep 1; }
CP=127.0.0.1:50050
Q() { AETHER_COORDINATOR_ADDR=$CP ./target/debug/examples/cluster_query "$@"; }

say "Building Aether (debug)…"
cargo build -q -p coordinator -p shard-node --examples

cleanup() { pkill -f 'target/debug/shard-node' 2>/dev/null || true; pkill -f 'target/debug/coordinator' 2>/dev/null || true; }
trap cleanup EXIT
cleanup; sleep 1

say "Start a 2-shard cluster — a leader AND a follower per shard (snappy 6s failover)"
AETHER_COORDINATOR_ADDR=$CP AETHER_SHARD_COUNT=2 AETHER_LIVENESS_TIMEOUT_SECS=6 \
  ./target/debug/coordinator >/tmp/demo-coord.log 2>&1 &
sleep 2
# Track only shard 0's leader PID (the one we'll kill). Plain var — macOS bash 3.2 has no
# associative arrays. Cleanup of the rest is by pkill.
S0_LEADER_PID=""
p=50060
for shard in 0 1; do
  for role in leader follower; do
    AETHER_NODE_ID=s${shard}-$role AETHER_SHARD_ADDR=127.0.0.1:$p AETHER_SHARD_INDEX=$shard AETHER_SHARD_COUNT=2 \
      AETHER_ROLE=$role AETHER_COORDINATOR_ADDR=$CP AETHER_SOURCE=synthetic AETHER_POLL_SECS=1 AETHER_HEARTBEAT_SECS=2 \
      ./target/debug/shard-node >/tmp/demo-s${shard}-$role.log 2>&1 &
    if [ "$shard" = 0 ] && [ "$role" = leader ]; then S0_LEADER_PID=$!; fi
    p=$((p+1))
  done
done
# Detach the background jobs from this shell's job control so cleanup on exit doesn't print
# "Terminated" notifications into the recording. pkill still reaps them by name.
disown -a 2>/dev/null || true
# Wait until both shards answer.
for _ in $(seq 1 40); do Q Synthetica 1 2>/dev/null | grep -q "2/2 shards" && break; sleep 0.5; done

say "Search across the cluster — note the provenance: 2/2 shards answered"
run Q Synthetica 3

say "Aggregate across the cluster — altitude percentiles, merged from a t-digest per shard"
run bash -c "AETHER_COORDINATOR_ADDR=$CP ./target/debug/examples/aggregate percentiles altitude"

say "CHAOS: kill shard 0's LEADER (kill -9) — a real process, mid-flight"
run kill -9 "$S0_LEADER_PID"

say "Query immediately: coverage drops to PARTIAL — the answer is honest, not failed"
run Q Synthetica 3

say "Wait for failover… the coordinator reaps the dead leader and promotes the follower"
for _ in $(seq 1 30); do Q Synthetica 1 2>/dev/null | grep -q "2/2 shards" && break; sleep 1; done

say "Query again: the promoted follower serves shard 0 — coverage is back to 2/2, zero failed queries"
run Q Synthetica 3

say "More: ./scripts/live-eval-local.sh (LLM over the cluster, 10/10) · ./scripts/bench.sh (throughput) · cargo run -p dashboard (live UI)"
