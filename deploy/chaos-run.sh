#!/usr/bin/env bash
# The real-machine chaos run: against the provisioned EC2 cluster, watch a live query
# stream, terminate the CURRENT RAFT LEADER's instance via the AWS API, and record the
# failover (new leader elected + routed, data growing again, zero query errors) with
# timestamps. Redirect stdout to capture the artifact.
set -euo pipefail
cd "$(dirname "$0")"

REGION=$(python3 -c "import json;print(json.load(open('cluster.json'))['region'])")
COORD=$(python3 -c "import json;print(json.load(open('cluster.json'))['coordinator']['public'])")

DASH_PORT=8091
state()   { curl -fsS "http://127.0.0.1:$DASH_PORT/api/state"; }
leader()  { state 2>/dev/null | python3 -c "import json,sys;s=json.load(sys.stdin);ns=[n for n in s['nodes'] if n.get('role')=='Leader'];print(ns[0]['node_id'] if ns else '')" 2>/dev/null || echo ""; }
matched() { state 2>/dev/null | python3 -c "import json,sys;s=json.load(sys.stdin);print((s['query'].get('last') or {}).get('total_matched',0))" 2>/dev/null || echo 0; }
qcount()  { state 2>/dev/null | python3 -c "import json,sys;s=json.load(sys.stdin);print(s['query']['$1'])" 2>/dev/null || echo 0; }

echo "== starting local dashboard in remote mode against $COORD:50050 =="
AETHER_DASHBOARD_REMOTE="$COORD:50050" AETHER_SOURCE=synthetic \
AETHER_DASHBOARD_ADDR="127.0.0.1:$DASH_PORT" \
  ../target/debug/dashboard > /tmp/aether-chaos-dashboard.log 2>&1 &
DASH_PID=$!
trap 'kill $DASH_PID 2>/dev/null || true' EXIT

echo "== waiting for steady state (routed leader + growing data) =="
LEADER=""; MATCHED=0
for _ in $(seq 1 120); do
  LEADER=$(leader); MATCHED=$(matched)
  if [ -n "$LEADER" ] && [ "${MATCHED:-0}" -gt 20 ]; then break; fi
  sleep 2
done
[ -n "$LEADER" ] || { echo "no leader reached"; state | python3 -m json.tool || true; exit 1; }
echo "steady: leader=$LEADER matched=$MATCHED"

VICTIM_ID=$(python3 -c "
import json
c=json.load(open('cluster.json'))
print(next(m['id'] for m in c['members'] if m['node_id']=='$LEADER'))")
T0=$(date +%s)
echo "== TERMINATING leader instance $VICTIM_ID ($LEADER) via AWS API at t=0 =="
aws ec2 terminate-instances --region "$REGION" --instance-ids "$VICTIM_ID" >/dev/null

echo "== watching for failover =="
NEW_LEADER=""; NOW_MATCHED=0; RECOVERED=0
for _ in $(seq 1 120); do
  NEW_LEADER=$(leader); NOW_MATCHED=$(matched)
  if [ -n "$NEW_LEADER" ] && [ "$NEW_LEADER" != "$LEADER" ] && [ "${NOW_MATCHED:-0}" -gt "$MATCHED" ]; then
    RECOVERED=1; break
  fi
  sleep 2
done
T1=$(date +%s)
[ "$RECOVERED" = "1" ] || { echo "FAILOVER NOT OBSERVED"; state | python3 -m json.tool || true; exit 1; }

OKS=$(qcount ok); ERRS=$(qcount err)
echo ""
echo "== REAL-MACHINE FAILOVER ARTIFACT =="
echo "{ \"killed\": \"$LEADER\", \"instance\": \"$VICTIM_ID\","
echo "  \"new_leader\": \"$NEW_LEADER\", \"failover_observed_secs\": $((T1-T0)),"
echo "  \"matched_before\": $MATCHED, \"matched_after\": $NOW_MATCHED,"
echo "  \"queries_ok\": $OKS, \"queries_err\": $ERRS }"
[ "$ERRS" = "0" ] || { echo "QUERY ERRORS OCCURRED"; exit 1; }
echo "zero-error-under-failure: HOLDS on real machines"
