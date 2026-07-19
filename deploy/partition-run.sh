#!/usr/bin/env bash
# Real-machine partition: isolate the CURRENT RAFT LEADER's instance with a security-group
# swap (no ingress from the cluster, no egress at all — the instance is alive but off the
# fabric; only the operator's IP can still reach it). Measure failover, prove the isolated
# store freezes, heal by swapping the group back, and verify convergence — all with the
# query error counter at zero. Redirect stdout to capture the artifact.
set -euo pipefail
cd "$(dirname "$0")"

REGION=$(python3 -c "import json;print(json.load(open('cluster.json'))['region'])")
COORD=$(python3 -c "import json;print(json.load(open('cluster.json'))['coordinator']['public'])")
MYIP="$(curl -fsS https://checkip.amazonaws.com)/32"
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --region "$REGION" --query 'Vpcs[0].VpcId' --output text)

DASH_PORT=8092
state()   { curl -fsS "http://127.0.0.1:$DASH_PORT/api/state"; }
leader()  { state 2>/dev/null | python3 -c "import json,sys;s=json.load(sys.stdin);ns=[n for n in s['nodes'] if n.get('role')=='Leader'];print(ns[0]['node_id'] if ns else '')" 2>/dev/null || echo ""; }
matched() { state 2>/dev/null | python3 -c "import json,sys;s=json.load(sys.stdin);print((s['query'].get('last') or {}).get('total_matched',0))" 2>/dev/null || echo 0; }
qcount()  { state 2>/dev/null | python3 -c "import json,sys;s=json.load(sys.stdin);print(s['query']['$1'])" 2>/dev/null || echo 0; }
node_seen() { state 2>/dev/null | python3 -c "import json,sys;s=json.load(sys.stdin);ns=[n for n in s['nodes'] if n['node_id']=='$1'];print(ns[0]['millis_since_seen'] if ns else 999999)" 2>/dev/null || echo 999999; }
shard_count() { ../target/debug/examples/shard_query "$1" 2>/dev/null | head -1 | cut -d= -f2 || echo ""; }

echo "== [prep] direct-probe ingress (50051 from $MYIP) on the cluster SG =="
CLUSTER_SG=$(aws ec2 describe-security-groups --region "$REGION" \
  --filters Name=group-name,Values=aether-sg Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)
aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$CLUSTER_SG" \
  --protocol tcp --port 50051 --cidr "$MYIP" 2>/dev/null || true

echo "== [prep] isolation SG: operator-only ingress, ZERO egress =="
ISO_SG=$(aws ec2 describe-security-groups --region "$REGION" \
  --filters Name=group-name,Values=aether-isolated Name=vpc-id,Values="$VPC" \
  --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ "$ISO_SG" = "None" ] || [ -z "$ISO_SG" ]; then
  ISO_SG=$(aws ec2 create-security-group --group-name aether-isolated \
    --description "aether partition harness: off the fabric" --vpc-id "$VPC" --region "$REGION" \
    --tag-specifications 'ResourceType=security-group,Tags=[{Key=aether,Value=1}]' \
    --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$ISO_SG" --protocol tcp --port 22 --cidr "$MYIP"
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$ISO_SG" --protocol tcp --port 50051 --cidr "$MYIP"
  # A fresh SG allows all egress; revoke it — the isolated instance may originate NOTHING.
  aws ec2 revoke-security-group-egress --region "$REGION" --group-id "$ISO_SG" --protocol -1 --cidr 0.0.0.0/0
fi
echo "cluster_sg=$CLUSTER_SG isolation_sg=$ISO_SG"

echo "== starting local dashboard in remote mode against $COORD:50050 =="
AETHER_DASHBOARD_REMOTE="$COORD:50050" AETHER_SOURCE=synthetic \
AETHER_DASHBOARD_ADDR="127.0.0.1:$DASH_PORT" \
  ../target/debug/dashboard > /tmp/aether-partition-dashboard.log 2>&1 &
DASH_PID=$!
trap 'kill $DASH_PID 2>/dev/null || true' EXIT

echo "== waiting for steady state =="
LEADER=""; MATCHED=0
for _ in $(seq 1 120); do
  LEADER=$(leader); MATCHED=$(matched)
  if [ -n "$LEADER" ] && [ "${MATCHED:-0}" -gt 20 ]; then break; fi
  sleep 2
done
[ -n "$LEADER" ] || { echo "no leader reached"; exit 1; }

VICTIM_ID=$(python3 -c "
import json
c=json.load(open('cluster.json'))
print(next(m['id'] for m in c['members'] if m['node_id']=='$LEADER'))")
VICTIM_PUB=$(python3 -c "
import json
c=json.load(open('cluster.json'))
print(next(m['public'] for m in c['members'] if m['node_id']=='$LEADER'))")
SURVIVOR_PUB=$(python3 -c "
import json
c=json.load(open('cluster.json'))
print(next(m['public'] for m in c['members'] if m['node_id']!='$LEADER'))")
echo "steady: leader=$LEADER ($VICTIM_ID @ $VICTIM_PUB) matched=$MATCHED"

T0=$(date +%s)
echo "== ISOLATING $LEADER: swapping $VICTIM_ID into $ISO_SG at t=0 =="
aws ec2 modify-instance-attribute --region "$REGION" --instance-id "$VICTIM_ID" --groups "$ISO_SG"

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
[ "$RECOVERED" = "1" ] || { echo "FAILOVER NOT OBSERVED"; exit 1; }
echo "failover: new_leader=$NEW_LEADER after $((T1-T0))s"

echo "== the isolated store must FREEZE (minority cannot commit) =="
FROZEN_A=$(shard_count "$VICTIM_PUB:50051")
GROW_BASE=$(shard_count "$SURVIVOR_PUB:50051")
for _ in $(seq 1 60); do
  NOW=$(shard_count "$SURVIVOR_PUB:50051")
  if [ -n "$NOW" ] && [ -n "$GROW_BASE" ] && [ "$NOW" -gt $((GROW_BASE + 5)) ]; then break; fi
  sleep 2
done
FROZEN_B=$(shard_count "$VICTIM_PUB:50051")
echo "isolated store: $FROZEN_A -> $FROZEN_B while the majority grew ($GROW_BASE -> $NOW)"
[ "$FROZEN_A" = "$FROZEN_B" ] || { echo "SPLIT BRAIN: the isolated member kept committing"; exit 1; }

T2=$(date +%s)
echo "== HEALING: swapping $VICTIM_ID back into $CLUSTER_SG =="
aws ec2 modify-instance-attribute --region "$REGION" --instance-id "$VICTIM_ID" --groups "$CLUSTER_SG"

echo "== watching for rejoin + convergence =="
REJOINED=0
for _ in $(seq 1 120); do
  SEEN=$(node_seen "$LEADER")
  VC=$(shard_count "$VICTIM_PUB:50051"); SC=$(shard_count "$SURVIVOR_PUB:50051")
  if [ "${SEEN:-999999}" -lt 3000 ] && [ -n "$VC" ] && [ -n "$SC" ] && [ "$VC" -ge "$SC" ]; then
    REJOINED=1; break
  fi
  sleep 2
done
T3=$(date +%s)
[ "$REJOINED" = "1" ] || { echo "REJOIN/CONVERGENCE NOT OBSERVED (victim=$VC survivor=$SC seen=${SEEN}ms)"; exit 1; }

OKS=$(qcount ok); ERRS=$(qcount err)
echo ""
echo "== REAL-MACHINE PARTITION ARTIFACT =="
echo "{ \"isolated\": \"$LEADER\", \"instance\": \"$VICTIM_ID\", \"method\": \"sg-swap zero-egress\","
echo "  \"new_leader\": \"$NEW_LEADER\", \"failover_observed_secs\": $((T1-T0)),"
echo "  \"isolated_store\": { \"before\": $FROZEN_A, \"after\": $FROZEN_B, \"frozen\": true },"
echo "  \"majority_growth_during_isolation\": [$GROW_BASE, $NOW],"
echo "  \"heal_to_convergence_secs\": $((T3-T2)),"
echo "  \"queries_ok\": $OKS, \"queries_err\": $ERRS }"
[ "$ERRS" = "0" ] || { echo "QUERY ERRORS OCCURRED"; exit 1; }
echo "zero-error-under-partition: HOLDS on real machines"
