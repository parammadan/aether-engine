#!/usr/bin/env bash
# Destroy EVERYTHING tagged aether=1 in the region, then verify nothing remains.
set -euo pipefail
cd "$(dirname "$0")"
REGION="${AETHER_REGION:-us-east-2}"

IDS=$(aws ec2 describe-instances --region "$REGION" \
  --filters Name=tag:aether,Values=1 Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output text)
if [ -n "$IDS" ]; then
  echo "terminating: $IDS"
  aws ec2 terminate-instances --region "$REGION" --instance-ids $IDS >/dev/null
  aws ec2 wait instance-terminated --region "$REGION" --instance-ids $IDS
fi

SGS=$(aws ec2 describe-security-groups --region "$REGION" --filters Name=tag:aether,Values=1 \
  --query 'SecurityGroups[].GroupId' --output text)
for SG in $SGS; do
  for _ in $(seq 1 12); do
    aws ec2 delete-security-group --region "$REGION" --group-id "$SG" 2>/dev/null && break || sleep 10
  done
done

aws ec2 delete-key-pair --region "$REGION" --key-name aether-key 2>/dev/null || true
rm -rf .keys cluster.json

echo "== verify: zero tagged resources must remain =="
LEFT_I=$(aws ec2 describe-instances --region "$REGION" \
  --filters Name=tag:aether,Values=1 Name=instance-state-name,Values=pending,running,stopping,stopped \
  --query 'Reservations[].Instances[].InstanceId' --output text)
LEFT_S=$(aws ec2 describe-security-groups --region "$REGION" --filters Name=tag:aether,Values=1 \
  --query 'SecurityGroups[].GroupId' --output text)
if [ -z "$LEFT_I" ] && [ -z "$LEFT_S" ]; then
  echo "TEARDOWN VERIFIED: no aether=1 instances or security groups remain"
else
  echo "TEARDOWN INCOMPLETE: instances='$LEFT_I' sgs='$LEFT_S'" >&2
  exit 1
fi
