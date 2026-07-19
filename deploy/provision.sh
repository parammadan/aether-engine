#!/usr/bin/env bash
# Provision the Aether demo cluster: 1 coordinator + one 3-member raft group on
# 4 x t4g.small (Graviton). EVERY resource is tagged aether=1; ./teardown.sh destroys by
# tag and verifies nothing is left. Cost and max-session rules: see COST.md.
set -euo pipefail
cd "$(dirname "$0")"

REGION="${AETHER_REGION:-us-east-2}"
TYPE="t4g.small"
KEY_NAME="aether-key"
SG_NAME="aether-sg"
RUN_USER="ec2-user"

echo "== [1/6] building linux/arm64 release binaries in docker =="
mkdir -p target-linux .keys
docker volume create aether-cargo-cache >/dev/null
docker run --rm --platform linux/arm64 \
  -v "$(cd .. && pwd)":/src -w /src \
  -v aether-cargo-cache:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/src/deploy/target-linux \
  rust:1-bookworm bash -c \
  "apt-get update -qq && apt-get install -y -qq protobuf-compiler >/dev/null && cargo build --release -p coordinator -p shard-node"
ls -lh target-linux/release/{coordinator,shard-node}

echo "== [2/6] key pair + security group =="
MYIP="$(curl -fsS https://checkip.amazonaws.com)/32"
if ! aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" >/dev/null 2>&1; then
  aws ec2 create-key-pair --key-name "$KEY_NAME" --region "$REGION" \
    --tag-specifications 'ResourceType=key-pair,Tags=[{Key=aether,Value=1}]' \
    --query KeyMaterial --output text > .keys/aether-key.pem
  chmod 600 .keys/aether-key.pem
fi
VPC=$(aws ec2 describe-vpcs --filters Name=is-default,Values=true --region "$REGION" --query 'Vpcs[0].VpcId' --output text)
SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values="$SG_NAME" Name=vpc-id,Values="$VPC" --region "$REGION" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
if [ "$SG" = "None" ] || [ -z "$SG" ]; then
  SG=$(aws ec2 create-security-group --group-name "$SG_NAME" --description "aether demo" --vpc-id "$VPC" --region "$REGION" \
    --tag-specifications 'ResourceType=security-group,Tags=[{Key=aether,Value=1}]' --query GroupId --output text)
  aws ec2 authorize-security-group-ingress --group-id "$SG" --region "$REGION" --protocol tcp --port 22 --cidr "$MYIP"
  aws ec2 authorize-security-group-ingress --group-id "$SG" --region "$REGION" --protocol tcp --port 50050 --cidr "$MYIP"
  # intra-cluster: everything, but only from members of this same SG
  aws ec2 authorize-security-group-ingress --group-id "$SG" --region "$REGION" --protocol tcp --port 0-65535 --source-group "$SG"
fi
echo "sg=$SG vpc=$VPC myip=$MYIP"

echo "== [3/6] launching 4 x $TYPE =="
AMI=$(aws ssm get-parameter --region "$REGION" \
  --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64 \
  --query Parameter.Value --output text)
IDS=()
for NAME in aether-coordinator aether-m0 aether-m1 aether-m2; do
  ID=$(aws ec2 run-instances --region "$REGION" --image-id "$AMI" --instance-type "$TYPE" \
    --key-name "$KEY_NAME" --security-group-ids "$SG" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=aether,Value=1},{Key=Name,Value=$NAME}]" \
    --query 'Instances[0].InstanceId' --output text)
  IDS+=("$ID")
  echo "  $NAME -> $ID"
done
aws ec2 wait instance-running --region "$REGION" --instance-ids "${IDS[@]}"

declare -A PUB PRIV
for i in 0 1 2 3; do
  read -r P V < <(aws ec2 describe-instances --region "$REGION" --instance-ids "${IDS[$i]}" \
    --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)
  PUB[$i]=$P; PRIV[$i]=$V
done
COORD_PRIV="${PRIV[0]}"; COORD_PUB="${PUB[0]}"

echo "== [4/6] waiting for ssh =="
SSH="ssh -i .keys/aether-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=5"
for i in 0 1 2 3; do
  until $SSH "$RUN_USER@${PUB[$i]}" true 2>/dev/null; do sleep 5; done
  echo "  ${PUB[$i]} up"
done

echo "== [5/6] installing binaries + systemd units =="
install_node() { # idx name unit_body
  local ip="${PUB[$1]}"
  scp -i .keys/aether-key.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q \
    target-linux/release/coordinator target-linux/release/shard-node "$RUN_USER@$ip:/tmp/"
  $SSH "$RUN_USER@$ip" "sudo mv /tmp/coordinator /tmp/shard-node /usr/local/bin/ && sudo chmod +x /usr/local/bin/coordinator /usr/local/bin/shard-node && sudo mkdir -p /var/lib/aether && echo '$3' | sudo tee /etc/systemd/system/$2.service >/dev/null && sudo systemctl daemon-reload && sudo systemctl enable --now $2"
}

COORD_UNIT="[Unit]
Description=aether coordinator
[Service]
Environment=AETHER_COORDINATOR_ADDR=0.0.0.0:50050
Environment=AETHER_SHARD_COUNT=1
Environment=AETHER_LIVENESS_TIMEOUT_SECS=6
ExecStart=/usr/local/bin/coordinator
Restart=on-failure
[Install]
WantedBy=multi-user.target"
install_node 0 aether-coordinator "$COORD_UNIT"

for i in 1 2 3; do
  M=$((i-1))
  MEMBER_UNIT="[Unit]
Description=aether member m$M
[Service]
Environment=AETHER_NODE_ID=ec2-m$M
Environment=AETHER_SHARD_ADDR=0.0.0.0:50051
Environment=AETHER_ADVERTISE_ADDR=${PRIV[$i]}:50051
Environment=AETHER_SHARD_INDEX=0
Environment=AETHER_SHARD_COUNT=1
Environment=AETHER_CONSENSUS=raft
Environment=AETHER_GROUP_SIZE=3
Environment=AETHER_COORDINATOR_ADDR=$COORD_PRIV:50050
Environment=AETHER_HEARTBEAT_SECS=1
Environment=AETHER_SOURCE=synthetic
Environment=AETHER_POLL_SECS=1
Environment=AETHER_DATA_DIR=/var/lib/aether
ExecStart=/usr/local/bin/shard-node
Restart=on-failure
[Install]
WantedBy=multi-user.target"
  install_node $i "aether-member" "$MEMBER_UNIT"
done

echo "== [6/6] cluster manifest =="
cat > cluster.json <<JSON
{ "region": "$REGION",
  "coordinator": { "id": "${IDS[0]}", "public": "$COORD_PUB", "private": "$COORD_PRIV" },
  "members": [
    { "id": "${IDS[1]}", "public": "${PUB[1]}", "private": "${PRIV[1]}", "node_id": "ec2-m0" },
    { "id": "${IDS[2]}", "public": "${PUB[2]}", "private": "${PRIV[2]}", "node_id": "ec2-m1" },
    { "id": "${IDS[3]}", "public": "${PUB[3]}", "private": "${PRIV[3]}", "node_id": "ec2-m2" } ] }
JSON
cat cluster.json
echo ""
echo "cluster up. Coordinator: $COORD_PUB:50050  — REMEMBER: ./teardown.sh within 2h (COST.md)"
