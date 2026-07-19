#!/usr/bin/env bash
# Dev CA + per-role mTLS certificates for the cluster.
#
#   ./scripts/gen-certs.sh [out-dir] [extra-ip ...]
#
# Produces in out-dir (default ./certs — gitignored, like every other credential):
#   ca.crt                        the CA every peer trusts
#   coordinator.{crt,key}         coordinator server+client identity
#   member.{crt,key}              shard-node server+client identity
#   operator.{crt,key}            operator/client-tool identity
#
# Every cert carries SAN DNS:localhost + IP:127.0.0.1 plus any extra IPs given
# (pass instance IPs when generating for real machines). Keys are PKCS#8 (rustls).
set -euo pipefail

OUT="${1:-certs}"
[ $# -gt 0 ] && shift
SAN="DNS:localhost,IP:127.0.0.1"
for ip in "$@"; do SAN="$SAN,IP:$ip"; done

mkdir -p "$OUT"

if [ ! -f "$OUT/ca.crt" ]; then
  openssl ecparam -genkey -name prime256v1 -out "$OUT/ca.key"
  openssl req -x509 -new -key "$OUT/ca.key" -sha256 -days 730 \
    -subj "/CN=aether-dev-ca" -out "$OUT/ca.crt"
  echo "generated CA: $OUT/ca.crt"
fi

for role in coordinator member operator; do
  openssl ecparam -genkey -name prime256v1 -out "$OUT/$role.ec"
  openssl pkcs8 -topk8 -nocrypt -in "$OUT/$role.ec" -out "$OUT/$role.key"
  rm "$OUT/$role.ec"
  openssl req -new -key "$OUT/$role.key" -subj "/CN=aether-$role" -out "$OUT/$role.csr"
  # Both usages on every role: members and coordinators are servers AND clients of
  # each other (heartbeats, fan-out, raft), and mTLS makes every client a presenter.
  openssl x509 -req -in "$OUT/$role.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" \
    -CAcreateserial -days 365 -sha256 -out "$OUT/$role.crt" \
    -extfile <(printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n" "$SAN")
  rm "$OUT/$role.csr"
  openssl verify -CAfile "$OUT/ca.crt" "$OUT/$role.crt" >/dev/null
  echo "generated $role identity: $OUT/$role.crt"
done

echo "SANs: $SAN"
