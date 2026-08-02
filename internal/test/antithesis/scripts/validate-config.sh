#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/../testnets/dingo-praos/docker-compose.yaml"
rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

docker compose -f "$COMPOSE_FILE" config >"$rendered"

grep -Fq 'CARDANO_PRIVATE_BIND_ADDR: 0.0.0.0' "$rendered"
grep -Fq 'TXPUMP_NODE_ADDR: p1.example:3002' "$rendered"
grep -Fq 'TXPUMP_STARTUP_TIMEOUT: "60"' "$rendered"
grep -Fq 'TXPUMP_TYPES: payment,delegation,governance,plutus' "$rendered"
for pool in 1 2 3 4 5; do
  grep -Fq "/logs/p${pool}.log" "$rendered"
done

echo "Antithesis compose invariants are valid"
