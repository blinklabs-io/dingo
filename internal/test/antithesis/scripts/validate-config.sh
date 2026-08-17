#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
validate_compose() {
  local compose_file="$1"
  local rendered
  rendered="$(mktemp)"
  trap 'rm -f "$rendered"' RETURN
  docker compose -f "$compose_file" config >"$rendered"

  grep -Eq 'CARDANO_PRIVATE_BIND_ADDR:[[:space:]]*"?0\.0\.0\.0"?' "$rendered"
  grep -Eq 'TXPUMP_STARTUP_TIMEOUT:[[:space:]]*"?60"?' "$rendered"
  grep -Eq 'TXPUMP_TYPES:[[:space:]]*"?payment,delegation,governance,plutus"?' "$rendered"
  if [[ "$compose_file" == *dingo-praos* ]]; then
    grep -Eq 'TXPUMP_NODE_ADDR:[[:space:]]*"?p1\.example:3002"?' "$rendered"
  else
    grep -Eq 'TXPUMP_NODE_ADDR:[[:space:]]*"?/ipc/dingo\.socket"?' "$rendered"
  fi
  if [[ "$compose_file" == *dingo-praos* ]]; then
    for pool in 1 2 3 4 5; do
      grep -Fq "/logs/p${pool}.log" "$rendered"
    done
  fi
}

validate_compose "${SCRIPT_DIR}/../docker-compose.yaml"
validate_compose "${SCRIPT_DIR}/../testnets/dingo-praos/docker-compose.yaml"

echo "Antithesis compose invariants are valid"
