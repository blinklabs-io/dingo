#!/usr/bin/env bash
set -euo pipefail

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${EXAMPLES_DIR}"

POSTGRES_DB="${POSTGRES_DB:-dingo_metadata}"
POSTGRES_USER="${POSTGRES_USER:-dingo}"
GOV_LENS_PORT="${GOV_LENS_PORT:-${APP_PORT:-8088}}"

# dingo waits for the one-shot dingo-sync job to complete successfully before
# starting `dingo serve`, so a single `up` brings the whole stack up in order.
docker compose up -d postgres dingo-sync dingo gov-lens

for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${GOV_LENS_PORT}/api/status" >/tmp/dingo-gov-lens-status.json; then
    break
  fi
  sleep 2
done

curl -fsS "http://127.0.0.1:${GOV_LENS_PORT}/api/status" | jq .

docker compose exec -T postgres psql \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  --tuples-only \
  --command "SELECT network, storage_mode FROM node_settings WHERE id = 1;" |
  awk '{ gsub(/^ +| +$/, ""); print }'

docker compose exec -T postgres psql \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  --command "SELECT (SELECT COUNT(*) FROM drep) AS dreps, (SELECT COUNT(*) FROM governance_proposal) AS proposals;"
