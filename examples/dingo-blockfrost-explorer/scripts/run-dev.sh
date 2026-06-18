#!/usr/bin/env sh
set -eu

export DINGO_BLOCKFROST_URL="${DINGO_BLOCKFROST_URL:-http://127.0.0.1:3000}"
export DINGO_METRICS_URL="${DINGO_METRICS_URL:-http://127.0.0.1:12798}"

exec npm run dev -- --host 0.0.0.0
