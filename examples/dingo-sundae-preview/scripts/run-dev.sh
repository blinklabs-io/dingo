#!/usr/bin/env bash
set -euo pipefail

export DINGO_UTXORPC_URL="${DINGO_UTXORPC_URL:-http://127.0.0.1:9090}"
npm run dev -- --host 0.0.0.0
