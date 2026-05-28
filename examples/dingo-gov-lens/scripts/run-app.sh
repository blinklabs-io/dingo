#!/usr/bin/env bash
set -euo pipefail

export ADDR="${ADDR:-127.0.0.1:8088}"
export GOVTOOL_BASE_URL="${GOVTOOL_BASE_URL:-https://preview.gov.tools}"

cd "$(dirname "$0")/.."

exec go run .
