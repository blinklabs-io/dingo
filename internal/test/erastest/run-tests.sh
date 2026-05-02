#!/usr/bin/env bash

# Copyright 2026 Blink Labs Software
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run era-transition end-to-end tests against a private DevNet that
# traverses Shelley → Allegra → Mary → Alonzo → Babbage → Conway via the
# Test{Era}HardForkAtEpoch configuration overrides.
#
# Usage:
#   ./run-tests.sh                        # Run all era-transition tests
#   ./run-tests.sh -run TestEraSchedule   # Run a specific test pattern
#   ./run-tests.sh --keep-up              # Don't tear down on success

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
TESTNET_YAML="${SCRIPT_DIR}/testnet.yaml"

KEEP_UP=false
TEST_ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --keep-up) KEEP_UP=true ;;
    *)         TEST_ARGS+=("${arg}") ;;
  esac
done

log()  { echo "[eras-tests] $*"; }
warn() { echo "[eras-tests] WARNING: $*" >&2; }
die()  { echo "[eras-tests] ERROR: $*" >&2; exit 1; }

cleanup() {
  local exit_code=$?
  if [[ "${KEEP_UP}" == "true" ]] && [[ ${exit_code} -eq 0 ]]; then
    log "Tests passed. DevNet left running (--keep-up)."
    log "To stop:  docker compose -f ${COMPOSE_FILE} down -v"
    return
  fi
  if [[ ${exit_code} -ne 0 ]]; then
    log "Collecting logs before teardown..."
    docker compose -f "${COMPOSE_FILE}" logs --tail=200 2>/dev/null || true
    local logs_dir="/tmp/erastest-logs"
    mkdir -p "${logs_dir}" 2>/dev/null || true
    for svc in eras-dingo-producer eras-cardano-producer eras-cardano-relay eras-configurator; do
      docker logs "${svc}" >"${logs_dir}/${svc}.log" 2>&1 || true
    done
    log "Full per-service logs dumped to ${logs_dir}/"
  fi
  log "Tearing down DevNet..."
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
}
trap cleanup EXIT

if ! command -v docker &>/dev/null; then
  die "docker is not installed"
fi
if ! docker compose version &>/dev/null; then
  die "docker compose plugin is not installed"
fi

log "Building eras-stack images..."
docker compose -f "${COMPOSE_FILE}" build dingo-producer configurator

log "Starting DevNet containers..."
docker compose -f "${COMPOSE_FILE}" up -d

log "Waiting for nodes to become healthy..."
MAX_WAIT=180
ELAPSED=0
while [[ ${ELAPSED} -lt ${MAX_WAIT} ]]; do
  HEALTHY=0
  for svc in eras-dingo-producer eras-cardano-producer eras-cardano-relay; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "${svc}" 2>/dev/null || echo "missing")
    if [[ "${status}" == "healthy" ]]; then
      HEALTHY=$((HEALTHY + 1))
    fi
  done
  if [[ ${HEALTHY} -ge 3 ]]; then
    log "All 3 nodes are healthy"
    break
  fi
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  log "  Waiting... (${ELAPSED}s, ${HEALTHY}/3 healthy)"
done

if [[ ${ELAPSED} -ge ${MAX_WAIT} ]]; then
  warn "Not all nodes became healthy within ${MAX_WAIT}s"
  docker compose -f "${COMPOSE_FILE}" ps
  docker compose -f "${COMPOSE_FILE}" logs --tail=80
  die "Node health check timeout"
fi

log "Running era-transition tests..."
cd "${PROJECT_ROOT}"

DINGO_PORT="${ERASTEST_DINGO_PORT:-3020}"
CARDANO_PORT="${ERASTEST_CARDANO_PORT:-3021}"
RELAY_PORT="${ERASTEST_RELAY_PORT:-3022}"
export ERASTEST_DINGO_ADDR="localhost:${DINGO_PORT}"
export ERASTEST_CARDANO_ADDR="localhost:${CARDANO_PORT}"
export ERASTEST_RELAY_ADDR="localhost:${RELAY_PORT}"
export ERASTEST_TESTNET_YAML="${TESTNET_YAML}"

# Default timeout: ~6 epochs * 75s ≈ 7.5 min for a healthy traversal
# plus margin for bootstrap and the test cascade. Set TEST_TIMEOUT in
# the environment to override (e.g. for slow CI runners or extended
# debugging).
TEST_TIMEOUT="${TEST_TIMEOUT:-8m}"
set +e
go test \
  -tags erastest \
  -count=1 \
  -v \
  -timeout "${TEST_TIMEOUT}" \
  ${TEST_ARGS[@]+"${TEST_ARGS[@]}"} \
  ./internal/erastest/...
TEST_EXIT=$?
set -e

if [[ ${TEST_EXIT} -eq 0 ]]; then
  log "All era-transition tests PASSED"
else
  log "Era-transition tests FAILED (exit code: ${TEST_EXIT})"
fi

exit ${TEST_EXIT}
