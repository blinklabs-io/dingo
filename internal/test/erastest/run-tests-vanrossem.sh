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

# Run the vanRossem (PV11) variant of the erastest DevNet. Brings up the
# same dingo-producer + cardano-producer + cardano-relay topology as
# run-tests.sh, but bootstraps the chain directly into Conway at PV10
# and drives PV10 to PV11 rather than starting in Shelley and traversing every era.
#
# Usage:
#   ./run-tests-vanrossem.sh                     # run the vanRossem suite
#   ./run-tests-vanrossem.sh -run TestVanRossem  # forward -run to go test
#   ./run-tests-vanrossem.sh --keep-up           # leave the DevNet running

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_BASE="${SCRIPT_DIR}/docker-compose.yml"
COMPOSE_OVERRIDE="${SCRIPT_DIR}/docker-compose.vanrossem.yml"
TESTNET_YAML="${SCRIPT_DIR}/testnet-vanrossem.yaml"

KEEP_UP=false
TEST_ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --keep-up) KEEP_UP=true ;;
    *)         TEST_ARGS+=("${arg}") ;;
  esac
done

log()  { echo "[vanrossem-tests] $*"; }
warn() { echo "[vanrossem-tests] WARNING: $*" >&2; }
die()  { echo "[vanrossem-tests] ERROR: $*" >&2; exit 1; }

compose() {
  docker compose -f "${COMPOSE_BASE}" -f "${COMPOSE_OVERRIDE}" "$@"
}

cleanup() {
  local exit_code=$?
  if [[ "${KEEP_UP}" == "true" ]] && [[ ${exit_code} -eq 0 ]]; then
    log "Tests passed. DevNet left running (--keep-up)."
    log "To stop:  docker compose -f ${COMPOSE_BASE} -f ${COMPOSE_OVERRIDE} down -v"
    return
  fi
  log "Collecting full per-service logs before teardown..."
  if [[ ${exit_code} -ne 0 ]]; then
    compose logs --tail=200 2>/dev/null || true
  fi
  local logs_dir="/tmp/erastest-vanrossem-logs"
  mkdir -p "${logs_dir}" 2>/dev/null || true
  for svc in eras-dingo-producer eras-cardano-producer eras-cardano-relay eras-configurator; do
    docker logs "${svc}" >"${logs_dir}/${svc}.log" 2>&1 || true
  done
  log "Full per-service logs dumped to ${logs_dir}/"
  log "Tearing down DevNet..."
  compose down -v 2>/dev/null || true
}
trap cleanup EXIT

if ! command -v docker &>/dev/null; then
  die "docker is not installed"
fi
if ! docker compose version &>/dev/null; then
  die "docker compose plugin is not installed"
fi

log "Building images..."
compose build dingo-producer configurator

log "Starting DevNet containers..."
compose up -d

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
  compose ps
  compose logs --tail=80
  die "Node health check timeout"
fi

log "Running vanRossem tests..."
cd "${PROJECT_ROOT}"

DINGO_PORT="${ERASTEST_DINGO_PORT:-3020}"
CARDANO_PORT="${ERASTEST_CARDANO_PORT:-3021}"
RELAY_PORT="${ERASTEST_RELAY_PORT:-3022}"
export ERASTEST_DINGO_ADDR="localhost:${DINGO_PORT}"
export ERASTEST_CARDANO_ADDR="localhost:${CARDANO_PORT}"
export ERASTEST_RELAY_ADDR="localhost:${RELAY_PORT}"
export ERASTEST_TESTNET_YAML="${TESTNET_YAML}"

# The vanRossem variant runs the HFI driver end-to-end:
#   * 2 epoch boundaries for the stake snapshot to pick up DRep delegation
#   * 1 epoch of pre-bump baseline observation
#   * HFI proposal + 3 vote txs (a few seconds each)
#   * 2 epoch boundaries for RATIFY then ENACT
#   * 1 epoch boundary of post-bump validation
# With 75-slot/75s epochs that's ~6 wall-clock epochs ≈ 7.5 min, plus
# bootstrap warmup. Default to 15m so flaky block-rate variance doesn't
# clip the run; override with TEST_TIMEOUT.
TEST_TIMEOUT="${TEST_TIMEOUT:-15m}"
set +e
go test \
  -tags erastest \
  -count=1 \
  -v \
  -timeout "${TEST_TIMEOUT}" \
  -run 'TestVanRossem|TestPV11Readiness' \
  ${TEST_ARGS[@]+"${TEST_ARGS[@]}"} \
  ./internal/test/erastest/...
TEST_EXIT=$?
set -e

if [[ ${TEST_EXIT} -eq 0 ]]; then
  log "All vanRossem tests PASSED"
else
  log "vanRossem tests FAILED (exit code: ${TEST_EXIT})"
fi

exit ${TEST_EXIT}
