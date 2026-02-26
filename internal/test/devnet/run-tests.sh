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

# Run DevNet end-to-end tests.
#
# This script:
#   1. Starts the DevNet (configurator + all nodes)
#   2. Waits for all nodes to become healthy
#   3. Runs the Go integration tests tagged with //go:build devnet
#   4. Tears down the DevNet and reports results
#
# Usage:
#   ./run-tests.sh                    # Run all devnet tests
#   ./run-tests.sh -run TestBasic     # Run specific test pattern
#   ./run-tests.sh --keep-up          # Don't tear down on success (for debugging)
#   ./run-tests.sh --keep-volumes     # Don't delete volumes on teardown

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

# Parse arguments
KEEP_UP=false
KEEP_VOLUMES=false
TEST_ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --keep-up)      KEEP_UP=true ;;
    --keep-volumes) KEEP_VOLUMES=true ;;
    *)              TEST_ARGS+=("${arg}") ;;
  esac
done

# Build the volume flag for docker compose down
DOWN_ARGS=()
if [[ "${KEEP_VOLUMES}" != "true" ]]; then
  DOWN_ARGS+=("-v")
fi

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #

log()  { echo "[run-tests] $*"; }
warn() { echo "[run-tests] WARNING: $*" >&2; }
die()  { echo "[run-tests] ERROR: $*" >&2; exit 1; }

# --------------------------------------------------------------------------- #
# Cleanup on exit
# --------------------------------------------------------------------------- #

cleanup() {
  local exit_code=$?
  if [[ "${KEEP_UP}" == "true" ]] && [[ ${exit_code} -eq 0 ]]; then
    log "Tests passed. DevNet left running (--keep-up)."
    log "To stop:  docker compose -f ${COMPOSE_FILE} down ${DOWN_ARGS[*]+${DOWN_ARGS[*]}}"
    return
  fi
  if [[ ${exit_code} -ne 0 ]]; then
    log "Collecting logs before teardown..."
    docker compose -f "${COMPOSE_FILE}" logs --tail=500 2>/dev/null || true
  fi
  log "Tearing down DevNet..."
  docker compose -f "${COMPOSE_FILE}" down ${DOWN_ARGS[@]+"${DOWN_ARGS[@]}"} 2>/dev/null || true
}
trap cleanup EXIT

# --------------------------------------------------------------------------- #
# Pre-flight checks
# --------------------------------------------------------------------------- #

if ! command -v docker &>/dev/null; then
  die "docker is not installed"
fi

if ! docker compose version &>/dev/null; then
  die "docker compose plugin is not installed"
fi

# --------------------------------------------------------------------------- #
# Start DevNet
# --------------------------------------------------------------------------- #

# Always remove volumes on initial cleanup to ensure a clean slate.
# KEEP_VOLUMES only affects the final teardown (for debugging).
log "Cleaning up any previous DevNet state..."
docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true

log "Building Dingo Docker image..."
docker compose -f "${COMPOSE_FILE}" build dingo-producer

log "Starting DevNet containers..."
docker compose -f "${COMPOSE_FILE}" up -d

# --------------------------------------------------------------------------- #
# Wait for all nodes to become healthy
# --------------------------------------------------------------------------- #

log "Waiting for nodes to become healthy..."

MAX_WAIT=120
ELAPSED=0
while [[ ${ELAPSED} -lt ${MAX_WAIT} ]]; do
  HEALTHY=0
  for svc in dingo-producer cardano-producer cardano-relay; do
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
  log "Dumping container status:"
  docker compose -f "${COMPOSE_FILE}" ps
  log "Dumping recent logs:"
  docker compose -f "${COMPOSE_FILE}" logs --tail=50
  die "Node health check timeout"
fi

# --------------------------------------------------------------------------- #
# Run tests
# --------------------------------------------------------------------------- #

log "Running DevNet integration tests..."

cd "${PROJECT_ROOT}"

# Run tests with the devnet build tag
# The -count=1 flag disables test caching
set +e
go test \
  -tags devnet \
  -count=1 \
  -v \
  -timeout 10m \
  ${TEST_ARGS[@]+"${TEST_ARGS[@]}"} \
  ./internal/devnet/...
TEST_EXIT=$?
set -e

if [[ ${TEST_EXIT} -eq 0 ]]; then
  log "All DevNet tests PASSED"
else
  log "DevNet tests FAILED (exit code: ${TEST_EXIT})"
fi

exit ${TEST_EXIT}
