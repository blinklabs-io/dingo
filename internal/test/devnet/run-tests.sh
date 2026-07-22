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
#   ./run-tests.sh                    # Run all devnet tests (default: all-dingo network)
#   ./run-tests.sh --conformance      # Run against the dingo + cardano-node reference network
#   ./run-tests.sh -run TestBasic     # Run specific test pattern
#   ./run-tests.sh --keep-up          # Don't tear down on success (for debugging)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

# Parse arguments
KEEP_UP=false
# Mode selection: default dingo (all-dingo network), --conformance for the
# dingo + cardano-node reference network.
MODE="${MODE:-dingo}"
TEST_ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --keep-up)     KEEP_UP=true ;;
    --conformance) MODE="conformance" ;;
    *)             TEST_ARGS+=("${arg}") ;;
  esac
done

# Derive mode-specific variables. COMPOSE_PROFILES is exported unconditionally
# (not defaulted) so the --conformance flag always wins over any pre-existing
# environment or .env value.
if [[ "${MODE}" == "conformance" ]]; then
  export COMPOSE_PROFILES="conformance"
  GO_TAGS="devnet devnet_conformance"
  HEALTH_SERVICES=(dingo-producer cardano-producer cardano-relay)
  TXPUMP_SERVICE="txpump"
else
  export COMPOSE_PROFILES="dingo"
  GO_TAGS="devnet"
  HEALTH_SERVICES=(dingo-1 dingo-2 dingo-3 dingo-relay)
  TXPUMP_SERVICE="txpump-dingo"
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
    log "To stop:  docker compose -f ${COMPOSE_FILE} down -v"
    return
  fi
  if [[ ${exit_code} -ne 0 ]]; then
    log "Collecting logs before teardown..."
    docker compose -f "${COMPOSE_FILE}" logs --tail=100 2>/dev/null || true
  fi
  log "Tearing down DevNet..."
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
  if [[ -n "${STAKE_KEYS_HOST_DIR:-}" ]]; then
    rm -rf "${STAKE_KEYS_HOST_DIR}"
  fi
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

log "Building DevNet Docker images..."
# No service names: compose only builds services in the active
# COMPOSE_PROFILES, so this is scoped correctly for either mode.
docker compose -f "${COMPOSE_FILE}" build

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
  for svc in "${HEALTH_SERVICES[@]}"; do
    status=$(docker inspect --format='{{.State.Health.Status}}' "${svc}" 2>/dev/null || echo "missing")
    if [[ "${status}" == "healthy" ]]; then
      HEALTHY=$((HEALTHY + 1))
    fi
  done
  if [[ ${HEALTHY} -ge ${#HEALTH_SERVICES[@]} ]]; then
    log "All ${#HEALTH_SERVICES[@]} nodes are healthy"
    break
  fi
  sleep 2
  ELAPSED=$((ELAPSED + 2))
  log "  Waiting... (${ELAPSED}s, ${HEALTHY}/${#HEALTH_SERVICES[@]} healthy)"
done

if [[ ${ELAPSED} -ge ${MAX_WAIT} ]]; then
  warn "Not all nodes became healthy within ${MAX_WAIT}s"
  log "Dumping container status:"
  docker compose -f "${COMPOSE_FILE}" ps
  log "Dumping recent logs:"
  docker compose -f "${COMPOSE_FILE}" logs --tail=100
  die "Node health check timeout"
fi

# --------------------------------------------------------------------------- #
# Verify txpump is running
# --------------------------------------------------------------------------- #

log "Checking txpump is running..."
TXPUMP_RUNNING=$(docker compose -f "${COMPOSE_FILE}" ps --status running --quiet "${TXPUMP_SERVICE}" 2>/dev/null || true)
if [[ -z "${TXPUMP_RUNNING}" ]]; then
  log "txpump container status:"
  docker compose -f "${COMPOSE_FILE}" ps "${TXPUMP_SERVICE}"
  log "txpump logs:"
  docker compose -f "${COMPOSE_FILE}" logs "${TXPUMP_SERVICE}"
  die "txpump is not running — mempool traffic will be absent; aborting"
fi
log "txpump is running"

# --------------------------------------------------------------------------- #
# Copy genesis stake keys to the host (dingo mode only)
# --------------------------------------------------------------------------- #

if [[ "${MODE}" == "dingo" ]]; then
  log "Copying genesis stake keys from the utxo-keys volume..."
  UTXO_KEYS_VOLUME="devnet_utxo-keys"
  if ! docker volume inspect "${UTXO_KEYS_VOLUME}" &>/dev/null; then
    warn "Docker volume ${UTXO_KEYS_VOLUME} not found; discovering by compose label"
    UTXO_KEYS_VOLUME=$(docker volume ls --filter label=com.docker.compose.volume=utxo-keys --format '{{.Name}}' | head -n1)
  fi
  STAKE_KEYS_HOST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dingo-devnet-stake-keys.XXXXXX")"
  if [[ -z "${UTXO_KEYS_VOLUME}" ]]; then
    warn "Unable to locate the utxo-keys Docker volume; skipping stake-keys copy"
  else
    # Never let a copy failure abort the run. Missing stake keys are handled
    # below by disabling the opt-in CIP-50 scenario for this invocation.
    docker run --rm \
      -v "${UTXO_KEYS_VOLUME}:/k:ro" \
      -v "${STAKE_KEYS_HOST_DIR}:/out" \
      alpine sh -c 'cp -r /k/stake /out/stake' 2>/dev/null || true
  fi
  if [[ -d "${STAKE_KEYS_HOST_DIR}/stake" ]]; then
    export DEVNET_STAKE_KEYS_DIR="${STAKE_KEYS_HOST_DIR}/stake"
    log "DEVNET_STAKE_KEYS_DIR=${DEVNET_STAKE_KEYS_DIR}"
  else
    unset DEVNET_STAKE_KEYS_DIR
    if [[ "${DEVNET_CIP50_TEST:-}" == "1" ]]; then
      warn "Genesis stake keys were not copied; skipping the CIP-50 scenario"
      unset DEVNET_CIP50_TEST
    fi
  fi
fi

# --------------------------------------------------------------------------- #
# Run tests
# --------------------------------------------------------------------------- #

log "Running DevNet integration tests..."

cd "${PROJECT_ROOT}"

# Propagate host port overrides to the Go test harness so the test
# endpoints match the docker-compose port mappings.  Honour pre-set
# port env vars, then fall back to DEVNET_* variants, then hardcoded
# defaults, per mode.
if [[ "${MODE}" == "conformance" ]]; then
  DINGO_PORT="${DINGO_PORT:-${DEVNET_DINGO_PORT:-3010}}"
  CARDANO_PORT="${CARDANO_PORT:-${DEVNET_CARDANO_PORT:-3011}}"
  RELAY_PORT="${RELAY_PORT:-${DEVNET_RELAY_PORT:-3012}}"
  export DEVNET_DINGO_ADDR="localhost:${DINGO_PORT}"
  export DEVNET_CARDANO_ADDR="localhost:${CARDANO_PORT}"
  export DEVNET_RELAY_ADDR="localhost:${RELAY_PORT}"
else
  export DEVNET_DINGO1_ADDR="localhost:${DEVNET_DINGO1_PORT:-3010}"
  export DEVNET_DINGO2_ADDR="localhost:${DEVNET_DINGO2_PORT:-3013}"
  export DEVNET_DINGO3_ADDR="localhost:${DEVNET_DINGO3_PORT:-3014}"
  export DEVNET_DINGO_RELAY_ADDR="localhost:${DEVNET_DINGO_RELAY_PORT:-3015}"
  export DEVNET_DINGO1_NTC_ADDR="${DEVNET_DINGO1_NTC_ADDR:-localhost:${DEVNET_DINGO1_NTC_PORT:-3020}}"
  export DEVNET_DINGO2_NTC_ADDR="${DEVNET_DINGO2_NTC_ADDR:-localhost:${DEVNET_DINGO2_NTC_PORT:-3021}}"
  export DEVNET_DINGO3_NTC_ADDR="${DEVNET_DINGO3_NTC_ADDR:-localhost:${DEVNET_DINGO3_NTC_PORT:-3022}}"
  export DEVNET_DINGO_RELAY_NTC_ADDR="${DEVNET_DINGO_RELAY_NTC_ADDR:-localhost:${DEVNET_DINGO_RELAY_NTC_PORT:-3023}}"
fi

# Run tests with the mode's build tags.
# The -count=1 flag disables test caching
TEST_TIMEOUT="${TEST_TIMEOUT:-20m}"
set +e
go test \
  -tags "${GO_TAGS}" \
  -count=1 \
  -v \
  -timeout "${TEST_TIMEOUT}" \
  ${TEST_ARGS[@]+"${TEST_ARGS[@]}"} \
  ./internal/test/devnet/...
TEST_EXIT=$?
set -e

if [[ ${TEST_EXIT} -eq 0 ]]; then
  log "All DevNet tests PASSED"
else
  log "DevNet tests FAILED (exit code: ${TEST_EXIT})"
fi

log "Checking txpump accepted submissions..."
set +e
TXPUMP_COUNTS=$(
  docker compose -f "${COMPOSE_FILE}" exec -T "${TXPUMP_SERVICE}" sh -c '
    if [ ! -f /logs/txpump.log ]; then
      echo "0 0 0"
      exit 0
    fi
    submitted=$(grep -c "\"status\":\"submitted\"" /logs/txpump.log 2>/dev/null || true)
    rejected=$(grep -c "\"status\":\"rejected\"" /logs/txpump.log 2>/dev/null || true)
    errors=$(grep -c "\"status\":\"error\"" /logs/txpump.log 2>/dev/null || true)
    printf "%s %s %s\n" "${submitted}" "${rejected}" "${errors}"
  ' 2>/dev/null
)
COUNTS_EXIT=$?
set -e

if [[ ${COUNTS_EXIT} -ne 0 || -z "${TXPUMP_COUNTS}" ]]; then
  warn "Unable to inspect txpump transaction log"
  TEST_EXIT=1
else
  read -r TXPUMP_SUBMITTED TXPUMP_REJECTED TXPUMP_ERRORS <<<"${TXPUMP_COUNTS}"
  log "txpump submitted=${TXPUMP_SUBMITTED} rejected=${TXPUMP_REJECTED} error=${TXPUMP_ERRORS}"
  if [[ "${TXPUMP_SUBMITTED}" -eq 0 ]]; then
    warn "txpump produced zero accepted submissions"
    TEST_EXIT=1
  fi
fi

exit ${TEST_EXIT}
