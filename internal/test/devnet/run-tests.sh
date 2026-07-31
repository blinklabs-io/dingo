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

# Collect logs, generated genesis/configuration, and container status into
# DEVNET_ARTIFACT_DIR before teardown removes the volumes. No-op when the
# variable is unset (interactive runs); CI sets it and uploads the directory.
# Every step is a warning at worst: artifact collection must never change the
# outcome of the run.
collect_artifacts() {
  local dir="${DEVNET_ARTIFACT_DIR:-}"
  [[ -n "${dir}" ]] || return 0
  if ! mkdir -p "${dir}"; then
    warn "could not create artifact directory ${dir}; skipping collection"
    return 0
  fi
  log "Collecting DevNet artifacts into ${dir}"
  docker compose -f "${COMPOSE_FILE}" ps -a >"${dir}/container-status.txt" 2>&1 ||
    warn "could not capture container status"
  docker compose -f "${COMPOSE_FILE}" logs --no-color --timestamps \
    >"${dir}/compose-logs.txt" 2>&1 || warn "could not capture compose logs"
  local svc
  for svc in "${HEALTH_SERVICES[@]}" "${TXPUMP_SERVICE}"; do
    docker compose -f "${COMPOSE_FILE}" logs --no-color --timestamps "${svc}" \
      >"${dir}/${svc}.log" 2>&1 || warn "could not capture logs for ${svc}"
  done
  # Generated genesis and node configuration live on the configurator's config
  # volume, mounted into the first node of the active profile.
  docker compose -f "${COMPOSE_FILE}" cp "${HEALTH_SERVICES[0]}:/configs" \
    "${dir}/generated-configs" >/dev/null 2>&1 ||
    warn "could not copy generated genesis/configuration"
  docker compose -f "${COMPOSE_FILE}" cp "${TXPUMP_SERVICE}:/logs/txpump.log" \
    "${dir}/txpump.log" >/dev/null 2>&1 || warn "could not copy the txpump log"
  return 0
}

# Remove the host copy of the genesis stake keys.
#
# The copy step below runs as root inside a container, so ${dir}/stake and its
# contents were owned by uid 0 even though ${dir} itself is host-owned. A plain
# host "rm -rf" then fails with "Permission denied" on every file under stake/,
# and because this script runs under "set -e", that unguarded failure inside the
# EXIT trap aborted the trap and made the shell exit 1: a run where all scenarios
# passed and txpump was healthy still reported failure, and the temp directories
# leaked. The copy step now chowns the tree to the invoking user, this helper
# falls back to removing it from inside a container (as the owning uid) for trees
# copied before that change, and any residual failure is a warning only. See
# TestDevNetRunTestsExitStatusContract in internal/test/ci/.
remove_stake_keys_dir() {
  local dir="${1:-}"
  [[ -n "${dir}" && -d "${dir}" ]] || return 0
  rm -rf "${dir}" 2>/dev/null || true
  if [[ -d "${dir}" ]]; then
    docker run --rm -v "${dir}:/out" alpine sh -c 'rm -rf /out/stake' \
      >/dev/null 2>&1 || true
    rm -rf "${dir}" 2>/dev/null || true
  fi
  if [[ -d "${dir}" ]]; then
    warn "could not remove ${dir}; remove it manually"
  fi
  return 0
}

cleanup() {
  local exit_code=$?
  if [[ "${KEEP_UP}" == "true" ]] && [[ ${exit_code} -eq 0 ]]; then
    log "Tests passed. DevNet left running (--keep-up)."
    log "To stop:  docker compose -f ${COMPOSE_FILE} down -v"
    exit "${exit_code}"
  fi
  if [[ ${exit_code} -ne 0 ]]; then
    log "Collecting logs before teardown..."
    docker compose -f "${COMPOSE_FILE}" logs --tail=100 2>/dev/null || true
  fi
  collect_artifacts
  log "Tearing down DevNet..."
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
  remove_stake_keys_dir "${STAKE_KEYS_HOST_DIR:-}"
  # Exit with the status that triggered the trap, so teardown noise cannot decide
  # whether the run passed. Every command above must stay guarded: under "set -e"
  # an unguarded failure here aborts the trap before this line and the shell exits
  # 1 regardless of the test result.
  exit "${exit_code}"
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
    #
    # The copy runs as root inside the container (the volume's files are owned by
    # the node image's uid 100), so chown the result to the invoking user. Without
    # it the copied tree is root-owned and the host cannot remove it — teardown
    # then failed with "Permission denied" and, under "set -e", aborted the EXIT
    # trap so a fully passing run exited 1.
    docker run --rm \
      -v "${UTXO_KEYS_VOLUME}:/k:ro" \
      -v "${STAKE_KEYS_HOST_DIR}:/out" \
      -e "HOST_UID=$(id -u)" \
      -e "HOST_GID=$(id -g)" \
      alpine sh -c 'cp -r /k/stake /out/stake && chown -R "${HOST_UID}:${HOST_GID}" /out/stake' \
      2>/dev/null || true
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
