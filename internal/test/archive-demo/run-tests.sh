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

# Bring up the archive-demo stack, run integration tests, tear down.
#
# Usage:
#   ./run-tests.sh                     # run all archive-demo tests
#   ./run-tests.sh -run TestArchive    # filter
#   ./run-tests.sh --keep-up           # leave stack running on success

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PRUNING_DATA_DIR="${SCRIPT_DIR}/tmp/dingo-pruning-data"

KEEP_UP=false
TEST_ARGS=()
for arg in "$@"; do
  case "${arg}" in
    --keep-up) KEEP_UP=true ;;
    *)         TEST_ARGS+=("${arg}") ;;
  esac
done

log()  { echo "[archive-demo run-tests] $*"; }
warn() { echo "[archive-demo run-tests] WARNING: $*" >&2; }
die()  { echo "[archive-demo run-tests] ERROR: $*" >&2; exit 1; }

cleanup() {
  local exit_code=$?
  if [[ -n "${INSPECT_DIR:-}" ]] && [[ -d "${INSPECT_DIR}" ]]; then
    rm -rf "${INSPECT_DIR}"
  fi
  if [[ "${KEEP_UP}" == "true" ]] && [[ ${exit_code} -eq 0 ]]; then
    log "Tests passed. Stack left running (--keep-up)."
    log "To stop:  docker compose -f ${COMPOSE_FILE} down -v"
    return
  fi
  if [[ ${exit_code} -ne 0 ]]; then
    log "Collecting logs before teardown..."
    docker compose -f "${COMPOSE_FILE}" logs --tail=200 2>/dev/null || true
  fi
  log "Tearing down..."
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
  rm -rf "${SCRIPT_DIR}/tmp"
}
trap cleanup EXIT

command -v docker >/dev/null || die "docker is not installed"
command -v go     >/dev/null || die "go is not installed"

log "Building inspect-blob helper..."
INSPECT_DIR="$(mktemp -d)"
INSPECT_BIN="${INSPECT_DIR}/inspect-blob"
( cd "${PROJECT_ROOT}" && go build -o "${INSPECT_BIN}" ./internal/test/archive-demo/cmd/inspect-blob )

log "Bringing up archive-demo stack..."
mkdir -p "${PRUNING_DATA_DIR}"
chmod 777 "${PRUNING_DATA_DIR}"
docker compose -f "${COMPOSE_FILE}" up -d --build

log "Waiting for dingo-pruning to become healthy..."
deadline=$(( $(date +%s) + 240 ))
while true; do
  status="$(docker inspect -f '{{.State.Health.Status}}' archivedemo-dingo-pruning 2>/dev/null || echo missing)"
  if [[ "${status}" == "healthy" ]]; then
    break
  fi
  if [[ $(date +%s) -ge ${deadline} ]]; then
    die "dingo-pruning did not become healthy (last status: ${status})"
  fi
  sleep 3
done
log "All services healthy."

log "Running tests..."
cd "${PROJECT_ROOT}"
ARCHIVEDEMO_INSPECT_BIN="${INSPECT_BIN}" \
ARCHIVEDEMO_PRUNING_DATA_DIR="${PRUNING_DATA_DIR}" \
go test -tags archive_demo -count=1 -v -timeout 15m \
  ./internal/test/archive-demo/internal/archivedemo/scenarios/ "${TEST_ARGS[@]}"
