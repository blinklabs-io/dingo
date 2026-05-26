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

# Orchestration for the intersect_origin_one_rollforward capture
# scenario.
#
# Lifecycle:
#   1. Build images (configurator + capture-sidecar).
#   2. `docker compose up -d` brings configurator + cardano-node up
#      detached. The configurator exits 0 after seeding; cardano-node
#      then forges. The sidecar is profile-gated and does NOT start
#      here — that prevents the configurator's clean exit from
#      tripping --abort-on-container-exit.
#   3. Poll cardano-node's healthcheck until it reports healthy.
#   4. `docker compose run --rm capture-sidecar` runs the capture to
#      completion. The sidecar bind-mounts the host output dir, so
#      the vector lands directly on disk — no docker cp round-trip.
#   5. Tear down.
#
# Invoked by ../../capture-scenario.sh intersect_origin_one_rollforward.
#
# Usage (via dispatcher):
#   capture-scenario.sh intersect_origin_one_rollforward [-out <path>] [--keep-up]
#
# Direct invocation is supported but `capture-scenario.sh` is the
# documented entry point.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"

OUT_PATH=""
KEEP_UP=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        -out|--out)
            if [[ $# -lt 2 || "${2:-}" == -* ]]; then
                echo "$1 requires a path" >&2
                exit 2
            fi
            OUT_PATH="$2"
            shift 2
            ;;
        --keep-up)
            KEEP_UP=true; shift;;
        --skip-golden)
            # No-op here — this scenario has no committed golden to
            # diff against. Accepted so the bulk capture-all.sh
            # wrapper can pass --skip-golden uniformly to every
            # scenario's run.sh without per-scenario branching.
            shift;;
        -h|--help)
            cat <<USAGE
usage: $0 [-out <path>] [--keep-up] [--skip-golden]

  -out <path>     Destination path for the captured vector.
                  Defaults to ${SCRIPT_DIR}/output/vector.json.
  --keep-up       Leave the docker-compose stack running on success.
  --skip-golden   No-op; accepted for bulk-wrapper compatibility.
USAGE
            exit 0;;
        *)
            echo "unknown argument: $1" >&2
            exit 2;;
    esac
done

if [[ -z "${OUT_PATH}" ]]; then
    OUT_PATH="${SCRIPT_DIR}/output/vector.json"
fi
OUT_DIR="$(cd "$(dirname "${OUT_PATH}")" 2>/dev/null && pwd || true)"
if [[ -z "${OUT_DIR}" ]]; then
    mkdir -p "$(dirname "${OUT_PATH}")"
    OUT_DIR="$(cd "$(dirname "${OUT_PATH}")" && pwd)"
fi
OUT_BASENAME="$(basename "${OUT_PATH}")"

# The sidecar always writes vector.json into its bind-mounted output
# dir. If the caller asked for a different basename, rename afterwards.
export CONSENSUS_CAPTURE_OUTPUT_DIR="${OUT_DIR}"

log()  { echo "[consensus-capture] $*"; }
warn() { echo "[consensus-capture] WARNING: $*" >&2; }
die()  { echo "[consensus-capture] ERROR: $*" >&2; exit 1; }

cleanup() {
    local exit_code=$?
    if [[ "${KEEP_UP}" == "true" && ${exit_code} -eq 0 ]]; then
        log "Capture succeeded. Stack left running (--keep-up)."
        log "To stop:  docker compose -f ${COMPOSE_FILE} down -v"
        return
    fi
    if [[ ${exit_code} -ne 0 ]]; then
        log "Capture failed; collecting service logs."
        docker compose -f "${COMPOSE_FILE}" logs --tail=200 cardano-node 2>/dev/null || true
        docker compose -f "${COMPOSE_FILE}" logs --tail=200 configurator 2>/dev/null || true
    fi
    log "Tearing down stack..."
    docker compose -f "${COMPOSE_FILE}" --profile capture down -v 2>/dev/null || true
}
trap cleanup EXIT

command -v docker &>/dev/null || die "docker not installed"
docker compose version &>/dev/null || die "docker compose plugin not installed"

log "Building images..."
docker compose -f "${COMPOSE_FILE}" --profile capture build configurator capture-sidecar

log "Bringing configurator + cardano-node up (detached)..."
docker compose -f "${COMPOSE_FILE}" up -d configurator cardano-node

log "Waiting for cardano-node to become healthy..."
MAX_WAIT=180
ELAPSED=0
while [[ ${ELAPSED} -lt ${MAX_WAIT} ]]; do
    status="$(docker inspect --format='{{.State.Health.Status}}' \
        consensus-capture-cardano-node 2>/dev/null || echo missing)"
    case "${status}" in
        healthy)
            log "cardano-node healthy after ${ELAPSED}s"
            break ;;
        unhealthy)
            die "cardano-node reported unhealthy" ;;
    esac
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    if (( ELAPSED % 20 == 0 )); then
        log "  still waiting (${ELAPSED}s, status=${status})..."
    fi
done
if [[ ${ELAPSED} -ge ${MAX_WAIT} ]]; then
    die "cardano-node did not become healthy within ${MAX_WAIT}s"
fi

# cardano-node's healthcheck just checks for the local socket file;
# it's ready for local clients well before NtN serving stabilizes
# (handshake handlers come up a few seconds later, and there are no
# blocks to serve until system-start + first leader slot fires).
# Give it a brief grace period so the sidecar's handshake doesn't race
# the listener.
log "Brief settle delay so NtN listener is ready..."
sleep 5

log "Running capture-sidecar (output dir: ${OUT_DIR})..."
# Run as the host user so the bind-mounted vector.json lands with the
# caller's ownership, not root's.
docker compose -f "${COMPOSE_FILE}" --profile capture run --rm \
    --user "$(id -u):$(id -g)" \
    capture-sidecar

if [[ "${OUT_BASENAME}" != "vector.json" ]]; then
    mv "${OUT_DIR}/vector.json" "${OUT_PATH}"
fi
log "Vector written to ${OUT_PATH}"
