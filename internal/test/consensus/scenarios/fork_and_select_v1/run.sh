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

# Orchestration for the fork_and_select_v1 capture scenario.
#
# Lifecycle:
#   1. Build images (configurator + capture-sidecar + compose).
#   2. `docker compose up -d` brings configurator + cardano-peer-a +
#      cardano-peer-b + cardano-observation up detached. The configurator
#      runs three forge phases (shared prefix → peer A → peer B) and
#      exits 0; the three cardano-* services then start, with the
#      observation node connecting to both peers.
#   3. Poll healthchecks until all three cardano-* report healthy.
#   4. Brief settle delay so the observation node has stabilized on
#      its selected chain (Praos longest-chain across the two peers).
#   5. Run the capture-sidecar three times via `compose run --rm`,
#      once each against peer A, peer B, and observation. Outputs land
#      in the bind-mounted host output dir.
#   6. Run the composer once via `compose run --rm` to merge the three
#      single-peer captures into the multi-peer vector, with
#      -golden against the committed corpus for regression.
#   7. Move the composed vector to the user-supplied -out path (or the
#      scenario's default).
#   8. Tear down.
#
# Invoked by ../../capture-scenario.sh fork_and_select_v1.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
GOLDEN_PATH="${REPO_ROOT}/internal/test/consensus/testdata/captured/fork_and_select_v1.json"

OUT_PATH=""
KEEP_UP=false
SKIP_GOLDEN=false
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
        --keep-up)         KEEP_UP=true; shift;;
        --skip-golden)     SKIP_GOLDEN=true; shift;;
        -h|--help)
            cat <<USAGE
usage: $0 [-out <path>] [--keep-up] [--skip-golden]

  -out <path>     Destination path for the composed vector.
                  Defaults to the committed golden path:
                  ${GOLDEN_PATH}
  --keep-up       Leave the docker-compose stack running on success.
  --skip-golden   Skip the regression check against the committed
                  golden. Use when intentionally regenerating.
USAGE
            exit 0;;
        *) echo "unknown argument: $1" >&2; exit 2;;
    esac
done

if [[ -z "${OUT_PATH}" ]]; then
    OUT_PATH="${GOLDEN_PATH}"
fi
mkdir -p "$(dirname "${OUT_PATH}")"
OUT_DIR="$(cd "$(dirname "${OUT_PATH}")" && pwd)"

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
        for svc in configurator cardano-peer-a cardano-peer-b cardano-observation; do
            docker compose -f "${COMPOSE_FILE}" logs --tail=200 "${svc}" 2>/dev/null || true
        done
    fi
    log "Tearing down stack..."
    docker compose -f "${COMPOSE_FILE}" --profile capture down -v 2>/dev/null || true
}
trap cleanup EXIT

command -v docker &>/dev/null || die "docker not installed"
docker compose version &>/dev/null || die "docker compose plugin not installed"

# Map host uid:gid into compose runs so bind-mounted output files are
# owned by the caller, not root.
USER_SPEC="$(id -u):$(id -g)"

log "Building images..."
docker compose -f "${COMPOSE_FILE}" --profile capture build \
    configurator capture-sidecar composer

log "Bringing configurator + cardano-* up (detached)..."
docker compose -f "${COMPOSE_FILE}" up -d \
    configurator cardano-peer-a cardano-peer-b cardano-observation

log "Waiting for cardano-* to become healthy..."
MAX_WAIT=900
ELAPSED=0
while [[ ${ELAPSED} -lt ${MAX_WAIT} ]]; do
    HEALTHY=0
    for svc in consensus-fork-cardano-peer-a \
               consensus-fork-cardano-peer-b \
               consensus-fork-cardano-observation; do
        status="$(docker inspect --format='{{.State.Health.Status}}' \
            "${svc}" 2>/dev/null || echo missing)"
        case "${status}" in
            healthy)   HEALTHY=$((HEALTHY + 1));;
            unhealthy) die "${svc} reported unhealthy";;
        esac
    done
    if [[ ${HEALTHY} -ge 3 ]]; then
        log "all 3 cardano-* healthy after ${ELAPSED}s"
        break
    fi
    sleep 2
    ELAPSED=$((ELAPSED + 2))
    if (( ELAPSED % 30 == 0 )); then
        log "  still waiting (${ELAPSED}s, ${HEALTHY}/3 healthy)..."
    fi
done
if [[ ${ELAPSED} -ge ${MAX_WAIT} ]]; then
    die "cardano-* services did not all become healthy within ${MAX_WAIT}s"
fi

# Observation node needs time to chainsync both peers' chains and
# settle on its selected one. 30s comfortably accommodates the small
# per-peer chains (≤200 blocks, 1s slots) the configurator stages.
log "Settle delay (30s) so observation node stabilizes..."
sleep 30

run_sidecar() {
    local address="$1" out_basename="$2" title="$3"
    log "Capturing ${title} (address=${address})..."
    # --no-deps: don't re-trigger upstream services (cardano-* +
    # configurator are already up from the earlier `compose up -d`).
    # Without this, `compose run` re-runs the dependency chain and
    # the configurator forges the chain a SECOND time on top of the
    # already-populated per-peer volumes, producing a corrupt DB the
    # runtime cardano-* then chokes on.
    docker compose -f "${COMPOSE_FILE}" --profile capture run --rm \
        --no-deps \
        --user "${USER_SPEC}" \
        capture-sidecar \
        -address "${address}" \
        -network-magic 42 \
        -conversation /scenario/capture-conversation.json \
        -out "/capture-output/${out_basename}" \
        -title "${title}" \
        -timeout 360s
}

run_sidecar cardano-peer-a:3001       peer-a.json      fork_and_select_v1.peer_a
run_sidecar cardano-peer-b:3001       peer-b.json      fork_and_select_v1.peer_b
run_sidecar cardano-observation:3001  observation.json fork_and_select_v1.observation

log "Composing multi-peer vector..."
GOLDEN_ARGS=()
if [[ "${SKIP_GOLDEN}" != "true" && -f "${GOLDEN_PATH}" ]]; then
    # The golden lives at the host repo path; the composer container
    # only sees /capture-output. Copy the golden into the bind-mounted
    # output dir under a temp name so the composer can read it.
    cp "${GOLDEN_PATH}" "${OUT_DIR}/.golden.json"
    GOLDEN_ARGS=(-golden /capture-output/.golden.json)
fi

set +e
docker compose -f "${COMPOSE_FILE}" --profile capture run --rm \
    --no-deps \
    --user "${USER_SPEC}" \
    composer \
    -peer /capture-output/peer-a.json \
    -peer /capture-output/peer-b.json \
    -observation /capture-output/observation.json \
    -title fork_and_select_v1 \
    -out /capture-output/composed.json \
    "${GOLDEN_ARGS[@]}"
COMPOSE_EXIT=$?
set -e

# Clean up the staged golden regardless of compose result.
rm -f "${OUT_DIR}/.golden.json"

if [[ ${COMPOSE_EXIT} -ne 0 ]]; then
    log "Composer reported a golden mismatch (or other error)."
    log "Composed vector at ${OUT_DIR}/composed.json for inspection."
    exit ${COMPOSE_EXIT}
fi

mv "${OUT_DIR}/composed.json" "${OUT_PATH}"
# Drop the per-peer intermediates so the caller's output dir is clean.
rm -f "${OUT_DIR}/peer-a.json" \
      "${OUT_DIR}/peer-b.json" \
      "${OUT_DIR}/observation.json"

log "Vector written to ${OUT_PATH}"
