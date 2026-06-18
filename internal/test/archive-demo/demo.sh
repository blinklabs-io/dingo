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

# Operator-facing guided demo of the archive-node + history-expiry-node + bark
# proxy story. Stands the stack up, narrates chain growth and expiry
# activity, then shows a live BlockFetch from outside the stack against
# a block whose local history has already expired.
#
# Usage:
#   ./demo.sh             # full guided demo
#   ./demo.sh --keep-up   # leave stack running on success
#
# What you should see:
#   1. Stack comes up (Minio + cardano-producer + dingo-archive + dingo-pruning).
#   2. Chain advances; both Dingo nodes track it.
#   3. Minio bucket fills with block CBOR objects as the archive node writes.
#   4. History Expiry kicks in once tip is past the stability window (3k/f = 300
#      slots for testnet.yaml); local Badger blob count drops.
#   5. demo-fetch BlockFetches a block at slot ~50 from the expiry node
#      and prints "OK: fetched X bytes from dingo-pruning in Yms" once the
#      bark proxy returns the block from the archive.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
PRUNING_DATA_DIR="${SCRIPT_DIR}/tmp/dingo-pruning-data"

# Mirror Compose's .env handling so port overrides set there reach the bash
# side (host-port references like demo-fetch's --addr). Without this, .env-only
# overrides take effect for `docker compose up` but not for our localhost
# clients, leaving them aimed at the default ports.
if [[ -f "${SCRIPT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/.env"
  set +a
fi

# LAN address used for the Minio console URL we print to the operator. We pick
# the source IP for the default route so a viewer on another machine can open
# the link; falls back to localhost if detection fails.
ARCHIVEDEMO_HOST="${ARCHIVEDEMO_HOST:-$(ip route get 1.1.1.1 2>/dev/null | awk '{print $7; exit}')}"
ARCHIVEDEMO_HOST="${ARCHIVEDEMO_HOST:-localhost}"

KEEP_UP=false
for arg in "$@"; do
  case "${arg}" in
    --keep-up) KEEP_UP=true ;;
    *)         echo "unknown arg: ${arg}" >&2; exit 2 ;;
  esac
done

say()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
die()  { echo "[demo] ERROR: $*" >&2; exit 1; }

cleanup() {
  local exit_code=$?
  if [[ -n "${DEMO_BIN_DIR:-}" ]] && [[ -d "${DEMO_BIN_DIR}" ]]; then
    rm -rf "${DEMO_BIN_DIR}"
  fi
  if [[ "${KEEP_UP}" == "true" ]] && [[ ${exit_code} -eq 0 ]]; then
    say "Demo finished. Stack left running (--keep-up)."
    note "Minio console:  http://${ARCHIVEDEMO_HOST}:${ARCHIVEDEMO_MINIO_CONSOLE_PORT:-9101} (demo / demodemo)"
    note "Stop:           ${SCRIPT_DIR}/stop.sh"
    return
  fi
  say "Tearing down..."
  docker compose -f "${COMPOSE_FILE}" down -v 2>/dev/null || true
  # The pruning data dir is bind-mounted into a container that runs as uid
  # 100, so files inside may be unreadable to the host user. Wipe via a
  # one-shot container before letting the host rm finish the parent.
  if [[ -d "${SCRIPT_DIR}/tmp" ]]; then
    docker run --rm -v "${SCRIPT_DIR}/tmp":/cleanup alpine \
      sh -c 'rm -rf /cleanup/* /cleanup/.[!.]* 2>/dev/null || true' \
      >/dev/null 2>&1 || true
    rm -rf "${SCRIPT_DIR}/tmp" 2>/dev/null || true
  fi
}
trap cleanup EXIT

command -v docker >/dev/null || die "docker is not installed"
command -v go     >/dev/null || die "go is not installed"

# ---------------------------------------------------------------------------
# Build helpers (demo-fetch lives under cmd/, separate from the test).
# ---------------------------------------------------------------------------
say "Building demo-fetch helper..."
DEMO_BIN_DIR="$(mktemp -d)"
DEMO_FETCH="${DEMO_BIN_DIR}/demo-fetch"
( cd "${PROJECT_ROOT}" && go build -o "${DEMO_FETCH}" ./internal/test/archive-demo/cmd/demo-fetch )
note "built: ${DEMO_FETCH}"

# ---------------------------------------------------------------------------
# Bring the stack up.
# ---------------------------------------------------------------------------
say "Starting archive-demo stack..."
mkdir -p "${PRUNING_DATA_DIR}"
chmod 777 "${PRUNING_DATA_DIR}"
docker compose -f "${COMPOSE_FILE}" up -d --build >/dev/null

note "Waiting for dingo-pruning to become healthy..."
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
note "All four services healthy."
note "Minio console: http://${ARCHIVEDEMO_HOST}:${ARCHIVEDEMO_MINIO_CONSOLE_PORT:-9101} (demo / demodemo)"

# Configure mc once so subsequent ls calls work cheaply.
docker exec archivedemo-minio mc alias set local http://localhost:9000 demo demodemo >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Helpers for periodic stats.
# ---------------------------------------------------------------------------
last_tip_slot() {
  docker logs archivedemo-dingo-pruning 2>&1 \
    | grep -o 'chain extended, new tip: [0-9a-f]\+ at slot [0-9]\+' \
    | tail -1 \
    | grep -o '[0-9]\+$'
}

minio_object_count() {
  docker exec archivedemo-minio mc ls --recursive local/dingo-archive 2>/dev/null | wc -l
}

minio_block_objects() {
  # Block CBOR keys begin with "bp" which is hex-encoded as "6270".
  docker exec archivedemo-minio mc ls --recursive local/dingo-archive 2>/dev/null \
    | awk '{print $NF}' | grep -c '^6270' || true
}

badger_blob_size() {
  # Query inside the container; the bind-mounted host path contains files
  # owned by the container's dingo uid (100), which the host user usually
  # can't read.
  docker exec archivedemo-dingo-pruning du -sh /data/db/blob 2>/dev/null \
    | awk '{print $1}' || echo "?"
}

expiry_rounds() {
  docker logs archivedemo-dingo-pruning 2>&1 | grep -c 'history expiry: completed round' || true
}

expiry_skipped() {
  docker logs archivedemo-dingo-pruning 2>&1 | grep -c 'history expiry: skipped because current slot is not high enough' || true
}

# ---------------------------------------------------------------------------
# Watch the system grow: print a stats line every 10s until tip clears the
# security window and History Expiry has had time to act on slot ~50.
# ---------------------------------------------------------------------------
say "Watching chain grow and History Expiry act..."
note "Stability window: 300 slots (3k/f from testnet.yaml). Target: tip >= 400 (History Expiry will have expired slots 0..99)."
TARGET_TIP=400
deadline=$(( $(date +%s) + 600 ))
prev_expiry=0
while true; do
  tip="$(last_tip_slot 2>/dev/null || true)"
  tip="${tip:-0}"
  obj_total=$(minio_object_count)
  obj_bp=$(minio_block_objects)
  badger=$(badger_blob_size)
  rounds=$(expiry_rounds)
  delta=$(( rounds - prev_expiry ))
  prev_expiry=${rounds}

  printf '   tip=%-4s  minio_objects=%-5s  minio_block_cbor=%-4s  expiry_node_blob=%-7s  expiry_rounds=%s (+%d)\n' \
    "${tip}" "${obj_total}" "${obj_bp}" "${badger}" "${rounds}" "${delta}"

  if [[ ${tip} -ge ${TARGET_TIP} ]]; then
    break
  fi
  if [[ $(date +%s) -ge ${deadline} ]]; then
    die "chain did not reach slot ${TARGET_TIP} within timeout"
  fi
  sleep 10
done

note "Tip is past the security window. History Expiry has run $(expiry_rounds) rounds."

# ---------------------------------------------------------------------------
# BlockFetch a block well behind the security window. Show timing.
# ---------------------------------------------------------------------------
say "BlockFetch test: requesting a block at slot ~50 from dingo-pruning"
note "  slot 50 is ~350 slots behind the current tip (well past the 300-slot window)"
note "  it has expired from dingo-pruning's local Badger"
note "  if the bark proxy is wired correctly, dingo-pruning fetches it from the archive"
note ""
note "Running demo-fetch (timeout 60s)..."
echo
set +e
"${DEMO_FETCH}" \
  --addr "localhost:${ARCHIVEDEMO_DINGO_PRUNING_PORT:-3113}" \
  --name dingo-pruning \
  --resolve-addr "localhost:${ARCHIVEDEMO_DINGO_ARCHIVE_PORT:-3111}" \
  --resolve-name dingo-archive \
  --slot 50 \
  --timeout 60s
fetch_exit=$?
set -e
echo

case "${fetch_exit}" in
  0)
    say "Demo complete: transparent proxy via bark works end to end."
    ;;
  *)
    die "demo-fetch failed (exit ${fetch_exit}); see logs above"
    ;;
esac
