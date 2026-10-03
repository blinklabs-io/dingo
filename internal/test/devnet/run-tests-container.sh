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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
die() { echo "[apple-container] $*" >&2; exit 1; }

[[ "$(uname -s)" == Darwin && "$(uname -m)" == arm64 ]] ||
  die "Apple Container requires an Apple silicon Mac"
command -v container >/dev/null || die "Install https://github.com/apple/container first"
container system status >/dev/null || die "Start Apple Container with: container system start"

RUNNER="dingo-devnet-apple-$(printf '%s' "${PROJECT_ROOT}" | cksum | awk '{print $1}')"
STATE_DIR="${PROJECT_ROOT}/.devnet/apple-container"
mkdir -p "${STATE_DIR}"
# A shared runner cannot safely host two simultaneous Compose teardown cycles.
mkdir "${STATE_DIR}/lock" 2>/dev/null ||
  die "Another run holds ${STATE_DIR}/lock; remove it only if that run has exited"
KEEP_UP=false
for arg in "$@"; do
  [[ "${arg}" != --keep-up ]] || KEEP_UP=true
done
STARTED=false
cleanup() {
  local result=$?
  trap - EXIT
  if [[ "${STARTED}" == true ]]; then
    if [[ "${KEEP_UP}" == true && ${result} -eq 0 ]]; then
      echo "[apple-container] Runner left running: ${RUNNER}"
      echo "[apple-container] Stop DevNet inside it with: container exec -w '${PROJECT_ROOT}' ${RUNNER} bash internal/test/devnet/stop.sh (add --conformance if used)"
      echo "[apple-container] Then stop the VM with: container stop ${RUNNER}"
    else
      if ! container stop --time 30 "${RUNNER}"; then
        # A nested cgroup can outlive dockerd during the first stop attempt.
        container stop --time 0 "${RUNNER}" ||
          echo "[apple-container] Could not stop ${RUNNER}; inspect it with container list" >&2
      fi
    fi
  fi
  rmdir "${STATE_DIR}/lock" || true
  exit "${result}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Bind at the same absolute path so Compose paths and artifact paths retain
# their meaning inside Linux. Docker's socket is private to the guest VM.
if container inspect "${RUNNER}" >/dev/null 2>&1; then
  if ! container list --quiet | grep -Fxq "${RUNNER}"; then
    container start "${RUNNER}"
  fi
else
  container run -d --name "${RUNNER}" \
    --cpus "${DEVNET_CONTAINER_CPUS:-6}" \
    --memory "${DEVNET_CONTAINER_MEMORY:-12G}" \
    --cap-add ALL --masked-path NONE --read-only-path NONE \
    --entrypoint dockerd -v "${PROJECT_ROOT}:${PROJECT_ROOT}" \
    docker.io/library/docker:28.5.2-dind@sha256:2a232a42256f70d78e3cc5d2b5d6b3276710a0de0596c145f627ecfae90282ac \
    --host=unix:///var/run/docker.sock --storage-driver=vfs
fi
STARTED=true
echo "[apple-container] Using ${RUNNER}; its stopped filesystem retains build caches"

# The bootstrap script expands variables inside Linux, not in the host shell.
# shellcheck disable=SC2016
container exec "${RUNNER}" sh -ec '
  if ! command -v go >/dev/null || ! command -v bash >/dev/null || ! command -v gcc >/dev/null; then
    apk add --no-cache bash go git gcc musl-dev make curl
  fi
  attempts=0
  until docker info >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [ "$attempts" -ge 30 ]; then
      echo "Docker inside the Apple Container VM did not become ready" >&2
      exit 1
    fi
    sleep 1
  done
'

EXEC_ENV=()
# Forward test knobs, not the host's PATH, Docker socket, or Go installation.
while IFS= read -r key; do
  case "${key}" in
    DEVNET_RUNTIME|DEVNET_CONTAINER_*) ;;
    DEVNET_*|COMPOSE_PROJECT_NAME|COMPOSE_PROFILES|MODE|DINGO_PORT|CARDANO_PORT|RELAY_PORT)
      EXEC_ENV+=(-e "${key}=${!key}") ;;
  esac
done < <(compgen -e)
if [[ -z "${DEVNET_ARTIFACT_DIR:-}" ]]; then
  DEVNET_ARTIFACT_DIR="$(mktemp -d "${STATE_DIR}/run.XXXXXX")"
fi
case "${DEVNET_ARTIFACT_DIR}" in
  "${PROJECT_ROOT}/"*) ;;
  *) die "DEVNET_ARTIFACT_DIR must be inside ${PROJECT_ROOT} for the shared mount" ;;
esac
echo "[apple-container] Artifacts: ${DEVNET_ARTIFACT_DIR}"
container exec -w "${PROJECT_ROOT}" \
  "${EXEC_ENV[@]+"${EXEC_ENV[@]}"}" \
  -e DEVNET_RUNTIME=docker -e GOTOOLCHAIN=auto \
  -e "DEVNET_ARTIFACT_DIR=${DEVNET_ARTIFACT_DIR}" \
  "${RUNNER}" bash internal/test/devnet/run-tests.sh "$@"
