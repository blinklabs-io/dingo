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

# Start the DevNet for manual testing.
#
# The configurator service generates genesis files and pool keys
# automatically before nodes start (via depends_on in docker-compose.yml).
#
# Usage:
#   ./start.sh               # all-dingo network (default)
#   ./start.sh --conformance # dingo + cardano-node reference network
#   ./start.sh --accelerated # bring the network up on the accelerated spec

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=compose-project.sh
source "${SCRIPT_DIR}/compose-project.sh"
devnet_compose_project
export DEVNET_COMPOSE_PROJECT="${COMPOSE_PROJECT_NAME}"
devnet_render_topology
devnet_ports

# Mode selection precedence: CLI, COMPOSE_PROFILES, then dingo.
MODE=""
ACCELERATED=false
for arg in "$@"; do
  case "${arg}" in
    --conformance) MODE="conformance" ;;
    --accelerated) ACCELERATED=true ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done
MODE="${MODE:-${COMPOSE_PROFILES:-dingo}}"
case "${MODE}" in
  conformance) export COMPOSE_PROFILES="conformance" ;;
  dingo)       export COMPOSE_PROFILES="dingo" ;;
  *)
    echo "Unsupported COMPOSE_PROFILES mode: ${MODE}" >&2
    exit 1
    ;;
esac

# Mirror docker-compose.yml's host-port defaults so anything printed below
# matches the actual mappings, whether devnet_ports derived a worktree
# block or the caller overrode individual DEVNET_*_PORT variables.
if [[ "${MODE}" == "conformance" ]]; then
  DINGO_PORT="${DEVNET_DINGO_PORT:-3010}"
  CARDANO_PORT="${DEVNET_CARDANO_PORT:-3011}"
  RELAY_PORT="${DEVNET_RELAY_PORT:-3012}"
else
  DINGO1_PORT="${DEVNET_DINGO1_PORT:-3010}"
  DINGO2_PORT="${DEVNET_DINGO2_PORT:-3013}"
  DINGO3_PORT="${DEVNET_DINGO3_PORT:-3014}"
  DINGO_RELAY_PORT="${DEVNET_DINGO_RELAY_PORT:-3015}"
fi

# Pick the network spec the configurator generates genesis from. The
# accelerated spec compresses slot, epoch and security-parameter timing so
# a full scenario fits the reference-runner budget; the canonical spec is
# what soak and canary runs use.
if [[ "${ACCELERATED}" == "true" ]]; then
  if [[ "${MODE}" == "conformance" ]]; then
    export DEVNET_CONFORMANCE_SPEC="./testnet-accelerated.yaml"
    ACTIVE_SPEC="${DEVNET_CONFORMANCE_SPEC}"
  else
    export DEVNET_DINGO_SPEC="./testnet-dingo-accelerated.yaml"
    ACTIVE_SPEC="${DEVNET_DINGO_SPEC}"
  fi
  echo "Using accelerated network spec: ${ACTIVE_SPEC}"
  echo "Run the scenario with:"
  echo "  COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME} \\"
  echo "  DEVNET_COMPOSE_PROJECT=${COMPOSE_PROJECT_NAME} \\"
  echo "  DEVNET_ACCELERATED=1 \\"
  echo "  DEVNET_TESTNET_YAML=${SCRIPT_DIR}/${ACTIVE_SPEC#./} \\"
  echo "  DEVNET_COMPOSE_FILE=${SCRIPT_DIR}/docker-compose.yml \\"
  # The Go harness reads full host:port DEVNET_*_ADDR variables, not the
  # DEVNET_*_PORT variables docker-compose.yml reads — devnet_ports may
  # have derived non-default ports, so these have to be spelled out here
  # or the copy-pasted command below would connect to the wrong ports.
  if [[ "${MODE}" == "conformance" ]]; then
    echo "  DEVNET_DINGO_ADDR=localhost:${DINGO_PORT} \\"
    echo "  DEVNET_CARDANO_ADDR=localhost:${CARDANO_PORT} \\"
    echo "  DEVNET_RELAY_ADDR=localhost:${RELAY_PORT} \\"
  else
    echo "  DEVNET_DINGO1_ADDR=localhost:${DINGO1_PORT} \\"
    echo "  DEVNET_DINGO2_ADDR=localhost:${DINGO2_PORT} \\"
    echo "  DEVNET_DINGO3_ADDR=localhost:${DINGO3_PORT} \\"
    echo "  DEVNET_DINGO_RELAY_ADDR=localhost:${DINGO_RELAY_PORT} \\"
  fi
  echo "  go test -tags devnet -run TestAcceleratedScenarioTimeline \\"
  echo "    -timeout 8m ./internal/test/devnet/scenarios/"
fi

echo "Starting DevNet containers (mode: ${MODE}, project: ${COMPOSE_PROJECT_NAME}, net: ${DEVNET_NET_BASE}.0/24)..."
devnet_compose_up "${SCRIPT_DIR}/docker-compose.yml"
echo "Compose network (final): ${DEVNET_NET_BASE}.0/24"

echo ""
if [[ "${MODE}" == "conformance" ]]; then
  echo "DevNet started (conformance mode)."
  echo "  Dingo producer:   localhost:${DINGO_PORT}"
  echo "  Cardano producer: localhost:${CARDANO_PORT}"
  echo "  Cardano relay:    localhost:${RELAY_PORT}"
else
  echo "DevNet started (dingo mode)."
  echo "  dingo-1:     localhost:${DINGO1_PORT}"
  echo "  dingo-2:     localhost:${DINGO2_PORT}"
  echo "  dingo-3:     localhost:${DINGO3_PORT}"
  echo "  dingo-relay: localhost:${DINGO_RELAY_PORT}"
fi
echo ""
echo "View logs:  COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME} docker compose -f ${SCRIPT_DIR}/docker-compose.yml logs -f"
echo "Stop:       COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME} ${SCRIPT_DIR}/stop.sh"
