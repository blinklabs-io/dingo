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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Mode selection: default dingo (all-dingo network), --conformance for the
# dingo + cardano-node reference network.
MODE="dingo"
for arg in "$@"; do
  case "${arg}" in
    --conformance) MODE="conformance" ;;
  esac
done
if [[ "${MODE}" == "conformance" ]]; then
  export COMPOSE_PROFILES="conformance"
else
  export COMPOSE_PROFILES="dingo"
fi

echo "Starting DevNet containers (mode: ${MODE})..."
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d

echo ""
if [[ "${MODE}" == "conformance" ]]; then
  # Mirror docker-compose.yml's host-port defaults so the printed addresses
  # match the actual mappings (and respect any DEVNET_*_PORT overrides).
  DINGO_PORT="${DEVNET_DINGO_PORT:-3010}"
  CARDANO_PORT="${DEVNET_CARDANO_PORT:-3011}"
  RELAY_PORT="${DEVNET_RELAY_PORT:-3012}"
  echo "DevNet started (conformance mode)."
  echo "  Dingo producer:   localhost:${DINGO_PORT}"
  echo "  Cardano producer: localhost:${CARDANO_PORT}"
  echo "  Cardano relay:    localhost:${RELAY_PORT}"
else
  DINGO1_PORT="${DEVNET_DINGO1_PORT:-3010}"
  DINGO2_PORT="${DEVNET_DINGO2_PORT:-3013}"
  DINGO3_PORT="${DEVNET_DINGO3_PORT:-3014}"
  DINGO_RELAY_PORT="${DEVNET_DINGO_RELAY_PORT:-3015}"
  echo "DevNet started (dingo mode)."
  echo "  dingo-1:     localhost:${DINGO1_PORT}"
  echo "  dingo-2:     localhost:${DINGO2_PORT}"
  echo "  dingo-3:     localhost:${DINGO3_PORT}"
  echo "  dingo-relay: localhost:${DINGO_RELAY_PORT}"
fi
echo ""
echo "View logs:  docker compose -f ${SCRIPT_DIR}/docker-compose.yml logs -f"
echo "Stop:       ${SCRIPT_DIR}/stop.sh"
