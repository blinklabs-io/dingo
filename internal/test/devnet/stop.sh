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

# Stop the DevNet and remove all volumes.
#
# Usage:
#   ./stop.sh               # all-dingo network (default)
#   ./stop.sh --conformance # dingo + cardano-node reference network
#   ./stop.sh --accelerated # accepted and ignored; teardown removes the
#                           # profile's volumes regardless of which network
#                           # spec generated them

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Mode selection precedence: CLI, COMPOSE_PROFILES, then dingo.
MODE=""
for arg in "$@"; do
  case "${arg}" in
    --conformance) MODE="conformance" ;;
    # Teardown is spec-independent, but accept the flag so a --accelerated
    # bring-up can be torn down with the same arguments it was started
    # with.
    --accelerated) ;;
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

echo "Stopping DevNet containers and removing volumes (mode: ${MODE})..."
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" down -v

echo "DevNet stopped."
