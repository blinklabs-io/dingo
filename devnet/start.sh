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
# Usage: ./start.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting DevNet containers..."
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d

echo ""
echo "DevNet started."
echo "  Dingo producer:   localhost:3001"
echo "  Cardano producer: localhost:3011"
echo "  Cardano relay:    localhost:3012"
echo ""
echo "View logs:  docker compose -f ${SCRIPT_DIR}/docker-compose.yml logs -f"
echo "Stop:       ${SCRIPT_DIR}/stop.sh"
