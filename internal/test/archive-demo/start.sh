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

# Start the archive-node demo for manual exploration.
# The bind-mount tmp/dingo-pruning-data is created up front so the
# history-expiry node has a stable host path the integration test can later
# open with Badger.
#
# Usage: ./start.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "${SCRIPT_DIR}/tmp/dingo-pruning-data"
# The dingo container runs as uid 100. Loosen permissions so the container
# can write to the bind-mounted directory regardless of host ownership.
chmod 777 "${SCRIPT_DIR}/tmp/dingo-pruning-data"

echo "Starting archive-demo containers..."
docker compose -f "${SCRIPT_DIR}/docker-compose.yml" up -d

echo ""
echo "Archive demo started."
echo "  Cardano producer: localhost:${ARCHIVEDEMO_CARDANO_PORT:-3110}"
echo "  Dingo archive:    localhost:${ARCHIVEDEMO_DINGO_ARCHIVE_PORT:-3111}"
echo "  Bark gRPC:        localhost:${ARCHIVEDEMO_BARK_PORT:-3112}"
echo "  Dingo expiry:     localhost:${ARCHIVEDEMO_DINGO_PRUNING_PORT:-3113}"
echo "  Minio S3:         localhost:${ARCHIVEDEMO_MINIO_PORT:-9100}"
echo "  Minio console:    localhost:${ARCHIVEDEMO_MINIO_CONSOLE_PORT:-9101} (demo/demodemo)"
echo ""
echo "View logs:  docker compose -f ${SCRIPT_DIR}/docker-compose.yml logs -f"
echo "Stop:       ${SCRIPT_DIR}/stop.sh"
