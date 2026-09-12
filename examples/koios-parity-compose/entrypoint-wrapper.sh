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

# Koios-parity toggle wrapper for the koios-parity-compose example.
#
# This does not replace or modify the image's own /bin/entrypoint.sh (which
# still handles Mithril bootstrap detection and signal forwarding); it only
# decides which extra dingo CLI arguments to hand it, based on one
# environment variable:
#
#   KOIOS_PARITY_ENABLED=true   -> appends:
#     --koios-parity-enabled --koios-parity-strict=false
#     --koios-parity-base-url "$KOIOS_PARITY_BASE_URL"
#   KOIOS_PARITY_ENABLED=false  -> appends nothing (default); dingo runs with
#     no --koios-parity-* flags at all.
#
# --koios-parity-strict=false is intentional here: this stack is for local
# observability/validation, not enforcement, so a Koios mismatch or transient
# API error logs rather than stopping the node (see dingo's
# --koios-parity-strict flag, which defaults true).

set -euo pipefail

args=("$@")

if [[ "${KOIOS_PARITY_ENABLED:-false}" == "true" ]]; then
  args+=(
    "--koios-parity-enabled"
    "--koios-parity-strict=false"
    "--koios-parity-base-url"
    "${KOIOS_PARITY_BASE_URL:-https://preview-koios.tosidrop.me/api/v1}"
  )
fi

exec /bin/entrypoint.sh "${args[@]}"
