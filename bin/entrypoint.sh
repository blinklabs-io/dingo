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

# Dingo Docker entrypoint script
#
# This script handles:
#   - Environment variable configuration and validation
#   - First-run detection and Mithril snapshot bootstrapping
#   - Resumable dingo load operations
#   - Signal forwarding for graceful shutdown
#   - Debug mode with verbose logging
#
# Environment variables:
#   CARDANO_NETWORK       - Named network: mainnet, preprod, preview, devnet (default: preview)
#   CARDANO_CONFIG        - Path to cardano node config.json (auto-set from network)
#   CARDANO_DATABASE_PATH - Database storage location (default: /data/db)
#   DINGO_SOCKET_PATH     - Unix socket path for NtC (default: /ipc/dingo.socket)
#   DINGO_DEBUG           - Set to any value to enable debug logging and set -x
#   RESTORE_SNAPSHOT      - Set to any value to bootstrap from Mithril snapshot on first run
#   GENESIS_VERIFICATION_KEY - Mithril genesis verification key (required when RESTORE_SNAPSHOT is set)

set -euo pipefail

# --------------------------------------------------------------------------- #
# Debug mode
# --------------------------------------------------------------------------- #

if [[ -n "${DINGO_DEBUG:-}" ]]; then
  set -x
fi

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #

CARDANO_NETWORK="${CARDANO_NETWORK:-preview}"
CARDANO_DATABASE_PATH="${CARDANO_DATABASE_PATH:-/data/db}"
DINGO_SOCKET_PATH="${DINGO_SOCKET_PATH:-/ipc/dingo.socket}"
CARDANO_NODE_SOCKET_PATH="${DINGO_SOCKET_PATH}"

# Export variables so dingo picks them up via envconfig
export CARDANO_NETWORK
export CARDANO_DATABASE_PATH
export DINGO_SOCKET_PATH
export CARDANO_NODE_SOCKET_PATH
export CARDANO_SOCKET_PATH="${DINGO_SOCKET_PATH}"

# --------------------------------------------------------------------------- #
# Logging helpers
# --------------------------------------------------------------------------- #

log()  { echo "[entrypoint] $*"; }
warn() { echo "[entrypoint] WARNING: $*" >&2; }
die()  { echo "[entrypoint] ERROR: $*" >&2; exit 1; }

# --------------------------------------------------------------------------- #
# Configuration validation
# --------------------------------------------------------------------------- #

# Map known networks to their bundled config paths (from cardano-configs image)
config_path_for_network() {
  case "$1" in
    mainnet) echo "/opt/cardano/config/mainnet/config.json" ;;
    preprod) echo "/opt/cardano/config/preprod/config.json" ;;
    preview) echo "/opt/cardano/config/preview/config.json" ;;
    devnet)  echo "" ;; # devnet uses embedded config, no external file needed
    *)       echo "" ;;
  esac
}

# Set CARDANO_CONFIG from network if not explicitly provided
if [[ -z "${CARDANO_CONFIG:-}" ]]; then
  default_config="$(config_path_for_network "${CARDANO_NETWORK}")"
  if [[ -n "${default_config}" ]]; then
    CARDANO_CONFIG="${default_config}"
    export CARDANO_CONFIG
    log "Using config for network '${CARDANO_NETWORK}': ${CARDANO_CONFIG}"
  else
    log "No default config path for network '${CARDANO_NETWORK}'"
  fi
else
  export CARDANO_CONFIG
fi

# Validate that CARDANO_CONFIG matches CARDANO_NETWORK for known networks
validate_config_network_match() {
  local config="${CARDANO_CONFIG:-}"
  local network="${CARDANO_NETWORK}"

  # Skip validation if config is empty or network is devnet/unknown
  if [[ -z "${config}" ]] || [[ "${network}" == "devnet" ]]; then
    return 0
  fi

  # Check that the config path contains the expected network name
  local expected_config
  expected_config="$(config_path_for_network "${network}")"
  if [[ -n "${expected_config}" ]] && [[ "${config}" != "${expected_config}" ]]; then
    # Only warn if the config path references a different known network
    for known_net in mainnet preprod preview; do
      if [[ "${known_net}" != "${network}" ]] && [[ "${config}" == *"/${known_net}/"* ]]; then
        warn "CARDANO_CONFIG '${config}' appears to be for '${known_net}' but CARDANO_NETWORK is '${network}'"
        warn "This mismatch may cause unexpected behavior"
        return 0
      fi
    done
  fi

  # Verify the config file actually exists
  if [[ -n "${config}" ]] && [[ ! -f "${config}" ]]; then
    die "CARDANO_CONFIG file does not exist: ${config}"
  fi
}

validate_config_network_match

# --------------------------------------------------------------------------- #
# Mithril aggregator endpoints for known networks
# --------------------------------------------------------------------------- #

aggregator_endpoint_for_network() {
  case "$1" in
    mainnet) echo "https://aggregator.release-mainnet.api.mithril.network/aggregator" ;;
    preprod) echo "https://aggregator.release-preprod.api.mithril.network/aggregator" ;;
    preview) echo "https://aggregator.pre-release-preview.api.mithril.network/aggregator" ;;
    *)       echo "" ;;
  esac
}

# --------------------------------------------------------------------------- #
# First-run detection and Mithril snapshot bootstrap
# --------------------------------------------------------------------------- #

# Marker file to track load completion. If this file does not exist but the
# snapshot download directory does, we know a previous load was interrupted
# and should be resumed.
LOAD_COMPLETE_MARKER="${CARDANO_DATABASE_PATH}/.load_complete"
SNAPSHOT_DIR="${CARDANO_DATABASE_PATH}/.mithril_snapshot"

is_first_run() {
  # Database is considered empty if the data directory does not exist or
  # contains no badger/metadata files. We check for the presence of the
  # database path and for any non-hidden entries within it.
  if [[ ! -d "${CARDANO_DATABASE_PATH}" ]]; then
    return 0
  fi
  # If the directory exists but has no non-hidden entries, treat as first run
  local entry
  entry="$(find "${CARDANO_DATABASE_PATH}" -mindepth 1 -maxdepth 1 -not -name '.*' -print -quit 2>/dev/null)"
  if [[ -z "${entry}" ]]; then
    return 0
  fi
  return 1
}

needs_load_resume() {
  # A load needs resuming if the snapshot directory exists but the completion
  # marker is absent (i.e., a previous dingo load was interrupted).
  [[ -d "${SNAPSHOT_DIR}" ]] && [[ ! -f "${LOAD_COMPLETE_MARKER}" ]]
}

download_mithril_snapshot() {
  local network="${CARDANO_NETWORK}"
  local endpoint

  endpoint="$(aggregator_endpoint_for_network "${network}")"
  if [[ -z "${endpoint}" ]]; then
    die "Mithril snapshot bootstrap is not supported for network '${network}'"
  fi

  # Check that mithril-client is available
  if ! command -v mithril-client &>/dev/null; then
    die "mithril-client is not installed but RESTORE_SNAPSHOT is set"
  fi

  if [[ -z "${GENESIS_VERIFICATION_KEY:-}" ]]; then
    die "GENESIS_VERIFICATION_KEY must be set when RESTORE_SNAPSHOT is enabled"
  fi

  log "Downloading Mithril snapshot for '${network}'..."
  mkdir -p "${SNAPSHOT_DIR}"

  AGGREGATOR_ENDPOINT="${endpoint}" \
    mithril-client cardano-db download \
      --genesis-verification-key "${GENESIS_VERIFICATION_KEY}" \
      --download-dir "${SNAPSHOT_DIR}" \
      latest

  log "Mithril snapshot download complete"
}

load_snapshot() {
  local immutable_path="${SNAPSHOT_DIR}/db/immutable"

  if [[ ! -d "${immutable_path}" ]]; then
    die "Snapshot immutable directory not found: ${immutable_path}"
  fi

  log "Loading snapshot into Dingo database..."

  # Build dingo load arguments
  local load_args=("load")
  if [[ -n "${DINGO_DEBUG:-}" ]]; then
    load_args=("--debug" "load")
  fi
  load_args+=("${immutable_path}")

  dingo "${load_args[@]}"

  # Mark load as complete
  touch "${LOAD_COMPLETE_MARKER}"
  log "Snapshot load complete"

  # Clean up snapshot data to reclaim disk space
  log "Cleaning up snapshot download..."
  rm -rf "${SNAPSHOT_DIR}"
  log "Snapshot cleanup complete"
}

bootstrap_if_needed() {
  # Resume an interrupted load if detected
  if needs_load_resume; then
    warn "Detected incomplete snapshot load, resuming..."
    load_snapshot
    return 0
  fi

  # Only bootstrap on first run when RESTORE_SNAPSHOT is set
  if [[ -n "${RESTORE_SNAPSHOT:-}" ]] && is_first_run; then
    log "First run detected with RESTORE_SNAPSHOT set, bootstrapping from Mithril..."
    download_mithril_snapshot
    load_snapshot
    return 0
  fi

  if [[ -n "${RESTORE_SNAPSHOT:-}" ]] && ! is_first_run; then
    log "Database already exists, skipping Mithril snapshot bootstrap"
  fi
}

# --------------------------------------------------------------------------- #
# Signal handling for graceful shutdown
# --------------------------------------------------------------------------- #

cleanup() {
  if [[ -n "${DINGO_PID:-}" ]]; then
    log "Received shutdown signal, forwarding to dingo (PID ${DINGO_PID})..."
    kill -TERM "${DINGO_PID}" 2>/dev/null || true
    wait "${DINGO_PID}" 2>/dev/null || true
  fi
  exit 0
}

# --------------------------------------------------------------------------- #
# Ensure data directories exist
# --------------------------------------------------------------------------- #

mkdir -p "${CARDANO_DATABASE_PATH}"
mkdir -p "$(dirname "${DINGO_SOCKET_PATH}")"

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

# Run Mithril bootstrap if applicable
bootstrap_if_needed

# Build the dingo command arguments
DINGO_ARGS=()

# Add debug flag if requested
if [[ -n "${DINGO_DEBUG:-}" ]]; then
  DINGO_ARGS+=("--debug")
fi

# If no arguments were passed to the entrypoint, default to "serve"
if [[ $# -eq 0 ]]; then
  DINGO_ARGS+=("serve")
else
  DINGO_ARGS+=("$@")
fi

log "Starting dingo ${DINGO_ARGS[*]}"

# Start dingo in the background so we can forward signals
dingo "${DINGO_ARGS[@]}" &
DINGO_PID=$!
trap cleanup SIGTERM SIGINT

# Wait for dingo to exit
set +e
wait "${DINGO_PID}"
exit_code=$?
set -e
DINGO_PID=""
exit "${exit_code}"
