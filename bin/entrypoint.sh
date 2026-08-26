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
#   - First-run detection and Mithril snapshot bootstrapping via dingo's
#     built-in mithril sync (no external mithril-client needed)
#   - Signal forwarding for graceful shutdown
#   - Debug mode with verbose logging
#
# Environment variables:
#   CARDANO_NETWORK       - Named network: mainnet, preprod, preview, devnet (default: preview)
#   CARDANO_CONFIG        - Path to cardano node config.json (auto-set from network)
#   CARDANO_DATABASE_PATH - Database storage location (default: /data/db)
#   DINGO_SOCKET_PATH     - Unix socket path for NtC (default: /ipc/dingo.socket)
#   DINGO_DEBUG           - Set to any value to enable debug logging and set -x
#   DINGO_LOG_FILE        - Optional file receiving dingo stdout/stderr
#   RESTORE_SNAPSHOT      - Set to any value to bootstrap from Mithril snapshot on first run

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
# Managed child and signal handling
# --------------------------------------------------------------------------- #

MANAGED_CHILD_PID=""
PENDING_SIGNAL=""
PENDING_SIGNAL_EXIT_CODE=""
SIGNAL_FORWARDING="false"
MANAGED_WAIT_INTERRUPTED="false"

forward_signal() {
  local signal_name="$1"
  local signal_exit_code="$2"

  # A trap can run between starting a background child and assigning $!.
  # Retain that signal so run_managed_dingo either exits before launch or
  # forwards it immediately after publishing the child PID.
  if [[ -z "${MANAGED_CHILD_PID}" ]]; then
    PENDING_SIGNAL="${signal_name}"
    PENDING_SIGNAL_EXIT_CODE="${signal_exit_code}"
    return 0
  fi

  # A second signal can interrupt the wait in the first handler. Forward it
  # too, then let the first handler re-wait for the child's actual status.
  if [[ "${SIGNAL_FORWARDING}" == "true" ]]; then
    if kill -s "${signal_name}" "${MANAGED_CHILD_PID}" 2>/dev/null; then
      MANAGED_WAIT_INTERRUPTED="true"
    fi
    return 0
  fi

  SIGNAL_FORWARDING="true"
  if ! kill -s "${signal_name}" "${MANAGED_CHILD_PID}" 2>/dev/null; then
    # The ordinary wait may already have reaped the child. Leave its captured
    # status intact instead of replacing it with a second wait's status 127.
    SIGNAL_FORWARDING="false"
    return 0
  fi
  log "Received ${signal_name}, forwarding to dingo (PID ${MANAGED_CHILD_PID})..."

  local child_exit_code
  while true; do
    MANAGED_WAIT_INTERRUPTED="false"
    if wait "${MANAGED_CHILD_PID}"; then
      child_exit_code=0
    else
      child_exit_code=$?
    fi
    if [[ "${MANAGED_WAIT_INTERRUPTED}" != "true" ]]; then
      break
    fi
  done
  MANAGED_CHILD_PID=""
  exit "${child_exit_code}"
}

run_managed_dingo() {
  local output_file="$1"
  shift

  # A shutdown request received before a child exists must not start new work.
  if [[ -n "${PENDING_SIGNAL}" ]]; then
    exit "${PENDING_SIGNAL_EXIT_CODE}"
  fi

  if [[ -n "${output_file}" ]]; then
    dingo "$@" >>"${output_file}" 2>&1 &
  else
    dingo "$@" &
  fi
  MANAGED_CHILD_PID=$!

  # Close the start/$! assignment race described in forward_signal.
  if [[ -n "${PENDING_SIGNAL}" ]]; then
    forward_signal "${PENDING_SIGNAL}" "${PENDING_SIGNAL_EXIT_CODE}"
  fi

  local child_exit_code
  if wait "${MANAGED_CHILD_PID}"; then
    child_exit_code=0
  else
    child_exit_code=$?
  fi
  MANAGED_CHILD_PID=""
  return "${child_exit_code}"
}

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
# First-run detection and Mithril snapshot bootstrap
# --------------------------------------------------------------------------- #

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

has_incomplete_sync() {
  # The Docker image defaults to sqlite metadata storage. If a previous
  # Mithril bootstrap failed after creating the DB, serve will refuse to
  # start until the sync is resumed.
  local sqlite_db="${CARDANO_DATABASE_PATH}/metadata.sqlite"
  if [[ ! -f "${sqlite_db}" ]]; then
    return 1
  fi

  local status
  status="$(sqlite3 "${sqlite_db}" "SELECT value FROM sync_state WHERE sync_key = 'sync_status' LIMIT 1;" 2>/dev/null || true)"
  if [[ -n "${status}" ]]; then
    return 0
  fi
  return 1
}

bootstrap_if_needed() {
  # Only bootstrap on first run when RESTORE_SNAPSHOT is set
  if [[ -n "${RESTORE_SNAPSHOT:-}" ]] && is_first_run; then
    log "First run detected with RESTORE_SNAPSHOT set, bootstrapping from Mithril..."

    # Build dingo mithril sync arguments
    local sync_args=()
    if [[ -n "${DINGO_DEBUG:-}" ]]; then
      sync_args+=("--debug")
    fi
    sync_args+=("mithril" "sync")

    if run_managed_dingo "" "${sync_args[@]}"; then
      log "Mithril bootstrap complete"
      return 0
    else
      return $?
    fi
  fi

  if [[ -n "${RESTORE_SNAPSHOT:-}" ]] && has_incomplete_sync; then
    log "Incomplete Mithril sync detected, resuming..."

    local sync_args=()
    if [[ -n "${DINGO_DEBUG:-}" ]]; then
      sync_args+=("--debug")
    fi
    sync_args+=("mithril" "sync")

    if run_managed_dingo "" "${sync_args[@]}"; then
      log "Mithril sync resume complete"
      return 0
    else
      return $?
    fi
  fi

  if [[ -n "${RESTORE_SNAPSHOT:-}" ]] && ! is_first_run; then
    log "Database already exists, skipping Mithril snapshot bootstrap"
  fi
}

# --------------------------------------------------------------------------- #
# Ensure data directories exist
# --------------------------------------------------------------------------- #

mkdir -p "${CARDANO_DATABASE_PATH}"
mkdir -p "$(dirname "${DINGO_SOCKET_PATH}")"

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

# Install the handlers before a first-run or resumed Mithril bootstrap starts.
trap 'forward_signal SIGTERM 143' SIGTERM
trap 'forward_signal SIGINT 130' SIGINT

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

# Persist structured output for test harnesses when requested, while keeping
# stdout as the default for normal deployments.
if [[ -n "${DINGO_LOG_FILE:-}" ]]; then
  mkdir -p "$(dirname "${DINGO_LOG_FILE}")"
  touch "${DINGO_LOG_FILE}"
  chmod 0644 "${DINGO_LOG_FILE}"
fi

# Start dingo and preserve its exit status, including after a forwarded signal.
if run_managed_dingo "${DINGO_LOG_FILE:-}" "${DINGO_ARGS[@]}"; then
  exit_code=0
else
  exit_code=$?
fi
exit "${exit_code}"
