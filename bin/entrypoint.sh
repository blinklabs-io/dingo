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
#   CARDANO_NETWORK       - Named network: mainnet, preprod, preview, dijkstra, devnet (default: preview)
#   CARDANO_CONFIG        - Path to cardano node config.json (auto-set from network)
#   CARDANO_DATABASE_PATH - Database storage location (default: /data/db)
#   DINGO_SOCKET_PATH     - Unix socket path for NtC (default: /ipc/dingo.socket)
#   DINGO_DEBUG           - Set to any value to enable debug logging and set -x
#                           (tracing stops before the dingo command line is built)
#   DINGO_LOG_FILE        - Optional new file receiving dingo stdout/stderr
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

# Secret-bearing flags and query names are matched by their final word, so
# --koios-parity-api-key and api_key are redacted but
# --token-registry-interval and file-path names are not.
SECRET_NAME_PATTERN='^.*(tokens?|secrets?|passwords?|passwd|passphrase|pwd|credentials?|bearer|sas|signatures?|api([-_.+]|[[:space:]])?key|access([-_.+]|[[:space:]])?key(id)?|private([-_.+]|[[:space:]])?key|shared([-_.+]|[[:space:]])?key|signing([-_.+]|[[:space:]])?key)$'
URL_USERINFO_PATTERN='^([A-Za-z][A-Za-z0-9+.-]*://)[^/?#@[:space:]]+@(.*)$'
AUTH_VALUE_PATTERN='^(.*(authorization|bearer)[:= ]+)(.*)$'

# decode_query_name decodes the escapes that can hide separators in a query
# parameter name. Malformed names fail closed in is_secret_name.
decode_query_name() {
  local encoded="$1" decoded="" byte hex i=0
  while (( i < ${#encoded} )); do
    byte="${encoded:i:1}"
    if [[ "${byte}" == "+" ]]; then
      decoded+=" "
      i=$((i + 1))
    elif [[ "${byte}" == "%" ]]; then
      if (( i + 2 >= ${#encoded} )); then
        return 1
      fi
      hex="${encoded:i+1:2}"
      if [[ ! "${hex}" =~ ^[[:xdigit:]]{2}$ ]] || [[ "${hex}" == "00" ]]; then
        return 1
      fi
      printf -v byte '%b' "\\x${hex}"
      decoded+="${byte}"
      i=$((i + 3))
    else
      decoded+="${byte}"
      i=$((i + 1))
    fi
  done
  printf '%s' "${decoded}"
}

is_secret_name() {
  local decoded
  if ! decoded="$(decode_query_name "$1")"; then
    return 0
  fi
  [[ "${decoded}" =~ ${SECRET_NAME_PATTERN} ]]
}

is_secret_flag() {
  if [[ "$1" != -* ]]; then
    return 1
  fi
  local name="${1#-}"
  name="${name#-}"
  is_secret_name "${name}"
}

# redact_url_query masks credential parameters while retaining endpoint and
# non-secret query values that are useful in startup diagnostics.
redact_url_query() {
  local value="$1" head tail query fragment="" rendered pair separator name
  if [[ "${value}" != *\?* ]]; then
    printf '%s' "${value}"
    return
  fi
  head="${value%%\?*}"
  tail="${value#*\?}"
  if [[ "${tail}" == *#* ]]; then
    query="${tail%%#*}"
    fragment="#${tail#*#}"
  else
    query="${tail}"
  fi
  rendered="${head}?"
  while true; do
    if [[ "${query}" =~ ^([^\&\;]*)([\&\;])(.*)$ ]]; then
      pair="${BASH_REMATCH[1]}"
      separator="${BASH_REMATCH[2]}"
      query="${BASH_REMATCH[3]}"
    else
      pair="${query}"
      separator=""
      query=""
    fi
    if [[ "${pair}" == *=* ]]; then
      name="${pair%%=*}"
      if is_secret_name "${name}"; then
        pair="${name}=<redacted>"
      fi
    fi
    rendered+="${pair}${separator}"
    if [[ -z "${separator}" ]]; then
      break
    fi
  done
  printf '%s' "${rendered}${fragment}"
}

# redact_value masks credentials embedded in a free-form value: URL userinfo
# (scheme://user:pass@host), query parameters, and
# "Authorization:"/"Bearer" header values.
redact_value() {
  local value="$1"
  if [[ "${value}" =~ ${URL_USERINFO_PATTERN} ]]; then
    value="${BASH_REMATCH[1]}<redacted>@${BASH_REMATCH[2]}"
  fi
  value="$(redact_url_query "${value}")"
  if [[ "${value}" =~ ${AUTH_VALUE_PATTERN} ]]; then
    value="${BASH_REMATCH[1]}<redacted>"
  fi
  printf '%s' "${value}"
}

# redact_args prints its arguments space-separated with the values of
# secret-bearing flags replaced by "<redacted>". Both "--flag value" and
# "--flag=value" forms are handled, and credentials embedded in any other
# argument are masked.
redact_args() {
  local out=() arg name redact_next="false"
  shopt -s nocasematch
  for arg in "$@"; do
    if [[ "${redact_next}" == "true" ]]; then
      # A secret flag followed by another secret flag has no value of its own;
      # keep the pending redaction for the next flag's value.
      if [[ "${arg}" != *=* ]] && is_secret_flag "${arg}"; then
        out+=("${arg}")
      else
        out+=("<redacted>")
        redact_next="false"
      fi
    elif [[ "${arg}" == -*=* ]]; then
      name="${arg%%=*}"
      if is_secret_flag "${name}"; then
        out+=("${name}=<redacted>")
      else
        out+=("${name}=$(redact_value "${arg#*=}")")
      fi
    elif is_secret_flag "${arg}"; then
      out+=("${arg}")
      redact_next="true"
    else
      out+=("$(redact_value "${arg}")")
    fi
  done
  shopt -u nocasematch
  echo "${out[*]}"
}

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
  local log_file="$1"
  shift

  # A shutdown request received before a child exists must not start new work.
  if [[ -n "${PENDING_SIGNAL}" ]]; then
    exit "${PENDING_SIGNAL_EXIT_CODE}"
  fi

  # DINGO_LOG_FILE persists structured output for test harnesses; stdout
  # stays the default for normal deployments.
  if [[ -n "${log_file}" ]]; then
    perl -e "${DINGO_LOG_OPENER}" -- "${log_file}" dingo "$@" &
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

# Bash redirections cannot open with O_EXCL|O_NOFOLLOW or relative to a
# directory descriptor, so any check before them leaves a window in which a
# FIFO or a symlinked parent can appear. This perl-base helper walks the
# parents with O_NOFOLLOW descriptors, creates the log with one exclusive
# no-follow open, sets the mode on the descriptor (a default ACL would
# otherwise override the umask), and execs dingo with stdout and stderr on it.
# Running it as the managed child keeps the descriptor out of the entrypoint.
# shellcheck disable=SC2016
DINGO_LOG_OPENER='
use strict;
use warnings;
use Errno qw(EEXIST ENOENT);
use Fcntl qw(O_CREAT O_DIRECTORY O_EXCL O_NOFOLLOW O_RDONLY O_WRONLY);

my $path = shift @ARGV;
sub fail {
  print STDERR "[entrypoint] ERROR: DINGO_LOG_FILE $_[0]\n";
  exit 1;
}
my @parts = split m{/}, $path, -1;
my $name = pop @parts;
if ($name eq q() || $name eq q(.) || $name eq q(..)) {
  fail("must name a new file: $path");
}
sysopen(my $cwd, q(.), O_RDONLY | O_DIRECTORY)
  or fail("cannot open the working directory: $!");
if ($path =~ m{^/}) {
  sysopen(my $root, q(/), O_RDONLY | O_DIRECTORY)
    or fail("cannot open /: $!");
  chdir($root) or fail("cannot enter /: $!");
}
my $walked = $path =~ m{^/} ? q() : q(.);
for my $part (@parts) {
  next if $part eq q() || $part eq q(.);
  $walked .= "/$part";
  my $dir;
  my $opened = sysopen($dir, $part, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  if (!$opened && $! == ENOENT) {
    mkdir($part) or $! == EEXIST
      or fail("parent cannot be created: $walked: $!");
    $opened = sysopen($dir, $part, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  }
  if (!$opened) {
    fail("must not traverse a symlink: $walked") if -l $part;
    fail("parent is not a directory: $walked: $!");
  }
  chdir($dir) or fail("cannot enter $walked: $!");
}
sysopen(my $log, $name, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0644)
  or fail("must name a new file: $path");
chmod(0644, $log) or fail("cannot set mode on $path: $!");
chdir($cwd) or fail("cannot restore the working directory: $!");
open(STDOUT, q(>&), $log) or fail("cannot redirect stdout: $!");
open(STDERR, q(>&), $log) or fail("cannot redirect stderr: $!");
exec { $ARGV[0] } @ARGV or do {
  print STDERR "[entrypoint] ERROR: cannot run $ARGV[0]: $!\n";
  exit 127;
};
'

# --------------------------------------------------------------------------- #
# Configuration validation
# --------------------------------------------------------------------------- #

# Map known networks to their bundled config paths (from cardano-configs image)
config_path_for_network() {
  case "$1" in
    mainnet) echo "/opt/cardano/config/mainnet/config.json" ;;
    preprod) echo "/opt/cardano/config/preprod/config.json" ;;
    preview) echo "/opt/cardano/config/preview/config.json" ;;
    dijkstra) echo "/opt/cardano/config/dijkstra/config.json" ;;
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
    for known_net in mainnet preprod preview dijkstra; do
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

# Command-line arguments may carry secrets, and xtrace would print them while
# they are expanded below and at launch. Stop tracing here; the bootstrap and
# setup steps above have already been traced.
{ set +x; } 2>/dev/null

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

log "Starting dingo $(redact_args "${DINGO_ARGS[@]}")"

# Start dingo and preserve its exit status, including after a forwarded signal.
if run_managed_dingo "${DINGO_LOG_FILE:-}" "${DINGO_ARGS[@]}"; then
  exit_code=0
else
  exit_code=$?
fi
exit "${exit_code}"
