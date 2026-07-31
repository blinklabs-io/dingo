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

# Classify a change set as consensus-sensitive (DevNet suites must run) or not.
#
# Used by the classify job in .github/workflows/devnet.yml and reproducible
# locally:
#
#   # classify an explicit list of paths
#   printf 'ledger/chainsync.go\n' | ./.github/scripts/devnet-path-filter.sh -
#
#   # classify the current branch against main
#   git diff --name-only origin/main...HEAD > /tmp/changed.txt
#   ./.github/scripts/devnet-path-filter.sh /tmp/changed.txt
#
# With no argument it derives the change set from the GitHub Actions event: any
# event other than pull_request runs both suites unconditionally, and for pull
# requests the changed files come from the GitHub API (falling back to git).
#
# Outputs "run_devnet" (true/false) and "reason" to $GITHUB_OUTPUT when set, and
# always prints the decision. Anything it cannot determine fails safe by
# selecting the suites.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
PATTERN_FILE="${DEVNET_PATTERN_FILE:-${REPO_ROOT}/.github/devnet-paths.txt}"

log() { echo "[devnet-path-filter] $*"; }

if [[ ! -f "${PATTERN_FILE}" ]]; then
  echo "[devnet-path-filter] ERROR: pattern file not found: ${PATTERN_FILE}" >&2
  exit 1
fi

# Strip comments and blank lines into a grep -E pattern file.
PATTERNS_TMP="$(mktemp)"
trap 'rm -f "${PATTERNS_TMP}"' EXIT
grep -v -E '^[[:space:]]*(#|$)' "${PATTERN_FILE}" >"${PATTERNS_TMP}" || [[ $? -eq 1 ]]
if [[ ! -s "${PATTERNS_TMP}" ]]; then
  echo "[devnet-path-filter] ERROR: ${PATTERN_FILE} contains no patterns" >&2
  exit 1
fi

emit() {
  local run_devnet="$1" reason="$2"
  log "run_devnet=${run_devnet} (${reason})"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    {
      echo "run_devnet=${run_devnet}"
      echo "reason=${reason}"
    } >>"${GITHUB_OUTPUT}"
  fi
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    echo "DevNet path classification: run_devnet=${run_devnet} (${reason})" \
      >>"${GITHUB_STEP_SUMMARY}"
  fi
}

# --------------------------------------------------------------------------- #
# Collect the change set
# --------------------------------------------------------------------------- #

CHANGED_TMP="$(mktemp)"
trap 'rm -f "${PATTERNS_TMP}" "${CHANGED_TMP}"' EXIT

if [[ $# -gt 0 ]]; then
  if [[ "$1" == "-" ]]; then
    cat >"${CHANGED_TMP}"
  else
    cat "$1" >"${CHANGED_TMP}"
  fi
else
  EVENT="${GITHUB_EVENT_NAME:-}"
  if [[ -n "${EVENT}" && "${EVENT}" != "pull_request" && "${EVENT}" != "pull_request_target" ]]; then
    # Pushes to main, release tags, the scheduled run, and manual dispatches
    # always run both suites: path filtering exists only to spare unrelated
    # pull requests, and the scheduled run is what catches filter omissions.
    emit true "event ${EVENT} always runs both suites"
    exit 0
  fi
  PR_NUMBER="${PR_NUMBER:-}"
  if [[ -z "${PR_NUMBER}" && -n "${GITHUB_EVENT_PATH:-}" && -f "${GITHUB_EVENT_PATH}" ]]; then
    PR_NUMBER="$(jq -r '.pull_request.number // empty' "${GITHUB_EVENT_PATH}")"
  fi
  if [[ -n "${PR_NUMBER}" && -n "${GITHUB_REPOSITORY:-}" ]] && command -v gh &>/dev/null; then
    log "listing changed files for pull request #${PR_NUMBER}"
    if ! gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}/files" \
      --paginate --jq '.[].filename' >"${CHANGED_TMP}"; then
      log "WARNING: unable to list changed files from the API"
      : >"${CHANGED_TMP}"
    fi
  fi
  if [[ ! -s "${CHANGED_TMP}" ]]; then
    BASE_REF="${GITHUB_BASE_REF:-main}"
    log "falling back to git diff against ${BASE_REF}"
    if ! git -C "${REPO_ROOT}" diff --name-only "origin/${BASE_REF}...HEAD" \
      >"${CHANGED_TMP}" 2>/dev/null; then
      : >"${CHANGED_TMP}"
    fi
  fi
  if [[ ! -s "${CHANGED_TMP}" ]]; then
    # Never let an undetermined change set silently skip the suites.
    emit true "could not determine the changed files; selecting both suites"
    exit 0
  fi
fi

CHANGED_COUNT="$(wc -l <"${CHANGED_TMP}" | tr -d ' ')"
log "changed files: ${CHANGED_COUNT}"

# --------------------------------------------------------------------------- #
# Match
# --------------------------------------------------------------------------- #

MATCHED="$(grep -E -f "${PATTERNS_TMP}" "${CHANGED_TMP}" || true)"

if [[ -n "${MATCHED}" ]]; then
  MATCHED_COUNT="$(printf '%s\n' "${MATCHED}" | wc -l | tr -d ' ')"
  log "consensus-sensitive paths changed:"
  printf '%s\n' "${MATCHED}" | head -n 40 | sed 's/^/  /'
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo "### DevNet gate: consensus-sensitive paths changed"
      echo
      printf '%s\n' "${MATCHED}" | head -n 40 | sed 's/^/- `/;s/$/`/'
    } >>"${GITHUB_STEP_SUMMARY}"
  fi
  emit true "${MATCHED_COUNT} of ${CHANGED_COUNT} changed paths are consensus-sensitive"
  exit 0
fi

log "no consensus-sensitive paths changed"
emit false "none of the ${CHANGED_COUNT} changed paths are consensus-sensitive"
