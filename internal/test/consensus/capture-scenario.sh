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

# Thin dispatcher: resolves scenarios/<name>/run.sh and execs it. Each
# scenario owns its own orchestration shape (stack lifecycle, number of
# sidecar invocations, optional composer step). See README.md for the
# rationale and how to add a new scenario.
#
# Usage:
#   ./capture-scenario.sh <scenario-name> [extra args passed to run.sh]

set -euo pipefail

SCENARIO="${1:-}"
if [[ -z "${SCENARIO}" ]]; then
    echo "usage: $0 <scenario-name> [extra args]" >&2
    echo "" >&2
    echo "Available scenarios:" >&2
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ -d "${SCRIPT_DIR}/scenarios" ]]; then
        for dir in "${SCRIPT_DIR}/scenarios"/*/; do
            [[ -d "${dir}" ]] || continue
            echo "  - $(basename "${dir}")" >&2
        done
    fi
    exit 1
fi
shift

# Reject anything that could escape the scenarios/ subtree before we
# resolve a path or exec under it. First char must be alphanumeric so
# names like ".." or "-foo" can't slip through the character class.
if [[ ! "${SCENARIO}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
    echo "capture-scenario: invalid scenario name: ${SCENARIO}" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO_DIR="${SCRIPT_DIR}/scenarios/${SCENARIO}"

if [[ ! -d "${SCENARIO_DIR}" ]]; then
    echo "capture-scenario: scenario not found: ${SCENARIO_DIR}" >&2
    exit 1
fi
if [[ ! -x "${SCENARIO_DIR}/run.sh" ]]; then
    echo "capture-scenario: ${SCENARIO_DIR}/run.sh missing or not executable" >&2
    exit 1
fi

exec "${SCENARIO_DIR}/run.sh" "$@"
