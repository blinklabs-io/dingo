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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
LOAD_LOG="$(mktemp "${TMPDIR:-/tmp}/dingo-test-load.XXXXXX.log")"

cleanup() {
	rm -f "${LOAD_LOG}"
}
trap cleanup EXIT

cd "${PROJECT_ROOT}"
./dingo load database/immutable/testdata 2>&1 | tee "${LOAD_LOG}"

if grep -Fq \
	"Plutus evaluation disagrees with block producer" \
	"${LOAD_LOG}"; then
	echo "test-load: Plutus accounting diverged from the block producer" >&2
	exit 1
fi
