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
GATE_LOG="$(mktemp "${TMPDIR:-/tmp}/dingo-test-load-gate.XXXXXX.log")"

cleanup() {
	rm -f "${LOAD_LOG}" "${GATE_LOG}"
}
trap cleanup EXIT

cd "${PROJECT_ROOT}"

# The producer-disagreement warning below detects over-accounting. Exercise a
# Haskell-derived exact-cost case first so this gate also detects a missing
# successful-return CEK flush, which would under-account without a warning.
#
# `go test -run` exits 0 when the pattern matches nothing, so a rename or a
# move would turn this gate into a silent no-op. Require the named test to
# have actually reported a pass. TestLoadGateGuardTestExists keeps the name
# below in step with the test itself.
GATE_TEST="TestPlutusBudgetComparisonIncludesFinalSlippageBatch"

go test \
	-count=1 \
	-v \
	-run "^${GATE_TEST}\$" \
	./ledger/eras 2>&1 | tee "${GATE_LOG}"

if ! grep -Fq -- "--- PASS: ${GATE_TEST}" "${GATE_LOG}"; then
	echo "test-load: ${GATE_TEST} did not run; the" \
		"under-accounting gate is not being exercised" >&2
	exit 1
fi

./dingo load database/immutable/testdata 2>&1 | tee "${LOAD_LOG}"

# Kept in step with the Warn call in ledger/state.go by
# TestLoadGateWarningMatchesLedger; a reworded log line would otherwise make
# this grep silently pass while accounting had diverged.
DISAGREEMENT_MARKER="Plutus evaluation disagrees with block producer"

if grep -Fq -- "${DISAGREEMENT_MARKER}" "${LOAD_LOG}"; then
	echo "test-load: Plutus accounting diverged from the block producer" >&2
	exit 1
fi
