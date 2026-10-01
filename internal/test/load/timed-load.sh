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

# Times `dingo load` against an immutable directory in a fresh data directory
# and reports chunk count, wall time, blocks/s and peak RSS.
#
#   DINGO_LOAD_IMMUTABLE_DIR  immutable directory to load
#                             (default: database/immutable/testdata)
#   DINGO_LOAD_PROFILE        non-empty: also write cpu.prof and mem.prof
#                             into the run directory
#   DINGO_LOAD_KEEP           non-empty: keep the run directory
#   DINGO_LOAD_MAX_SECONDS    when set, exit non-zero if the load takes longer

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
IMMUTABLE_DIR="${DINGO_LOAD_IMMUTABLE_DIR:-${PROJECT_ROOT}/database/immutable/testdata}"
DINGO_BIN="${DINGO_BIN:-${PROJECT_ROOT}/dingo}"

if [[ ! -d "${IMMUTABLE_DIR}" ]]; then
	echo "timed-load: immutable directory not found: ${IMMUTABLE_DIR}" >&2
	exit 1
fi
if [[ ! -x "${DINGO_BIN}" ]]; then
	echo "timed-load: ${DINGO_BIN} is not executable; run make build" >&2
	exit 1
fi
IMMUTABLE_DIR="$(cd "${IMMUTABLE_DIR}" && pwd)"

# One .chunk file per chunk; the primary and secondary index files do not
# count.
CHUNKS="$(find "${IMMUTABLE_DIR}" -maxdepth 1 -name '*.chunk' | wc -l | tr -d ' ')"
if [[ "${CHUNKS}" -eq 0 ]]; then
	echo "timed-load: no .chunk files in ${IMMUTABLE_DIR}" >&2
	exit 1
fi

# dingo resolves its database path (.dingo) and any dingo.yaml relative to
# the working directory, so a fresh working directory is a fresh data dir.
RUN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dingo-timed-load.XXXXXX")"
cleanup() {
	if [[ -z "${DINGO_LOAD_KEEP:-}" ]]; then
		rm -rf "${RUN_DIR}"
	fi
}
trap cleanup EXIT

LOAD_LOG="${RUN_DIR}/load.log"
TIME_LOG="${RUN_DIR}/time.log"

ARGS=()
if [[ -n "${DINGO_LOAD_PROFILE:-}" ]]; then
	ARGS+=("--cpuprofile=${RUN_DIR}/cpu.prof" "--memprofile=${RUN_DIR}/mem.prof")
fi
ARGS+=(load "${IMMUTABLE_DIR}")

if /usr/bin/time -v true >/dev/null 2>&1; then
	TIME_FLAVOR=gnu
	TIME_ARGS=(-v -o "${TIME_LOG}")
elif /usr/bin/time -l true >/dev/null 2>&1; then
	TIME_FLAVOR=bsd
	TIME_ARGS=(-l -o "${TIME_LOG}")
else
	TIME_FLAVOR=none
	TIME_ARGS=()
fi

START="$(date +%s)"
cd "${RUN_DIR}"
if [[ "${TIME_FLAVOR}" == none ]]; then
	"${DINGO_BIN}" "${ARGS[@]}" >"${LOAD_LOG}" 2>&1 || {
		cat "${LOAD_LOG}" >&2
		exit 1
	}
else
	/usr/bin/time "${TIME_ARGS[@]}" "${DINGO_BIN}" "${ARGS[@]}" \
		>"${LOAD_LOG}" 2>&1 || {
		cat "${LOAD_LOG}" >&2
		exit 1
	}
fi
END="$(date +%s)"
ELAPSED=$((END - START))

# Kept in step with the Info call in internal/node/load.go by
# TestTimedLoadMarkerMatchesLoader; a reworded log line would otherwise
# report zero blocks.
BLOCKS_MARKER="finished processing blocks from immutable DB"
BLOCKS="$(grep -F -- "${BLOCKS_MARKER}" "${LOAD_LOG}" |
	sed -n 's/.*blocks_copied=\([0-9][0-9]*\).*/\1/p' | tail -n 1)"
if [[ -z "${BLOCKS}" ]]; then
	echo "timed-load: could not find '${BLOCKS_MARKER}' in the load log" >&2
	exit 1
fi

PEAK_RSS_KB="unavailable"
case "${TIME_FLAVOR}" in
gnu)
	PEAK_RSS_KB="$(sed -n 's/.*Maximum resident set size (kbytes): *//p' "${TIME_LOG}")"
	;;
bsd)
	# BSD time reports bytes.
	BYTES="$(awk '/maximum resident set size/ {print $1}' "${TIME_LOG}")"
	PEAK_RSS_KB="$((BYTES / 1024))"
	;;
esac

if [[ "${ELAPSED}" -gt 0 ]]; then
	RATE="$(awk -v b="${BLOCKS}" -v s="${ELAPSED}" 'BEGIN { printf "%.1f", b / s }')"
else
	RATE="n/a (under 1s)"
fi

echo "timed-load: immutable_dir=${IMMUTABLE_DIR}"
echo "timed-load: chunks=${CHUNKS}"
echo "timed-load: blocks=${BLOCKS}"
echo "timed-load: wall_seconds=${ELAPSED}"
echo "timed-load: blocks_per_second=${RATE}"
echo "timed-load: peak_rss_kb=${PEAK_RSS_KB}"
if [[ -n "${DINGO_LOAD_PROFILE:-}" ]]; then
	if [[ -z "${DINGO_LOAD_KEEP:-}" ]]; then
		for f in cpu.prof mem.prof; do
			cp "${RUN_DIR}/${f}" "${PROJECT_ROOT}/${f}"
		done
		echo "timed-load: profiles written to ${PROJECT_ROOT}/{cpu,mem}.prof"
	else
		echo "timed-load: profiles in ${RUN_DIR}"
	fi
fi

if [[ -n "${DINGO_LOAD_MAX_SECONDS:-}" &&
	"${ELAPSED}" -gt "${DINGO_LOAD_MAX_SECONDS}" ]]; then
	echo "timed-load: ${ELAPSED}s exceeds DINGO_LOAD_MAX_SECONDS=${DINGO_LOAD_MAX_SECONDS}" >&2
	exit 1
fi
