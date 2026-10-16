#!/usr/bin/env bash
#
# Generate the ledger-rules, UPLC conformance and UPLC turbo reports for a
# release tag, and compress them alongside the CBOR report.
#
# A report is published even when it records failures: the generators exit
# non-zero for those, so success is the report file existing rather than the
# exit status. A generator that wrote no report is a real error.
#
# The UPLC turbo run is Linux-only and measures an evenly spaced subset of the
# corpus, because the whole archive takes hours.
#
# Inputs:
#   RELEASE_TAG           required, for example v0.71.0
#   DIST_DIR              optional, defaults to dist
#   CONFORMANCE_DIR       optional, defaults to .conformance; holds the
#                         amaru and node-diversity-conformance checkouts
#   TURBO_URL             required, the turbo corpus archive
#   TURBO_SHA256          required, digest of that archive
#   TURBO_SAMPLES         optional, timed samples per program, defaults to 10
#   TURBO_MAX_PROGRAMS    optional, programs measured, defaults to 10000

set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG must be set}"
: "${TURBO_URL:?TURBO_URL must be set}"
: "${TURBO_SHA256:?TURBO_SHA256 must be set}"

DIST_DIR="${DIST_DIR:-dist}"
CONFORMANCE_DIR="${CONFORMANCE_DIR:-.conformance}"
TURBO_SAMPLES="${TURBO_SAMPLES:-10}"
TURBO_MAX_PROGRAMS="${TURBO_MAX_PROGRAMS:-10000}"

version="${RELEASE_TAG#v}"
report_date="$(date -u +%Y%m%d)"
prefix="${DIST_DIR}/dingo-${version}-${report_date}"

# run_generator REPORT COMMAND...
# Runs a generator that writes REPORT, tolerating a non-zero exit when the
# report was written.
run_generator() {
  local report="$1"
  shift
  rm -f "$report"
  local status=0
  "$@" || status=$?
  if [ ! -s "$report" ]; then
    echo "generator exited ${status} without writing ${report}" >&2
    exit 1
  fi
  if [ "$status" -ne 0 ]; then
    echo "generator reported failures (exit ${status}); keeping ${report}" >&2
  fi
}

mkdir -p "$DIST_DIR"

run_generator "${prefix}-ledger-rules.json" \
  go run ./internal/ledgerrulesconformance/cmd \
  -fixtures "${CONFORMANCE_DIR}/node-diversity-conformance/scenarios/ledger-rules/fixtures" \
  -output "${prefix}-ledger-rules.json"

run_generator "${prefix}-uplc-conformance.json" \
  go run ./internal/uplcconformance/cmd \
  -corpus "${CONFORMANCE_DIR}/amaru/crates/amaru-uplc/tests" \
  -output "${prefix}-uplc-conformance.json"

turbo_dir="$(mktemp -d)"
trap 'rm -rf "$turbo_dir"' EXIT
curl --fail --show-error --location --retry 3 -o "${turbo_dir}/turbo.tar.xz" "$TURBO_URL"
echo "${TURBO_SHA256}  ${turbo_dir}/turbo.tar.xz" | sha256sum --check --strict
mkdir "${turbo_dir}/corpus"
tar -xf "${turbo_dir}/turbo.tar.xz" -C "${turbo_dir}/corpus"

run_generator "${prefix}-uplc-turbo.json" \
  go run ./internal/uplcconformance/turbocmd \
  -corpus "${turbo_dir}/corpus" \
  -corpus-digest "$TURBO_SHA256" \
  -version "$version" \
  -samples "$TURBO_SAMPLES" \
  -max-programs "$TURBO_MAX_PROGRAMS" \
  -output "${prefix}-uplc-turbo.json"

shopt -s nullglob
for report in "${DIST_DIR}"/dingo-*-conformance.json "${DIST_DIR}"/dingo-*-ledger-rules.json "${DIST_DIR}"/dingo-*-uplc-turbo.json; do
  gzip -n -9 -f "$report"
done
shopt -u nullglob

ls -l "${DIST_DIR}"/dingo-*.json.gz
