#!/usr/bin/env bash
#
# Generate the CBOR, ledger-rules, UPLC conformance and UPLC turbo reports for
# a release tag.
#
# Reporting never stops a release. A generator exits non-zero when it records
# failures, and that is fine: the report file is what counts. When a generator
# crashes or its inputs cannot be prepared, an error report naming the scenario
# and the cause is written in its place so every report kind is still
# published.
#
# The UPLC turbo run is Linux-only and measures an evenly spaced subset of the
# corpus, because the whole archive takes hours.
#
# Inputs:
#   RELEASE_TAG           required, for example v0.71.0
#   DIST_DIR              optional, defaults to dist
#   CONFORMANCE_DIR       optional, defaults to .conformance; holds the
#                         amaru and node-diversity-conformance checkouts
#   CBOR_DATASET_DIR      required, the checked out cardano-cbor-dataset
#   CBOR_DATASET_REVISION required, the pinned dataset commit
#   TURBO_URL             required, the turbo corpus archive
#   TURBO_SHA256          required, digest of that archive
#   TURBO_SAMPLES         optional, timed samples per program, defaults to 10
#   TURBO_MAX_PROGRAMS    optional, programs measured, defaults to 10000

set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG must be set}"
: "${CBOR_DATASET_DIR:?CBOR_DATASET_DIR must be set}"
: "${CBOR_DATASET_REVISION:?CBOR_DATASET_REVISION must be set}"
: "${TURBO_URL:?TURBO_URL must be set}"
: "${TURBO_SHA256:?TURBO_SHA256 must be set}"

DIST_DIR="${DIST_DIR:-dist}"
CONFORMANCE_DIR="${CONFORMANCE_DIR:-.conformance}"
TURBO_SAMPLES="${TURBO_SAMPLES:-10}"
TURBO_MAX_PROGRAMS="${TURBO_MAX_PROGRAMS:-10000}"

version="${RELEASE_TAG#v}"
report_date="$(date -u +%Y%m%d)"
prefix="${DIST_DIR}/dingo-${version}-${report_date}"

# write_error_report REPORT SCENARIO MESSAGE
# Writes a report that carries no results, only the reason there are none.
write_error_report() {
  local report="$1" scenario="$2" message="$3"
  jq -n \
    --arg scenario "$scenario" \
    --arg generated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg version "$version" \
    --arg error "$message" \
    '{schema_version: "1", scenario: $scenario, generated_at: $generated_at,
      implementation: {name: "dingo", version: $version}, error: $error}' \
    >"$report"
  echo "::warning::${scenario} report could not be generated: ${message}" >&2
}

# log_excerpt FILE
# Prints a log on one line, keeping the start, where a panic message is, and
# the end of a long one.
log_excerpt() {
  if [ "$(wc -l <"$1")" -gt 8 ]; then
    { head -n 3 "$1"; echo ...; tail -n 5 "$1"; } | tr '\n' ' '
  else
    tr '\n' ' ' <"$1"
  fi
}

# run_generator REPORT SCENARIO COMMAND...
# Runs a generator that writes REPORT. A non-zero exit with the report written
# is a report of failures and is kept. Without a report, an error report is
# written instead.
run_generator() {
  local report="$1" scenario="$2"
  shift 2
  rm -f "$report"
  local log status=0
  log="$(mktemp)"
  "$@" 2>"$log" || status=$?
  cat "$log" >&2
  if [ ! -s "$report" ]; then
    write_error_report "$report" "$scenario" \
      "generator exited ${status} without writing a report: $(log_excerpt "$log")"
  elif ! jq -e -s 'length == 1 and (.[0] | type == "object")' "$report" >/dev/null 2>&1; then
    write_error_report "$report" "$scenario" \
      "generator exited ${status} with an invalid report: $(log_excerpt "$log")"
  elif [ "$status" -ne 0 ]; then
    echo "generator reported failures (exit ${status}); keeping ${report}" >&2
  fi
  rm -f "$log"
}

# prepare_turbo_corpus DIR
# Downloads, verifies and unpacks the turbo corpus into DIR/corpus.
prepare_turbo_corpus() {
  local dir="$1"
  curl --fail --silent --show-error --location --retry 3 -o "${dir}/turbo.tar.xz" "$TURBO_URL" &&
    echo "${TURBO_SHA256}  ${dir}/turbo.tar.xz" | sha256sum --check --strict &&
    mkdir "${dir}/corpus" &&
    tar -xf "${dir}/turbo.tar.xz" -C "${dir}/corpus"
}

mkdir -p "$DIST_DIR"

dataset_log="$(mktemp)"
if ! dataset_revision="$(git -C "$CBOR_DATASET_DIR" rev-parse HEAD 2>"$dataset_log")"; then
  write_error_report "${prefix}-cbor-conformance.json" cbor-decoding \
    "cannot read the CBOR dataset revision: $(log_excerpt "$dataset_log")"
elif [ "$dataset_revision" = "$CBOR_DATASET_REVISION" ]; then
  run_generator "${prefix}-cbor-conformance.json" cbor-decoding \
    go run ./internal/cborconformance/cmd \
    -dataset "${CBOR_DATASET_DIR}/dataset/conway" \
    -corpus-revision "$dataset_revision" \
    -output "${prefix}-cbor-conformance.json"
else
  write_error_report "${prefix}-cbor-conformance.json" cbor-decoding \
    "CBOR dataset checkout does not match the pinned revision ${CBOR_DATASET_REVISION}"
fi

run_generator "${prefix}-ledger-rules.json" ledger-rules \
  go run ./internal/ledgerrulesconformance/cmd \
  -fixtures "${CONFORMANCE_DIR}/node-diversity-conformance/scenarios/ledger-rules/fixtures" \
  -output "${prefix}-ledger-rules.json"

run_generator "${prefix}-uplc-conformance.json" uplc-conformance \
  go run ./internal/uplcconformance/cmd \
  -corpus "${CONFORMANCE_DIR}/amaru/crates/amaru-uplc/tests" \
  -output "${prefix}-uplc-conformance.json"

turbo_dir="$(mktemp -d)"
trap 'rm -rf "$turbo_dir"' EXIT
if prepare_turbo_corpus "$turbo_dir" 2>"${turbo_dir}/prepare.log"; then
  run_generator "${prefix}-uplc-turbo.json" uplc-turbo \
    go run ./internal/uplcconformance/turbocmd \
    -corpus "${turbo_dir}/corpus" \
    -corpus-digest "$TURBO_SHA256" \
    -version "$version" \
    -samples "$TURBO_SAMPLES" \
    -max-programs "$TURBO_MAX_PROGRAMS" \
    -output "${prefix}-uplc-turbo.json"
else
  write_error_report "${prefix}-uplc-turbo.json" uplc-turbo \
    "turbo corpus could not be downloaded, verified or unpacked: $(log_excerpt "${turbo_dir}/prepare.log")"
fi

shopt -s nullglob
for report in "${DIST_DIR}"/dingo-*.json; do
  gzip -n -9 -f "$report"
done
shopt -u nullglob

ls -l "${DIST_DIR}"/dingo-*.json.gz
