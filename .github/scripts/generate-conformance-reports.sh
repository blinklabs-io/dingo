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
# The UPLC turbo run is Linux-only and measures up to 10,000 evenly spaced
# programs from the corpus.
#
# Inputs:
#   RELEASE_TAG           required, for example v0.71.0
#   DIST_DIR              optional, defaults to dist
#   CONFORMANCE_DIR       optional, defaults to .conformance; holds the
#                         node-diversity-conformance checkout
#   CBOR_DATASET_DIR      required, the checked out cardano-cbor-dataset
#   CBOR_DATASET_REVISION required, the pinned dataset commit
#   UPLC_CORPUS_SHA256     required, digest of the NDC UPLC corpus archive
#   TURBO_CORPUS_SHA256    required, digest of the NDC Turbo corpus archive
#   TURBO_SAMPLES         optional, timed samples per program, defaults to 50
#   TURBO_MAX_PROGRAMS    optional, programs measured, defaults to 10000

set -euo pipefail

: "${RELEASE_TAG:?RELEASE_TAG must be set}"
: "${CBOR_DATASET_DIR:?CBOR_DATASET_DIR must be set}"
: "${CBOR_DATASET_REVISION:?CBOR_DATASET_REVISION must be set}"
: "${UPLC_CORPUS_SHA256:?UPLC_CORPUS_SHA256 must be set}"
: "${TURBO_CORPUS_SHA256:?TURBO_CORPUS_SHA256 must be set}"

DIST_DIR="${DIST_DIR:-dist}"
CONFORMANCE_DIR="${CONFORMANCE_DIR:-.conformance}"
TURBO_SAMPLES="${TURBO_SAMPLES:-50}"
TURBO_MAX_PROGRAMS="${TURBO_MAX_PROGRAMS:-10000}"

# The report step has a 60-minute limit. Generator budgets total 43 minutes;
# corpus preparation is capped at six minutes, leaving 11 minutes for gzip.
CBOR_GENERATOR_TIMEOUT_SECONDS=120
LEDGER_GENERATOR_TIMEOUT_SECONDS=120
UPLC_GENERATOR_TIMEOUT_SECONDS=240
TURBO_GENERATOR_TIMEOUT_SECONDS=2100
UPLC_CORPUS_TIMEOUT_SECONDS=180
TURBO_CORPUS_TIMEOUT_SECONDS=180

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
    --arg error "$message" \
    '{version: "1", scenario: $scenario, generated_at: $generated_at,
      implementation: {name: "dingo"}, error: $error}' \
    >"$report"
  echo "::warning::${scenario} report could not be generated: ${message}" >&2
}

# redact_log
# Removes workspace and temporary paths from public report diagnostics.
redact_log() {
  sed "s|$PWD|<workspace>|g; s|${TMPDIR:-/tmp}/[^[:space:]]*|<temporary>|g; s|/tmp/[^[:space:]]*|<temporary>|g"
}

# log_excerpt FILE
# Prints a log on one line, keeping the start, where a panic message is, and
# the end of a long one.
log_excerpt() {
  if [ "$(wc -l <"$1")" -gt 8 ]; then
    { head -n 3 "$1"; echo ...; tail -n 5 "$1"; } | redact_log | tr '\n' ' '
  else
    redact_log <"$1" | tr '\n' ' '
  fi
}

# valid_report REPORT SCENARIO
# Rejects incomplete generator output before it can replace a report error.
valid_report() {
  local report="$1" scenario="$2"
  jq -e -s --arg scenario "$scenario" '
    def common:
      .version == "1" and
      .scenario == $scenario and
      (.generated_at | type == "string" and length > 0);
    def nullable_number: type == "number" or . == null;
    length == 1 and
    (.[0] as $report |
      ($report | common) and
      if ($report.error | type) == "string" then
        ($report.error | length > 0) and
        ($report.implementation.name | type == "string" and length > 0) and
        (["benchmarks", "scenarios", "failures", "totals"] |
          all(. as $key | $report | has($key) | not))
      else
        ($report | if $scenario == "cbor-decoding" then
          (.corpus | type == "string" and length > 0) and
          (.corpus_revision | type == "string" and length > 0) and
          (.protocol_version | type == "string" and length > 0) and
          (.successful | type == "boolean") and
          (.totals | type == "object") and
          (.rules | type == "object") and
          (.failures | type == "array")
        elif $scenario == "ledger-rules" then
          (.implementation.name | type == "string" and length > 0) and
          (.scenarios | type == "array") and
          all(.scenarios[]; (.id | type == "string") and (.result | type == "string"))
        elif $scenario == "uplc-conformance" then
          (.implementation.name | type == "string" and length > 0) and
          (.scenarios | type == "array") and
          all(.scenarios[];
            (.id | type == "string") and
            (.title | type == "string") and
            (.group | type == "string") and
            (.plutus_version | type == "string") and
            (.protocol_version | type == "string") and
            (.evaluation | type == "string") and
            ((.budget == null) or
              ((.budget | type == "object") and
                (.budget.cpu | type == "number") and
                (.budget.mem | type == "number")))
          )
        elif $scenario == "uplc-turbo" then
          (.implementation.name | type == "string" and length > 0) and
          (.machine | type == "object") and
          (.benchmarks | type == "array") and
          all(.benchmarks[];
            (.bench | type == "string") and
            (.plutus_version | type == "string") and
            (.protocol_version | type == "string") and
            (.budget_cpu | nullable_number) and
            (.budget_mem | nullable_number) and
            (.cpu_us | type == "number") and
            (.mem_bytes | nullable_number)
          )
        else false end)
      end)
  ' "$report" >/dev/null 2>&1
}

# run_generator REPORT SCENARIO TIMEOUT_SECONDS COMMAND...
# Runs a generator that writes REPORT. A non-zero exit with the report written
# is a report of failures and is kept. Without a report, an error report is
# written instead. Timed-out generators always get an error report, even if
# they left behind a syntactically valid partial report.
run_generator() {
  local report="$1" scenario="$2"
  local timeout_seconds="$3"
  shift 3
  rm -f "$report"
  local log status=0
  log="$(mktemp)"
  timeout --signal=TERM --kill-after=5s "${timeout_seconds}s" "$@" 2>"$log" || status=$?
  redact_log <"$log" >&2
  if [ "$status" -eq 124 ]; then
    write_error_report "$report" "$scenario" \
      "generator exceeded its ${timeout_seconds}s time limit (exit ${status}): $(log_excerpt "$log")"
  elif [ "$status" -eq 137 ]; then
    write_error_report "$report" "$scenario" \
      "generator ended with exit 137 (SIGKILL) before completing: $(log_excerpt "$log")"
  elif [ ! -s "$report" ]; then
    write_error_report "$report" "$scenario" \
      "generator exited ${status} without writing a report: $(log_excerpt "$log")"
  elif ! valid_report "$report" "$scenario"; then
    write_error_report "$report" "$scenario" \
      "generator exited ${status} with an invalid report: $(log_excerpt "$log")"
  elif [ "$status" -ne 0 ]; then
    echo "generator reported failures (exit ${status}); keeping ${report}" >&2
  fi
  rm -f "$log"
}

# prepare_ndc_corpus DIR ARCHIVE SHA256 TIMEOUT_SECONDS
# Downloads, verifies and unpacks an NDC corpus archive into DIR/corpus.
prepare_ndc_corpus() {
  local dir="$1"
  local archive="$2"
  local sha256="$3"
  local timeout_seconds="$4"
  timeout --signal=TERM --kill-after=5s "${timeout_seconds}s" \
    bash -s -- "$dir" "$archive" "$sha256" <<'PREPARE_NDC_CORPUS'
set -euo pipefail
dir="$1"
archive="$2"
sha256="$3"
base_url="https://pub-2239d82d9a074482b2eb2c886191cb4e.r2.dev"
curl --fail --silent --show-error --location --retry 3 \
  -o "${dir}/${archive}" "${base_url}/${archive}"
echo "${sha256}  ${dir}/${archive}" | sha256sum --check --strict
mkdir -p "${dir}/corpus"
zstd -dc "${dir}/${archive}" | tar -xf - -C "${dir}/corpus"
PREPARE_NDC_CORPUS
}

mkdir -p "$DIST_DIR"

dataset_log="$(mktemp)"
if ! dataset_revision="$(git -C "$CBOR_DATASET_DIR" rev-parse HEAD 2>"$dataset_log")"; then
  write_error_report "${prefix}-cbor-conformance.json" cbor-decoding \
    "cannot read the CBOR dataset revision: $(log_excerpt "$dataset_log")"
elif [ "$dataset_revision" = "$CBOR_DATASET_REVISION" ]; then
  run_generator "${prefix}-cbor-conformance.json" cbor-decoding "$CBOR_GENERATOR_TIMEOUT_SECONDS" \
    go run ./internal/cborconformance/cmd \
    -dataset "${CBOR_DATASET_DIR}/dataset/conway" \
    -corpus-revision "$dataset_revision" \
    -output "${prefix}-cbor-conformance.json"
else
  write_error_report "${prefix}-cbor-conformance.json" cbor-decoding \
    "CBOR dataset checkout does not match the pinned revision ${CBOR_DATASET_REVISION}"
fi
rm -f "$dataset_log"

run_generator "${prefix}-ledger-rules.json" ledger-rules "$LEDGER_GENERATOR_TIMEOUT_SECONDS" \
  go run ./internal/ledgerrulesconformance/cmd \
  -fixtures "${CONFORMANCE_DIR}/node-diversity-conformance/scenarios/ledger-rules/fixtures" \
  -output "${prefix}-ledger-rules.json"

uplc_dir="$(mktemp -d)"
turbo_dir="$(mktemp -d)"
trap 'rm -rf "$uplc_dir" "$turbo_dir"' EXIT
if prepare_ndc_corpus "$uplc_dir" uplc-conformance.tar.zst "$UPLC_CORPUS_SHA256" \
  "$UPLC_CORPUS_TIMEOUT_SECONDS" \
  2>"${uplc_dir}/prepare.log"; then
  run_generator "${prefix}-uplc-conformance.json" uplc-conformance "$UPLC_GENERATOR_TIMEOUT_SECONDS" \
    go run ./internal/uplcconformance/cmd \
    -corpus "${uplc_dir}/corpus" \
    -output "${prefix}-uplc-conformance.json"
else
  prepare_status=$?
  if [ "$prepare_status" -eq 124 ] || [ "$prepare_status" -eq 137 ]; then
    prepare_error="UPLC corpus preparation exceeded its ${UPLC_CORPUS_TIMEOUT_SECONDS}s time limit (exit ${prepare_status})"
  else
    prepare_error="UPLC corpus could not be downloaded, verified or unpacked (exit ${prepare_status})"
  fi
  write_error_report "${prefix}-uplc-conformance.json" uplc-conformance \
    "${prepare_error}: $(log_excerpt "${uplc_dir}/prepare.log")"
fi

if prepare_ndc_corpus "$turbo_dir" turbo.tar.zst "$TURBO_CORPUS_SHA256" \
  "$TURBO_CORPUS_TIMEOUT_SECONDS" \
  2>"${turbo_dir}/prepare.log"; then
  run_generator "${prefix}-uplc-turbo.json" uplc-turbo "$TURBO_GENERATOR_TIMEOUT_SECONDS" \
    go run ./internal/uplcconformance/turbocmd \
    -corpus "${turbo_dir}/corpus" \
    -samples "$TURBO_SAMPLES" \
    -max-programs "$TURBO_MAX_PROGRAMS" \
    -output "${prefix}-uplc-turbo.json"
else
  prepare_status=$?
  if [ "$prepare_status" -eq 124 ] || [ "$prepare_status" -eq 137 ]; then
    prepare_error="turbo corpus preparation exceeded its ${TURBO_CORPUS_TIMEOUT_SECONDS}s time limit (exit ${prepare_status})"
  else
    prepare_error="turbo corpus could not be downloaded, verified or unpacked (exit ${prepare_status})"
  fi
  write_error_report "${prefix}-uplc-turbo.json" uplc-turbo \
    "${prepare_error}: $(log_excerpt "${turbo_dir}/prepare.log")"
fi

shopt -s nullglob
for report in "${DIST_DIR}"/dingo-*.json; do
  gzip -n -9 -f "$report"
done
shopt -u nullglob

ls -l "${DIST_DIR}"/dingo-*.json.gz
