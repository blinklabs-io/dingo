#!/usr/bin/env bash
#
# Upload every binary archive and the optional conformance reports (CBOR,
# ledger rules, UPLC conformance and UPLC turbo) to the draft release.
#
# Split out of the per-target build-binaries matrix so one job can upload the
# several archives it now builds. The matrix version uploaded exactly one file
# per job, inline.
#
# The upload curl is checked. The inline version it replaces had no --fail, so
# an upload that came back 4xx or 5xx still exited 0 and finalize-release
# published a release missing that asset with nothing in the log to say so.
#
# Inputs:
#   RELEASE_ID            required, from create-draft-release
#   RELEASE_UPLOAD_TOKEN  required, GITHUB_TOKEN with contents: write
#   REPOSITORY_OWNER      required, github.repository_owner
#   APPLICATION_NAME      optional, defaults to dingo; also the repository name
#   DIST_DIR                      optional, defaults to dist
#   REQUIRE_CONFORMANCE_REPORT    optional, requires exactly one report of each
#                                 kind when true; a report is .json or .json.gz

set -euo pipefail

: "${RELEASE_ID:?RELEASE_ID must be set}"
: "${RELEASE_UPLOAD_TOKEN:?RELEASE_UPLOAD_TOKEN must be set}"
: "${REPOSITORY_OWNER:?REPOSITORY_OWNER must be set}"

APPLICATION_NAME="${APPLICATION_NAME:-dingo}"
DIST_DIR="${DIST_DIR:-dist}"

shopt -s nullglob
archives=("${DIST_DIR}"/*.tar.gz)
report_kinds=(cbor-conformance ledger-rules uplc-conformance uplc-turbo)
reports=()
for kind in "${report_kinds[@]}"; do
  kind_reports=("${DIST_DIR}/${APPLICATION_NAME}"-*-"${kind}".json "${DIST_DIR}/${APPLICATION_NAME}"-*-"${kind}".json.gz)
  if [ "${#kind_reports[@]}" -gt 1 ]; then
    echo "expected at most one ${APPLICATION_NAME} ${kind} report in ${DIST_DIR}" >&2
    exit 1
  fi
  if [ "${REQUIRE_CONFORMANCE_REPORT:-false}" = "true" ] && [ "${#kind_reports[@]}" -ne 1 ]; then
    echo "expected one ${APPLICATION_NAME} ${kind} report in ${DIST_DIR}" >&2
    exit 1
  fi
  # Bash 3.2 treats an empty array expansion as unset under nounset.
  if [ "${#kind_reports[@]}" -gt 0 ]; then
    reports+=("${kind_reports[@]}")
  fi
done
shopt -u nullglob

if [ "${#archives[@]}" -eq 0 ]; then
  echo "no archives found in ${DIST_DIR}; build-targets.sh runs with RELEASE_TAG set on a tag" >&2
  exit 1
fi

upload_url="https://uploads.github.com/repos/${REPOSITORY_OWNER}/${APPLICATION_NAME}/releases/${RELEASE_ID}/assets"

upload_asset() {
  asset="$1"
  name="$(basename "$asset")"
  content_type="application/octet-stream"
  case "$name" in
    *.json) content_type="application/json" ;;
    *.json.gz) content_type="application/gzip" ;;
  esac
  echo "uploading ${name}"
  curl \
    --silent \
    --show-error \
    --fail-with-body \
    --retry 3 \
    --retry-connrefused \
    -H "Authorization: token ${RELEASE_UPLOAD_TOKEN}" \
    -H "Content-Type: ${content_type}" \
    --data-binary "@${asset}" \
    "${upload_url}?name=${name}"
  echo
}

for archive in "${archives[@]}"; do
  upload_asset "$archive"
done
# Bash 3.2 treats an empty array expansion as unset under nounset.
if [ "${#reports[@]}" -gt 0 ]; then
  for report in "${reports[@]}"; do
    upload_asset "$report"
  done
fi

echo "uploaded ${#archives[@]} archive(s) and ${#reports[@]} conformance report(s) to release ${RELEASE_ID}"
