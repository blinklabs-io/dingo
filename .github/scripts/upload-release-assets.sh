#!/usr/bin/env bash
#
# Upload every archive build-targets.sh produced to the draft release.
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
#   DIST_DIR              optional, defaults to dist

set -euo pipefail

: "${RELEASE_ID:?RELEASE_ID must be set}"
: "${RELEASE_UPLOAD_TOKEN:?RELEASE_UPLOAD_TOKEN must be set}"
: "${REPOSITORY_OWNER:?REPOSITORY_OWNER must be set}"

APPLICATION_NAME="${APPLICATION_NAME:-dingo}"
DIST_DIR="${DIST_DIR:-dist}"

shopt -s nullglob
archives=("${DIST_DIR}"/*.tar.gz)
shopt -u nullglob

if [ "${#archives[@]}" -eq 0 ]; then
  echo "no archives found in ${DIST_DIR}; build-targets.sh runs with RELEASE_TAG set on a tag" >&2
  exit 1
fi

upload_url="https://uploads.github.com/repos/${REPOSITORY_OWNER}/${APPLICATION_NAME}/releases/${RELEASE_ID}/assets"

for archive in "${archives[@]}"; do
  name="$(basename "$archive")"
  echo "uploading ${name}"
  curl \
    --silent \
    --show-error \
    --fail-with-body \
    --retry 3 \
    --retry-connrefused \
    -H "Authorization: token ${RELEASE_UPLOAD_TOKEN}" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@${archive}" \
    "${upload_url}?name=${name}"
  echo
done

echo "uploaded ${#archives[@]} archive(s) to release ${RELEASE_ID}"
