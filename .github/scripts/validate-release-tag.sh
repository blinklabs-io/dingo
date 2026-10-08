#!/usr/bin/env bash

set -euo pipefail

tag="${1:-}"
core='(0|[1-9][0-9]*)'
prerelease='([0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*|0|[1-9][0-9]*)'
build='[0-9A-Za-z-]+'
semver="^v${core}\\.${core}\\.${core}(-${prerelease}(\\.${prerelease})*)?(\\+${build}(\\.${build})*)?$"

if [[ ! "$tag" =~ $semver ]]; then
  echo "invalid release tag: $tag" >&2
  exit 1
fi

printf '%s\n' "${tag#v}"
