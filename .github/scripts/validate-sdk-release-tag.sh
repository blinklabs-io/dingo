#!/usr/bin/env bash

set -euo pipefail

# Validates a plugin SDK release tag and prints the module version it names.
# The SDK stays at major version 0, with every contract experimental, until a
# 1.0 is declared; declaring it means changing the major-version check here.

tag="${1:-}"
case "$tag" in
  sdk/v0.*) ;;
  *)
    echo "invalid SDK release tag: $tag (want sdk/v0.MINOR.PATCH)" >&2
    exit 1
    ;;
esac

version="$("$(dirname "$0")/validate-release-tag.sh" "${tag#sdk/}")"
printf 'v%s\n' "$version"
