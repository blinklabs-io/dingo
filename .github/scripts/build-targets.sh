#!/usr/bin/env bash
#
# Cross-compile every target in BUILD_TARGETS on one runner.
#
# The release matrix used to be one GitHub job per <goos>/<goarch> pair, seven
# jobs that each spent most of their wall time on checkout, toolchain setup and
# module download. Every binary builds CGO_ENABLED=0 (see the $(BINARIES) rule
# in the Makefile), so a runner can produce every target that shares its OS --
# and in fact every target at all. The jobs are still split by runner OS so
# each one can be gated on that OS's own test job, which is the point of the
# split: a Windows test failure must not hold up the Linux binaries.
#
# Inputs:
#   BUILD_TARGETS   required, space-separated <goos>/<goarch> pairs
#   RELEASE_TAG     optional; when set, each target is also packaged as
#                   <app>-<tag>-<goos>-<goarch>.tar.gz for release upload
#   APPLICATION_NAME  optional, defaults to dingo; the binary that ships
#   DIST_DIR        optional, defaults to dist
#
# Outputs, under DIST_DIR:
#   <goos>-<goarch>/<binary>[.exe]   every binary in cmd/, for attestation
#   <app>-<tag>-<goos>-<goarch>.tar.gz   only when RELEASE_TAG is set

set -euo pipefail

: "${BUILD_TARGETS:?BUILD_TARGETS must list <goos>/<goarch> pairs}"

APPLICATION_NAME="${APPLICATION_NAME:-dingo}"
DIST_DIR="${DIST_DIR:-dist}"

# The same list the Makefile's BINARIES builds. Deriving it here rather than
# hardcoding `dingo` keeps a new cmd/ entry cross-compiled for every target
# instead of silently building only on the host platform.
binaries=()
for dir in cmd/*/; do
  name="$(basename "$dir")"
  case "$name" in
    common*) continue ;;
  esac
  binaries+=("$name")
done

if [ "${#binaries[@]}" -eq 0 ]; then
  echo "no binaries found under cmd/" >&2
  exit 1
fi

mkdir -p "$DIST_DIR"

for target in $BUILD_TARGETS; do
  goos="${target%%/*}"
  goarch="${target##*/}"
  if [ "$goos" = "$target" ] || [ -z "$goarch" ]; then
    echo "malformed target ${target}; expected <goos>/<goarch>" >&2
    exit 1
  fi

  suffix=""
  if [ "$goos" = "windows" ]; then
    suffix=".exe"
  fi

  out="${DIST_DIR}/${goos}-${goarch}"
  mkdir -p "$out"

  # make treats a binary that is newer than its sources as up to date, so the
  # previous target's output has to go before the next build or every target
  # after the first would silently ship the first one's GOOS.
  for name in "${binaries[@]}"; do
    rm -f "$name" "${name}.exe"
  done

  echo "::group::build ${goos}/${goarch}"
  GOOS="$goos" GOARCH="$goarch" make build
  echo "::endgroup::"

  for name in "${binaries[@]}"; do
    if [ ! -f "${name}${suffix}" ]; then
      echo "make build produced no ${name}${suffix} for ${target}" >&2
      exit 1
    fi
    mv "${name}${suffix}" "${out}/${name}${suffix}"
  done

  # Only the shipped application is packaged for release. The other cmd/
  # entries are parity and diagnostic tools; they are built above so a broken
  # cross-compile still fails this job, but they are not release artifacts.
  if [ -n "${RELEASE_TAG:-}" ]; then
    tar czf \
      "${DIST_DIR}/${APPLICATION_NAME}-${RELEASE_TAG}-${goos}-${goarch}.tar.gz" \
      -C "$out" "${APPLICATION_NAME}${suffix}"
  fi
done

echo "built targets: ${BUILD_TARGETS}"
ls -la "$DIST_DIR"
