#!/usr/bin/env bash
# Regenerates database.tar.zst: the data directory `dingo load` of the v0.94.2
# release leaves in API storage mode over the first chunks of the preview
# ImmutableDB fixture. Run from anywhere inside the repository.
set -euo pipefail

VERSION=v0.94.2
CHUNKS=21

root=$(git rev-parse --show-toplevel)
out=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'git -C "$root" worktree remove --force "$work/src" 2>/dev/null || true; rm -rf "$work"' EXIT

git -C "$root" worktree add --detach "$work/src" "$VERSION"
(cd "$work/src" && go build -o "$work/dingo" ./cmd/dingo)

mkdir -p "$work/immutable"
for i in $(seq 0 $((CHUNKS - 1))); do
	cp "$root/database/immutable/testdata/$(printf %05d "$i")".* "$work/immutable/"
done

"$work/dingo" --network preview --storage-mode api \
	--data-dir "$work/data" load "$work/immutable"

tar -C "$work/data" -cf - . | zstd -19 -q -f -o "$out/database.tar.zst"
ls -l "$out/database.tar.zst"
