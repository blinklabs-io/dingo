#!/usr/bin/env bash
#
# Delete every Actions cache entry on main whose key starts with a prefix,
# except the one just saved.
#
# Each main push saves a new build cache under a key that ends in the commit
# SHA, so the entries it supersedes are dead weight against the repository's
# 10 GB cache limit. Evicting by age instead would also evict the other
# flavors' current entries, which the merge queue restores.
#
# Usage: prune-go-cache.sh <key-prefix> <keep-key>
# Env:   GH_TOKEN with actions: write, GITHUB_REPOSITORY

set -euo pipefail

if [ "$#" -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
  echo "usage: $0 <key-prefix> <keep-key>" >&2
  exit 2
fi
prefix="$1"
keep="$2"

case "$keep" in
  "$prefix"*) ;;
  *)
    echo "keep key ${keep} does not start with prefix ${prefix}" >&2
    exit 2
    ;;
esac

# gh matches --key as a prefix, but the listing is filtered again here so a
# change in that behavior can never delete another flavor's entry.
listing="$(gh cache list --repo "$GITHUB_REPOSITORY" --ref refs/heads/main \
  --key "$prefix" --limit 100 --json id,key \
  --jq '.[] | [.id, .key] | @tsv')"

while IFS=$'\t' read -r id key; do
  [ -n "$id" ] || continue
  case "$key" in
    "$prefix"*) ;;
    *) continue ;;
  esac
  [ "$key" != "$keep" ] || continue
  gh cache delete "$id" --repo "$GITHUB_REPOSITORY"
done <<< "$listing"
