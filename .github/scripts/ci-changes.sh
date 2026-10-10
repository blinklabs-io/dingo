#!/usr/bin/env bash
set -euo pipefail

# Default to full CI whenever the diff is unavailable or ambiguous.
run_ci=true
if [[ ${EVENT_NAME:-} == pull_request && -n ${BASE_SHA:-} && -n ${HEAD_SHA:-} ]]; then
    changed_files=$(mktemp)
    trap 'rm -f "$changed_files"' EXIT
    # Disable rename detection so moving code into a documentation path still
    # exposes the deleted source path. NUL delimiters preserve unusual names.
    if git diff --no-renames --name-only -z "$BASE_SHA...$HEAD_SHA" -- > "$changed_files"; then
        if [[ -s "$changed_files" ]]; then
            run_ci=false
            while IFS= read -r -d '' path; do
                case "$path" in
                    docs/*.md) ;;
                    *.md)
                        if [[ "$path" == */* ]]; then
                            run_ci=true
                            break
                        fi
                        ;;
                    *) run_ci=true; break ;;
                esac
            done < "$changed_files"
        fi
    fi
fi
printf 'run-ci=%s\n' "$run_ci" >> "${GITHUB_OUTPUT:?}"
printf 'Full CI required: %s\n' "$run_ci"
