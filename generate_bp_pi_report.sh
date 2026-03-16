#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_FILE="${1:-benchmark_results_bp_pi.md}"
CACHE_DIR="${CACHE_DIR:-/tmp/dingo-bp-pi-go-cache}"
GOMAXPROCS_VALUE="${GOMAXPROCS_VALUE:-4}"
BENCH_TIME="${BENCH_TIME:-5s}"
IMMUTABLE_PATH="${IMMUTABLE_PATH:-database/immutable/testdata}"
DEFAULT_DB_DIR="${DEFAULT_DB_DIR:-}"
DEFAULT_LOG_FILE="${DEFAULT_LOG_FILE:-}"
DB_DIR_BASE="${DB_DIR_BASE:-/tmp}"
TMP_BENCH_FILE="$(mktemp)"
TIME_FORMAT_STYLE=""
TIME_CMD=()
AUTO_DEFAULT_DB_DIR=0
AUTO_DEFAULT_LOG_FILE=0

cleanup() {
  local exit_status=$?

  rm -f "$TMP_BENCH_FILE"
  if [[ "$exit_status" -ne 0 ]]; then
    if [[ "$AUTO_DEFAULT_LOG_FILE" -eq 1 ]]; then
      printf 'preserving profile log for debugging: %s\n' \
        "$DEFAULT_LOG_FILE" >&2
    fi
    if [[ "$AUTO_DEFAULT_DB_DIR" -eq 1 ]]; then
      printf 'preserving profile database for debugging: %s\n' \
        "$DEFAULT_DB_DIR" >&2
    fi
    return "$exit_status"
  fi

  if [[ "$AUTO_DEFAULT_LOG_FILE" -eq 1 ]]; then
    rm -f "$DEFAULT_LOG_FILE"
  fi
  if [[ "$AUTO_DEFAULT_DB_DIR" -eq 1 ]]; then
    rm -rf "$DEFAULT_DB_DIR"
  fi
  return "$exit_status"
}
trap cleanup EXIT

create_temp_dir() {
  mktemp -d "${DB_DIR_BASE%/}/dingo-bp-pi-db-default.XXXXXX"
}

create_temp_file() {
  mktemp "${DB_DIR_BASE%/}/dingo-bp-pi-default.XXXXXX.log"
}

resolve_path() {
  local path="$1"
  local current
  local candidate
  local part
  local -a parts=()

  if [[ -z "$path" ]]; then
    return 1
  fi

  if [[ "$path" == /* ]]; then
    current="/"
  else
    current="$(pwd -P)"
  fi

  local IFS='/'
  read -r -a parts <<< "$path"
  for part in "${parts[@]}"; do
    case "$part" in
      ""|".")
        continue
        ;;
      "..")
        if [[ "$current" != "/" ]]; then
          current="${current%/*}"
          if [[ -z "$current" ]]; then
            current="/"
          fi
        fi
        ;;
      *)
        if [[ "$current" == "/" ]]; then
          candidate="/$part"
        else
          candidate="$current/$part"
        fi
        if [[ -d "$candidate" ]]; then
          current="$(cd "$candidate" && pwd -P)"
        else
          current="$candidate"
        fi
        ;;
    esac
  done

  printf '%s\n' "$current"
}

configure_time_cmd() {
  if command -v gtime >/dev/null 2>&1 &&
    gtime -f "elapsed=%e rss_kb=%M" true >/dev/null 2>&1; then
    TIME_FORMAT_STYLE="gnu"
    TIME_CMD=(gtime -f "elapsed=%e rss_kb=%M")
    return 0
  fi

  if /usr/bin/time -f "elapsed=%e rss_kb=%M" true >/dev/null 2>&1; then
    TIME_FORMAT_STYLE="gnu"
    TIME_CMD=(/usr/bin/time -f "elapsed=%e rss_kb=%M")
    return 0
  fi

  if /usr/bin/time -l true >/dev/null 2>&1; then
    TIME_FORMAT_STYLE="bsd"
    TIME_CMD=(/usr/bin/time -l)
    return 0
  fi

  echo "error: failed to find a supported time command" >&2
  return 1
}

validate_db_dir() {
  local db_dir="$1"
  local log_file="$2"
  local resolved_db_dir
  local resolved_base_dir

  if [[ -z "$db_dir" || "$db_dir" == "/" || "$db_dir" == "." || "$db_dir" == "~" ]]; then
    printf 'error: refusing to remove unsafe db_dir value %q\n' "$db_dir" >"$log_file"
    return 1
  fi

  if ! resolved_db_dir="$(resolve_path "$db_dir")"; then
    printf 'error: failed to resolve db_dir %q\n' "$db_dir" >"$log_file"
    return 1
  fi
  if ! resolved_base_dir="$(resolve_path "$DB_DIR_BASE")"; then
    printf 'error: failed to resolve DB_DIR_BASE %q\n' "$DB_DIR_BASE" >"$log_file"
    return 1
  fi

  if [[ "$resolved_db_dir" == "$resolved_base_dir" ]]; then
    printf 'error: refusing to remove db_dir %q because it resolves to the base directory %q\n' \
      "$resolved_db_dir" "$resolved_base_dir" >"$log_file"
    return 1
  fi

  case "$resolved_db_dir" in
    "$resolved_base_dir"/*) ;;
    *)
      printf 'error: refusing to remove db_dir %q outside allowed base %q\n' \
        "$resolved_db_dir" "$resolved_base_dir" >"$log_file"
      return 1
      ;;
  esac
}

extract_profile_line() {
  local log_file="$1"

  case "$TIME_FORMAT_STYLE" in
    gnu)
      awk '/elapsed=/{ line = $0 } END { print line }' "$log_file"
      ;;
    bsd)
      awk '
        /^[[:space:]]*[0-9]+([.][0-9]+)?[[:space:]]+real([[:space:]]|$)/ {
          elapsed = $1
        }
        /^[[:space:]]*[0-9]+[[:space:]]+maximum resident set size([[:space:]]|$)/ {
          rss_kb = $1
        }
        END {
          if (elapsed != "" || rss_kb != "") {
            printf "elapsed=%s rss_kb=%s\n", elapsed, rss_kb
          }
        }
      ' "$log_file"
      ;;
    *)
      return 1
      ;;
  esac
}

run_load_profile() {
  local db_dir="$1"
  local log_file="$2"
  shift 2

  validate_db_dir "$db_dir" "$log_file"
  rm -rf "$db_dir"
  : > "$log_file"
  env -i \
    PATH="$PATH" \
    HOME="${HOME:-/tmp}" \
    GOMAXPROCS="$GOMAXPROCS_VALUE" \
    DINGO_RUN_MODE=serve \
    DINGO_STORAGE_MODE=core \
    "${TIME_CMD[@]}" \
    ./dingo \
    --config /dev/null \
    --blob badger \
    --metadata sqlite \
    --data-dir "$db_dir" \
    load "$IMMUTABLE_PATH" "$@" \
    > "$log_file" 2>&1

  extract_profile_line "$log_file"
}

parse_metric() {
  local line="$1"
  local key="$2"
  echo "$line" | awk -v needle="$key" '{
    for (i = 1; i <= NF; i++) {
      split($i, kv, "=")
      if (kv[1] == needle) {
        print kv[2]
        exit
      }
    }
  }'
}

configure_time_cmd

DB_DIR_BASE="$(resolve_path "$DB_DIR_BASE")"

if [[ -z "$DEFAULT_DB_DIR" ]]; then
  DEFAULT_DB_DIR="$(create_temp_dir)"
  AUTO_DEFAULT_DB_DIR=1
fi

if [[ -z "$DEFAULT_LOG_FILE" ]]; then
  DEFAULT_LOG_FILE="$(create_temp_file)"
  AUTO_DEFAULT_LOG_FILE=1
fi

pushd "$ROOT_DIR" >/dev/null

env \
  GOCACHE="$CACHE_DIR" \
  GOMAXPROCS="$GOMAXPROCS_VALUE" \
  ./generate_benchmarks.sh \
  "$TMP_BENCH_FILE" \
  --write \
  --packages ./ledger \
  --bench 'Benchmark(BlockfetchNearTipThroughput|BlockfetchNearTipThroughputPredecoded|BlockfetchNearTipQueuedHeaderPredecoded|BlockfetchNearTipFlushOnlyPredecoded|VerifyBlockHeader)$' \
  --benchtime "$BENCH_TIME" \
  --title "Four-Thread Block Producer Sizing Benchmark Results (Non-Pi Host)" \
  --scope "Four-thread block producer path (GOMAXPROCS=${GOMAXPROCS_VALUE}) in core mode on a non-Pi host" \
  --data-source "database/immutable/testdata plus local immutable load runs"

env \
  GOCACHE="$CACHE_DIR" \
  GOFLAGS=-buildvcs=false \
  CGO_ENABLED=0 \
  go build -o ./dingo ./cmd/dingo

default_line="$(run_load_profile "$DEFAULT_DB_DIR" "$DEFAULT_LOG_FILE")"

default_elapsed="$(parse_metric "$default_line" "elapsed")"
default_rss_kb="$(parse_metric "$default_line" "rss_kb")"

if [[ ! "$default_elapsed" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "error: failed to parse numeric elapsed value from $DEFAULT_LOG_FILE: ${default_elapsed:-<empty>}" >&2
  exit 1
fi
if [[ ! "$default_rss_kb" =~ ^[0-9]+$ ]]; then
  echo "error: failed to parse numeric rss_kb value from $DEFAULT_LOG_FILE: ${default_rss_kb:-<empty>}" >&2
  exit 1
fi

default_rss_mb="$(awk "BEGIN { printf \"%.1f\", ${default_rss_kb} / 1024 }")"

{
  echo "# Four-Thread Block Producer Sizing Report (Non-Pi Host)"
  echo
  echo "Assumptions:"
  printf -- "- \`storageMode=core\`\n"
  printf -- "- \`runMode=serve\`, \`blob=badger\`, \`metadata=sqlite\`\n"
  printf -- "- \`GOMAXPROCS=%s\`\n" "$GOMAXPROCS_VALUE"
  printf -- "- immutable fixture: \`%s\`\n" "$IMMUTABLE_PATH"
  echo "- measurements were collected on non-Pi hardware and are a sizing proxy, not Raspberry Pi device benchmarks"
  echo "- block producer peer count remains small; this is not a relay profile"
  echo
  echo "## Load Footprint"
  echo
  echo "| Profile | Elapsed | Peak RSS | Notes |"
  echo "| --- | --- | --- | --- |"
  printf -- "| \`core-default\` | \`%ss\` | \`%s MB\` | Current default \`serve/core\` cache profile |\n" "$default_elapsed" "$default_rss_mb"
  echo
  echo "## Recommendation"
  echo
  printf -- "For Raspberry Pi-class sizing in \`storageMode=core\`, the current default \`serve/core\` profile is already in the measured low-memory range on this four-thread non-Pi host.\n"
  echo "Explicit Badger cache settings still override these defaults if an operator wants to tune further."
  echo "Treat this as a sizing baseline rather than a direct Raspberry Pi hardware measurement."
  echo
  cat "$TMP_BENCH_FILE"
} > "$OUTPUT_FILE"

popd >/dev/null
