#!/usr/bin/env bash
# Runs the pull-request benchmark gate set against two checkouts on one
# machine and writes benchstat-readable result files.
#
#   bench-gate.sh <base-dir> <head-dir> <out-dir> [rounds]
#
# Each package's test binary is built once per checkout, then every round runs
# the base binary and the head binary back to back, alternating which goes
# first. Interleaving spreads slow drift (thermal state, noisy neighbours,
# page cache) evenly over both sides instead of attributing it to whichever
# revision happened to run second. A benchmark missing from the base checkout
# (a newly added one) simply produces no base samples.
#
# Writes <out-dir>/base.txt and <out-dir>/head.txt.
#
#   bench-gate.sh --record <dir> <out-file> [rounds]
#
# Runs only the one checkout, appending every round to <out-file>. This is how
# a baseline for the gate set is recorded on main.
set -euo pipefail

# Keep in sync with benchci.GateBenchmarks; a test enforces it.
BENCH_REGEX='^Benchmark(ChainSyncFromGenesis|RealBlockProcessing|BlockProcessingThroughput|BlockProcessingThroughputPredecoded|ReadParamsRequest)$'
PACKAGES=(ledger api/utxorpc)
BUILD_TAGS="${BUILD_TAGS-dingo_extra_plugins}"

usage() {
    echo "usage: bench-gate.sh <base-dir> <head-dir> <out-dir> [rounds]" >&2
    echo "       bench-gate.sh --record <dir> <out-file> [rounds]" >&2
    exit 2
}

record_only=false
record_file=
if [[ ${1:-} == --record ]]; then
    record_only=true
    [[ $# -ge 3 && $# -le 4 ]] || usage
    base_dir=$(cd "$2" && pwd)
    head_dir=$base_dir
    record_file=$(realpath -m "$3")
    out_dir=$(mktemp -d)
    rounds=${4:-10}
else
    [[ $# -ge 3 && $# -le 4 ]] || usage
    base_dir=$(cd "$1" && pwd)
    head_dir=$(cd "$2" && pwd)
    out_dir=$3
    rounds=${4:-8}
fi
if ! [[ $rounds =~ ^[0-9]+$ ]] || ((rounds < 1)); then
    echo "rounds must be a positive integer: $rounds" >&2
    exit 2
fi
mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)
: > "$out_dir/base.txt"
: > "$out_dir/head.txt"

tag_args=()
if [[ -n $BUILD_TAGS ]]; then
    tag_args=(-tags "$BUILD_TAGS")
fi

build_binaries() {
    local side=$1 dir=$2 pkg
    mkdir -p "$out_dir/bin/$side"
    for pkg in "${PACKAGES[@]}"; do
        (cd "$dir" && go test "${tag_args[@]}" -c \
            -o "$out_dir/bin/$side/${pkg//\//_}.test" "./$pkg")
    done
}

# Test binaries resolve testdata relative to the package directory, so each
# runs from its own checkout's package directory.
run_side() {
    local side=$1 dir=$2 pkg
    for pkg in "${PACKAGES[@]}"; do
        (cd "$dir/$pkg" && "$out_dir/bin/$side/${pkg//\//_}.test" \
            -test.run='^$' -test.bench="$BENCH_REGEX" -test.benchmem \
            -test.count=1 -test.timeout=20m) >> "$out_dir/$side.txt"
    done
}

if $record_only; then
    build_binaries head "$head_dir"
    for ((i = 1; i <= rounds; i++)); do
        echo "round $i/$rounds" >&2
        run_side head "$head_dir"
    done
    mv "$out_dir/head.txt" "$record_file"
    exit 0
fi

build_binaries base "$base_dir"
build_binaries head "$head_dir"

for ((i = 1; i <= rounds; i++)); do
    echo "round $i/$rounds" >&2
    if ((i % 2 == 1)); then
        run_side base "$base_dir"
        run_side head "$head_dir"
    else
        run_side head "$head_dir"
        run_side base "$base_dir"
    fi
done
