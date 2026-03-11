#!/usr/bin/env bash

# Copyright 2025 Blink Labs Software
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Script to generate benchmark results for Dingo ledger and database with historical tracking
# Usage: ./generate_benchmarks.sh [output_file] [--write] [--bench regex] [--packages pkg1,pkg2] [--benchtime duration] [--title title] [--scope scope] [--data-source source]
#   --write: Write results to file (default: display only)

WRITE_TO_FILE=false
OUTPUT_FILE="benchmark_results.md"
BENCH_REGEX="."
BENCH_PACKAGES="./..."
REPORT_TITLE="Dingo Ledger & Database Benchmark Results"
REPORT_SCOPE=""
REPORT_DATA_SOURCE="Real Cardano preview testnet data (40k+ blocks, slots 0-863,996)"
BENCH_TIME=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --write)
            WRITE_TO_FILE=true
            shift
            ;;
        --bench)
            [[ -z "${2:-}" || "$2" == -* ]] && { echo "Error: --bench requires a value"; exit 1; }
            BENCH_REGEX="$2"
            shift 2
            ;;
        --packages)
            [[ -z "${2:-}" || "$2" == -* ]] && { echo "Error: --packages requires a value"; exit 1; }
            BENCH_PACKAGES="$2"
            shift 2
            ;;
        --benchtime)
            [[ -z "${2:-}" || "$2" == -* ]] && { echo "Error: --benchtime requires a value"; exit 1; }
            BENCH_TIME="$2"
            shift 2
            ;;
        --title)
            [[ -z "${2:-}" || "$2" == -* ]] && { echo "Error: --title requires a value"; exit 1; }
            REPORT_TITLE="$2"
            shift 2
            ;;
        --scope)
            [[ -z "${2:-}" || "$2" == -* ]] && { echo "Error: --scope requires a value"; exit 1; }
            REPORT_SCOPE="$2"
            shift 2
            ;;
        --data-source)
            [[ -z "${2:-}" || "$2" == -* ]] && { echo "Error: --data-source requires a value"; exit 1; }
            REPORT_DATA_SOURCE="$2"
            shift 2
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Usage: $0 [output_file] [--write] [--bench regex] [--packages pkg1,pkg2] [--benchtime duration] [--title title] [--scope scope] [--data-source source]"
            exit 1
            ;;
        *)
            # First non-option argument is the output file
            if [[ -z "$OUTPUT_FILE_SET" ]]; then
                OUTPUT_FILE="$1"
                OUTPUT_FILE_SET=true
            else
                echo "Too many arguments. Usage: $0 [output_file] [--write] [--bench regex] [--packages pkg1,pkg2] [--benchtime duration] [--title title] [--scope scope] [--data-source source]"
                exit 1
            fi
            shift
            ;;
    esac
done

DATE=$(date +"%B %d, %Y")

# Initialize environment information for benchmark report
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
OS=$(uname -s)
ARCH=$(uname -m)
CPU_CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo "unknown")

echo "Running Dingo benchmarks..."
echo "==============================="

# Run benchmarks with progress output first
echo "Executing benchmarks (this may take a few minutes)..."

# Enable pipefail to catch go test failures in the pipeline
set -o pipefail

# Run go test once, capture output while showing progress
IFS=',' read -r -a BENCHMARK_PACKAGE_ARGS <<< "$BENCH_PACKAGES"
BENCHMARK_ARGS=(-bench="$BENCH_REGEX" -benchmem -run=^$)
if [[ -n "$BENCH_TIME" ]]; then
    BENCHMARK_ARGS+=("-benchtime=$BENCH_TIME")
fi
BENCHMARK_OUTPUT=$(go test "${BENCHMARK_ARGS[@]}" "${BENCHMARK_PACKAGE_ARGS[@]}" 2>&1)
GO_TEST_EXIT_CODE=$?

# Show progress by parsing benchmark names from output
echo "$BENCHMARK_OUTPUT" | grep "^Benchmark" | sed 's/Benchmark//' | sed 's/-[0-9]*$//' | while read -r name rest; do
    echo "Running: $name"
done

# Check if go test succeeded
if [[ $GO_TEST_EXIT_CODE -ne 0 ]]; then
    echo "Benchmark run failed!"
    exit 1
fi

# Count benchmarks
BENCHMARK_COUNT=$(echo "$BENCHMARK_OUTPUT" | grep "^Benchmark" | wc -l)

echo "Found $BENCHMARK_COUNT benchmarks across all packages"
echo ""

# Function to parse benchmark line
parse_benchmark() {
    local line="$1"
    local name
    name=$(echo "$line" | awk '{print $1}' | sed 's/Benchmark//' | sed 's/-[0-9]*$//')
    local iterations
    iterations=$(echo "$line" | awk '{print $2}' | sed 's/,//g')
    local time_val
    time_val=$(echo "$line" | awk '{print $3}')
    local time_unit
    time_unit=$(echo "$line" | awk '{print $4}')

    local extra_metrics=""
    local mem_op="N/A"
    local allocs_op="N/A"
    local idx=5
    local token_count
    token_count=$(echo "$line" | awk '{print NF}')

    while [[ $idx -le $token_count ]]; do
        local metric_val
        metric_val=$(echo "$line" | awk -v i="$idx" '{print $i}')
        local metric_unit
        metric_unit=$(echo "$line" | awk -v i="$((idx + 1))" '{print $i}')

        if [[ -z "$metric_val" || -z "$metric_unit" ]]; then
            break
        fi

        case "$metric_unit" in
            "B/op")
                if [[ "$metric_val" =~ ^[0-9]+$ && $metric_val -gt 1000 ]]; then
                    mem_kb=$((metric_val / 1000))
                    mem_op="${mem_kb}KB"
                else
                    mem_op="${metric_val}B"
                fi
                ;;
            "allocs/op")
                allocs_op="$metric_val"
                ;;
            *)
                if [[ -n "$extra_metrics" ]]; then
                    extra_metrics="${extra_metrics}, "
                fi
                extra_metrics="${extra_metrics}${metric_val} ${metric_unit}"
                ;;
        esac

        idx=$((idx + 2))
    done

    # Format time
    if [[ "$time_unit" == "ns/op" ]]; then
        time_op="${time_val}ns"
    elif [[ "$time_unit" == "μs/op" ]] || [[ "$time_unit" == "µs/op" ]]; then
        time_op="${time_val}μs"
    elif [[ "$time_unit" == "ms/op" ]]; then
        time_op="${time_val}ms"
    elif [[ "$time_unit" == "s/op" ]]; then
        time_op="${time_val}s"
    else
        time_op="${time_val}${time_unit}"
    fi

    if [[ -z "$extra_metrics" ]]; then
        extra_metrics="-"
    fi

    # Format benchmark name nicely
    formatted_name=$(echo "$name" | sed 's/\([A-Z]\)/ \1/g' | sed 's/^ //' | sed 's/NoData$/ (No Data)/' | sed 's/RealData$/ (Real Data)/')

    echo "$formatted_name|$iterations|$time_op|$extra_metrics|$mem_op|$allocs_op"
}

# Parse current results into temporary file, tracking current package
# go test outputs "pkg: <import-path>" before each package's benchmarks
CURRENT_RESULTS_FILE=$(mktemp "${TMPDIR:-/tmp}/dingo_bench_XXXXXX")
current_pkg=""
while IFS= read -r line; do
    if [[ "$line" =~ ^pkg:[[:space:]]+(.+)$ ]]; then
        current_pkg="${BASH_REMATCH[1]}"
    elif [[ "$line" =~ ^ok[[:space:]] || "$line" =~ ^FAIL[[:space:]] ]]; then
        current_pkg=""
    elif [[ "$line" =~ ^Benchmark ]]; then
        parsed=$(parse_benchmark "$line")
        name=$(echo "$parsed" | cut -d'|' -f1)
        data=$(echo "$parsed" | cut -d'|' -f2-)
        if [[ -n "$current_pkg" ]]; then
            pkg_short=$(basename "$current_pkg")
            echo "${pkg_short}:${name}|$data" >> "$CURRENT_RESULTS_FILE"
        else
            echo "$name|$data" >> "$CURRENT_RESULTS_FILE"
        fi
    fi
done <<< "$BENCHMARK_OUTPUT"

# Deduplicate benchmark names (composite key: package+benchmark)
sort -t'|' -k1,1 -u "$CURRENT_RESULTS_FILE" > "$CURRENT_RESULTS_FILE.tmp" && mv "$CURRENT_RESULTS_FILE.tmp" "$CURRENT_RESULTS_FILE"

# Display current results summary
echo "Current Benchmark Summary"
echo "-------------------------"
echo "Most iterations (>100k iters):"
sort -t'|' -k2,2nr "$CURRENT_RESULTS_FILE" | head -3 | while IFS='|' read -r name iterations _; do
    echo "  - $name: ${iterations} iterations"
done

echo ""
echo "Fewest iterations (<1k iters):"
awk -F'|' '$2 < 1000 {print $1 "|" $2}' "$CURRENT_RESULTS_FILE" | while IFS='|' read -r name iterations; do
    echo "  - $name: ${iterations} iterations"
done

echo ""
echo "Memory usage:"
awk -F'|' '
function mem_bytes(v) {
    if (v ~ /KB$/) {
        sub(/KB$/, "", v)
        return v + 0
    }
    if (v ~ /B$/) {
        sub(/B$/, "", v)
        return (v + 0) / 1000
    }
    return -1
}
{
    print mem_bytes($5) "|" $1 "|" $5
}
' "$CURRENT_RESULTS_FILE" | sort -t'|' -k1,1nr | head -3 | while IFS='|' read -r _ name mem; do
    echo "  - $name: ${mem} per op"
done

# Read previous results if file exists and we're comparing
PREVIOUS_RESULTS_FILE=$(mktemp "${TMPDIR:-/tmp}/dingo_bench_XXXXXX")
trap 'rm -f "$CURRENT_RESULTS_FILE" "$PREVIOUS_RESULTS_FILE"' EXIT
previous_date=""
MAJOR_CHANGES=false

if [[ -f "$OUTPUT_FILE" && "$WRITE_TO_FILE" == "true" ]]; then
    echo ""
    echo "Comparing with previous results..."
    # Extract previous date
    previous_date=$(grep "\*\*Date\*\*:" "$OUTPUT_FILE" | head -1 | sed 's/.*\*\*Date\*\*: //' || echo "")

    # Parse previous benchmark table
    in_table=false
    while IFS= read -r line; do
        # Stop parsing at performance changes or historical results sections
        if [[ "$line" == "## Performance Changes" || "$line" == "## Historical Results" ]]; then
            break
        fi
        if [[ "$line" == "| Benchmark | Operations/sec | Time/op | Memory/op | Allocs/op |" ||
              "$line" == "| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |" ]]; then
            in_table=true
            continue
        fi
        # Skip separator rows (e.g. |---|---|...)
        if [[ "$line" =~ ^\|[-[:space:]\|]+\|$ ]]; then
            continue
        fi
        if [[ "$in_table" == true && "$line" =~ ^\|.*\|.*\|.*\|.*\|.*\|$ ]]; then
            # Parse table row
            benchmark=$(echo "$line" | sed 's/^| //' | cut -d'|' -f1 | sed 's/ *$//')
            # Count fields to detect 5-column vs 6-column format
            field_count=$(echo "$line" | awk -F'|' '{print NF - 2}')

            if [[ "$field_count" -le 5 ]]; then
                # Old 5-column format: Benchmark | Operations/sec | Time/op | Memory/op | Allocs/op
                iterations="-"
                time_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f3 | sed 's/ //g')
                extra_metrics=$(echo "$line" | sed 's/^| //' | cut -d'|' -f2 | sed 's/^ *//' | sed 's/ *$//')
                mem_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f4 | sed 's/ //g')
                allocs_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f5 | sed 's/ //g')
            else
                # Current 6-column format
                iterations=$(echo "$line" | sed 's/^| //' | cut -d'|' -f2 | sed 's/ //g' | sed 's/,//g')
                time_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f3 | sed 's/ //g')
                extra_metrics=$(echo "$line" | sed 's/^| //' | cut -d'|' -f4 | sed 's/^ *//' | sed 's/ *$//')
                mem_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f5 | sed 's/ //g')
                allocs_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f6 | sed 's/ //g')
            fi

            if [[ -n "$benchmark" && -n "$time_op" ]]; then
                echo "$benchmark|$iterations|$time_op|$extra_metrics|$mem_op|$allocs_op" >> "$PREVIOUS_RESULTS_FILE"
            fi
        fi
        if [[ "$in_table" == true && "$line" == "" ]]; then
            in_table=false
        fi
    done < "$OUTPUT_FILE"
fi

# Deduplicate previous benchmark names
sort -t'|' -k1,1 -u "$PREVIOUS_RESULTS_FILE" > "$PREVIOUS_RESULTS_FILE.tmp" && mv "$PREVIOUS_RESULTS_FILE.tmp" "$PREVIOUS_RESULTS_FILE"

# Helper functions to get data from temp files
get_current_data() {
    local benchmark="$1"
    awk -F'|' '$1 == "'"$benchmark"'" {print substr($0, index($0, "|")+1)}' "$CURRENT_RESULTS_FILE"
}

get_previous_data() {
    local benchmark="$1"
    awk -F'|' '$1 == "'"$benchmark"'" {print substr($0, index($0, "|")+1)}' "$PREVIOUS_RESULTS_FILE"
}

list_current_benchmarks() {
    awk -F'|' '!seen[$1]++ {print $1}' "$CURRENT_RESULTS_FILE"
}

list_previous_benchmarks() {
    awk -F'|' '!seen[$1]++ {print $1}' "$PREVIOUS_RESULTS_FILE"
}

# Generate performance comparison if we have previous results
if [[ -n "$previous_date" && "$WRITE_TO_FILE" == "true" ]]; then
    # Track changes
    faster_benchmarks=""
    slower_benchmarks=""
    new_benchmarks=""
    removed_benchmarks=""

    # Compare results
    while IFS= read -r benchmark; do
        if [[ -n "$(get_previous_data "$benchmark")" ]]; then
            # Benchmark exists in both
            current_data=$(get_current_data "$benchmark")
            previous_data=$(get_previous_data "$benchmark")

            current_iters=$(echo "$current_data" | cut -d'|' -f1)
            previous_iters=$(echo "$previous_data" | cut -d'|' -f1)

            if [[ "$current_iters" =~ ^[0-9]+$ && "$previous_iters" =~ ^[0-9]+$ && $previous_iters -gt 0 ]]; then
                change=$(( (current_iters - previous_iters) * 100 / previous_iters ))
                if [[ $change -gt 10 ]]; then
                    faster_benchmarks="$faster_benchmarks
$benchmark (+${change}%)"
                elif [[ $change -lt -10 ]]; then
                    change_abs=$(( (previous_iters - current_iters) * 100 / previous_iters ))
                    slower_benchmarks="$slower_benchmarks
$benchmark (-${change_abs}%)"
                    MAJOR_CHANGES=true
                fi
            fi
        else
            new_benchmarks="$new_benchmarks
$benchmark"
        fi
    done < <(list_current_benchmarks)

    # Check for removed benchmarks
    while IFS= read -r benchmark; do
        if [[ -z "$(get_current_data "$benchmark")" ]]; then
            removed_benchmarks="$removed_benchmarks
$benchmark"
        fi
    done < <(list_previous_benchmarks)

    # Count items (remove empty lines and count)
    faster_count=$(echo "$faster_benchmarks" | sed '/^$/d' | wc -l)
    slower_count=$(echo "$slower_benchmarks" | sed '/^$/d' | wc -l)
    new_count=$(echo "$new_benchmarks" | sed '/^$/d' | wc -l)
    removed_count=$(echo "$removed_benchmarks" | sed '/^$/d' | wc -l)

    echo ""
    echo "Performance Changes Summary:"
    echo "  Faster: $faster_count | Slower: $slower_count | New: $new_count | Removed: $removed_count"

    # Report changes if any improvements, regressions, or new benchmarks detected
    if [[ $faster_count -gt 0 || $slower_count -gt 0 || $new_count -gt 0 ]]; then
        MAJOR_CHANGES=true
    fi
fi

# Decide whether to write to file
if [[ "$WRITE_TO_FILE" == "true" ]]; then
    echo ""
    if [[ "$MAJOR_CHANGES" == "true" ]]; then
        echo "Writing results to file (major changes detected)..."
    elif [[ -z "$previous_date" ]]; then
        echo "Writing results to file (first benchmark run)..."
    else
        echo "Writing results to file (--write flag used)..."
    fi

        # Generate performance comparison for file
        generate_comparison() {
            echo "## Performance Changes"
            echo ""
            if [[ -z "$previous_date" ]]; then
                echo "No previous results found. This is the first benchmark run."
                echo ""
                return
            fi

            echo "Changes since **$previous_date**:"
            echo ""

            # Track changes
            faster_benchmarks=""
            slower_benchmarks=""
            new_benchmarks=""
            removed_benchmarks=""

            # Compare results
            while IFS= read -r benchmark; do
                if [[ -n "$(get_previous_data "$benchmark")" ]]; then
                    # Benchmark exists in both
                    current_data=$(get_current_data "$benchmark")
                    previous_data=$(get_previous_data "$benchmark")

                    current_iters=$(echo "$current_data" | cut -d'|' -f1)
                    previous_iters=$(echo "$previous_data" | cut -d'|' -f1)

                    if [[ "$current_iters" =~ ^[0-9]+$ && "$previous_iters" =~ ^[0-9]+$ && $previous_iters -gt 0 ]]; then
                        if [[ $current_iters -gt $previous_iters ]]; then
                            change=$(( (current_iters - previous_iters) * 100 / previous_iters ))
                            faster_benchmarks="$faster_benchmarks
$benchmark (+${change}%)"
                        elif [[ $current_iters -lt $previous_iters ]]; then
                            change=$(( (previous_iters - current_iters) * 100 / previous_iters ))
                            slower_benchmarks="$slower_benchmarks
$benchmark (-${change}%)"
                        fi
                    fi
                else
                    new_benchmarks="$new_benchmarks
$benchmark"
                fi
            done < <(list_current_benchmarks)

            # Check for removed benchmarks
            while IFS= read -r benchmark; do
                if [[ -z "$(get_current_data "$benchmark")" ]]; then
                    removed_benchmarks="$removed_benchmarks
$benchmark"
                fi
            done < <(list_previous_benchmarks)

            # Count items
            faster_count=$(echo "$faster_benchmarks" | sed '/^$/d' | wc -l)
            slower_count=$(echo "$slower_benchmarks" | sed '/^$/d' | wc -l)
            new_count=$(echo "$new_benchmarks" | sed '/^$/d' | wc -l)
            removed_count=$(echo "$removed_benchmarks" | sed '/^$/d' | wc -l)

            echo "### Summary"
            echo "- **Faster benchmarks**: $faster_count"
            echo "- **Slower benchmarks**: $slower_count"
            echo "- **New benchmarks**: $new_count"
            echo "- **Removed benchmarks**: $removed_count"
            echo ""

            if [[ $faster_count -gt 0 ]]; then
                echo "### Top Improvements"
                echo "$faster_benchmarks" | grep "^." | sort -t'(' -k2 -nr | head -5 | sed 's/^/- /'
                echo ""
            fi

            if [[ $slower_count -gt 0 ]]; then
                echo "### Performance Regressions"
                echo "$slower_benchmarks" | grep "^." | sort -t'(' -k2 -nr | head -5 | sed 's/^/- /'
                echo ""
            fi

            if [[ $new_count -gt 0 ]]; then
                echo "### New Benchmarks Added"
                echo "$new_benchmarks" | grep "^." | sed 's/^/- /'
                echo ""
            fi

            if [[ $removed_count -gt 0 ]]; then
                echo "### Benchmarks Removed"
                echo "$removed_benchmarks" | grep "^." | sed 's/^/- /'
                echo ""
            fi
        }

        # Create the markdown file
        cat > "$OUTPUT_FILE.tmp" << EOF
# $REPORT_TITLE

## Latest Results

### Test Environment
- **Date**: $DATE
- **Go Version**: $GO_VERSION
- **OS**: $OS
- **Architecture**: $ARCH
- **CPU Cores**: $CPU_CORES
- **Data Source**: $REPORT_DATA_SOURCE
EOF

        if [[ -n "$REPORT_SCOPE" ]]; then
            cat >> "$OUTPUT_FILE.tmp" << EOF
- **Scope**: $REPORT_SCOPE
EOF
        fi

        cat >> "$OUTPUT_FILE.tmp" << EOF

### Benchmark Results

All benchmarks run with \`-benchmem\` flag. Iterations are Go benchmark iteration counts; benchmark-specific throughput metrics (for example \`blocks/sec\`) are reported separately.

| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |
|-----------|------------|---------|---------------|-----------|-----------|
EOF

        # Add current results to table
        while IFS= read -r benchmark; do
            data=$(get_current_data "$benchmark")
            iterations=$(echo "$data" | cut -d'|' -f1)
            time_op=$(echo "$data" | cut -d'|' -f2)
            extra_metrics=$(echo "$data" | cut -d'|' -f3)
            mem_op=$(echo "$data" | cut -d'|' -f4)
            allocs_op=$(echo "$data" | cut -d'|' -f5)
            echo "| $benchmark | $iterations | $time_op | $extra_metrics | $mem_op | $allocs_op |" >> "$OUTPUT_FILE.tmp"
        done < <(list_current_benchmarks)

        # Add comparison section
        generate_comparison >> "$OUTPUT_FILE.tmp"

        # Add historical section if previous results exist
        if [[ -n "$previous_date" ]]; then
            echo "" >> "$OUTPUT_FILE.tmp"
            echo "## Historical Results" >> "$OUTPUT_FILE.tmp"
            echo "" >> "$OUTPUT_FILE.tmp"
            echo "### $previous_date" >> "$OUTPUT_FILE.tmp"
            echo "" >> "$OUTPUT_FILE.tmp"
            echo "| Benchmark | Iterations | Time/op | Extra Metrics | Memory/op | Allocs/op |" >> "$OUTPUT_FILE.tmp"
            echo "|-----------|------------|---------|---------------|-----------|-----------|" >> "$OUTPUT_FILE.tmp"

            # Add previous results
            while IFS= read -r benchmark; do
                data=$(get_previous_data "$benchmark")
                iterations=$(echo "$data" | cut -d'|' -f1)
                time_op=$(echo "$data" | cut -d'|' -f2)
                extra_metrics=$(echo "$data" | cut -d'|' -f3)
                mem_op=$(echo "$data" | cut -d'|' -f4)
                allocs_op=$(echo "$data" | cut -d'|' -f5)
                echo "| $benchmark | $iterations | $time_op | $extra_metrics | $mem_op | $allocs_op |" >> "$OUTPUT_FILE.tmp"
            done < <(list_previous_benchmarks)
        fi

        # Move temp file to final location
        mv "$OUTPUT_FILE.tmp" "$OUTPUT_FILE"

        echo "Benchmark results saved to $OUTPUT_FILE"
else
    echo ""
    echo "To save these results to file, run: ./generate_benchmarks.sh --write"
    echo "Results are only saved when major performance changes are detected."
fi

echo ""
echo "Benchmark run complete!"
