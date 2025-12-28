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
# Usage: ./generate_benchmarks.sh [output_file] [--write]
#   --write: Write results to file (default: display only)

WRITE_TO_FILE=false
OUTPUT_FILE="benchmark_results.md"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --write)
            WRITE_TO_FILE=true
            shift
            ;;
        -*)
            echo "Unknown option: $1"
            echo "Usage: $0 [output_file] [--write]"
            exit 1
            ;;
        *)
            # First non-option argument is the output file
            if [[ -z "$OUTPUT_FILE_SET" ]]; then
                OUTPUT_FILE="$1"
                OUTPUT_FILE_SET=true
            else
                echo "Too many arguments. Usage: $0 [output_file] [--write]"
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

echo "Running all Dingo benchmarks..."
echo "==============================="

# Run benchmarks with progress output first
echo "Executing benchmarks (this may take a few minutes)..."

# Enable pipefail to catch go test failures in the pipeline
set -o pipefail

# Run go test once, capture output while showing progress
BENCHMARK_OUTPUT=$(go test -bench=. -benchmem ./... -run=^$ 2>&1)
GO_TEST_EXIT_CODE=$?

# Show progress by parsing benchmark names from output
echo "$BENCHMARK_OUTPUT" | grep "^Benchmark" | sed 's/Benchmark//' | sed 's/-[0-9]*$//' | while read -r name rest; do
    echo "Running: $name-128"
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
    local ops_sec
    ops_sec=$(echo "$line" | awk '{print $2}' | sed 's/,//g')
    local time_val
    time_val=$(echo "$line" | awk '{print $3}')
    local time_unit
    time_unit=$(echo "$line" | awk '{print $4}')
    local mem_val
    mem_val=$(echo "$line" | awk '{print $5}')
    local mem_unit
    mem_unit=$(echo "$line" | awk '{print $6}')
    local allocs_op
    allocs_op=$(echo "$line" | awk '{print $7}')

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

    # Format memory
    if [[ "$mem_unit" == "B/op" ]]; then
        if [[ $mem_val -gt 1000 ]]; then
            mem_kb=$((mem_val / 1000))
            mem_op="${mem_kb}KB"
        else
            mem_op="${mem_val}B"
        fi
    else
        mem_op="${mem_val}${mem_unit}"
    fi

    # Format benchmark name nicely
    formatted_name=$(echo "$name" | sed 's/\([A-Z]\)/ \1/g' | sed 's/^ //' | sed 's/NoData$/ (No Data)/' | sed 's/RealData$/ (Real Data)/')

    echo "$formatted_name|$ops_sec|$time_op|$mem_op|$allocs_op"
}

# Parse current results into temporary file
CURRENT_RESULTS_FILE=$(mktemp "${TMPDIR:-/tmp}/dingo_bench_XXXXXX")
while IFS= read -r line; do
    if [[ "$line" =~ ^Benchmark ]]; then
        parsed=$(parse_benchmark "$line")
        name=$(echo "$parsed" | cut -d'|' -f1)
        data=$(echo "$parsed" | cut -d'|' -f2-)
        echo "$name|$data" >> "$CURRENT_RESULTS_FILE"
    fi
done <<< "$BENCHMARK_OUTPUT"

# Deduplicate benchmark names
sort -t'|' -k1,1 -u "$CURRENT_RESULTS_FILE" > "$CURRENT_RESULTS_FILE.tmp" && mv "$CURRENT_RESULTS_FILE.tmp" "$CURRENT_RESULTS_FILE"

# Display current results summary
echo "Current Benchmark Summary"
echo "-------------------------"
echo "Fastest benchmarks (>100k ops/sec):"
echo "$BENCHMARK_OUTPUT" | grep "^Benchmark" | sort -k2 -nr | head -3 | while read -r line; do
    name=$(echo "$line" | awk '{print $1}' | sed 's/Benchmark//' | sed 's/-128$//')
    ops=$(echo "$line" | awk '{print $2}' | sed 's/,//g')
    echo "  - $name: ${ops} ops/sec"
done

echo ""
echo "Slowest benchmarks (<1k ops/sec):"
echo "$BENCHMARK_OUTPUT" | grep "^Benchmark" | awk '$2 < 1000' | while read -r line; do
    name=$(echo "$line" | awk '{print $1}' | sed 's/Benchmark//' | sed 's/-128$//')
    ops=$(echo "$line" | awk '{print $2}' | sed 's/,//g')
    echo "  - $name: ${ops} ops/sec"
done

echo ""
echo "Memory usage:"
echo "$BENCHMARK_OUTPUT" | grep "^Benchmark" | sort -k5 -nr | head -3 | while read -r line; do
    name=$(echo "$line" | awk '{print $1}' | sed 's/Benchmark//' | sed 's/-128$//')
    mem=$(echo "$line" | awk '{print $5}')
    echo "  - $name: ${mem}B per op"
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
        if [[ "$line" == "| Benchmark | Operations/sec | Time/op | Memory/op | Allocs/op |" ]]; then
            in_table=true
            continue
        fi
        if [[ "$in_table" == true && "$line" =~ ^\|.*\|.*\|.*\|.*\|.*\|$ && "$line" != "|-----------|*" ]]; then
            # Parse table row
            benchmark=$(echo "$line" | sed 's/^| //' | cut -d'|' -f1 | sed 's/ *$//')
            ops_sec=$(echo "$line" | sed 's/^| //' | cut -d'|' -f2 | sed 's/ //g' | sed 's/,//g')
            time_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f3 | sed 's/ //g')
            mem_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f4 | sed 's/ //g')
            allocs_op=$(echo "$line" | sed 's/^| //' | cut -d'|' -f5 | sed 's/ //g')
            if [[ -n "$benchmark" && -n "$ops_sec" ]]; then
                echo "$benchmark|$ops_sec|$time_op|$mem_op|$allocs_op" >> "$PREVIOUS_RESULTS_FILE"
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

            current_ops=$(echo "$current_data" | cut -d'|' -f1)
            previous_ops=$(echo "$previous_data" | cut -d'|' -f1)

            if [[ "$current_ops" =~ ^[0-9]+$ && "$previous_ops" =~ ^[0-9]+$ && $previous_ops -gt 0 ]]; then
                change=$(( (current_ops - previous_ops) * 100 / previous_ops ))
                if [[ $change -gt 10 ]]; then
                    faster_benchmarks="$faster_benchmarks
$benchmark (+${change}%)"
                elif [[ $change -lt -10 ]]; then
                    change_abs=$(( (previous_ops - current_ops) * 100 / previous_ops ))
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

                    current_ops=$(echo "$current_data" | cut -d'|' -f1)
                    previous_ops=$(echo "$previous_data" | cut -d'|' -f1)

                    if [[ "$current_ops" =~ ^[0-9]+$ && "$previous_ops" =~ ^[0-9]+$ && $previous_ops -gt 0 ]]; then
                        if [[ $current_ops -gt $previous_ops ]]; then
                            change=$(( (current_ops - previous_ops) * 100 / previous_ops ))
                            faster_benchmarks="$faster_benchmarks
$benchmark (+${change}%)"
                        elif [[ $current_ops -lt $previous_ops ]]; then
                            change=$(( (previous_ops - current_ops) * 100 / previous_ops ))
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
# Dingo Ledger & Database Benchmark Results

## Latest Results

### Test Environment
- **Date**: $DATE
- **Go Version**: $GO_VERSION
- **OS**: $OS
- **Architecture**: $ARCH
- **CPU Cores**: $CPU_CORES
- **Data Source**: Real Cardano preview testnet data (40k+ blocks, slots 0-863,996)

### Benchmark Results

All benchmarks run with \`-benchmem\` flag showing memory allocations and operation counts.

| Benchmark | Operations/sec | Time/op | Memory/op | Allocs/op |
|-----------|----------------|---------|-----------|-----------|
EOF

        # Add current results to table
        while IFS= read -r benchmark; do
            data=$(get_current_data "$benchmark")
            ops_sec=$(echo "$data" | cut -d'|' -f1)
            time_op=$(echo "$data" | cut -d'|' -f2)
            mem_op=$(echo "$data" | cut -d'|' -f3)
            allocs_op=$(echo "$data" | cut -d'|' -f4)
            echo "| $benchmark | $ops_sec | $time_op | $mem_op | $allocs_op |" >> "$OUTPUT_FILE.tmp"
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
            echo "| Benchmark | Operations/sec | Time/op | Memory/op | Allocs/op |" >> "$OUTPUT_FILE.tmp"
            echo "|-----------|----------------|---------|-----------|-----------|" >> "$OUTPUT_FILE.tmp"

            # Add previous results
            while IFS= read -r benchmark; do
                data=$(get_previous_data "$benchmark")
                ops_sec=$(echo "$data" | cut -d'|' -f1)
                time_op=$(echo "$data" | cut -d'|' -f2)
                mem_op=$(echo "$data" | cut -d'|' -f3)
                allocs_op=$(echo "$data" | cut -d'|' -f4)
                echo "| $benchmark | $ops_sec | $time_op | $mem_op | $allocs_op |" >> "$OUTPUT_FILE.tmp"
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
