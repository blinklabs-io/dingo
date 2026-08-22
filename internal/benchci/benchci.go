// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package benchci implements CI benchmark regression detection for Dingo's
// weekly benchmark workflow (blinklabs-io/dingo#1895). It shells out to
// golang.org/x/perf/cmd/benchstat to compare two `go test -bench`-format
// result files, restricts the comparison to a curated set of benchmark
// names, and flags a benchmark as regressed only when both:
//
//   - its "sec/op" delta exceeds RegressionThresholdPercent, and
//   - benchstat marked that delta statistically significant (i.e. it did
//     not print the "~" marker).
//
// The AND-of-both-signals is a deliberate mitigation for GitHub-hosted
// runner timing noise: a large delta with high variance (not significant)
// and a small statistically-clean delta (below threshold) are both treated
// as non-regressions.
package benchci

import (
	"bytes"
	"fmt"
	"math"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// BenchstatVersion pins golang.org/x/perf/cmd/benchstat, mirroring the
	// Makefile's one-shot-CLI convention for sqlc
	// (`SQLC = go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION)`):
	// invoked via `go run <module>@<version>` with no go.mod entry.
	//
	// golang.org/x/perf has never published a semver-tagged release --
	// https://proxy.golang.org/golang.org/x/perf/@v/list returns an empty
	// body, confirming there is no vX.Y.Z tag to pin to -- so the pin here
	// is necessarily a commit pseudo-version rather than a tagged version.
	BenchstatVersion = "v0.0.0-20260819171926-ebcb4798430d"

	benchstatModule = "golang.org/x/perf/cmd/benchstat@" + BenchstatVersion

	// RegressionThresholdPercent is the minimum "sec/op" delta, in percent,
	// that (combined with benchstat-reported statistical significance)
	// flags a curated benchmark as regressed. Matches the 5% figure from
	// issue #1895.
	RegressionThresholdPercent = 5.0

	// secOpMetric is the metric name benchstat prints in the CSV header of
	// its primary timing table (its other tables are "B/op" and
	// "allocs/op" when the benchmarks call b.ReportAllocs(), which our
	// curated benchmarks do not track for regression purposes).
	secOpMetric = "sec/op"

	// nonSignificantMarker is the exact string benchstat prints in the
	// delta column instead of a percentage when it did not consider the
	// change statistically significant at its alpha level (default 0.05).
	nonSignificantMarker = "~"
)

// gomaxprocsSuffix matches the trailing "-<GOMAXPROCS>" go test appends to
// every benchmark name (e.g. "BenchmarkFoo-8"), including under a -cpu
// sweep where the same base name recurs once per swept value.
var gomaxprocsSuffix = regexp.MustCompile(`-[0-9]+$`)

// Row is one curated benchmark's old-vs-new "sec/op" comparison.
type Row struct {
	// Name is the full benchstat row name, including any trailing
	// "-<GOMAXPROCS>" suffix (e.g. "BenchmarkConcurrentQueries-16").
	Name string
	// Old and New are human-readable durations (e.g. "634.9ns"), empty
	// when Found is false.
	Old, New string
	// DeltaPercent is the signed percent change benchstat reported.
	// Meaningless (and zero) when Significant is false.
	DeltaPercent float64
	// Significant reports whether benchstat did not print "~" for this
	// row's delta.
	Significant bool
	// Found reports whether this benchmark appeared in benchstat's
	// "sec/op" table at all. False means it was skipped, renamed, removed,
	// or otherwise absent from one or both input files.
	Found bool
	// Flagged reports whether this row counts as a regression: Found,
	// Significant, and DeltaPercent > RegressionThresholdPercent.
	Flagged bool
}

// Compare shells out to benchstat to compare oldFile against newFile (both
// in `go test -bench`/benchstat text format), restricts the result to the
// given curated benchmark names, and renders a markdown report. It returns
// the report text and whether any curated row regressed.
//
// curated names are matched against benchstat's row names tolerating a
// trailing "-<GOMAXPROCS>" suffix, so a single curated name (e.g.
// "BenchmarkConcurrentQueries") matches every GOMAXPROCS variant present in
// the input (e.g. under -cpu=1,4,8,16) and each variant is reported as its
// own row.
func Compare(oldFile, newFile string, curated []string) (string, bool, error) {
	csv, err := runBenchstat(oldFile, newFile)
	if err != nil {
		return "", false, err
	}
	return Report(csv, curated)
}

// Report parses raw `benchstat -format=csv` output and renders the curated
// markdown regression report. It is split out from Compare so tests can
// exercise the parsing/flagging logic against fixture CSV text without
// shelling out to benchstat.
func Report(csv string, curated []string) (string, bool, error) {
	rows, err := parseSecOpTable(csv)
	if err != nil {
		return "", false, err
	}
	results := evaluateCurated(rows, curated)
	return renderMarkdown(results), anyFlagged(results), nil
}

func anyFlagged(results []Row) bool {
	for _, r := range results {
		if r.Flagged {
			return true
		}
	}
	return false
}

func runBenchstat(oldFile, newFile string) (string, error) {
	// oldFile/newFile are workflow-controlled local file paths (a prior
	// benchmark-data run file and this run's fresh output), not
	// attacker-influenced input; benchstatModule is a compile-time constant.
	cmd := exec.Command("go", "run", benchstatModule, "-format=csv", oldFile, newFile) //nolint:gosec // G204: paths are workflow-controlled, not attacker input
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf(
			"benchci: benchstat %s %s: %w (stderr: %s)",
			oldFile, newFile, err, strings.TrimSpace(stderr.String()),
		)
	}
	return stdout.String(), nil
}

// benchstatRow is one raw data row parsed from benchstat's CSV "sec/op"
// table.
type benchstatRow struct {
	old, new     string
	deltaPercent float64
	significant  bool
}

// parseSecOpTable extracts every data row from benchstat's "sec/op" CSV
// table(s), keyed by full row name (including any "-<GOMAXPROCS>" suffix).
//
// benchstat's CSV output (verified directly against
// `go run golang.org/x/perf/cmd/benchstat@<BenchstatVersion> -format=csv`)
// is organized as blocks separated by a single blank line. A block may open
// with zero or more bare "key: value" metadata lines (goos/goarch/pkg/cpu --
// "pkg:" alone recurs before each new package's tables when the input spans
// multiple packages), followed by a ",<old file>,,<new file>,,," header row
// and a ",<metric>,CI,<metric>,CI,vs base,P" row identifying the table's
// metric ("sec/op", "B/op", or "allocs/op"), then one data row per
// benchmark, then a trailing "geomean" row. Only "sec/op" blocks are kept.
func parseSecOpTable(csv string) (map[string]benchstatRow, error) {
	csv = strings.ReplaceAll(csv, "\r\n", "\n")
	blocks := strings.Split(strings.TrimRight(csv, "\n"), "\n\n")

	rows := make(map[string]benchstatRow)
	foundSecOpTable := false
	for _, block := range blocks {
		lines := strings.Split(block, "\n")
		for len(lines) > 0 && !strings.Contains(lines[0], ",") {
			lines = lines[1:]
		}
		if len(lines) < 2 {
			continue
		}
		metricHeader := strings.Split(lines[1], ",")
		if len(metricHeader) < 2 || metricHeader[1] != secOpMetric {
			continue
		}
		foundSecOpTable = true
		for _, line := range lines[2:] {
			if strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Split(line, ",")
			if len(fields) < 6 {
				continue
			}
			name := fields[0]
			if name == "geomean" {
				continue
			}
			oldSeconds, oldErr := strconv.ParseFloat(fields[1], 64)
			newSeconds, newErr := strconv.ParseFloat(fields[3], 64)
			deltaPercent, significant, deltaOK := parseDelta(fields[5])
			if oldErr != nil || newErr != nil || !deltaOK {
				// benchstat couldn't compute a clean comparison for this
				// row (e.g. one side missing); leave it out so the
				// corresponding curated benchmark reports Found=false
				// rather than a fabricated value.
				continue
			}
			rows[name] = benchstatRow{
				old:          formatSeconds(oldSeconds),
				new:          formatSeconds(newSeconds),
				deltaPercent: deltaPercent,
				significant:  significant,
			}
		}
	}
	if !foundSecOpTable {
		return nil, fmt.Errorf("benchci: no %q table found in benchstat output", secOpMetric)
	}
	return rows, nil
}

// parseDelta parses benchstat's "vs base" delta column: either the literal
// non-significant marker "~", or a signed percentage such as "+49.27%" or
// "-50.60%".
func parseDelta(field string) (percent float64, significant bool, ok bool) {
	field = strings.TrimSpace(field)
	if field == nonSignificantMarker {
		return 0, false, true
	}
	v, err := strconv.ParseFloat(strings.TrimSuffix(field, "%"), 64)
	if err != nil {
		return 0, false, false
	}
	return v, true, true
}

// formatSeconds renders a benchstat "sec/op" value (a raw float64 number of
// seconds) as a human-readable duration, e.g. 6.3495e-07 -> "634.95ns" ->
// (rounded to the nearest nanosecond) "635ns".
func formatSeconds(seconds float64) string {
	return time.Duration(math.Round(seconds * float64(time.Second))).String()
}

// evaluateCurated matches every curated benchmark name against benchstat's
// parsed row names, tolerating a trailing "-<GOMAXPROCS>" suffix and a
// "/<subname>" sub-benchmark path.
//
// Two real-world wrinkles, both verified directly against real `go test`
// runs rather than assumed:
//
//   - benchstat's default row naming strips the leading "Benchmark"
//     (`go test -bench=BenchmarkHotCacheGet` compared with benchstat prints
//     its row as "HotCacheGet-10", not "BenchmarkHotCacheGet-10"), so
//     curated names (which carry the prefix, as they appear in Go source
//     and the Makefile -bench regex) are compared with "Benchmark" trimmed
//     from both sides.
//   - Several curated benchmarks (e.g. BenchmarkUpdateConnectionMetrics,
//     BenchmarkVerifyBlockHeader, BenchmarkStorageModeIngest,
//     BenchmarkTestLoad) call b.Run internally, so their rows appear as
//     "UpdateConnectionMetrics/1000-10", never as a bare
//     "UpdateConnectionMetrics-10". A curated name therefore matches a row
//     whose GOMAXPROCS-stripped name equals it exactly OR starts with it
//     plus "/", and every matching sub-benchmark is reported as its own Row.
//
// A curated name can match multiple rows (one per swept GOMAXPROCS value,
// one per sub-benchmark, or both); every match is reported as its own Row,
// in ascending row-name order, with its display Name reconstructed with the
// "Benchmark" prefix restored for readability. A curated name with no match
// at all is reported as a single not-found Row so missing data is visible
// rather than silently dropped.
func evaluateCurated(rows map[string]benchstatRow, curated []string) []Row {
	names := make([]string, 0, len(rows))
	for name := range rows {
		names = append(names, name)
	}
	sort.Strings(names)

	results := make([]Row, 0, len(curated))
	for _, base := range curated {
		shortBase := strings.TrimPrefix(base, "Benchmark")
		matched := false
		for _, name := range names {
			// nameNoPrefix strips a "Benchmark" prefix if present (real
			// benchstat output never has one, but tolerate it for
			// robustness) without touching the "-<GOMAXPROCS>" suffix or
			// any "/<subname>" path, so it can be compared directly
			// against shortBase.
			nameNoPrefix := strings.TrimPrefix(name, "Benchmark")
			shortName := gomaxprocsSuffix.ReplaceAllString(nameNoPrefix, "")
			if shortName != shortBase && !strings.HasPrefix(shortName, shortBase+"/") {
				continue
			}
			matched = true
			// rest is whatever follows shortBase in the actual row name:
			// "", "-<GOMAXPROCS>", "/<subname>", or "/<subname>-<GOMAXPROCS>".
			rest := strings.TrimPrefix(nameNoPrefix, shortBase)
			row := rows[name]
			results = append(results, Row{
				Name:         base + rest,
				Old:          row.old,
				New:          row.new,
				DeltaPercent: row.deltaPercent,
				Significant:  row.significant,
				Found:        true,
				Flagged:      row.significant && row.deltaPercent > RegressionThresholdPercent,
			})
		}
		if !matched {
			results = append(results, Row{Name: base, Found: false})
		}
	}
	return results
}

func renderMarkdown(results []Row) string {
	var b strings.Builder
	b.WriteString("| Benchmark | Old | New | Delta | Significant | Flagged |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, r := range results {
		if !r.Found {
			fmt.Fprintf(&b, "| %s | - | - | - | - | not found |\n", r.Name)
			continue
		}
		delta := nonSignificantMarker
		if r.Significant {
			delta = fmt.Sprintf("%+.2f%%", r.DeltaPercent)
		}
		flagged := "no"
		if r.Flagged {
			flagged = "**yes**"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %t | %s |\n", r.Name, r.Old, r.New, delta, r.Significant, flagged)
	}
	return b.String()
}
