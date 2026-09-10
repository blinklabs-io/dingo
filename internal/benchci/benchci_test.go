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

package benchci

import (
	"strings"
	"testing"
)

// fixtureCSV mirrors the exact structure of real
// `go run golang.org/x/perf/cmd/benchstat@<BenchstatVersion> -format=csv`
// output, verified by running benchstat against real `go test -bench
// -count=10` result files: a metadata header, a "sec/op" table, then "B/op"
// and "allocs/op" tables (present whenever the benchmarks call
// b.ReportAllocs(), as ours do), each block separated by exactly one blank
// line and ending in a "geomean" row. Row names drop the "Benchmark" prefix
// entirely (a real `go test -bench=BenchmarkFoo` result compared with
// benchstat prints its row as "Foo-8", not "BenchmarkFoo-8"); the fixture
// benchmark names below (NoRegression, Regressed, ...) are deliberately
// written without the prefix to match, and the tests pass the "Benchmark"
// -prefixed form to Report to exercise evaluateCurated's un-prefixing.
//
// It covers all four regression-detection cases in one comparison:
//   - BenchmarkNoRegression: tiny delta, not significant -> not flagged.
//   - BenchmarkRegressed: >5% delta, significant -> flagged.
//   - BenchmarkNoisyRegression: >5% apparent delta but benchstat printed
//     "~" (not significant, e.g. too few samples or too much variance) ->
//     not flagged. This is the noise-suppression case the AND-of-both
//     -signals logic exists for.
//   - BenchmarkImproved: significant, but a negative (faster) delta ->
//     never flagged regardless of magnitude.
const fixtureCSV = `goos: linux
goarch: amd64
pkg: github.com/blinklabs-io/dingo/ledger
cpu: AMD EPYC 7763
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
NoRegression-8,6.352e-07,1%,6.379e-07,0%,~,p=0.287 n=10
Regressed-8,6.349e-07,0%,7.13e-07,1%,+12.34%,p=0.000 n=10
NoisyRegression-8,1e-06,5%,1.2e-06,5%,~,p=0.200 n=10
Improved-8,1.2875e-06,7%,1.1845e-06,1%,-8.00%,p=0.000 n=10
geomean,8.501e-07,,7.8e-07,,-8.25%,

,old.txt,,new.txt,,,
,B/op,CI,B/op,CI,vs base,P
NoRegression-8,0,0%,0,0%,~,p=1.000 n=10
Regressed-8,0,0%,0,0%,~,p=1.000 n=10
NoisyRegression-8,0,0%,0,0%,~,p=1.000 n=10
Improved-8,0,0%,0,0%,~,p=1.000 n=10
geomean,,,,,+0.00%,

,old.txt,,new.txt,,,
,allocs/op,CI,allocs/op,CI,vs base,P
NoRegression-8,0,0%,0,0%,~,p=1.000 n=10
Regressed-8,0,0%,0,0%,~,p=1.000 n=10
NoisyRegression-8,0,0%,0,0%,~,p=1.000 n=10
Improved-8,0,0%,0,0%,~,p=1.000 n=10
geomean,,,,,+0.00%,
`

// A tiny, non-significant delta must never be flagged.
func TestReport_NoRegression(t *testing.T) {
	report, regressed, err := Report(fixtureCSV, []string{"BenchmarkNoRegression"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if regressed {
		t.Fatalf("expected no regression, report:\n%s", report)
	}
	if !strings.Contains(report, "BenchmarkNoRegression-8") {
		t.Fatalf("report missing benchmark row:\n%s", report)
	}
}

// The core positive case: a >5% delta that benchstat also marks
// statistically significant must be flagged.
func TestReport_RegressedAndSignificant_Flagged(t *testing.T) {
	report, regressed, err := Report(fixtureCSV, []string{"BenchmarkRegressed"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !regressed {
		t.Fatalf("expected a regression to be flagged, report:\n%s", report)
	}
	if !strings.Contains(report, "**yes**") {
		t.Fatalf("report should mark the row flagged:\n%s", report)
	}
}

// The noise-suppression case the AND-of-both-signals rule exists for: a large
// apparent delta that benchstat did not consider significant (printed "~")
// must not be flagged, since it's most likely CI runner noise rather than a
// real regression.
func TestReport_RegressedButNotSignificant_NotFlagged(t *testing.T) {
	report, regressed, err := Report(fixtureCSV, []string{"BenchmarkNoisyRegression"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if regressed {
		t.Fatalf("a non-significant delta must not be flagged, report:\n%s", report)
	}
}

// A significant improvement (a negative, faster delta) must never be
// flagged, regardless of how large the improvement is.
func TestReport_Improvement_NotFlagged(t *testing.T) {
	report, regressed, err := Report(fixtureCSV, []string{"BenchmarkImproved"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if regressed {
		t.Fatalf("a significant improvement must not be flagged, report:\n%s", report)
	}
}

// With all four fixture benchmarks curated together (the shape a real
// bench-ci run produces), exactly the one that actually regressed must be
// flagged, and every curated name must still appear in the report -- the
// no-regression/noisy/improved rows should not be silently dropped.
func TestReport_MixedSet_OnlyRegressedFlags(t *testing.T) {
	curated := []string{
		"BenchmarkNoRegression",
		"BenchmarkRegressed",
		"BenchmarkNoisyRegression",
		"BenchmarkImproved",
	}
	report, regressed, err := Report(fixtureCSV, curated)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !regressed {
		t.Fatalf("expected overall regression=true because BenchmarkRegressed flags, report:\n%s", report)
	}
	// Exactly one row should be marked flagged.
	if got := strings.Count(report, "**yes**"); got != 1 {
		t.Fatalf("expected exactly 1 flagged row, got %d, report:\n%s", got, report)
	}
	for _, name := range curated {
		if !strings.Contains(report, name) {
			t.Fatalf("report missing %s:\n%s", name, report)
		}
	}
}

// A curated name absent from benchstat's output (renamed, removed, or a
// typo) must be reported as a visible "not found" row rather than silently
// dropped or treated as a false regression.
func TestReport_UnknownBenchmark_ReportedNotFound(t *testing.T) {
	report, regressed, err := Report(fixtureCSV, []string{"BenchmarkDoesNotExist"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if regressed {
		t.Fatalf("a missing benchmark must not be flagged, report:\n%s", report)
	}
	if !strings.Contains(report, "BenchmarkDoesNotExist") || !strings.Contains(report, "not found") {
		t.Fatalf("report should note the benchmark was not found:\n%s", report)
	}
}

// The parser must read timing values from the "sec/op" table specifically,
// not accidentally pick up the "B/op" or "allocs/op" tables that follow it.
func TestReport_IgnoresNonSecOpTables(t *testing.T) {
	// B/op and allocs/op both show 0 -> ~0.00% for every row in the
	// fixture; if the parser accidentally picked up one of those tables
	// instead of sec/op, none of the below assertions about the sec/op
	// deltas would hold.
	report, _, err := Report(fixtureCSV, []string{"BenchmarkRegressed"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !strings.Contains(report, "635ns") || !strings.Contains(report, "713ns") {
		t.Fatalf("expected sec/op values in report:\n%s", report)
	}
}

// fixtureMultiPkgCSV exercises benchstat's behavior when the input spans
// multiple packages: only the very first block carries the full
// goos/goarch/pkg/cpu metadata header; later blocks for a new package carry
// just a bare "pkg: ..." line (no blank line separating it from the
// metadata that would otherwise be expected), still followed by its own
// sec/op, B/op, and allocs/op tables. This mirrors `make bench-ci`'s output,
// which spans ledger, database, connmanager, event, peergov, ouroboros, and
// internal/integration.
const fixtureMultiPkgCSV = `goos: linux
goarch: amd64
pkg: github.com/blinklabs-io/dingo/database
cpu: AMD EPYC 7763
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
HotCacheGet-8,5e-08,1%,5.01e-08,1%,~,p=0.900 n=10
geomean,5e-08,,5.01e-08,,+0.20%,

,old.txt,,new.txt,,,
,B/op,CI,B/op,CI,vs base,P
HotCacheGet-8,0,0%,0,0%,~,p=1.000 n=10
geomean,,,,,+0.00%,

pkg: github.com/blinklabs-io/dingo/connmanager
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
UpdateConnectionMetrics-8,1e-07,1%,1.2e-07,1%,+20.00%,p=0.001 n=10
geomean,1e-07,,1.2e-07,,+20.00%,

,old.txt,,new.txt,,,
,B/op,CI,B/op,CI,vs base,P
UpdateConnectionMetrics-8,0,0%,0,0%,~,p=1.000 n=10
geomean,,,,,+0.00%,
`

// bench-ci's output spans several packages (ledger, database, connmanager,
// ...); the parser must read every package's sec/op table, not just the
// first one, even though only the first block carries the full metadata
// header.
func TestReport_MultiPackageInput(t *testing.T) {
	curated := []string{"BenchmarkHotCacheGet", "BenchmarkUpdateConnectionMetrics"}
	report, regressed, err := Report(fixtureMultiPkgCSV, curated)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !regressed {
		t.Fatalf("expected BenchmarkUpdateConnectionMetrics to flag a regression, report:\n%s", report)
	}
	if !strings.Contains(report, "BenchmarkHotCacheGet-8") {
		t.Fatalf("report missing row from the first package's block:\n%s", report)
	}
	if !strings.Contains(report, "BenchmarkUpdateConnectionMetrics-8") {
		t.Fatalf("report missing row from the second package's block:\n%s", report)
	}
}

// fixtureCPUSweepCSV exercises the GOMAXPROCS lock-contention sweep, where
// -cpu=1,4,8,16 makes the same base benchmark name recur once per swept
// value in the same sec/op table. Verified directly against a real `go test
// -cpu=1,4,8,16` run: go test's testing package only appends the
// "-<GOMAXPROCS>" suffix when GOMAXPROCS != 1, so the -cpu=1 row carries no
// suffix at all while -cpu=4/8/16 do.
const fixtureCPUSweepCSV = `goos: linux
goarch: amd64
pkg: github.com/blinklabs-io/dingo/database
cpu: AMD EPYC 7763
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
BlockLRUParallelReadHeavy,1e-07,1%,1.01e-07,1%,~,p=0.500 n=10
BlockLRUParallelReadHeavy-4,1.2e-07,1%,1.22e-07,1%,~,p=0.400 n=10
BlockLRUParallelReadHeavy-8,1.5e-07,1%,1.58e-07,1%,~,p=0.200 n=10
BlockLRUParallelReadHeavy-16,2e-07,1%,6.5e-07,1%,+225.00%,p=0.000 n=10
geomean,1.406e-07,,2.293e-07,,+63.09%,
`

// Under a -cpu=1,4,8,16 sweep, one curated name matches four separate rows
// (one per core count, including the suffix-less -cpu=1 row); each must be
// reported and evaluated independently, so a regression that only appears at
// high core counts (the actual shape of the lock-contention bug this
// benchmark family exists to catch) still gets flagged.
func TestReport_CPUSweep_PerCoreCountRows(t *testing.T) {
	report, regressed, err := Report(fixtureCPUSweepCSV, []string{"BenchmarkBlockLRUParallelReadHeavy"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !regressed {
		t.Fatalf("expected the -16 row's contention regression to flag, report:\n%s", report)
	}
	// The -cpu=1 row carries no suffix at all (see fixtureCPUSweepCSV). Match
	// on "| <name> |" so the bare-name row can't be satisfied by a
	// substring match against the "-4"/"-8"/"-16" rows.
	for _, name := range []string{
		"BenchmarkBlockLRUParallelReadHeavy",
		"BenchmarkBlockLRUParallelReadHeavy-4",
		"BenchmarkBlockLRUParallelReadHeavy-8",
		"BenchmarkBlockLRUParallelReadHeavy-16",
	} {
		if !strings.Contains(report, "| "+name+" |") {
			t.Fatalf("report missing row %s:\n%s", name, report)
		}
	}
	// Only the -16 row should be flagged.
	if got := strings.Count(report, "**yes**"); got != 1 {
		t.Fatalf("expected exactly 1 flagged row across the sweep, got %d, report:\n%s", got, report)
	}
}

// fixtureSubBenchmarkCSV mirrors a real capture of
// `go test -bench=BenchmarkUpdateConnectionMetrics -count=3` piped through
// benchstat: BenchmarkUpdateConnectionMetrics calls b.Run(strconv.Itoa(n),
// ...) internally, so it never appears as a bare row -- only as
// "UpdateConnectionMetrics/<n>-10" once per table-driven case. Several other
// curated benchmarks (BenchmarkVerifyBlockHeader, BenchmarkStorageModeIngest,
// BenchmarkTestLoad, BenchmarkReconcile, BenchmarkPublishSubscribers,
// BenchmarkHasInboundPeerAddress, BenchmarkTryReserveInboundSlotParallel)
// share this shape.
const fixtureSubBenchmarkCSV = `goos: darwin
goarch: arm64
pkg: github.com/blinklabs-io/dingo/connmanager
cpu: Apple M1 Pro
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
UpdateConnectionMetrics/10-10,1.364e-08,∞,1.397e-08,∞,~,p=0.100 n=3
UpdateConnectionMetrics/100-10,1.364e-08,∞,1.368e-08,∞,~,p=0.500 n=3
UpdateConnectionMetrics/500-10,1.363e-08,∞,1.7e-08,∞,+24.72%,p=0.001 n=3
UpdateConnectionMetrics/1000-10,1.364e-08,∞,1.401e-08,∞,~,p=0.100 n=3
geomean,1.364e-08,,1.466e-08,,+7.48%,
`

// A curated name that only ever appears as "<name>/<subname>-<N>" (because
// the benchmark calls b.Run internally) must match every sub-benchmark row,
// and a curated name with no matching rows at all must still report exactly
// one not-found row -- not one per swept dimension it could have matched.
func TestReport_SubBenchmarkFamily(t *testing.T) {
	report, regressed, err := Report(fixtureSubBenchmarkCSV, []string{"BenchmarkUpdateConnectionMetrics"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !regressed {
		t.Fatalf("expected the /500 sub-benchmark's regression to flag, report:\n%s", report)
	}
	for _, sub := range []string{"/10-10", "/100-10", "/500-10", "/1000-10"} {
		name := "BenchmarkUpdateConnectionMetrics" + sub
		if !strings.Contains(report, "| "+name+" |") {
			t.Fatalf("report missing sub-benchmark row %s:\n%s", name, report)
		}
	}
	if got := strings.Count(report, "**yes**"); got != 1 {
		t.Fatalf("expected exactly 1 flagged sub-benchmark, got %d, report:\n%s", got, report)
	}
	// A curated name with no sub-benchmark rows at all in the input must
	// still report a single not-found row, not one per swept dimension.
	report2, regressed2, err := Report(fixtureSubBenchmarkCSV, []string{"BenchmarkDoesNotExist"})
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if regressed2 {
		t.Fatalf("unrelated missing benchmark must not flag, report:\n%s", report2)
	}
	if strings.Count(report2, "not found") != 1 {
		t.Fatalf("expected exactly 1 not-found row, report:\n%s", report2)
	}
}

// parseDelta must handle benchstat's non-significant marker ("~"), a signed
// percentage in either direction, and reject unparseable input.
func TestParseDelta(t *testing.T) {
	cases := []struct {
		in          string
		wantPercent float64
		wantSig     bool
		wantOK      bool
	}{
		{"~", 0, false, true},
		{"+12.34%", 12.34, true, true},
		{"-8.00%", -8, true, true},
		{"not-a-number", 0, false, false},
	}
	for _, c := range cases {
		gotPercent, gotSig, gotOK := parseDelta(c.in)
		if gotOK != c.wantOK {
			t.Fatalf("parseDelta(%q) ok = %v, want %v", c.in, gotOK, c.wantOK)
		}
		if !c.wantOK {
			continue
		}
		if gotSig != c.wantSig {
			t.Fatalf("parseDelta(%q) significant = %v, want %v", c.in, gotSig, c.wantSig)
		}
		if gotPercent != c.wantPercent {
			t.Fatalf("parseDelta(%q) percent = %v, want %v", c.in, gotPercent, c.wantPercent)
		}
	}
}

// formatSeconds must render benchstat's raw seconds float as a readable
// duration at nanosecond, millisecond, and second scales.
func TestFormatSeconds(t *testing.T) {
	cases := map[float64]string{
		6.3495e-07: "635ns",
		1e-03:      "1ms",
		1.5:        "1.5s",
	}
	for in, want := range cases {
		if got := formatSeconds(in); got != want {
			t.Fatalf("formatSeconds(%v) = %q, want %q", in, got, want)
		}
	}
}
