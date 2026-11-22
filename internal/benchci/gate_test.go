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
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// gateCSV is benchstat -format=csv output (same structure as fixtureCSV)
// covering the gate decision table, one gate benchmark per case:
//   - ChainSyncFromGenesis: +7%, significant -> under the gate threshold.
//   - RealBlockProcessing: +30%, "~" -> noise, never flagged.
//   - BlockProcessingThroughput: +12.5%, significant -> flagged.
//   - BlockProcessingThroughputPredecoded: -20%, significant -> an
//     improvement, never flagged.
//
// ReadParamsRequest is absent, as for a benchmark the base revision lacks.
const gateCSV = `goos: linux
goarch: amd64
pkg: github.com/blinklabs-io/dingo/ledger
cpu: AMD EPYC 7763
,base.txt,,head.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
ChainSyncFromGenesis-4,1e-02,1%,1.07e-02,1%,+7.00%,p=0.002 n=8
RealBlockProcessing-4,1e-03,9%,1.3e-03,9%,~,p=0.200 n=8
BlockProcessingThroughput-4,1e-04,1%,1.125e-04,1%,+12.50%,p=0.001 n=8
BlockProcessingThroughputPredecoded-4,1e-04,1%,8e-05,1%,-20.00%,p=0.001 n=8
geomean,1e-03,,1e-03,,+0.00%,
`

func flaggedNames(report string) []string {
	var out []string
	for line := range strings.SplitSeq(report, "\n") {
		if strings.HasSuffix(line, "| **yes** |") {
			out = append(out, strings.TrimSpace(strings.Split(line, "|")[1]))
		}
	}
	return out
}

func TestGateReport_FlagsOnlySignificantRegressionBeyondThreshold(
	t *testing.T,
) {
	t.Parallel()
	report, regressed, err := GateReport(gateCSV)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	if !regressed {
		t.Fatalf("expected a regression, report:\n%s", report)
	}
	got := flaggedNames(report)
	want := []string{"BenchmarkBlockProcessingThroughput-4"}
	if !slices.Equal(got, want) {
		t.Fatalf("flagged %v, want %v; report:\n%s", got, want, report)
	}
}

// A significant delta below the gate threshold must pass, while the weekly
// 5% rule still flags the same data: the gate threshold is what keeps a
// shared-runner shift between 5% and 10% from failing a pull request.
func TestGateReport_UnderThresholdPasses(t *testing.T) {
	t.Parallel()
	csv := strings.Replace(
		gateCSV,
		"BlockProcessingThroughput-4,1e-04,1%,1.125e-04,1%,+12.50%",
		"BlockProcessingThroughput-4,1e-04,1%,1.07e-04,1%,+7.00%",
		1,
	)
	_, regressed, err := GateReport(csv)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	if regressed {
		t.Fatal("a significant +7% delta must not trip the gate")
	}
	_, weeklyRegressed, err := Report(csv, GateBenchmarks)
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if !weeklyRegressed {
		t.Fatal("the weekly 5% rule should flag the same +7% delta")
	}
}

func TestGateReport_NoiseOnlyPasses(t *testing.T) {
	t.Parallel()
	csv := strings.Replace(
		gateCSV,
		"+12.50%,p=0.001",
		"~,p=0.300",
		1,
	)
	csv = strings.Replace(csv, "1.125e-04,1%,~", "1.4e-04,9%,~", 1)
	report, regressed, err := GateReport(csv)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	if regressed {
		t.Fatalf("non-significant deltas must not fail the gate:\n%s", report)
	}
}

// A comparison that matched no gate benchmark has proven nothing, so it is an
// error rather than a pass.
func TestGateReport_NoGateRowsIsError(t *testing.T) {
	t.Parallel()
	_, _, err := GateReport(fixtureCSV)
	if !errors.Is(err, ErrNoGateRows) {
		t.Fatalf("err = %v, want ErrNoGateRows", err)
	}
}

func TestGateBenchmarksAreCurated(t *testing.T) {
	t.Parallel()
	for _, name := range GateBenchmarks {
		if !slices.Contains(CuratedBenchmarks, name) {
			t.Errorf("%s is gated but not in CuratedBenchmarks", name)
		}
	}
}

// bench-gate.sh cannot import GateBenchmarks, so its regex is checked here.
func TestGateScriptRegexMatchesGateBenchmarks(t *testing.T) {
	t.Parallel()
	script, err := os.ReadFile(
		filepath.Join("..", "..", ".github", "scripts", "bench-gate.sh"),
	)
	if err != nil {
		t.Fatalf("read script: %v", err)
	}
	m := regexp.MustCompile(`(?m)^BENCH_REGEX='\^Benchmark\(([^)]*)\)\$'$`).
		FindSubmatch(script)
	if m == nil {
		t.Fatal("BENCH_REGEX not found in bench-gate.sh")
	}
	var want []string
	for _, n := range GateBenchmarks {
		want = append(want, strings.TrimPrefix(n, "Benchmark"))
	}
	got := strings.Split(string(m[1]), "|")
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("script regex has %v, GateBenchmarks has %v", got, want)
	}
}

// benchstat prints a benchmark that only the base file holds as a row with
// the base columns alone. A gate benchmark the head no longer reports was
// deleted, renamed, or skipped, so the gate has no head measurement for it
// and must fail rather than pass it as not found.
func TestGateReport_MissingFromHeadFails(t *testing.T) {
	t.Parallel()
	csv := strings.Replace(
		gateCSV,
		"geomean,",
		"ReadParamsRequest-4,1e-04,1%\ngeomean,",
		1,
	)
	report, regressed, err := GateReport(csv)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	if !regressed {
		t.Fatalf("a gate benchmark missing from head must fail:\n%s", report)
	}
	got := flaggedNames(report)
	want := []string{
		"BenchmarkBlockProcessingThroughput-4",
		"BenchmarkReadParamsRequest-4",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("flagged %v, want %v; report:\n%s", got, want, report)
	}
}

// A benchmark only the head reports is new, and has nothing to regress from.
func TestGateReport_NewInHeadPasses(t *testing.T) {
	t.Parallel()
	csv := strings.Replace(
		gateCSV,
		"BlockProcessingThroughput-4,1e-04,1%,1.125e-04,1%,+12.50%",
		"BlockProcessingThroughput-4,1e-04,1%,1.07e-04,1%,+7.00%",
		1,
	)
	csv = strings.Replace(
		csv,
		"geomean,",
		"ReadParamsRequest-4,,,1e-04,1%\ngeomean,",
		1,
	)
	report, regressed, err := GateReport(csv)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	if regressed {
		t.Fatalf("a benchmark new in head must not fail the gate:\n%s", report)
	}
}

// liveHeadOnlyCSV is the sec/op part of benchstat's output for a real gate
// run, inputs labeled base and head as CompareGate labels them. The pull
// request added BenchmarkReadParamsRequest, so its whole package table holds
// only the head column, and its row has the same field count as a base-only
// row in a two-column table.
const liveHeadOnlyCSV = `pkg: github.com/blinklabs-io/dingo/api/utxorpc
,head,
,sec/op,CI
ReadParamsRequest-4,0.00020813850000000003,1%
geomean,0.0002081385000000001,

pkg: github.com/blinklabs-io/dingo/ledger
,base,,head,,,
,sec/op,CI,sec/op,CI,vs base,P
RealBlockProcessing-4,4.40965e-05,6%,4.3875500000000004e-05,4%,~,p=0.721 n=8
ChainSyncFromGenesis-4,0.0081762415,7%,0.0078413035,5%,~,p=0.382 n=8
BlockProcessingThroughput-4,4.80805e-05,6%,4.6280500000000005e-05,2%,~,p=0.195 n=8
BlockProcessingThroughputPredecoded/tracing=off-4,2.4505500000000004e-05,2%,2.4343e-05,2%,~,p=0.505 n=8
BlockProcessingThroughputPredecoded/tracing=unsampled-4,2.4959000000000003e-05,3%,2.4448e-05,3%,~,p=0.279 n=8
geomean,0.00010117739909709834,,9.892935193657792e-05,,-2.22%,
`

// liveBaseOnlyCSV is the same run with the inputs swapped, so the package
// table holds only the base column: the benchmark is gone from head.
const liveBaseOnlyCSV = `pkg: github.com/blinklabs-io/dingo/api/utxorpc
,base,
,sec/op,CI
ReadParamsRequest-4,0.00020813850000000003,1%
geomean,0.0002081385000000001,

pkg: github.com/blinklabs-io/dingo/ledger
,base,,head,,,
,sec/op,CI,sec/op,CI,vs base,P
RealBlockProcessing-4,4.3875500000000004e-05,4%,4.40965e-05,6%,~,p=0.721 n=8
ChainSyncFromGenesis-4,0.0078413035,5%,0.0081762415,7%,~,p=0.382 n=8
BlockProcessingThroughput-4,4.6280500000000005e-05,2%,4.80805e-05,6%,~,p=0.195 n=8
BlockProcessingThroughputPredecoded/tracing=off-4,2.4343e-05,2%,2.4505500000000004e-05,2%,~,p=0.505 n=8
BlockProcessingThroughputPredecoded/tracing=unsampled-4,2.4448e-05,3%,2.4959000000000003e-05,3%,~,p=0.279 n=8
geomean,9.892935193657792e-05,,0.00010117739909709834,,+2.27%,
`

func TestGateReport_LiveRunNewPackageInHeadPasses(t *testing.T) {
	t.Parallel()
	report, regressed, err := GateReport(liveHeadOnlyCSV)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	if regressed {
		t.Fatalf("a benchmark only head reports must not fail:\n%s", report)
	}
}

func TestGateReport_LiveRunPackageMissingFromHeadFails(t *testing.T) {
	t.Parallel()
	report, regressed, err := GateReport(liveBaseOnlyCSV)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	got := flaggedNames(report)
	want := []string{"BenchmarkReadParamsRequest-4"}
	if !regressed || !slices.Equal(got, want) {
		t.Fatalf(
			"regressed=%t flagged %v, want %v; report:\n%s",
			regressed, got, want, report,
		)
	}
}

// subVariantRemovedCSV is benchstat output for a real gate run whose head
// dropped one sub-benchmark of BlockProcessingThroughputPredecoded and kept
// the other.
const subVariantRemovedCSV = `pkg: github.com/blinklabs-io/dingo/ledger
,base,,head,,,
,sec/op,CI,sec/op,CI,vs base,P
RealBlockProcessing-4,4.40965e-05,6%,4.40965e-05,6%,~,p=1.000 n=8
ChainSyncFromGenesis-4,0.0081762415,7%,0.0081762415,7%,~,p=1.000 n=8
BlockProcessingThroughput-4,4.80805e-05,6%,4.80805e-05,6%,~,p=1.000 n=8
BlockProcessingThroughputPredecoded/tracing=off-4,2.4505500000000004e-05,2%,2.4505500000000004e-05,2%,~,p=1.000 n=8
BlockProcessingThroughputPredecoded/tracing=unsampled-4,2.4959000000000003e-05,3%
geomean,0.00010117739909709834,,0.0001435646754823904,,+0.00%,
`

// A gated benchmark's surviving sub-benchmark must not hide a removed one.
func TestGateReport_RemovedSubBenchmarkFails(t *testing.T) {
	t.Parallel()
	report, regressed, err := GateReport(subVariantRemovedCSV)
	if err != nil {
		t.Fatalf("GateReport: %v", err)
	}
	got := flaggedNames(report)
	want := []string{
		"BenchmarkBlockProcessingThroughputPredecoded/tracing=unsampled-4",
	}
	if !regressed || !slices.Equal(got, want) {
		t.Fatalf(
			"regressed=%t flagged %v, want %v; report:\n%s",
			regressed, got, want, report,
		)
	}
	if !strings.Contains(
		report,
		"| BenchmarkBlockProcessingThroughputPredecoded/tracing=off-4 |",
	) {
		t.Fatalf("surviving sub-benchmark missing from report:\n%s", report)
	}
}
