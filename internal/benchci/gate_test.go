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

func TestGateReport_FlagsOnlySignificantRegressionBeyondThreshold(t *testing.T) {
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
