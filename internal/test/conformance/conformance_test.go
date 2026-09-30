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

package conformance

import (
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

const expectedBlueprintVectorCount = 2575

// TestRulesConformanceVectors runs the Cardano Blueprint ledger-rule
// conformance corpus using Dingo's ledger implementation via the shared
// harness from
// ouroboros-mock/conformance.
//
// The test vectors exercise ledger rules across the pinned eras, including:
// - UTxO validation (inputs, outputs, fees, collateral)
// - Certificate processing (stake, pool, DRep, committee)
// - Governance (proposals, voting, enactment)
// - Script execution (native scripts, Plutus V1/V2/V3)
//
// Test vectors are embedded in the ouroboros-mock module and extracted at test
// time. This asserts and reports from a single corpus replay; see
// tests_cc29688c_test.go for why the replay is memoized per backend and what the
// previous separate statistics pass cost.
func TestRulesConformanceVectors(t *testing.T) {
	results := sqliteCorpusResults(t)
	reportCorpus(t, "sqlite", results)
	require.Equal(t, expectedBlueprintVectorCount, len(results))
	assertCorpus(t, "sqlite", results)
}

// corpusRun is one backend's memoized corpus replay. err is retained rather
// than failing inside the sync.Once, so that every test reading this backend
// reports the same construction or replay failure instead of only whichever
// test happened to trigger the Once first.
type corpusRun struct {
	results []conformance.VectorResult
	err     error
}

// replayCorpus runs the whole corpus once against sm and returns per-vector
// results. It uses RunAllVectorsWithResults rather than RunAllVectors so the
// single pass can serve both the gate and the statistics; assertCorpus turns
// the results back into per-vector subtests, so no subtest naming is lost.
func replayCorpus(sm *DingoStateManager) corpusRun {
	root, err := corpusTestdataRoot()
	if err != nil {
		return corpusRun{err: err}
	}
	harness := conformance.NewHarness(sm, conformance.HarnessConfig{
		TestdataRoot: root,
	})
	results, err := harness.RunAllVectorsWithResults()
	if err != nil {
		return corpusRun{err: fmt.Errorf("run vectors: %w", err)}
	}
	return corpusRun{results: results}
}

var (
	sqliteCorpusOnce sync.Once
	sqliteCorpusRun  corpusRun
)

// sqliteCorpusResults returns the SQLite backend's memoized corpus replay.
// SQLite needs no external service, so this is the backend every run has and
// the baseline the Postgres and MySQL comparisons measure against.
func sqliteCorpusResults(t *testing.T) []conformance.VectorResult {
	t.Helper()
	sqliteCorpusOnce.Do(func() {
		sm, err := NewDingoStateManager()
		if err != nil {
			sqliteCorpusRun = corpusRun{
				err: fmt.Errorf("new sqlite state manager: %w", err),
			}
			return
		}
		defer sm.Close()
		sqliteCorpusRun = replayCorpus(sm)
	})
	require.NoError(t, sqliteCorpusRun.err, "sqlite corpus replay")
	return sqliteCorpusRun.results
}

// assertCorpus is the pass/fail gate. Each vector becomes a named subtest, as
// harness.RunAllVectors produced, so a failure still identifies its vector by
// path in the test output; the result carries the event index the vector
// failed at, which the assertion path did not report.
func assertCorpus(
	t *testing.T,
	backend string,
	results []conformance.VectorResult,
) {
	t.Helper()
	require.NotEmpty(
		t,
		results,
		"%s: corpus replay produced no vectors; vector discovery or "+
			"extraction is broken, and an empty corpus would otherwise "+
			"report as a pass",
		backend,
	)
	for _, result := range results {
		t.Run(result.Path, func(t *testing.T) {
			if result.Success {
				return
			}
			t.Fatalf(
				"vector failed on %s at event %d of %d: %v (%s)",
				backend,
				result.FailedEvent,
				result.EventCount,
				result.Error,
				result.Title,
			)
		})
	}
}

// reportCorpus logs the progress statistics that a separate second replay per
// backend used to produce.
func reportCorpus(
	t *testing.T,
	backend string,
	results []conformance.VectorResult,
) {
	t.Helper()
	passed, failed := corpusCounts(results)

	t.Logf("Conformance Test Results (%s):", backend)
	t.Logf("  Total vectors: %d", len(results))
	t.Logf("  Passed: %d", passed)
	t.Logf("  Failed: %d", failed)
	if len(results) > 0 {
		t.Logf(
			"  Pass rate: %.1f%%",
			float64(passed)/float64(len(results))*100,
		)
	}
	coverage := conformance.SummarizeCoverage(results)
	t.Logf("  Coverage groups: %d", len(coverage))
	for _, key := range conformance.SortedCoverageKeys(coverage) {
		counts := coverage[key]
		t.Logf(
			"  Coverage %s/%s: total=%d passed=%d failed=%d",
			key.Era,
			key.RuleFamily,
			counts.Total,
			counts.Passed,
			counts.Failed,
		)
	}

	if failed > 0 && testing.Verbose() {
		t.Log("First failures:")
		failCount := 0
		for _, result := range results {
			if !result.Success && failCount < 5 {
				t.Logf("  %s: %v", result.Title, result.Error)
				failCount++
			}
		}
		if failed > 5 {
			t.Logf("  ... and %d more failures", failed-5)
		}
	}
}

// corpusCounts returns the passed and failed vector counts.
func corpusCounts(results []conformance.VectorResult) (int, int) {
	var passed, failed int
	for _, result := range results {
		if result.Success {
			passed++
		} else {
			failed++
		}
	}
	return passed, failed
}
