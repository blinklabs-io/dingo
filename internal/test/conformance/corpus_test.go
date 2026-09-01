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
	"os"
	"sync"
	"testing"

	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

// One full replay of the Amaru vector corpus is expensive -- it was 917s of
// this package's Linux CI time with real Postgres and MySQL attached -- and
// the corpus exercises gouroboros ledger rules, which do not vary by storage
// backend. Replaying it more than once per backend therefore buys no rule
// coverage.
//
// What the per-backend replays do buy is dialect divergence, and that is not
// hypothetical: #3599 found two real bugs this way, neither of them a rule bug.
// loadPoolAssociations held a pool_registration cursor open while issuing
// nested per-row queries on one connection, which SQLite tolerates and MySQL
// and PostgreSQL do not; and go-sql-driver/mysql reports rows changed rather
// than rows matched, so a DRep voting twice in an epoch with an unchanged
// expiry looked like a missing row. Both were found by driving the storage
// layer through the corpus's variety of access patterns, which needs one pass
// per dialect, not several.
//
// So each backend replays the corpus exactly once per `go test` process, and
// every consumer -- the pass/fail gate, the progress statistics, and the
// cross-backend comparison -- reads that one memoized result set. Before this,
// a Linux CI run replayed the corpus eight times: SQLite four (a plain pass, a
// statistics pass, and a fresh baseline rebuilt inside each of the two
// comparison tests), Postgres twice, and MySQL twice.

// corpusRun is one backend's memoized corpus replay. err is retained rather
// than failing inside the sync.Once, so that every test reading this backend
// reports the same construction or replay failure instead of only whichever
// test happened to trigger the Once first.
type corpusRun struct {
	results []conformance.VectorResult
	err     error
}

var (
	testdataOnce sync.Once
	testdataDir  string
	testdataRoot string
	testdataErr  error
)

// corpusTestdataRoot extracts the embedded vector corpus once per process.
// It deliberately does not use t.TempDir(): the extraction is shared across
// tests, so tying it to the lifetime of whichever test triggered it first
// would delete it while later tests still name its paths in failure output.
// TestMain removes it (see cleanupCorpusTestdata).
func corpusTestdataRoot() (string, error) {
	testdataOnce.Do(func() {
		dir, err := os.MkdirTemp("", "dingo-conformance-vectors-")
		if err != nil {
			testdataErr = fmt.Errorf("create testdata dir: %w", err)
			return
		}
		// Record the directory before extraction, not after: a failure
		// below still leaves a real directory on disk, and
		// cleanupCorpusTestdata's empty-string early return would skip it.
		testdataDir = dir
		// ExtractEmbeddedTestdata returns the extracted root, which is a
		// "testdata" subdirectory of dir, not dir itself. Use the returned
		// path; dir is only what gets removed on cleanup.
		root, err := conformance.ExtractEmbeddedTestdata(dir)
		if err != nil {
			testdataErr = fmt.Errorf("extract embedded testdata: %w", err)
			return
		}
		testdataRoot = root
	})
	return testdataRoot, testdataErr
}

// cleanupCorpusTestdata removes the shared extraction. Safe to call when the
// corpus was never extracted, which is the case for a run whose tests all
// skipped.
func cleanupCorpusTestdata() error {
	if testdataDir == "" {
		return nil
	}
	return os.RemoveAll(testdataDir)
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

// assertBackendMatchesSqlite compares an external backend's replay against the
// SQLite baseline. Vector discovery is backend-invariant, so a different count
// means extraction or discovery diverged rather than a rule behaving
// differently; a vector failing here that SQLite passed is a dialect
// divergence, which is what running the corpus on this backend is for.
//
// The vector comparison is deliberately one-directional. The opposite
// divergence -- SQLite failing a vector this backend passes -- is not silent:
// assertCorpus runs over every backend's own results, including SQLite's in
// TestRulesConformanceVectors, and fails on any vector that backend failed. So
// a divergence in either direction turns the run red; naming it here as well
// would only duplicate the SQLite gate. What this direction adds is
// attribution, pointing at the backend rather than at the corpus.
func assertBackendMatchesSqlite(
	t *testing.T,
	backend string,
	results []conformance.VectorResult,
) {
	t.Helper()
	sqliteResults := sqliteCorpusResults(t)

	require.Equal(
		t,
		len(sqliteResults),
		len(results),
		"%s backend exercised a different number of vectors than sqlite; "+
			"vector discovery/extraction should be backend-invariant",
		backend,
	)

	sqlitePassed := map[string]bool{}
	for _, result := range sqliteResults {
		sqlitePassed[result.Path] = result.Success
	}
	for _, result := range results {
		if result.Success {
			continue
		}
		require.Falsef(
			t,
			sqlitePassed[result.Path],
			"%s backend failed a vector sqlite passed (%s at event %d): %v",
			backend,
			result.Title,
			result.FailedEvent,
			result.Error,
		)
	}
}
