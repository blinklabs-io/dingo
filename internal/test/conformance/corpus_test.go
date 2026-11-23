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
	"testing"

	"github.com/blinklabs-io/ouroboros-mock/conformance"
	"github.com/stretchr/testify/require"
)

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
	assertCorpusSetsMatch(t, backend, sqliteCorpusResults(t), results)
}

// assertCorpusSetsMatch is assertBackendMatchesSqlite's comparison.
func assertCorpusSetsMatch(
	t *testing.T,
	backend string,
	sqliteResults []conformance.VectorResult,
	results []conformance.VectorResult,
) {
	t.Helper()

	require.Equal(
		t,
		len(sqliteResults),
		len(results),
		"%s backend exercised a different number of vectors than sqlite; "+
			"vector discovery/extraction should be backend-invariant",
		backend,
	)

	// Compare the path sets, not just their sizes. Equal counts over
	// different paths would otherwise slip through, and the pass lookup
	// below cannot catch it on its own: a backend path absent from the
	// sqlite map reads as false, which is exactly what the assertion
	// expects, so {a,b} against {a,c} would pass on both checks.
	require.ElementsMatch(
		t,
		corpusPaths(sqliteResults),
		corpusPaths(results),
		"%s backend exercised different vectors than sqlite; vector "+
			"discovery/extraction should be backend-invariant",
		backend,
	)

	sqlitePassed := make(map[string]bool, len(sqliteResults))
	for _, result := range sqliteResults {
		sqlitePassed[result.Path] = result.Success
	}
	for _, result := range results {
		if result.Success {
			continue
		}
		// No presence guard here: ElementsMatch above FailNows on any path
		// set difference, so every backend path exists in sqlitePassed by
		// the time this loop runs.
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

// corpusPaths returns each result's vector path, for set comparison between
// backends.
func corpusPaths(results []conformance.VectorResult) []string {
	paths := make([]string, len(results))
	for i, result := range results {
		paths[i] = result.Path
	}
	return paths
}
