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

package koiosparity

import (
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSourceTestCache(t *testing.T) *Cache {
	t.Helper()
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

// seedOracleRows writes one row into every table RecordKoiosSource is
// responsible for invalidating, so a test can tell "discarded" from "never
// written" without depending on the shape of any particular fetch path.
func seedOracleRows(t *testing.T, c *Cache, network string) {
	t.Helper()
	now := time.Now().UTC()
	require.NoError(t, c.UpsertEpochInfo(KoiosEpochInfo{
		Network:      network,
		Epoch:        7,
		ActiveStake:  "1",
		Fees:         "1",
		TotalRewards: "1",
		EpochEndTime: now,
		FetchedAt:    now,
	}))
	require.NoError(t, c.CommitAccountRewardsForEpoch(
		network,
		7,
		[]KoiosAccountRewards{{
			StakeAddress: "stake_test1seeded",
			RewardType:   "member",
			Earned:       "1",
			FetchedAt:    now,
		}},
		1,
		true,
		now,
	))
	require.NoError(t, c.CommitEpochMismatches(network, 7, []CheckMismatch{{
		Network:    network,
		Epoch:      7,
		Field:      "active_stake",
		DingoValue: "1",
		KoiosValue: "2",
		Category:   CategoryValueMismatch,
		CheckedAt:  now,
	}}))
	require.NoError(t, c.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:       network,
		Epoch:         7,
		LastCheckedAt: now,
		Status:        StatusFail,
		MismatchCount: 1,
	}))
	require.NoError(t, c.InsertCheckRun(CheckRun{
		Network:       network,
		RunAt:         now,
		EpochsChecked: 1,
		MismatchCount: 1,
	}))
	require.NoError(t, c.SaveAccountUniverse(
		network, []string{"stake_test1seeded"}, now,
	))
}

func countOracleRows(t *testing.T, c *Cache, network string) int {
	t.Helper()
	total := 0
	for _, table := range koiosSourcedTables {
		var n int
		require.NoError(t, c.db.QueryRow(
			"SELECT COUNT(*) FROM "+table+" WHERE network = ?", network,
		).Scan(&n))
		total += n
	}
	return total
}

// TestRecordKoiosSourceFirstRunKeepsRows pins that recording a source is not
// itself destructive: a cache written before this column existed is
// unattributed, not wrong, and must be claimed rather than thrown away.
func TestRecordKoiosSourceFirstRunKeepsRows(t *testing.T) {
	cache := newSourceTestCache(t)
	seedOracleRows(t, cache, "preview")
	before := countOracleRows(t, cache, "preview")
	require.Positive(t, before, "fixture must seed rows")

	change, err := cache.RecordKoiosSource(
		"preview", "https://preview.koios.rest/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.False(t, change.Changed)
	assert.Empty(t, change.Previous)
	assert.Equal(t, before, countOracleRows(t, cache, "preview"))

	got, ok, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "https://preview.koios.rest/api/v1", got)
}

// TestRecordKoiosSourceSameHostKeepsRows is the case that must not regress
// into "invalidate on every run": an unchanged source is the normal path, and
// discarding there would refetch the whole history on every start.
func TestRecordKoiosSourceSameHostKeepsRows(t *testing.T) {
	cache := newSourceTestCache(t)
	const url = "https://preview.koios.rest/api/v1"
	_, err := cache.RecordKoiosSource("preview", url, time.Now().UTC())
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	before := countOracleRows(t, cache, "preview")

	change, err := cache.RecordKoiosSource("preview", url, time.Now().UTC())
	require.NoError(t, err)
	assert.False(t, change.Changed)
	assert.Zero(t, change.RowsDiscarded)
	assert.Equal(t, before, countOracleRows(t, cache, "preview"))
}

// TestRecordKoiosSourceChangedHostDiscardsRows is the finding itself: without
// this, rows fetched from a self-hosted mirror and rows fetched from the
// public host are the same rows, and a run against the wrong oracle produces
// output indistinguishable from a run against the right one.
func TestRecordKoiosSourceChangedHostDiscardsRows(t *testing.T) {
	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	require.Positive(t, countOracleRows(t, cache, "preview"))

	change, err := cache.RecordKoiosSource(
		"preview", "https://preview.koios.rest/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.True(t, change.Changed)
	assert.Equal(t, "https://koios.example/api/v1", change.Previous)
	assert.Positive(t, change.RowsDiscarded)
	assert.Zero(t, countOracleRows(t, cache, "preview"),
		"no row fetched from the previous oracle may survive")

	got, _, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	assert.Equal(t, "https://preview.koios.rest/api/v1", got)
}

// TestRecordKoiosSourceChangeIsScopedToItsNetwork keeps the invalidation from
// becoming a bigger hammer than the problem: the base URL is resolved per
// network, so another network's rows were fetched from their own oracle and
// are not implicated by this one changing.
func TestRecordKoiosSourceChangeIsScopedToItsNetwork(t *testing.T) {
	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	seedOracleRows(t, cache, "preprod")
	other := countOracleRows(t, cache, "preprod")
	require.Positive(t, other)

	_, err = cache.RecordKoiosSource(
		"preview", "https://preview.koios.rest/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.Zero(t, countOracleRows(t, cache, "preview"))
	assert.Equal(t, other, countOracleRows(t, cache, "preprod"))
}

// TestRecordKoiosSourceCoversEveryKoiosTable stops koiosSourcedTables from
// silently falling behind the schema. A table added later that holds Koios
// answers but is missing from the list would survive a host change and put
// the mixed-oracle report back within reach — the exact failure this guard
// exists to prevent, reintroduced quietly.
func TestRecordKoiosSourceCoversEveryKoiosTable(t *testing.T) {
	cache := newSourceTestCache(t)
	rows, err := cache.db.Query(
		`SELECT name FROM sqlite_master
		WHERE type = 'table' AND (name LIKE 'koios_%' OR name LIKE 'check_%')`,
	)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck

	listed := make(map[string]struct{}, len(koiosSourcedTables))
	for _, t := range koiosSourcedTables {
		listed[t] = struct{}{}
	}
	seen := 0
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		// koios_source is the record itself, not a row it invalidates.
		if name == "koios_source" {
			continue
		}
		seen++
		_, ok := listed[name]
		assert.True(t, ok,
			"%s holds Koios-sourced rows but is not invalidated on a source change",
			name)
	}
	require.NoError(t, rows.Err())
	// Without this the test passes vacuously if the query ever stops
	// matching, which would retire the guard rather than satisfy it.
	require.Equal(t, len(koiosSourcedTables), seen,
		"every listed table must exist in the schema, and vice versa")
}

// TestKoiosSourcedTablesAreBareIdentifiers backs the G202 suppression on the
// DELETE in RecordKoiosSource. That annotation is only honest while every
// entry is a plain table identifier, so this pins the property rather than
// leaving the suppression to be quietly outgrown by a later entry.
func TestKoiosSourcedTablesAreBareIdentifiers(t *testing.T) {
	identifier := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	for _, table := range koiosSourcedTables {
		assert.Regexp(t, identifier, table,
			"%q is not a bare identifier, so it must not be concatenated into SQL",
			table)
	}
}
