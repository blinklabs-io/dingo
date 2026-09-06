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
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"sync/atomic"
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
	// CommitEpochData covers koios_epoch_info, koios_pool_epoch and
	// koios_totals in one call.
	require.NoError(t, c.CommitEpochData(
		KoiosEpochInfo{
			Network:      network,
			Epoch:        7,
			ActiveStake:  "1",
			Fees:         "1",
			TotalRewards: "1",
			EpochEndTime: now,
			FetchedAt:    now,
		},
		[]KoiosPoolEpoch{{
			Network:     network,
			Epoch:       7,
			PoolBech32:  "pool1seeded",
			ActiveStake: "1",
			FetchedAt:   now,
		}},
		&KoiosTotals{
			Network:   network,
			Epoch:     7,
			Treasury:  "1",
			Reserves:  "1",
			Fees:      "1",
			Reward:    "1",
			FetchedAt: now,
		},
	))
	// Staged chunk progress covers koios_account_fetch_staged_rows and
	// koios_account_checked.
	require.NoError(t, c.SaveAccountFetchChunkProgress(
		network,
		7,
		"chunkseed",
		[]KoiosAccountRewards{{
			StakeAddress: "stake_test1seeded",
			RewardType:   "member",
			Earned:       "1",
			FetchedAt:    now,
		}},
		[]string{"stake_test1seeded"},
		now,
	))
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

// TestSeedOracleRowsTouchesEveryInvalidatedTable keeps the fixture honest.
// The discard tests assert the row count reaches zero, so any table the
// fixture never writes is only ever "invalidated" while already empty, and a
// regression that stopped discarding it would still pass.
func TestSeedOracleRowsTouchesEveryInvalidatedTable(t *testing.T) {
	cache := newSourceTestCache(t)
	seedOracleRows(t, cache, "preview")
	for _, table := range koiosSourcedTables {
		var n int
		require.NoError(t, cache.db.QueryRow(
			"SELECT COUNT(*) FROM "+table+" WHERE network = ?", "preview",
		).Scan(&n))
		assert.Positive(t, n,
			"%s is invalidated on a source change but never seeded, so the discard is untested there",
			table)
	}
}

// TestRecordKoiosSourceFirstRunWithCustomRootDiscards is the upgrade path.
// A cache written before koios_source existed can only hold public-host rows,
// because no build without the column had an override to apply. Claiming them
// for a custom root would adopt the public host's answers as the mirror's —
// mixing two oracles on the exact path this guard exists to close.
func TestRecordKoiosSourceFirstRunWithCustomRootDiscards(t *testing.T) {
	cache := newSourceTestCache(t)
	seedOracleRows(t, cache, "preview")
	require.Positive(t, countOracleRows(t, cache, "preview"))

	change, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.True(t, change.Changed)
	assert.Equal(t, koiosBaseURLs["preview"], change.Previous,
		"an unattributed cache is attributed to the built-in public root")
	assert.Zero(t, countOracleRows(t, cache, "preview"),
		"public-host rows must not be adopted by a custom root")
}

// TestPendingKoiosSourceChangeMatchesRecord pins the two against each other.
// The probe gate reads Pending and the destruction happens in Record, so a
// disagreement would either skip the probe before a discard or demand one
// where nothing changes.
func TestPendingKoiosSourceChangeMatchesRecord(t *testing.T) {
	for _, tc := range []struct {
		name     string
		recorded string
		next     string
		want     bool
	}{
		{"unrecorded, public root", "", koiosBaseURLs["preview"], false},
		{"unrecorded, custom root", "", "https://koios.example/api/v1", true},
		{"recorded, same root", "https://a.example/api/v1", "https://a.example/api/v1", false},
		{"recorded, different root", "https://a.example/api/v1", "https://b.example/api/v1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := newSourceTestCache(t)
			if tc.recorded != "" {
				_, err := cache.RecordKoiosSource(
					"preview", tc.recorded, time.Now().UTC(),
				)
				require.NoError(t, err)
			}
			pending, _, err := cache.PendingKoiosSourceChange("preview", tc.next)
			require.NoError(t, err)
			assert.Equal(t, tc.want, pending)

			seedOracleRows(t, cache, "preview")
			change, err := cache.RecordKoiosSource(
				"preview", tc.next, time.Now().UTC(),
			)
			require.NoError(t, err)
			assert.Equal(t, pending, change.Changed,
				"PendingKoiosSourceChange must predict what RecordKoiosSource does")
		})
	}
}

// TestRecordKoiosSourceProbeFailureKeepsCache is the cost guard on the
// destructive path: a mistyped or unreachable new host must not discard the
// old host's rows, because recovering from that costs a full historical
// refetch — the expense that made the override worth guarding at all.
func TestRecordKoiosSourceProbeFailureKeepsCache(t *testing.T) {
	// 404, not 500: get() classifies 4xx as ErrKoiosPermanent and returns
	// immediately, while a 5xx would be retried three times with a 2s, 4s, 6s
	// backoff and put ~12s of sleep in every run of this package.
	dead := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}),
	)
	defer dead.Close()

	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	before := countOracleRows(t, cache, "preview")
	require.Positive(t, before)

	// httptest serves plain HTTP, so this needs the insecure escape hatch.
	client, err := NewKoiosClient("preview", "", dead.URL+"/api/v1", true)
	require.NoError(t, err)

	err = recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not answer")
	assert.Equal(t, before, countOracleRows(t, cache, "preview"),
		"a host that does not answer must not cost the cached reference data")

	got, _, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	assert.Equal(t, "https://koios.example/api/v1", got,
		"the source must not move to a host that never answered")
}

// TestRecordKoiosSourceProbeSuccessSwitches is the other half: once the new
// host answers, the switch goes through and the old oracle's rows go with it.
func TestRecordKoiosSourceProbeSuccessSwitches(t *testing.T) {
	live := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/v1/tip" {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer live.Close()

	cache := newSourceTestCache(t)
	_, err := cache.RecordKoiosSource(
		"preview", "https://koios.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)
	seedOracleRows(t, cache, "preview")
	require.Positive(t, countOracleRows(t, cache, "preview"))

	client, err := NewKoiosClient("preview", "", live.URL+"/api/v1", true)
	require.NoError(t, err)
	require.NoError(t, recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	))
	assert.Zero(t, countOracleRows(t, cache, "preview"))

	got, _, err := cache.GetKoiosSource("preview")
	require.NoError(t, err)
	assert.Equal(t, client.ResolvedBaseURL(), got)
}

// TestRecordKoiosSourceUnchangedMakesNoRequest keeps the probe off the
// ordinary start. Every run would otherwise pay a network round-trip before
// doing anything, and a transient blip on the common path would fail startup
// for a source that did not change.
func TestRecordKoiosSourceUnchangedMakesNoRequest(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			_, _ = w.Write([]byte(`[{"epoch_no":42}]`))
		}),
	)
	defer srv.Close()

	cache := newSourceTestCache(t)
	client, err := NewKoiosClient("preview", "", srv.URL+"/api/v1", true)
	require.NoError(t, err)

	// First call switches away from the attributed public root and probes.
	require.NoError(t, recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	))
	require.Equal(t, int32(1), hits.Load())

	// Second call changes nothing, so it must not probe again.
	require.NoError(t, recordKoiosSource(
		context.Background(), cache, "preview", client,
		slog.New(slog.DiscardHandler),
	))
	assert.Equal(t, int32(1), hits.Load(),
		"an unchanged source must not cost a request")
}

// TestClaimedSourceRefusesWritesAfterAnotherWriterRepoints is the concurrent
// case RecordKoiosSource alone cannot cover: it invalidates only the rows
// present when it runs, so a client already fetching from the old host would
// otherwise go on appending that host's answers under the new host's marker —
// reassembling the mixed-oracle state the marker exists to make impossible.
//
// The default cache path is shared across the standalone commands and the
// in-process observer, so an observer on one host and a `fetch --koios-url` on
// another are a reachable pair rather than a hypothetical one.
func TestClaimedSourceRefusesWritesAfterAnotherWriterRepoints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")

	first, err := OpenCache(path, nil)
	require.NoError(t, err)
	defer first.Close() //nolint:errcheck
	_, err = first.RecordKoiosSource(
		"preview", "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	// A write from the first handle is fine while it still owns the cache.
	require.NoError(t, first.CommitAccountRewardsForEpoch(
		"preview", 7, nil, 0, true, time.Now().UTC(),
	))

	// A second process re-points the same cache at another host.
	second, err := OpenCache(path, nil)
	require.NoError(t, err)
	defer second.Close() //nolint:errcheck
	_, err = second.RecordKoiosSource(
		"preview", "https://second.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	// The first handle must now refuse rather than mix.
	now := time.Now().UTC()
	err = first.CommitAccountRewardsForEpoch("preview", 7, nil, 0, true, now)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to write")

	require.Error(t, first.CommitEpochData(KoiosEpochInfo{
		Network:      "preview",
		Epoch:        7,
		ActiveStake:  "1",
		Fees:         "1",
		TotalRewards: "1",
		EpochEndTime: now,
		FetchedAt:    now,
	}, nil, nil))
	require.Error(t, first.SaveAccountUniverse(
		"preview", []string{"stake_test1x"}, now,
	))
	require.Error(t, first.SaveAccountFetchChunkProgress(
		"preview", 7, "chunk", nil, []string{"stake_test1x"}, now,
	))

	// The handle that owns the cache is unaffected.
	assert.NoError(t, second.CommitAccountRewardsForEpoch(
		"preview", 7, nil, 0, true, now,
	))
}

// TestUnclaimedCacheStillWrites keeps the guard off every read-only command
// and every existing caller: a handle that never recorded a source must not
// start refusing writes to a cache someone else stamped.
func TestUnclaimedCacheStillWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	owner, err := OpenCache(path, nil)
	require.NoError(t, err)
	defer owner.Close() //nolint:errcheck
	_, err = owner.RecordKoiosSource(
		"preview", "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	bystander, err := OpenCache(path, nil)
	require.NoError(t, err)
	defer bystander.Close() //nolint:errcheck
	assert.NoError(t, bystander.CommitAccountRewardsForEpoch(
		"preview", 7, nil, 0, true, time.Now().UTC(),
	))
}

// TestPreviousInferredDistinguishesAttributionFromRecord keeps the discard log
// honest. "The host we recorded" and "the host a legacy cache must have used"
// are different strengths of claim, and Previous alone cannot tell them apart.
func TestPreviousInferredDistinguishesAttributionFromRecord(t *testing.T) {
	t.Run("legacy cache is an inference", func(t *testing.T) {
		cache := newSourceTestCache(t)
		change, err := cache.RecordKoiosSource(
			"preview", "https://koios.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)
		require.True(t, change.Changed)
		assert.True(t, change.PreviousInferred)
		assert.Equal(t, koiosBaseURLs["preview"], change.Previous)
	})
	t.Run("a recorded source is a record", func(t *testing.T) {
		cache := newSourceTestCache(t)
		_, err := cache.RecordKoiosSource(
			"preview", "https://first.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)
		change, err := cache.RecordKoiosSource(
			"preview", "https://second.example/api/v1", time.Now().UTC(),
		)
		require.NoError(t, err)
		require.True(t, change.Changed)
		assert.False(t, change.PreviousInferred)
		assert.Equal(t, "https://first.example/api/v1", change.Previous)
	})
}

// TestPinnedSourceRefusesCheckWrites covers the derived half. RecordKoiosSource
// discards check evidence too, so a check already in flight would otherwise
// repopulate mismatches and status under a source its verdicts were never
// computed against — the same mixing, one layer up.
func TestPinnedSourceRefusesCheckWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	owner, err := OpenCache(path, nil)
	require.NoError(t, err)
	defer owner.Close() //nolint:errcheck
	_, err = owner.RecordKoiosSource(
		"preview", "https://first.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	// A checker pins whatever is recorded; it has no client to name a source.
	checker, err := OpenCache(path, nil)
	require.NoError(t, err)
	defer checker.Close() //nolint:errcheck
	require.NoError(t, checker.PinRecordedSource("preview"))

	now := time.Now().UTC()
	mismatch := []CheckMismatch{{
		Network: "preview", Epoch: 7, Field: "active_stake",
		DingoValue: "1", KoiosValue: "2",
		Category: CategoryValueMismatch, CheckedAt: now,
	}}
	status := CheckEpochStatus{
		Network: "preview", Epoch: 7, LastCheckedAt: now,
		Status: StatusFail, MismatchCount: 1,
	}
	run := CheckRun{Network: "preview", RunAt: now, EpochsChecked: 1}

	// While it still owns the pin, all three writes go through.
	require.NoError(t, checker.CommitEpochMismatches("preview", 7, mismatch))
	require.NoError(t, checker.UpsertCheckEpochStatus(status))
	require.NoError(t, checker.InsertCheckRun(run))

	// Another process re-points the cache mid-run.
	_, err = owner.RecordKoiosSource(
		"preview", "https://second.example/api/v1", time.Now().UTC(),
	)
	require.NoError(t, err)

	assert.Error(t, checker.CommitEpochMismatches("preview", 7, mismatch))
	assert.Error(t, checker.UpsertCheckEpochStatus(status))
	assert.Error(t, checker.InsertCheckRun(run))

	// The discarded evidence stays discarded rather than being rewritten.
	rows, err := owner.GetMismatches("preview", 7, "")
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// TestPinRecordedSourceOnUnstampedCacheEnforcesNothing keeps a check against a
// cache no writer has ever stamped behaving exactly as it did before.
func TestPinRecordedSourceOnUnstampedCacheEnforcesNothing(t *testing.T) {
	cache := newSourceTestCache(t)
	require.NoError(t, cache.PinRecordedSource("preview"))
	assert.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network: "preview", Epoch: 7,
		LastCheckedAt: time.Now().UTC(), Status: StatusPass,
	}))
}
