// Copyright 2025 Blink Labs Software
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
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// newTestDingoDataDir creates an empty-but-valid Dingo metadata.sqlite (WAL
// mode, schema migrated) and returns its containing directory, suitable for
// CheckConfig.DingoDB.DataDir. OpenDingoDB always opens read-only, so the
// journal mode must already be WAL before Check's read-only connection
// attaches — mirroring how a live Dingo node's own writable connection would
// have left it.
func newTestDingoDataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.sqlite")
	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", path)), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&models.EpochSummary{}, &models.RewardAdaPots{}))
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	return dir
}

// seedFreshStatus caches a Koios epoch_info row and a persisted
// CheckEpochStatus whose LastCheckedAt is newer than the epoch's FetchedAt —
// i.e. "fresh" per GetEpochsNeedingCheck, so Check will not select it for
// re-checking.
func seedFreshStatus(t *testing.T, cache *Cache, network string, epoch uint64, status string) {
	t.Helper()
	fetchedAt := time.Now().Add(-time.Hour).UTC()
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  "100",
		EpochEndTime: fetchedAt,
		FetchedAt:    fetchedAt,
	}, nil, nil))
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:       network,
		Epoch:         epoch,
		LastCheckedAt: fetchedAt.Add(time.Minute), // after FetchedAt: not stale
		Status:        status,
		MismatchCount: 1,
	}))
}

// TestCheckSurfacesPersistedFailWhenNothingNeedsRechecking guards against the
// false-success bug where a fresh cached FAIL (its reference row hasn't
// changed since the last check, so GetEpochsNeedingCheck returns nothing)
// produced an empty CheckResult — silently dropping the persisted failure
// because nothing was freshly (re)checked this run.
func TestCheckSurfacesPersistedFailWhenNothingNeedsRechecking(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	seedFreshStatus(t, cache, "preview", 100, StatusFail)

	result, err := Check(context.Background(), CheckConfig{
		Network:   "preview",
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: newTestDingoDataDir(t)},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 0, result.EpochsChecked, "nothing should have needed rechecking")
	require.Equal(t, []uint64{100}, result.FailEpochs,
		"a persisted FAIL must surface even though no epoch was freshly checked")
	require.Empty(t, result.ErrorEpochs)
}

// TestCheckSurfacesPersistedErrorWhenNothingNeedsRechecking is the ERROR-status
// counterpart to TestCheckSurfacesPersistedFailWhenNothingNeedsRechecking.
func TestCheckSurfacesPersistedErrorWhenNothingNeedsRechecking(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	seedFreshStatus(t, cache, "preview", 200, StatusError)

	result, err := Check(context.Background(), CheckConfig{
		Network:   "preview",
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: newTestDingoDataDir(t)},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 0, result.EpochsChecked)
	require.Equal(t, []uint64{200}, result.ErrorEpochs,
		"a persisted ERROR must surface even though no epoch was freshly checked")
	require.Empty(t, result.FailEpochs)
}

// TestCheckScopesPersistedOutcomeToFromThroughEpoch covers the 'check'
// subcommand's --from-epoch/--through-epoch scoping: a persisted FAIL outside
// the requested range must not fail the run, but one inside it must — the
// same effective-status computation Check performs must respect the caller's
// requested scope, not just the whole network's cache.
func TestCheckScopesPersistedOutcomeToFromThroughEpoch(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	seedFreshStatus(t, cache, "preview", 100, StatusFail) // outside requested scope below
	seedFreshStatus(t, cache, "preview", 300, StatusFail) // inside requested scope below

	dingoDir := newTestDingoDataDir(t)

	result, err := Check(context.Background(), CheckConfig{
		Network:      "preview",
		DingoDB:      DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath:    cachePath,
		FromEpoch:    250,
		ThroughEpoch: 350,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, []uint64{300}, result.FailEpochs,
		"only the persisted FAIL within [FromEpoch, ThroughEpoch] should surface")
}

func TestEffectiveCheckOutcome(t *testing.T) {
	statuses := []CheckEpochStatus{
		{Epoch: 1, Status: StatusPass},
		{Epoch: 2, Status: StatusFail},
		{Epoch: 3, Status: StatusError},
		{Epoch: 4, Status: StatusFail},
	}

	all := EffectiveCheckOutcome(statuses, 0, 0)
	require.Equal(t, []uint64{2, 4}, all.FailEpochs)
	require.Equal(t, []uint64{3}, all.ErrorEpochs)

	bounded := EffectiveCheckOutcome(statuses, 2, 3)
	require.Equal(t, []uint64{2}, bounded.FailEpochs)
	require.Equal(t, []uint64{3}, bounded.ErrorEpochs)
}
