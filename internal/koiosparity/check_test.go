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
	"encoding/hex"
	"log/slog"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
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
	gdb := openTestSQLDB(t, dir, false)
	require.NoError(t, gdb.Close())
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

// TestCheckAllReturnsZeroEpochsCheckedForUnfetchedEpoch documents the
// invariant `explain --live` relies on to detect a mistyped or out-of-range
// --epoch: with All:true and FromEpoch==ThroughEpoch==epoch, an epoch that
// was never fetched is simply absent from GetAllFetchedEpochs, so
// EpochsChecked stays 0 — as opposed to some other "nothing to do" reason
// that would also need distinguishing.
func TestCheckAllReturnsZeroEpochsCheckedForUnfetchedEpoch(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// Only epoch 100 is fetched; 999 never appears in the cache.
	seedFreshStatus(t, cache, "preview", 100, StatusPass)

	result, err := Check(context.Background(), CheckConfig{
		Network:      "preview",
		DingoDB:      DingoDBConfig{Plugin: "sqlite", DataDir: newTestDingoDataDir(t)},
		CachePath:    cachePath,
		All:          true,
		FromEpoch:    999,
		ThroughEpoch: 999,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 0, result.EpochsChecked, "epoch 999 was never fetched")
}

// newTestDingoDB creates an empty, WAL-mode, schema-migrated Dingo
// metadata.sqlite (matching newTestDingoDataDir) but returns a writable GORM
// handle to it too, so a test can seed reward_pool_input/reward_pool_output/
// epoch_summary rows directly before Check opens its own read-only
// connection against the same file.
func newTestDingoDB(t *testing.T) (dataDir string, gdb *testDB) {
	t.Helper()
	dir := t.TempDir()
	return dir, openTestSQLDB(t, dir, true)
}

// TestCheckAlignsRewardScheduleEpochsEndToEnd is an end-to-end boundary test
// for the check.go/dingo_db.go epoch-alignment fix, built from Dingo's real
// snapshot/reward lifecycle layout rather than two same-epoch structs with
// equal fields (see koiosStakeEpoch/koiosParamEpoch's doc comments and
// TestGetPoolEpochDataMapAlignsRewardScheduleEpochs in dingo_db_test.go for
// the field-by-field derivation). For Koios reporting epoch K=10 it seeds:
//
//   - epoch_summary at epoch 9 (K-1): the mark stake distribution Praos used
//     as epoch 10's active-stake basis.
//   - reward_pool_input at epoch 9 (K-1): DelegatedStake/DelegatorCount.
//   - reward_pool_output at epoch 9 (K-1): MemberRewardTotal.
//   - reward_pool_input at epoch 11 (K+1): BlocksProduced/Margin/FixedCost,
//     describing epoch 10 per rotation.go's buildRewardStateInputs.
//   - reward_ada_pots at epoch 10 (unshifted — a point-in-time pot balance,
//     not a delayed reward-calculation input; see ARCHITECTURE.md).
//   - a decoy reward_pool_input row at epoch 10 itself with wrong values in
//     every field, so a regression to the naive same-epoch read fails loudly.
//
// The cached Koios reference row for epoch 10 is built to match this real
// data exactly, so a fully correct field-level epoch mapping is the only way
// Check reports PASS.
func TestCheckAlignsRewardScheduleEpochsEndToEnd(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(10)
	poolHash := testPoolKeyHash(t, 0x03)
	poolBech32, err := PoolKeyHashHexToBech32(hex.EncodeToString(poolHash))
	require.NoError(t, err)

	dingoDir, gdb := newTestDingoDB(t)

	// Decoy at the naive "same epoch as Koios" (10): wrong in every field.
	badBlocks := uint64(999)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          koiosEpoch,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(1),
		DelegatorCount: 1,
		Cost:           types.Uint64(1),
		Margin:         &types.Rat{Rat: big.NewRat(1, 2)},
		BlocksProduced: &badBlocks,
	}).Error)

	// Stake epoch (9 = K-1).
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            9,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          9,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(5_000_000),
		DelegatorCount: 7,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             9,
		PoolKeyHash:       poolHash,
		MemberRewardTotal: types.Uint64(123_456),
	}).Error)

	// Param epoch (11 = K+1).
	realBlocks := uint64(4)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          11,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(2), // irrelevant at this epoch
		Cost:           types.Uint64(340_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
		BlocksProduced: &realBlocks,
	}).Error)

	// reward_ada_pots stays at koiosEpoch itself (unshifted).
	require.NoError(t, gdb.Create(&models.RewardAdaPots{
		Epoch:    koiosEpoch,
		Treasury: types.Uint64(1_000),
		Reserves: types.Uint64(2_000),
		Fees:     types.Uint64(300),
	}).Error)

	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	require.NoError(t, cache.CommitEpochData(
		KoiosEpochInfo{
			Network:      network,
			Epoch:        koiosEpoch,
			ActiveStake:  "5000000",
			EpochEndTime: fetchedAt,
			FetchedAt:    fetchedAt,
		},
		[]KoiosPoolEpoch{{
			Network:       network,
			Epoch:         koiosEpoch,
			PoolBech32:    poolBech32,
			ActiveStake:   "5000000",
			BlockCnt:      4,
			Delegators:    7,
			Margin:        "0.1",
			FixedCost:     "340000000",
			MemberRewards: "123456",
			FetchedAt:     fetchedAt,
		}},
		&KoiosTotals{
			Network:   network,
			Epoch:     koiosEpoch,
			Treasury:  "1000",
			Reserves:  "2000",
			Fees:      "300",
			FetchedAt: fetchedAt,
		},
	))

	result, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 1, result.EpochsChecked)
	require.Empty(t, result.FailEpochs)
	require.Empty(t, result.ErrorEpochs)

	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	require.Empty(t, mismatches, "correct field-level epoch mapping must produce zero mismatches")
}

// TestCheckDetectsMissingKoiosTotalsOnUpgradedCache is a regression test for
// the "upgraded cache" scenario the reviewer flagged: a cache.db that has a
// koios_epoch_info row (from before /totals fetching was added to this tool,
// or from a --skip-fetch run against such a cache) but no koios_totals row
// for the same epoch. Before this fix, CompareEpochTotals silently skipped
// comparison whenever koiosTotals was nil, so an epoch like this could report
// a clean PASS despite treasury/reserves/fees never actually being validated.
// This confirms Check now surfaces it as ERROR instead.
func TestCheckDetectsMissingKoiosTotalsOnUpgradedCache(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(10)

	dingoDir, gdb := newTestDingoDB(t)

	// Stake epoch (9 = K-1): total_active_stake matches Koios exactly so this
	// path alone contributes no mismatches.
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            9,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)

	// epoch_summary at koiosEpoch itself (unshifted) — GetEpochData reads this
	// row (not the stakeEpoch one above) to decide whether reward_ada_pots is
	// ready to compare at all.
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            koiosEpoch,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)

	// reward_ada_pots at koiosEpoch itself (unshifted), matching what will be
	// committed to the cache below — so the only mismatch produced is the
	// missing /totals reference row, not a value divergence.
	require.NoError(t, gdb.Create(&models.RewardAdaPots{
		Epoch:    koiosEpoch,
		Treasury: types.Uint64(1_000),
		Reserves: types.Uint64(2_000),
		Fees:     types.Uint64(300),
	}).Error)

	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	// totals is deliberately nil here — simulating a cache that has never
	// fetched /totals for this epoch (pre-upgrade cache, or --skip-fetch).
	require.NoError(t, cache.CommitEpochData(
		KoiosEpochInfo{
			Network:      network,
			Epoch:        koiosEpoch,
			ActiveStake:  "5000000",
			EpochEndTime: fetchedAt,
			FetchedAt:    fetchedAt,
		},
		nil,
		nil,
	))

	result, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 1, result.EpochsChecked)
	require.Empty(t, result.FailEpochs)
	require.Equal(t, []uint64{koiosEpoch}, result.ErrorEpochs,
		"a missing Koios /totals reference row must surface as ERROR, not a silent PASS")

	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	require.Len(t, mismatches, 1)
	require.Equal(t, "koios_totals", mismatches[0].Field)
	require.Equal(t, CategoryDBMissing, mismatches[0].Category)

	statuses, err := cache.GetStatusSummary(network)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	require.Equal(t, StatusError, statuses[0].Status)
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
