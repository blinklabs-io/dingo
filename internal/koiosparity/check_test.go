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
func seedFreshStatus(
	t *testing.T,
	cache *Cache,
	network string,
	epoch uint64,
	status string,
) {
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
		Network: "preview",
		DingoDB: DingoDBConfig{
			Plugin:  "sqlite",
			DataDir: newTestDingoDataDir(t),
		},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(
		t,
		0,
		result.EpochsChecked,
		"nothing should have needed rechecking",
	)
	require.Equal(
		t,
		[]uint64{100},
		result.FailEpochs,
		"a persisted FAIL must surface even though no epoch was freshly checked",
	)
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
		Network: "preview",
		DingoDB: DingoDBConfig{
			Plugin:  "sqlite",
			DataDir: newTestDingoDataDir(t),
		},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 0, result.EpochsChecked)
	require.Equal(
		t,
		[]uint64{200},
		result.ErrorEpochs,
		"a persisted ERROR must surface even though no epoch was freshly checked",
	)
	require.Empty(t, result.FailEpochs)
}

// TestCheckReselectsPoolOnlyEpochMissingAccountCoverage is the regression
// test for the account-coverage-blind epoch-selection bug found in review of
// #3097: an epoch fetched/checked before #3097 existed (pool-level Koios
// data cached, a fresh persisted PASS, and no koios_account_coverage row at
// all — the realistic pre-#3097-to-post-#3097 upgrade path for a Dingo
// deployment that already ran koios-parity) must be reselected by
// GetEpochsNeedingCheck/Check once AccountsEnabled is turned on, purely
// because its account coverage is missing — not left at its stale pool-only
// PASS forever just because nothing about its pool/aggregate data changed.
//
// Before the fix, GetEpochsNeedingCheck ignored koios_account_coverage
// entirely: this epoch's koios_epoch_info.fetched_at never changes and its
// check_epoch_status.last_checked_at is already newer, so it would never be
// reselected and compareEpochAccounts would never run for it — the persisted
// PASS would be reported forever with zero account-level validation ever
// attempted.
func TestCheckReselectsPoolOnlyEpochMissingAccountCoverage(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	const network = "preview"
	const epoch = uint64(50)
	fetchedAt := time.Now().Add(-time.Hour).UTC()

	// Seed a complete pool-level cache (epoch_info + totals, matching a real
	// pre-#3097 fetchEpoch commit) and a fresh persisted PASS — deliberately
	// with NO koios_account_coverage row, simulating an epoch that was
	// fetched/checked before #3097's per-account fetch/check phases existed.
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  "100",
		EpochEndTime: fetchedAt,
		FetchedAt:    fetchedAt,
	}, nil, &KoiosTotals{
		Treasury:  "1",
		Reserves:  "1",
		Fees:      "1",
		Reward:    "1",
		FetchedAt: fetchedAt,
	}))
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network: network,
		Epoch:   epoch,
		LastCheckedAt: fetchedAt.Add(
			time.Minute,
		), // fresh relative to fetched_at
		Status: StatusPass,
	}))

	// Sanity check: pool-only mode (AccountsEnabled=false, the standalone
	// CLI's default) must NOT change behavior — this is the existing,
	// already-correct case the fix must not regress.
	needingPoolOnly, err := cache.GetEpochsNeedingCheck(network, false)
	require.NoError(t, err)
	require.Empty(
		t,
		needingPoolOnly,
		"pool-only mode must not reselect a fresh persisted PASS",
	)

	// This is the actual bug: with accounts enabled, the epoch must be
	// reselected purely because koios_account_coverage is absent, even though
	// nothing about its pool data or check status changed.
	needingWithAccounts, err := cache.GetEpochsNeedingCheck(network, true)
	require.NoError(t, err)
	require.Equal(
		t,
		[]uint64{epoch},
		needingWithAccounts,
		"an epoch with no account coverage row must be reselected when accounts are enabled",
	)

	// Running Check end-to-end must actually invoke compareEpochAccounts for
	// this epoch (via GetEpochsNeedingCheck's selection above) rather than
	// silently leaving the stale PASS untouched. It has no account coverage
	// fetched yet, so it must surface a real acct_coverage_incomplete ERROR —
	// proof the account comparison phase actually ran — rather than reporting
	// the old PASS forever.
	result, err := Check(context.Background(), CheckConfig{
		Network: network,
		DingoDB: DingoDBConfig{
			Plugin:  "sqlite",
			DataDir: newTestDingoDataDir(t),
		},
		CachePath:       cachePath,
		AccountsEnabled: true,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(
		t,
		1,
		result.EpochsChecked,
		"the epoch must actually be (re)checked, not silently skipped",
	)
	require.Equal(t, []uint64{epoch}, result.ErrorEpochs)
	require.Empty(t, result.FailEpochs)

	mismatches, err := cache.GetMismatches(network, epoch, "")
	require.NoError(t, err)
	var sawAcctCoverageIncomplete bool
	for _, m := range mismatches {
		if m.Category == CategoryAcctCoverageIncomplete {
			sawAcctCoverageIncomplete = true
		}
	}
	require.True(
		t,
		sawAcctCoverageIncomplete,
		"a real acct_coverage_incomplete mismatch must be recorded — proof the account phase actually ran, not a silently-preserved stale PASS",
	)

	statuses, err := cache.GetStatusSummary(network)
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	require.Equal(
		t,
		StatusError,
		statuses[0].Status,
		"the stale pool-only PASS must be overwritten by a real account-aware result",
	)
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

	seedFreshStatus(
		t,
		cache,
		"preview",
		100,
		StatusFail,
	) // outside requested scope below
	seedFreshStatus(
		t,
		cache,
		"preview",
		300,
		StatusFail,
	) // inside requested scope below

	dingoDir := newTestDingoDataDir(t)

	result, err := Check(context.Background(), CheckConfig{
		Network:      "preview",
		DingoDB:      DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath:    cachePath,
		FromEpoch:    250,
		ThroughEpoch: 350,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(
		t,
		[]uint64{300},
		result.FailEpochs,
		"only the persisted FAIL within [FromEpoch, ThroughEpoch] should surface",
	)
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
		Network: "preview",
		DingoDB: DingoDBConfig{
			Plugin:  "sqlite",
			DataDir: newTestDingoDataDir(t),
		},
		CachePath:    cachePath,
		All:          true,
		FromEpoch:    999,
		ThroughEpoch: 999,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 0, result.EpochsChecked, "epoch 999 was never fetched")
}

// newTestDingoDB creates an empty, WAL-mode, schema-migrated Dingo
// metadata.sqlite (matching newTestDingoDataDir) but returns a writable SQL
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
//   - reward_pool_input at epoch 9 (K-1): DelegatedStake/DelegatorCount,
//     plus Margin/FixedCost — the parameters in force for epoch 10.
//   - reward_pool_output at epoch 9 (K-1): MemberRewardTotal.
//   - reward_pool_input at epoch 11 (K+1): BlocksProduced alone, describing
//     epoch 10 per rotation.go's buildRewardStateInputs.
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
		Cost:           types.Uint64(340_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             9,
		PoolKeyHash:       poolHash,
		MemberRewardTotal: types.Uint64(123_456),
	}).Error)

	// Param epoch (11 = K+1): blocks only. Its own stake and pool params
	// belong to a later epoch and must not surface.
	realBlocks := uint64(4)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          11,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(2), // irrelevant at this epoch
		Cost:           types.Uint64(999_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 2)},
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
	require.Empty(
		t,
		mismatches,
		"correct field-level epoch mapping must produce zero mismatches",
	)
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
	require.Equal(
		t,
		[]uint64{koiosEpoch},
		result.ErrorEpochs,
		"a missing Koios /totals reference row must surface as ERROR, not a silent PASS",
	)

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

// TestCheckEpochPreservesPriorMismatchEvidenceOnLaterReadFailure guards
// against #3410: checkEpoch used to clear an epoch's persisted mismatch rows
// before every fallible Dingo/cache read for that epoch had completed, so a
// later read failure (here, cache.GetTotals erroring instead of returning
// sql.ErrNoRows) erased the last known evidence and left nothing behind. The
// koios_totals table is dropped after seeding to force GetTotals to fail
// with a real DB error partway through checkEpoch, well after the point
// where the old code had already deleted the prior rows.
func TestCheckEpochPreservesPriorMismatchEvidenceOnLaterReadFailure(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(10)

	dingoDir, gdb := newTestDingoDB(t)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            9,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            koiosEpoch,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
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
		nil,
		nil,
	))

	// Prior evidence from an earlier successful check of this epoch.
	priorEvidence := []CheckMismatch{{
		Network:    network,
		Epoch:      koiosEpoch,
		Field:      "prior_evidence",
		DingoValue: "1",
		KoiosValue: "2",
		Category:   CategoryDBError,
		CheckedAt:  fetchedAt,
	}}
	require.NoError(t, cache.CommitEpochMismatches(network, koiosEpoch, priorEvidence))

	// Force cache.GetTotals to fail with a real DB error (not sql.ErrNoRows),
	// simulating the koios_totals read failing partway through checkEpoch.
	_, err = cache.db.Exec("DROP TABLE koios_totals")
	require.NoError(t, err)

	dingoSource, err := OpenDingoDB(DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir})
	require.NoError(t, err)
	defer dingoSource.Close() //nolint:errcheck

	_, err = CheckEpoch(
		context.Background(),
		cache,
		dingoSource,
		network,
		koiosEpoch,
		0,
		false,
		slog.New(slog.DiscardHandler),
	)
	require.Error(t, err, "a failed koios_totals read must surface as an error")

	got, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	require.Len(
		t,
		got,
		1,
		"a later read failure must never erase the prior mismatch evidence",
	)
	require.Equal(t, "prior_evidence", got[0].Field)
}

// TestCheckAccountsCoverageIncompleteIsError proves that enabling
// CheckConfig.AccountsEnabled without ever having run a successful account
// fetch (no koios_account_coverage row for the epoch) surfaces as ERROR
// (CategoryAcctCoverageIncomplete), never a silent PASS — the coverage gate
// compareEpochAccounts must consult before treating koios_account_rewards as
// a complete reference set.
func TestCheckAccountsCoverageIncompleteIsError(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(10)

	dingoDir, gdb := newTestDingoDB(t)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            9,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        koiosEpoch,
		ActiveStake:  "5000000",
		EpochEndTime: fetchedAt,
		FetchedAt:    fetchedAt,
	}, nil, &KoiosTotals{Network: network, Epoch: koiosEpoch, FetchedAt: fetchedAt}))

	result, err := Check(context.Background(), CheckConfig{
		Network:         network,
		DingoDB:         DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath:       cachePath,
		AccountsEnabled: true,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, []uint64{koiosEpoch}, result.ErrorEpochs)
	require.Empty(t, result.FailEpochs)

	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	found := false
	for _, m := range mismatches {
		if m.Category == CategoryAcctCoverageIncomplete {
			found = true
		}
	}
	require.True(t, found, "expected a CategoryAcctCoverageIncomplete mismatch")
}

// TestCheckAccountsCoverageDBErrorIsNotConflatedWithIncompleteCoverage guards
// against compareEpochAccounts treating a genuine koios_account_coverage
// query failure (e.g. a corrupted cache) the same as "no account fetch has
// been attempted yet" (sql.ErrNoRows). Replacing the table with one missing
// the "requested_count" column produces a real, non-ErrNoRows error from
// GetAccountCoverage's own SELECT — unlike a dropped table (which Check's own
// OpenCache call would silently recreate via "CREATE TABLE IF NOT EXISTS",
// a no-op once a table of that name already exists — the same reason
// TestAccountRewardsAdditiveColumnMigration builds its pre-migration table
// directly rather than relying on OpenCache) — while leaving
// GetEpochsNeedingCheck's own join (which never selects requested_count)
// unaffected, so this epoch is still selected for checking in the first
// place. This must surface as an explicit CategoryDBError
// ("koios_account_coverage" / dingo_db_error), not the
// CategoryAcctCoverageIncomplete this same field can also report for the
// legitimate "not fetched yet" case (see
// TestCheckAccountsCoverageIncompleteIsError).
func TestCheckAccountsCoverageDBErrorIsNotConflatedWithIncompleteCoverage(
	t *testing.T,
) {
	const network = "preview"
	const koiosEpoch = uint64(10)

	dingoDir, gdb := newTestDingoDB(t)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            9,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        koiosEpoch,
		ActiveStake:  "5000000",
		EpochEndTime: fetchedAt,
		FetchedAt:    fetchedAt,
	}, nil, &KoiosTotals{Network: network, Epoch: koiosEpoch, FetchedAt: fetchedAt}))

	// Simulate a genuine cache/DB failure distinct from "no fetch attempted
	// yet": replace koios_account_coverage with a table missing the
	// "requested_count" column GetAccountCoverage's SELECT requires, so that
	// specific query errors instead of returning sql.ErrNoRows.
	// GetEpochsNeedingCheck's own join never selects requested_count, so this
	// epoch is still correctly selected for (re)checking.
	_, err = cache.db.Exec("DROP TABLE koios_account_coverage")
	require.NoError(t, err)
	_, err = cache.db.Exec(`CREATE TABLE koios_account_coverage (
		id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
		fetched_count INTEGER NOT NULL DEFAULT 0,
		complete INTEGER NOT NULL DEFAULT 0, fetched_at DATETIME NOT NULL)`)
	require.NoError(t, err)

	result, err := Check(context.Background(), CheckConfig{
		Network:         network,
		DingoDB:         DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath:       cachePath,
		AccountsEnabled: true,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, []uint64{koiosEpoch}, result.ErrorEpochs)
	require.Empty(t, result.FailEpochs)

	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	var found *CheckMismatch
	for i := range mismatches {
		if mismatches[i].Field == "koios_account_coverage" {
			found = &mismatches[i]
		}
	}
	require.NotNil(t, found, "expected a koios_account_coverage mismatch")
	require.Equal(
		t,
		CategoryDBError,
		found.Category,
		"a genuine DB error querying coverage must be dingo_db_error, not acct_coverage_incomplete",
	)
}

// TestCheckAccountsEndToEndExactMatchAndMismatch is an end-to-end #3097 test:
// with account coverage marked complete, an exact-match account produces no
// mismatch and a 1-lovelace-off account produces a value_mismatch — proving
// the full path from CheckConfig.AccountsEnabled through checkEpoch's
// coverage gate, StakeAddressFromCredential resolution, and
// CompareAccountEpoch.
func TestCheckAccountsEndToEndExactMatchAndMismatch(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(10)
	const stakeEpoch = uint64(9) // K-1, per koiosStakeEpoch

	dingoDir, gdb := newTestDingoDB(t)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            stakeEpoch,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)

	okKey := testPoolKeyHash(t, 0x41)
	badKey := testPoolKeyHash(t, 0x42)
	poolKeyHash := testPoolKeyHash(t, 0x22)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch:       stakeEpoch,
		StakingKey:  okKey,
		PoolKeyHash: poolKeyHash,
		RewardType:  "member",
		Amount:      types.Uint64(1_000_000),
		Spendable:   true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch:       stakeEpoch,
		StakingKey:  badKey,
		PoolKeyHash: poolKeyHash,
		RewardType:  "member",
		Amount:      types.Uint64(2_000_000), // will differ from koios below
		Spendable:   true,
	}).Error)

	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	okAddr, err := StakeAddressFromCredential(okKey, 0)
	require.NoError(t, err)
	badAddr, err := StakeAddressFromCredential(badKey, 0)
	require.NoError(t, err)

	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        koiosEpoch,
		ActiveStake:  "5000000",
		EpochEndTime: fetchedAt,
		FetchedAt:    fetchedAt,
	}, nil, &KoiosTotals{Network: network, Epoch: koiosEpoch, FetchedAt: fetchedAt}))

	require.NoError(
		t,
		cache.CommitAccountRewardsForEpoch(
			network,
			koiosEpoch,
			[]KoiosAccountRewards{
				{
					StakeAddress: okAddr,
					RewardType:   "member",
					Earned:       "1000000",
					FetchedAt:    fetchedAt,
				},
				{
					StakeAddress: badAddr,
					RewardType:   "member",
					Earned:       "2000001",
					FetchedAt:    fetchedAt,
				},
			},
			2,
			true,
			fetchedAt,
		),
	)

	result, err := Check(context.Background(), CheckConfig{
		Network:         network,
		DingoDB:         DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath:       cachePath,
		AccountsEnabled: true,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, []uint64{koiosEpoch}, result.FailEpochs)
	require.Empty(t, result.ErrorEpochs)

	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	var acctMismatches []CheckMismatch
	for _, m := range mismatches {
		if m.Field == "account_reward_amount" {
			acctMismatches = append(acctMismatches, m)
		}
	}
	require.Len(t, acctMismatches, 1)
	require.Equal(t, badAddr, acctMismatches[0].StakeAddress)
	require.Equal(t, "2000000", acctMismatches[0].DingoValue)
	require.Equal(t, "2000001", acctMismatches[0].KoiosValue)
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

// seedPoolPresenceFixture builds the shared Koios-epoch-K fixture for the two
// pool-presence tests below: one established pool that matches Koios exactly
// at every field, plus a second pool seeded only where the caller asks for it.
// It returns the Dingo data dir and cache path ready for Check.
func seedPoolPresenceFixture(
	t *testing.T,
	network string,
	koiosEpoch uint64,
	secondPoolStakeEpochRow bool,
	secondPoolHash []byte,
) (dingoDir, cachePath string) {
	t.Helper()
	stakeEpoch := koiosEpoch - 1
	paramEpoch := koiosEpoch + 1

	poolHash := testPoolKeyHash(t, 0x03)
	poolBech32, err := PoolKeyHashHexToBech32(hex.EncodeToString(poolHash))
	require.NoError(t, err)

	dingoDir, gdb := newTestDingoDB(t)

	// Established pool: complete rows at both epochs.
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            stakeEpoch,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          stakeEpoch,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(5_000_000),
		DelegatorCount: 7,
		Cost:           types.Uint64(340_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             stakeEpoch,
		PoolKeyHash:       poolHash,
		MemberRewardTotal: types.Uint64(123_456),
	}).Error)
	realBlocks := uint64(4)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          paramEpoch,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(2),
		Cost:           types.Uint64(999_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 2)},
		BlocksProduced: &realBlocks,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAdaPots{
		Epoch:    koiosEpoch,
		Treasury: types.Uint64(1_000),
		Reserves: types.Uint64(2_000),
		Fees:     types.Uint64(300),
	}).Error)

	// Second pool. Its param-epoch row always exists; the stake-epoch row is
	// what distinguishes "registered during K, not yet in K's stake basis"
	// from "in K's stake basis but absent from Koios".
	if secondPoolStakeEpochRow {
		require.NoError(t, gdb.Create(&models.RewardPoolInput{
			Epoch:          stakeEpoch,
			PoolKeyHash:    secondPoolHash,
			DelegatedStake: types.Uint64(9_497_623_854),
			DelegatorCount: 1,
		}).Error)
	}
	newBlocks := uint64(0)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          paramEpoch,
		PoolKeyHash:    secondPoolHash,
		DelegatedStake: types.Uint64(9_497_623_854),
		DelegatorCount: 1,
		Cost:           types.Uint64(340_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
		BlocksProduced: &newBlocks,
	}).Error)

	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath = filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetchedAt := time.Now().Add(-time.Hour).UTC()
	// Koios lists only the established pool for epoch K.
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
	return dingoDir, cachePath
}

// TestCheckIgnoresParamEpochOnlyPoolForPresence covers a pool that registered
// during Koios epoch K. Its first mark snapshot is captured at the boundary
// into K+1, so Dingo has a reward_pool_input row at the param epoch (K+1) but
// none at the stake epoch (K-1) — it is not part of K's active-stake basis at
// all. Koios has no pool_history row for it until K+2.
//
// The presence check must therefore ignore it. Flagging it pool_only_dingo
// reports a Dingo defect where both sides in fact agree: the pool simply did
// not exist yet for the epoch being compared. Observed on preview epoch 7,
// where two pools registered mid-epoch failed the strict observer (dingo
// #3483).
func TestCheckIgnoresParamEpochOnlyPoolForPresence(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(10)
	newPoolHash := testPoolKeyHash(t, 0x07)

	dingoDir, cachePath := seedPoolPresenceFixture(
		t, network, koiosEpoch, false, newPoolHash,
	)

	result, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Empty(t, result.FailEpochs)

	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	require.Empty(
		t,
		mismatches,
		"a pool known only through the param epoch is not part of epoch "+
			"%d's stake basis and must not be reported as present only in "+
			"Dingo", koiosEpoch,
	)
}

// TestCheckFlagsStakeEpochPoolMissingFromKoios is the negative case: a pool
// that IS in epoch K's stake basis but absent from Koios's pool set is a real
// divergence and must still be flagged, so the fix above cannot be widened
// into suppressing genuine pool_only_dingo findings.
func TestCheckFlagsStakeEpochPoolMissingFromKoios(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(10)
	newPoolHash := testPoolKeyHash(t, 0x07)

	dingoDir, cachePath := seedPoolPresenceFixture(
		t, network, koiosEpoch, true, newPoolHash,
	)

	_, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)

	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)

	var found bool
	for _, m := range mismatches {
		if m.Field == "pool_presence" &&
			m.Category == CategoryPoolOnlyDingo {
			found = true
		}
	}
	require.True(
		t,
		found,
		"a pool in epoch %d's stake basis but absent from Koios is a real "+
			"divergence and must stay flagged", koiosEpoch,
	)
}

// TestCheckDepartedPoolDoesNotErrorEpoch is the end-to-end counterpart of
// compare_test.go's TestComparePoolEpochDepartedPoolIsInformational: it proves
// checkEpoch resolves the K+1 snapshot's readiness for itself, which the
// ComparePoolEpoch unit tests cannot.
//
// The fixture mirrors preview epoch 14. The pool is in epoch K's stake basis
// (a K-1 reward_pool_input row) and absent from the K+1 snapshot, which is
// itself committed — a ready epoch_summary row at K+1. That combination means
// the pool left the pool set, so the epoch must still report PASS rather than
// halting a strict-mode node (dingo #3485).
func TestCheckDepartedPoolDoesNotErrorEpoch(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(14)

	// The pool is absent from the K+1 pool set: it left, so its epoch-K block
	// count has no row to live on and the gap is informational.
	dingoDir, cachePath, _ := seedDepartureFixture(
		t, network, koiosEpoch, false,
	)

	result, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Empty(
		t,
		result.ErrorEpochs,
		"a departed pool must not put epoch %d into ERROR", koiosEpoch,
	)
	require.Empty(t, result.FailEpochs)

	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	require.Len(t, mismatches, 1)
	require.Equal(t, "reward_pool_input_params", mismatches[0].Field)
	require.Equal(
		t,
		CategoryPoolDeparted,
		mismatches[0].Category,
		"the uncomparable block count is recorded, not silently skipped",
	)
}

// seedDepartureFixture builds the epoch-K comparison fixture used by the
// departure tests. paramEpochPoolInSet controls whether the pool still appears
// in the K+1 mark pool_stake_snapshot, which is what separates a pool that
// left the pool set from one whose reward inputs are missing.
func seedDepartureFixture(
	t *testing.T,
	network string,
	koiosEpoch uint64,
	paramEpochPoolInSet bool,
) (dingoDir, cachePath string, poolBech32 string) {
	t.Helper()
	stakeEpoch := koiosEpoch - 1
	paramEpoch := koiosEpoch + 1

	poolHash := testPoolKeyHash(t, 0x09)
	poolBech32, err := PoolKeyHashHexToBech32(hex.EncodeToString(poolHash))
	require.NoError(t, err)

	dingoDir, gdb := newTestDingoDB(t)

	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            stakeEpoch,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.EpochSummary{
		Epoch:            paramEpoch,
		TotalActiveStake: types.Uint64(5_000_000),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolInput{
		Epoch:          stakeEpoch,
		PoolKeyHash:    poolHash,
		DelegatedStake: types.Uint64(5_000_000),
		DelegatorCount: 7,
		Cost:           types.Uint64(340_000_000),
		Margin:         &types.Rat{Rat: big.NewRat(1, 10)},
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardPoolOutput{
		Epoch:             stakeEpoch,
		PoolKeyHash:       poolHash,
		MemberRewardTotal: types.Uint64(123_456),
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAdaPots{
		Epoch:    koiosEpoch,
		Treasury: types.Uint64(1_000),
		Reserves: types.Uint64(2_000),
		Fees:     types.Uint64(300),
	}).Error)

	// Another pool always sits in the K+1 set, so membership is established
	// either way and the two cases differ only in this pool's presence.
	require.NoError(t, gdb.Create(&models.PoolStakeSnapshot{
		Epoch:        paramEpoch,
		SnapshotType: "mark",
		PoolKeyHash:  testPoolKeyHash(t, 0x0a),
		TotalStake:   types.Uint64(1_000),
	}).Error)
	if paramEpochPoolInSet {
		require.NoError(t, gdb.Create(&models.PoolStakeSnapshot{
			Epoch:        paramEpoch,
			SnapshotType: "mark",
			PoolKeyHash:  poolHash,
			TotalStake:   types.Uint64(5_000_000),
		}).Error)
	}

	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath = filepath.Join(t.TempDir(), "cache.db")
	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
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
			BlockCnt:      15,
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
	require.NoError(t, cache.Close())
	return dingoDir, cachePath, poolBech32
}

// TestCheckDegradedActivePoolStillErrors is the reviewer's degraded-active-pool
// case. buildRewardStateInputs (ledger/snapshot/rotation.go) drops a pool with
// stale registration data from reward_pool_input so one bad pool cannot wedge
// the whole boundary capture, and says so explicitly: degraded pools are
// excluded "and only from it — PoolStakeSnapshot and EpochSummary still
// reflect the true observed stake".
//
// Such a pool is still in the K+1 pool set, so its missing reward-input row is
// genuine missing input, not departure. Classifying it as pool_departed would
// turn a real ERROR into a PASS, which is why departure is decided from
// per-pool pool_stake_snapshot membership rather than from
// epoch_summary.SnapshotReady (dingo #3485).
func TestCheckDegradedActivePoolStillErrors(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(14)

	dingoDir, cachePath, _ := seedDepartureFixture(t, network, koiosEpoch, true)

	result, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Contains(
		t,
		result.ErrorEpochs,
		koiosEpoch,
		"a pool still in the K+1 pool set with no reward-input row is "+
			"missing input, not a departure",
	)

	cache, err := OpenCache(cachePath, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	require.Len(t, mismatches, 1)
	require.Equal(t, CategoryDBMissing, mismatches[0].Category)
}

// TestCheckMissingRewardBundleStillErrors is the reviewer's no-bundle case.
// saveSnapshotInTxn persists the mark pool_stake_snapshot and epoch summary on
// every transition "regardless of reward-input availability", so a ready
// epoch_summary at K+1 is compatible with the entire reward-input bundle
// having been skipped. Every pool then looks absent from reward_pool_input at
// once, and none of them departed.
//
// The pool set at K+1 still lists them, so each stays an ERROR.
func TestCheckMissingRewardBundleStillErrors(t *testing.T) {
	const network = "preview"
	const koiosEpoch = uint64(14)

	// paramEpochPoolInSet: the whole bundle is missing, so no pool has a K+1
	// reward_pool_input row while all of them remain in the pool set.
	dingoDir, cachePath, _ := seedDepartureFixture(t, network, koiosEpoch, true)

	result, err := Check(context.Background(), CheckConfig{
		Network:   network,
		DingoDB:   DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
		CachePath: cachePath,
	}, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Contains(
		t,
		result.ErrorEpochs,
		koiosEpoch,
		"a skipped reward-input bundle must not read as every pool departing",
	)
}
