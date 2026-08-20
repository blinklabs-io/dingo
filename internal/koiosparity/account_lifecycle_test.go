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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// TestAccountLifecycleMismatchesReportsZeroReward proves dingo #3099's
// zero-reward-confirmed reporting: an address Koios answered for with no
// reward rows is reported via CategoryAcctZeroReward — a dimension #3097's
// merged CompareAccountEpoch structurally cannot see (it only ever compares
// keys present in at least one side's row map). Reported as one aggregate
// row (count + a capped sample), not one row per address — see
// aggregateAccountLifecycleMismatch's doc comment. stakeEpoch=0 skips the
// separate lifecycle (newly-registered/deregistered) diff entirely, keeping
// this test focused on zero-reward alone.
func TestAccountLifecycleMismatchesReportsZeroReward(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now()
	// addr1 earns a reward (1 staged row); addr2 is confirmed checked with
	// zero reward rows (present in addressesInChunk, no matching row).
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview",
		500,
		"chunkA",
		[]KoiosAccountRewards{
			{StakeAddress: "addr1", RewardType: "member", Earned: "1000"},
		},
		[]string{"addr1", "addr2"},
		now,
	))

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, now,
	)
	require.Len(t, mismatches, 1)
	require.Equal(t, CategoryAcctZeroReward, mismatches[0].Category)
	require.Equal(
		t,
		"1",
		mismatches[0].KoiosValue,
		"KoiosValue carries the affected-address count, not a single address",
	)
	require.Contains(t, mismatches[0].DingoValue, "addr2")
}

// TestAccountLifecycleMismatchesReportsNewlyRegisteredAndDeregistered proves
// the epoch-over-epoch universe diff: an address present in the current
// stake epoch's Dingo-committed reward_account_output rows but not the
// previous stake epoch's is newly registered; the reverse is deregistered.
// Uses a real DingoDB (sqlite fixture), not a hand-rolled fake, per this
// package's existing RewardParitySource test convention
// (dingo_db_test.go/fetch_accounts_test.go).
func TestAccountLifecycleMismatchesReportsNewlyRegisteredAndDeregistered(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	addrOldKey := testPoolKeyHash(t, 0x01)
	addrBothKey := testPoolKeyHash(t, 0x02)
	addrNewKey := testPoolKeyHash(t, 0x03)
	poolKey := testPoolKeyHash(t, 0xAA)

	// Previous stake epoch (498): addrOld, addrBoth.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: addrOldKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: addrBothKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	// Current stake epoch (499): addrBoth, addrNew.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: addrBothKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: addrNewKey, PoolKeyHash: poolKey,
		RewardType: "member", Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	ctx := context.Background()
	currentOutputs, err := dingo.GetRewardAccountOutputs(ctx, 499)
	require.NoError(t, err)
	require.Len(t, currentOutputs, 2)

	now := time.Now()
	mismatches := accountLifecycleMismatches(
		ctx, cache, dingo, "preview", 500, 499, currentOutputs, now,
	)

	wantNewAddr, err := StakeAddressFromCredential(addrNewKey, 0)
	require.NoError(t, err)
	wantOldAddr, err := StakeAddressFromCredential(addrOldKey, 0)
	require.NoError(t, err)

	var sawNew, sawDeregistered bool
	for _, m := range mismatches {
		switch m.Category {
		case CategoryAcctNewlyRegistered:
			require.Equal(t, "1", m.KoiosValue)
			require.Contains(t, m.DingoValue, wantNewAddr)
			sawNew = true
		case CategoryAcctDeregistered:
			require.Equal(t, "1", m.KoiosValue)
			require.Contains(t, m.DingoValue, wantOldAddr)
			sawDeregistered = true
		default:
			t.Fatalf("unexpected category %q", m.Category)
		}
	}
	require.True(
		t,
		sawNew,
		"the new address must be reported as newly registered",
	)
	require.True(
		t,
		sawDeregistered,
		"the old address must be reported as deregistered",
	)
}

// TestAccountLifecycleMismatchesZeroRewardRowCountIsBounded proves the fix
// for a real scale problem: reporting one CheckMismatch row per zero-reward
// address would make cache growth, insert time, and JSON report size scale
// with the size of the account universe (Koios never emits a row at all for
// a zero-reward account, so on a large network most checked addresses can
// fall into this category). Regardless of how many zero-reward addresses
// exist, exactly one aggregate row must be produced, with an accurate total
// count and a sample capped at maxAccountLifecycleSample.
func TestAccountLifecycleMismatchesZeroRewardRowCountIsBounded(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now()
	const numZeroReward = maxAccountLifecycleSample * 5
	addrs := make([]string, numZeroReward)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("addr%04d", i)
	}
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview", 500, "chunkA", nil, addrs, now,
	))

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, now,
	)
	require.Len(
		t,
		mismatches,
		1,
		"any number of zero-reward addresses must still produce exactly one aggregate row",
	)
	require.Equal(t, strconv.Itoa(numZeroReward), mismatches[0].KoiosValue)
	sampleAddrs := strings.Split(
		strings.TrimPrefix(mismatches[0].DingoValue, "sample: "),
		",",
	)
	require.Len(
		t,
		sampleAddrs,
		maxAccountLifecycleSample,
		"the embedded sample must never grow with the total count",
	)
}

// TestAccountLifecycleMismatchesStakeEpochZeroSkipsLifecycleReport proves
// stakeEpoch==0 (no possible previous stake epoch, stakeEpoch-1 would
// underflow) skips the newly-registered/deregistered diff entirely, without
// ever dereferencing the dingo source.
func TestAccountLifecycleMismatchesStakeEpochZeroSkipsLifecycleReport(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, time.Now(),
	)
	require.Empty(t, mismatches)
}

// TestAccountLifecycleMismatchesPropagatesDingoErrorAsDBError proves a
// genuine Dingo DB failure while fetching the previous stake epoch's
// reward_account_output rows is reported as CategoryDBError, never silently
// swallowed as if there were simply no lifecycle changes to report.
func TestAccountLifecycleMismatchesPropagatesDingoErrorAsDBError(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	// Simulate a genuine Dingo DB failure (not "no rows") by closing the
	// underlying connection before it's queried — mirrors
	// TestCheckAccountsCoverageDBErrorIsNotConflatedWithIncompleteCoverage's
	// established technique for this exact distinction.
	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	mismatches := accountLifecycleMismatches(
		context.Background(),
		cache,
		dingo,
		"preview",
		500,
		499,
		nil,
		time.Now(),
	)
	require.Len(t, mismatches, 1)
	require.Equal(t, CategoryDBError, mismatches[0].Category)
}

// TestAccountLifecycleMismatchesReportsMalformedPreviousRowAsDBError proves
// a previous-stake-epoch reward_account_output row with an unsupported
// credential tag is reported as CategoryDBError rather than silently
// dropped — silently dropping it would make that row's address look
// deregistered (present last epoch, "gone" this epoch) purely because it
// failed to decode, not because it actually changed.
func TestAccountLifecycleMismatchesReportsMalformedPreviousRowAsDBError(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	goodKey := testPoolKeyHash(t, 0x10)
	badKey := testPoolKeyHash(t, 0x11)
	poolKey := testPoolKeyHash(t, 0xAA)

	// Previous stake epoch (498): one well-formed row, one with an
	// unsupported credential tag (only 0 and 1 are valid — see
	// StakeAddressFromCredential).
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: goodKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: badKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 9,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	ctx := context.Background()
	mismatches := accountLifecycleMismatches(
		ctx, cache, dingo, "preview", 500, 499, nil, time.Now(),
	)

	var sawDecodeError bool
	for _, m := range mismatches {
		if m.Category == CategoryDBError &&
			m.Field == "reward_account_output_address_decode" {
			sawDecodeError = true
			require.Contains(t, m.DingoValue, "1")
		}
	}
	require.True(
		t,
		sawDecodeError,
		"the malformed previous-epoch row must be reported, not silently dropped",
	)
}

// TestAccountLifecycleMismatchesSkipsLifecycleDiffWhenCurrentRowsFailToDecode
// proves the diff itself is skipped — not just reported alongside — when the
// *current* stake epoch has a malformed reward_account_output row. Before
// this fix, dingoRewardAddressSet's decodeErrs return value was discarded
// for currentOutputs, so a decode failure there silently produced an
// incomplete currSet: a well-formed address present in both epochs would
// then look deregistered purely because a different, unrelated row failed
// to decode, not because it actually changed.
func TestAccountLifecycleMismatchesSkipsLifecycleDiffWhenCurrentRowsFailToDecode(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	goodKey := testPoolKeyHash(t, 0x20)
	badKey := testPoolKeyHash(t, 0x21)
	poolKey := testPoolKeyHash(t, 0xAA)

	// Previous stake epoch (498): the well-formed address, plus badKey —
	// well-formed here (tag 0) so it decodes fine into prevSet. Without this,
	// badKey (only ever present in currentOutputs) would be absent from both
	// sets regardless of whether currDecodeErrs is honored, and the test
	// would pass even with the pre-fix code that silently discarded it — see
	// this test's own history for why that made it a non-regression-test.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: goodKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 498, StakingKey: badKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	// Current stake epoch (499): the same well-formed address (still
	// registered) plus badKey's row now with an unsupported credential tag —
	// so pre-fix, badKey would be dropped from currSet, found in prevSet, and
	// falsely reported as CategoryAcctDeregistered.
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: goodKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 0,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch: 499, StakingKey: badKey, PoolKeyHash: poolKey,
		RewardType: "member", CredentialTag: 9,
		Amount: types.Uint64(1000), Spendable: true,
	}).Error)

	ctx := context.Background()
	currentOutputs, err := dingo.GetRewardAccountOutputs(ctx, 499)
	require.NoError(t, err)
	require.Len(t, currentOutputs, 2)

	now := time.Now()
	mismatches := accountLifecycleMismatches(
		ctx, cache, dingo, "preview", 500, 499, currentOutputs, now,
	)

	for _, m := range mismatches {
		require.NotEqual(
			t,
			CategoryAcctNewlyRegistered,
			m.Category,
			"the lifecycle diff must be skipped entirely, not just under-reported",
		)
		require.NotEqual(
			t,
			CategoryAcctDeregistered,
			m.Category,
			"the still-registered address must never be misreported as deregistered "+
				"just because an unrelated current-epoch row failed to decode",
		)
	}
}

// TestAccountLifecycleMismatchesSkipsLifecycleDiffForPrunableSource proves
// the newly-registered/deregistered diff is skipped entirely for a
// *DatabaseSource — the in-process observer's reward source reads through
// core-mode's rolling pruning window and cannot distinguish "the previous
// stake epoch genuinely had no reward accounts" from "its rows have since
// been pruned" (both surface as an empty, error-free result). Treating that
// ambiguous empty result as a complete previous-epoch universe would make
// every current account look newly registered — so the diff must not even
// attempt it for this source type. Zero-reward reporting must still work,
// since it doesn't depend on any historical epoch's data.
func TestAccountLifecycleMismatchesSkipsLifecycleDiffForPrunableSource(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// addr is confirmed checked with zero reward — zero-reward reporting
	// should still fire even though the lifecycle diff below is skipped.
	now := time.Now()
	require.NoError(t, cache.SaveAccountFetchChunkProgress(
		"preview", 500, "chunkA", nil, []string{"addr1"}, now,
	))

	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, source, "preview", 500, 499, nil, now,
	)

	var sawZeroReward bool
	for _, m := range mismatches {
		require.NotEqual(
			t,
			CategoryAcctNewlyRegistered,
			m.Category,
			"the lifecycle diff must never run at all for a prunable source",
		)
		require.NotEqual(t, CategoryAcctDeregistered, m.Category)
		if m.Category == CategoryAcctZeroReward {
			sawZeroReward = true
		}
	}
	require.True(
		t,
		sawZeroReward,
		"zero-reward reporting must still work for a prunable source",
	)
}

// TestAccountLifecycleMismatchesPropagatesCacheErrorAsDBError proves a
// genuine cache failure while looking up zero-reward accounts is reported as
// CategoryDBError, never silently swallowed.
func TestAccountLifecycleMismatchesPropagatesCacheErrorAsDBError(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// Force a genuine query error (not "no rows") for
	// GetZeroRewardAccountsForEpoch by dropping the table its SELECT reads
	// from — mirrors the "break the schema, not just leave it empty"
	// technique check_test.go's own DB-error tests already use.
	_, err = cache.db.Exec("DROP TABLE koios_account_checked")
	require.NoError(t, err)

	mismatches := accountLifecycleMismatches(
		context.Background(), cache, nil, "preview", 500, 0, nil, time.Now(),
	)
	require.Len(t, mismatches, 1)
	require.Equal(t, CategoryDBError, mismatches[0].Category)
}

// TestDetermineStatusAccountLifecycleCategoriesAreInformational proves the
// three dingo #3099 categories never affect Status: alone they must PASS,
// and alongside a genuine FAIL-triggering mismatch they must not mask or
// alter that FAIL.
func TestDetermineStatusAccountLifecycleCategoriesAreInformational(
	t *testing.T,
) {
	now := time.Now()
	onlyInformational := []CheckMismatch{
		{Category: CategoryAcctZeroReward, CheckedAt: now},
		{Category: CategoryAcctNewlyRegistered, CheckedAt: now},
		{Category: CategoryAcctDeregistered, CheckedAt: now},
	}
	require.Equal(t, StatusPass, DetermineStatus(onlyInformational))

	withRealFailure := append(
		append([]CheckMismatch{}, onlyInformational...),
		CheckMismatch{Category: CategoryValueMismatch, CheckedAt: now},
	)
	require.Equal(t, StatusFail, DetermineStatus(withRealFailure))
}
