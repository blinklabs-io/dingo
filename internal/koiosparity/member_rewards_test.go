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
	"encoding/hex"
	"errors"
	"log/slog"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const memberRewardsPool = "pool1vvkurfxhajtj4f7x8wjkeet7rg8amz34duy5nux76per5sn3npx"

func memberRewardRows(pool string, amounts ...string) []KoiosAccountRewards {
	rows := make([]KoiosAccountRewards, 0, len(amounts))
	for i, amount := range amounts {
		rows = append(rows, KoiosAccountRewards{
			StakeAddress: "stake_test1member" + string(rune('a'+i)),
			RewardType:   "member",
			Earned:       amount,
			PoolIDBech32: pool,
		})
	}
	return rows
}

func memberRewardsDingo(spendable string) map[string]*DingoPoolEpochData {
	keyHex, err := PoolKeyHashHex(memberRewardsPool)
	if err != nil {
		panic(err)
	}
	return map[string]*DingoPoolEpochData{
		keyHex: {
			StakePresent:                 true,
			MemberRewardPresent:          true,
			MemberRewardTotal:            "999999999",
			SpendableMemberRewardPresent: true,
			SpendableMemberRewardTotal:   spendable,
		},
	}
}

func memberRewardsOf(ms []CheckMismatch, field string) []CheckMismatch {
	var out []CheckMismatch
	for _, m := range ms {
		if m.Field == field {
			out = append(out, m)
		}
	}
	return out
}

// TestComparePoolEpochDoesNotDecideMemberRewards pins that the aggregate pool
// comparison no longer reads pool_history.member_rewards: that value is a
// cached per-pool total that can include forfeited rewards, so it must not be
// able to fail Dingo from the fast phase.
func TestComparePoolEpochDoesNotDecideMemberRewards(t *testing.T) {
	t.Parallel()

	koios := &KoiosPoolEpoch{
		PoolBech32:    memberRewardsPool,
		MemberRewards: "140571288",
	}
	for name, dingo := range map[string]*DingoPoolEpochData{
		"differs": {
			MemberRewardPresent:          true,
			SpendableMemberRewardPresent: true,
			SpendableMemberRewardTotal:   "140569946",
		},
		"row absent": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ms := ComparePoolEpoch(
				"preprod", 302, koios, dingo, time.Now(), 0, time.Time{},
				false, false,
			)
			assert.Empty(t, memberRewardsOf(ms, "member_rewards"),
				"the aggregate phase must not produce a member_rewards verdict")
		})
	}
}

// TestCompareMemberRewardsForfeitedInPoolHistory is the shape measured on
// preprod epoch 302: Koios pool_history includes a reward the ledger forfeited,
// the per-account rows do not, and Dingo's spendable sum equals the per-account
// sum. Dingo is right, so the epoch passes and the Koios-side inconsistency is
// recorded as information only.
func TestCompareMemberRewardsForfeitedInPoolHistory(t *testing.T) {
	t.Parallel()

	ms := CompareMemberRewards(
		"preprod", 302,
		[]KoiosPoolEpoch{{PoolBech32: memberRewardsPool, MemberRewards: "1342"}},
		memberRewardRows(memberRewardsPool, "100", "200"),
		memberRewardsDingo("300"),
		time.Now(), 0, time.Time{},
	)
	require.Len(t, ms, 1)
	assert.Equal(t, CategoryKoiosPoolHistoryInconsistent, ms[0].Category)
	assert.Equal(t, memberRewardsPool, ms[0].PoolBech32)
	assert.Contains(t, ms[0].KoiosValue, "1342")
	assert.Contains(t, ms[0].KoiosValue, "300")
	assert.Equal(t, StatusPass, DetermineStatus(ms))
	assert.Zero(t, CountSignificant(ms))
	assert.Empty(t, memberRewardsOf(ms, "member_rewards"))
}

func TestCompareMemberRewardsAgreementRecordsNothing(t *testing.T) {
	t.Parallel()

	ms := CompareMemberRewards(
		"preprod", 302,
		[]KoiosPoolEpoch{{PoolBech32: memberRewardsPool, MemberRewards: "300"}},
		memberRewardRows(memberRewardsPool, "100", "200"),
		memberRewardsDingo("300"),
		time.Now(), 0, time.Time{},
	)
	assert.Empty(t, ms)
}

// TestCompareMemberRewardsDingoForfeitsWrongly: Dingo marks a reward
// unspendable that Koios's per-account rows pay. Koios pool_history happens to
// equal Dingo's smaller sum, which must not excuse the difference.
func TestCompareMemberRewardsDingoForfeitsWrongly(t *testing.T) {
	t.Parallel()

	ms := CompareMemberRewards(
		"preprod", 302,
		[]KoiosPoolEpoch{{PoolBech32: memberRewardsPool, MemberRewards: "100"}},
		memberRewardRows(memberRewardsPool, "100", "200"),
		memberRewardsDingo("100"),
		time.Now(), 0, time.Time{},
	)
	got := memberRewardsOf(ms, "member_rewards")
	require.Len(t, got, 1)
	assert.Equal(t, CategoryValueMismatch, got[0].Category)
	assert.Equal(t, "100", got[0].DingoValue)
	assert.Equal(t, "300", got[0].KoiosValue)
	assert.Equal(t, StatusFail, DetermineStatus(ms))
}

// TestCompareMemberRewardsDingoPaysWrongly: Dingo's spendable sum includes a
// reward Koios's per-account rows do not have. pool_history equals Dingo's
// larger sum, which must not excuse it either.
func TestCompareMemberRewardsDingoPaysWrongly(t *testing.T) {
	t.Parallel()

	ms := CompareMemberRewards(
		"preprod", 302,
		[]KoiosPoolEpoch{{PoolBech32: memberRewardsPool, MemberRewards: "300"}},
		memberRewardRows(memberRewardsPool, "100"),
		memberRewardsDingo("300"),
		time.Now(), 0, time.Time{},
	)
	got := memberRewardsOf(ms, "member_rewards")
	require.Len(t, got, 1)
	assert.Equal(t, CategoryValueMismatch, got[0].Category)
	assert.Equal(t, "300", got[0].DingoValue)
	assert.Equal(t, "100", got[0].KoiosValue)
	assert.Equal(t, StatusFail, DetermineStatus(ms))
}

// TestCompareMemberRewardsDingoPaysPoolKoiosHasNoRowsFor covers a pool that
// Koios has no member rows for at all while Dingo spends member rewards to it.
func TestCompareMemberRewardsDingoPaysPoolKoiosHasNoRowsFor(t *testing.T) {
	t.Parallel()

	ms := CompareMemberRewards(
		"preprod", 302, nil, nil, memberRewardsDingo("5"),
		time.Now(), 0, time.Time{},
	)
	got := memberRewardsOf(ms, "member_rewards")
	require.Len(t, got, 1)
	assert.Equal(t, CategoryValueMismatch, got[0].Category)
	assert.Equal(t, "0", got[0].KoiosValue)
}

func TestCompareMemberRewardsTiming(t *testing.T) {
	t.Parallel()

	rows := memberRewardRows(memberRewardsPool, "100", "200")
	t.Run("pending difference is reference_lag", func(t *testing.T) {
		t.Parallel()
		dingo := memberRewardsDingo("350")
		for _, d := range dingo {
			d.RewardsPending = true
		}
		ms := CompareMemberRewards(
			"preprod", 302, nil, rows, dingo, time.Now(), 0, time.Time{},
		)
		require.Len(t, ms, 1)
		assert.Equal(t, CategoryReferenceLag, ms[0].Category)
	})
	t.Run("row not written yet is reference_lag while pending",
		func(t *testing.T) {
			t.Parallel()
			ms := CompareMemberRewards(
				"preprod", 302, nil, rows,
				map[string]*DingoPoolEpochData{
					mustKeyHex(t): {RewardsPending: true},
				},
				time.Now(), 0, time.Time{},
			)
			require.Len(t, ms, 1)
			assert.Equal(t, CategoryReferenceLag, ms[0].Category)
		})
	t.Run("row missing past the window is dingo_db_missing",
		func(t *testing.T) {
			t.Parallel()
			ms := CompareMemberRewards(
				"preprod", 302, nil, rows, map[string]*DingoPoolEpochData{},
				time.Now(), 0, time.Time{},
			)
			require.Len(t, ms, 1)
			assert.Equal(t, CategoryDBMissing, ms[0].Category)
		})
	t.Run("row missing inside the grace window is reference_lag",
		func(t *testing.T) {
			t.Parallel()
			now := time.Now()
			ms := CompareMemberRewards(
				"preprod", 302, nil, rows, map[string]*DingoPoolEpochData{},
				now, 24, now.Add(-time.Hour),
			)
			require.Len(t, ms, 1)
			assert.Equal(t, CategoryReferenceLag, ms[0].Category)
		})
}

func mustKeyHex(t *testing.T) string {
	t.Helper()
	keyHex, err := PoolKeyHashHex(memberRewardsPool)
	require.NoError(t, err)
	return keyHex
}

type memberRewardsScenario struct {
	dingoRows    []models.RewardAccountOutput
	koiosRows    []KoiosAccountRewards
	poolHistory  string
	coverageDone bool
	// source, when set, wraps the Dingo source the check reads, with the
	// scenario's cache, so a test can act between the check's reads.
	source func(*Cache, RewardParitySource) RewardParitySource
}

// runMemberRewardsScenario drives the real Check entry point with accounts
// enabled over a Dingo database and Koios cache built from sc, and returns the
// stored mismatches.
func runMemberRewardsScenario(
	t *testing.T,
	sc memberRewardsScenario,
) []CheckMismatch {
	t.Helper()

	const network = "preview"
	const koiosEpoch = uint64(10)
	const stakeEpoch = uint64(9)
	poolHash := testPoolKeyHash(t, 0x51)
	poolBech32, err := PoolKeyHashHexToBech32(hex.EncodeToString(poolHash))
	require.NoError(t, err)

	dingoDir, gdb := newTestDingoDB(t)
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
		MemberRewardTotal: types.Uint64(999_999),
	}).Error)
	for i := range sc.dingoRows {
		row := sc.dingoRows[i]
		row.Epoch = stakeEpoch
		row.PoolKeyHash = poolHash
		row.RewardType = "member"
		require.NoError(t, gdb.Create(&row).Error)
	}
	sqlDB, err := gdb.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	cachePath := filepath.Join(t.TempDir(), "cache.db")
	cache, err := openTestCache(cachePath, nil)
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
			MemberRewards: sc.poolHistory,
			FetchedAt:     fetchedAt,
		}},
		&KoiosTotals{
			Network: network, Epoch: koiosEpoch, FetchedAt: fetchedAt,
		},
	))
	koiosRows := make([]KoiosAccountRewards, len(sc.koiosRows))
	for i, row := range sc.koiosRows {
		row.PoolIDBech32 = poolBech32
		row.RewardType = "member"
		row.FetchedAt = fetchedAt
		koiosRows[i] = row
	}
	require.NoError(t, cache.CommitAccountRewardsForEpoch(
		network, koiosEpoch, koiosRows, len(koiosRows), sc.coverageDone,
		fetchedAt,
	))

	if sc.source == nil {
		_, err = Check(context.Background(), CheckConfig{
			Network:         network,
			DingoDB:         DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir},
			CachePath:       cachePath,
			AccountsEnabled: true,
		}, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
	} else {
		dingo, err := OpenDingoDB(DingoDBConfig{Plugin: "sqlite", DataDir: dingoDir})
		require.NoError(t, err)
		defer dingo.Close() //nolint:errcheck
		_, err = CheckEpoch(
			context.Background(), cache, sc.source(cache, dingo), network,
			koiosEpoch, 0, true, slog.New(slog.DiscardHandler),
		)
		require.NoError(t, err)
	}

	mismatches, err := cache.GetMismatches(network, koiosEpoch, "")
	require.NoError(t, err)
	return mismatches
}

func memberAccount(
	t *testing.T,
	b byte,
	amount uint64,
	spendable bool,
) (models.RewardAccountOutput, KoiosAccountRewards) {
	t.Helper()
	key := testPoolKeyHash(t, b)
	addr, err := StakeAddressFromCredential(key, 0)
	require.NoError(t, err)
	return models.RewardAccountOutput{
			StakingKey: key,
			Amount:     types.Uint64(amount),
			Spendable:  spendable,
		}, KoiosAccountRewards{
			StakeAddress: addr,
			Earned:       big.NewInt(int64(amount)).String(),
		}
}

func accountScope(ms []CheckMismatch) []CheckMismatch {
	var out []CheckMismatch
	for _, m := range ms {
		if m.Scope == ScopeAccount {
			out = append(out, m)
		}
	}
	return out
}

// TestCheckMemberRewardsEpoch302Shape runs the forfeited-reward shape through
// Check: the account phase passes with one informational row, and the
// aggregate phase carries no member_rewards row.
func TestCheckMemberRewardsEpoch302Shape(t *testing.T) {
	t.Parallel()

	d1, k1 := memberAccount(t, 0x61, 100, true)
	d2, k2 := memberAccount(t, 0x62, 200, true)
	ms := runMemberRewardsScenario(t, memberRewardsScenario{
		dingoRows:    []models.RewardAccountOutput{d1, d2},
		koiosRows:    []KoiosAccountRewards{k1, k2},
		poolHistory:  "1642",
		coverageDone: true,
	})
	acct := accountScope(ms)
	assert.Equal(t, StatusPass, DetermineStatus(acct))
	assert.Zero(t, CountSignificant(acct))
	inconsistent := 0
	for _, m := range ms {
		assert.NotEqual(t, "member_rewards", m.Field,
			"no member_rewards verdict row expected: %+v", m)
		if m.Category == CategoryKoiosPoolHistoryInconsistent {
			inconsistent++
			assert.Equal(t, ScopeAccount, m.Scope)
		}
	}
	assert.Equal(t, 1, inconsistent)
}

func TestCheckMemberRewardsDingoForfeitsWrongly(t *testing.T) {
	t.Parallel()

	d1, k1 := memberAccount(t, 0x61, 100, true)
	d2, k2 := memberAccount(t, 0x62, 200, false)
	ms := runMemberRewardsScenario(t, memberRewardsScenario{
		dingoRows:    []models.RewardAccountOutput{d1, d2},
		koiosRows:    []KoiosAccountRewards{k1, k2},
		poolHistory:  "100",
		coverageDone: true,
	})
	got := memberRewardsOf(accountScope(ms), "member_rewards")
	require.Len(t, got, 1)
	assert.Equal(t, CategoryValueMismatch, got[0].Category)
	assert.Equal(t, "100", got[0].DingoValue)
	assert.Equal(t, "300", got[0].KoiosValue)
}

func TestCheckMemberRewardsDingoPaysWrongly(t *testing.T) {
	t.Parallel()

	d1, k1 := memberAccount(t, 0x61, 100, true)
	d2, _ := memberAccount(t, 0x62, 200, true)
	ms := runMemberRewardsScenario(t, memberRewardsScenario{
		dingoRows:    []models.RewardAccountOutput{d1, d2},
		koiosRows:    []KoiosAccountRewards{k1},
		poolHistory:  "300",
		coverageDone: true,
	})
	got := memberRewardsOf(accountScope(ms), "member_rewards")
	require.Len(t, got, 1)
	assert.Equal(t, CategoryValueMismatch, got[0].Category)
	assert.Equal(t, "300", got[0].DingoValue)
	assert.Equal(t, "100", got[0].KoiosValue)
}

// TestCheckMemberRewardsWaitsForCompleteAccountCoverage pins that no
// member_rewards verdict, pass or fail, is formed from incomplete account data.
func TestCheckMemberRewardsWaitsForCompleteAccountCoverage(t *testing.T) {
	t.Parallel()

	d1, k1 := memberAccount(t, 0x61, 100, true)
	d2, _ := memberAccount(t, 0x62, 200, true)
	ms := runMemberRewardsScenario(t, memberRewardsScenario{
		dingoRows:    []models.RewardAccountOutput{d1, d2},
		koiosRows:    []KoiosAccountRewards{k1},
		poolHistory:  "999",
		coverageDone: false,
	})
	for _, m := range ms {
		assert.NotEqual(t, "member_rewards", m.Field, "%+v", m)
		assert.NotEqual(t, CategoryKoiosPoolHistoryInconsistent, m.Category)
	}
	incomplete := false
	for _, m := range ms {
		if m.Category == CategoryAcctCoverageIncomplete {
			incomplete = true
		}
	}
	assert.True(t, incomplete)
}

// memberRewardsFor runs CompareMemberRewards for one pool whose Koios
// per-account member rows sum to koios.MemberRewards. It lets the timing and
// quantity cases that used to run through the pool comparison state their
// reference as a single number.
func memberRewardsFor(
	network string,
	epoch uint64,
	koios *KoiosPoolEpoch,
	dingo *DingoPoolEpochData,
	now time.Time,
	graceHours int,
	epochEndTime time.Time,
) []CheckMismatch {
	keyHex, err := PoolKeyHashHex(memberRewardsPool)
	if err != nil {
		panic(err)
	}
	return CompareMemberRewards(
		network,
		epoch,
		nil,
		memberRewardRows(memberRewardsPool, koios.MemberRewards),
		map[string]*DingoPoolEpochData{keyHex: dingo},
		now,
		graceHours,
		epochEndTime,
	)
}

// TestCompareEpochAccountsReferenceReadErrorsWithholdRows pins that every
// failed read of the Koios account reference is an ERROR and returns no rows,
// so the caller never forms a member_rewards verdict from a reference that
// was not read.
func TestCompareEpochAccountsReferenceReadErrorsWithholdRows(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, cache *Cache) ([]CheckMismatch, bool) {
		t.Helper()
		ms, rows, loaded := compareEpochAccounts(
			context.Background(), cache, nil, "preprod", 302, 301,
			time.Now(), 0, time.Time{}, false, slog.New(slog.DiscardHandler),
		)
		assert.Nil(t, rows)
		return ms, loaded
	}

	t.Run("closed cache", func(t *testing.T) {
		t.Parallel()
		cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
		require.NoError(t, err)
		require.NoError(t, cache.Close())
		ms, loaded := run(t, cache)
		assert.False(t, loaded)
		require.Len(t, ms, 1)
		assert.Equal(t, "koios_account_coverage", ms[0].Field)
		assert.Equal(t, StatusError, DetermineStatus(ms))
	})

	t.Run("rows unreadable", func(t *testing.T) {
		t.Parallel()
		cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
		require.NoError(t, err)
		defer cache.Close() //nolint:errcheck
		require.NoError(t, cache.CommitAccountRewardsForEpoch(
			"preprod", 302, nil, 1, true, time.Now(),
		))
		_, err = cache.db.Exec("DROP TABLE koios_account_rewards")
		require.NoError(t, err)
		ms, loaded := run(t, cache)
		assert.False(t, loaded)
		require.Len(t, ms, 1)
		assert.Equal(t, "koios_account_rewards", ms[0].Field)
		assert.Equal(t, CategoryDBError, ms[0].Category)
	})
}

// downgradingSource reports memberTotal as every pool's spendable member
// total and, when downgrade is set, marks the epoch's account coverage
// incomplete once the per-account comparison has read the Koios reference,
// standing in for an account fetch that refreshes the cache while a check
// runs.
type downgradingSource struct {
	RewardParitySource
	cache       *Cache
	memberTotal string
	downgrade   bool
}

func (s downgradingSource) GetPoolEpochDataMap(
	ctx context.Context,
	stakeEpoch, paramEpoch uint64,
) (map[string]*DingoPoolEpochData, error) {
	m, err := s.RewardParitySource.GetPoolEpochDataMap(ctx, stakeEpoch, paramEpoch)
	for _, p := range m {
		if p.SpendableMemberRewardPresent {
			p.SpendableMemberRewardTotal = s.memberTotal
		}
	}
	return m, err
}

func (s downgradingSource) GetProposalDepositRefunds(
	ctx context.Context,
	epoch uint64,
) (ProposalDepositRefunds, error) {
	if s.downgrade {
		if err := s.cache.MarkAccountCoverageIncomplete("preview", 10); err != nil {
			return ProposalDepositRefunds{}, err
		}
	}
	return s.RewardParitySource.GetProposalDepositRefunds(ctx, epoch)
}

// TestCheckMemberRewardsUsesTheAccountPhaseReference pins that member_rewards
// is compared against the reference the per-account comparison read, so a
// cache refresh between the two cannot leave member_rewards unexamined while
// the account scope reads as PASS. The per-account rows agree on both sides;
// only the pool's member total differs.
func TestCheckMemberRewardsUsesTheAccountPhaseReference(t *testing.T) {
	t.Parallel()

	d1, k1 := memberAccount(t, 0x61, 100, true)
	d2, k2 := memberAccount(t, 0x62, 200, true)
	for _, tc := range []struct {
		name      string
		downgrade bool
	}{
		{name: "steady cache"},
		{name: "coverage downgraded mid-check", downgrade: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ms := runMemberRewardsScenario(t, memberRewardsScenario{
				dingoRows:    []models.RewardAccountOutput{d1, d2},
				koiosRows:    []KoiosAccountRewards{k1, k2},
				poolHistory:  "300",
				coverageDone: true,
				source: func(c *Cache, src RewardParitySource) RewardParitySource {
					return downgradingSource{
						RewardParitySource: src,
						cache:              c,
						memberTotal:        "250",
						downgrade:          tc.downgrade,
					}
				},
			})
			acct := accountScope(ms)
			assert.NotEqual(t, StatusPass, DetermineStatus(acct))
			got := memberRewardsOf(acct, "member_rewards")
			require.Len(t, got, 1, "%+v", acct)
			assert.Equal(t, CategoryValueMismatch, got[0].Category)
			assert.Equal(t, "250", got[0].DingoValue)
			assert.Equal(t, "300", got[0].KoiosValue)
		})
	}
}

type failingPoolMapSource struct {
	RewardParitySource
}

func (failingPoolMapSource) GetPoolEpochDataMap(
	context.Context,
	uint64,
	uint64,
) (map[string]*DingoPoolEpochData, error) {
	return nil, errors.New("pool map read failed")
}

// TestCheckMemberRewardsPoolReadFailureIsAccountError pins that a failed read
// of Dingo's pool data, which leaves member_rewards unexamined, keeps the
// account scope's own stored verdict from reading as PASS.
func TestCheckMemberRewardsPoolReadFailureIsAccountError(t *testing.T) {
	t.Parallel()

	d1, k1 := memberAccount(t, 0x61, 100, true)
	ms := runMemberRewardsScenario(t, memberRewardsScenario{
		dingoRows:    []models.RewardAccountOutput{d1},
		koiosRows:    []KoiosAccountRewards{k1},
		poolHistory:  "100",
		coverageDone: true,
		source: func(_ *Cache, src RewardParitySource) RewardParitySource {
			return failingPoolMapSource{src}
		},
	})
	acct := accountScope(ms)
	assert.Equal(t, StatusError, DetermineStatus(acct))
	got := memberRewardsOf(acct, "member_rewards")
	require.Len(t, got, 1, "%+v", acct)
	assert.Equal(t, CategoryDBError, got[0].Category)
}
