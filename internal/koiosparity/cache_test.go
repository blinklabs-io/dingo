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
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetEpochsNeedingCheckDoesNotRequeueCheckedPreStakingEpoch(
	t *testing.T,
) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC()
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      "preview",
		Epoch:        0,
		PreStaking:   true,
		EpochEndTime: now.Add(-time.Hour),
		FetchedAt:    now.Add(-time.Minute),
	}, nil, nil))
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:       "preview",
		Epoch:         0,
		LastCheckedAt: now,
		Status:        StatusPass,
	}))

	epochs, err := cache.GetEpochsNeedingCheck("preview", true)
	require.NoError(t, err)
	require.Empty(
		t,
		epochs,
		"pre-staking epochs never have account coverage and must not be requeued for its absence",
	)
}

// TestCommitEpochDataWithTotals exercises the actual SQL generated for the
// koios_totals upsert against a real SQLite file — a pure-Go struct test
// cannot catch a column-name mismatch (e.g. DepositsDRep versus the persisted
// "deposits_drep" spelling; the column must be pinned explicitly in cache.go).
// CommitEpochData's
// AssignmentColumns list is a hardcoded string literal that must match the
// real migrated column for every field, so this has to run against a real
// DB, not just construct the struct in memory.
func TestCommitEpochDataWithTotals(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	info := KoiosEpochInfo{
		Network:      "preview",
		Epoch:        1367,
		ActiveStake:  "3250414888938614",
		Fees:         "1292170362",
		TotalRewards: "410988812047",
		EpochEndTime: now,
		FetchedAt:    now,
	}
	totals := &KoiosTotals{
		Treasury:           "6931231163186226",
		Reserves:           "7792082362166766",
		Fees:               "1245791321",
		Reward:             "292608261256804",
		Circulation:        "29979007817598883",
		Supply:             "37207917637833234",
		DepositsStake:      "536150000000",
		DepositsDRep:       "4474000000000",
		DepositsProposal:   "59000000000",
		TreasuryDonation:   "0",
		TreasuryWithdrawal: "0",
		ReservesWithdrawal: "0",
		FetchedAt:          now,
	}

	// First insert, then a second commit for the same (network, epoch) to
	// exercise the ON CONFLICT DO UPDATE path too, not just the initial INSERT.
	require.NoError(t, cache.CommitEpochData(info, nil, totals))
	require.NoError(t, cache.CommitEpochData(info, nil, totals))

	got, err := cache.GetTotals("preview", 1367)
	require.NoError(t, err)
	require.Equal(t, totals.Treasury, got.Treasury)
	require.Equal(t, totals.Reserves, got.Reserves)
	require.Equal(t, totals.Fees, got.Fees)
	require.Equal(t, totals.Reward, got.Reward)
	require.Equal(t, totals.DepositsDRep, got.DepositsDRep)
	require.Equal(t, totals.DepositsStake, got.DepositsStake)
	require.Equal(t, totals.DepositsProposal, got.DepositsProposal)
}

// TestCommitAccountRewardsForEpoch exercises the real SQL for the
// koios_account_rewards/koios_account_coverage atomic commit, including the
// widened (network, epoch, stake_address, reward_type) key that lets one
// account carry both a member and a leader row in the same epoch.
func TestCommitAccountRewardsForEpoch(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	rows := []KoiosAccountRewards{
		{
			StakeAddress:   "stake_test1uqevw2xnsc0pvn9t9r9c7qryan77xqk6etza9dprr8f80qq0e8ptn",
			RewardType:     "member",
			Earned:         "1000000",
			SpendableEpoch: 101,
			FetchedAt:      now,
		},
		{
			// Same address, different reward type — pool owner delegating to
			// their own pool. Must NOT collide with the row above.
			StakeAddress:   "stake_test1uqevw2xnsc0pvn9t9r9c7qryan77xqk6etza9dprr8f80qq0e8ptn",
			RewardType:     "leader",
			Earned:         "5000000",
			SpendableEpoch: 101,
			PoolIDBech32:   "pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq",
			FetchedAt:      now,
		},
	}

	require.NoError(
		t,
		cache.CommitAccountRewardsForEpoch("preview", 100, rows, 5, true, now),
	)

	got, err := cache.GetAccountRewardsForEpoch("preview", 100)
	require.NoError(t, err)
	require.Len(t, got, 2)
	types := map[string]string{
		got[0].RewardType: got[0].Earned,
		got[1].RewardType: got[1].Earned,
	}
	require.Equal(t, "1000000", types["member"])
	require.Equal(t, "5000000", types["leader"])
	require.Equal(
		t,
		"pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq",
		got[1].PoolIDBech32,
	)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.Equal(t, 5, cov.RequestedCount)
	require.Equal(t, 2, cov.FetchedCount)
	require.True(t, cov.Complete)

	// Re-commit with fewer rows and complete=false (simulating a subsequent
	// partial/failed fetch) must fully replace the prior set — no leftover
	// rows from the first commit, and coverage must reflect the new,
	// incomplete state rather than the stale "complete" one.
	require.NoError(
		t,
		cache.CommitAccountRewardsForEpoch(
			"preview",
			100,
			rows[:1],
			5,
			false,
			now,
		),
	)
	got, err = cache.GetAccountRewardsForEpoch("preview", 100)
	require.NoError(t, err)
	require.Len(t, got, 1)
	cov, err = cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.False(t, cov.Complete)
}

// TestPruneAccountCoverageBoundsCheckpointRows proves the per-account
// checkpoint tables are a rolling cache rather than a second copy of the
// entire account history. The authoritative reward rows must survive
// eviction so an old epoch still compares correctly after its resumability
// state ages out.
func TestPruneAccountCoverageBoundsCheckpointRows(t *testing.T) {
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	const firstEpoch = uint64(100)
	const epochs = accountCheckpointRetentionEpochs + 3
	for i := range uint64(epochs) {
		epoch := firstEpoch + i
		require.NoError(t, cache.SaveAccountFetchChunkProgress(
			"preview", epoch, fmt.Sprintf("chunk-%d", epoch),
			[]KoiosAccountRewards{{
				StakeAddress: "stake1reward", RewardType: "member", Earned: "42",
				FetchedAt: now,
			}},
			[]string{"stake1reward", "stake1zero", "stake1zero2"}, now,
		))
		require.NoError(t, cache.CommitAccountRewardsForEpoch(
			"preview", epoch,
			[]KoiosAccountRewards{{
				StakeAddress: "stake1reward", RewardType: "member", Earned: "42",
				FetchedAt: now,
			}}, 3, true, now,
		))
	}
	require.NoError(
		t,
		cache.PruneAccountCoverage("preview", firstEpoch+epochs-1),
	)

	var checked, staged int
	require.NoError(t, cache.db.QueryRow(
		"SELECT COUNT(*) FROM koios_account_checked WHERE network = ?",
		"preview",
	).Scan(&checked))
	require.NoError(t, cache.db.QueryRow(
		"SELECT COUNT(*) FROM koios_account_fetch_staged_rows WHERE network = ?",
		"preview",
	).Scan(&staged))
	require.LessOrEqual(t, checked, accountCheckpointRetentionEpochs*3)
	require.LessOrEqual(t, staged, accountCheckpointRetentionEpochs)

	oldEpoch := firstEpoch
	cov, err := cache.GetAccountCoverage("preview", oldEpoch)
	require.NoError(t, err)
	require.True(t, cov.Complete)
	oldRows, err := cache.GetAccountRewardsForEpoch("preview", oldEpoch)
	require.NoError(t, err)
	require.Len(t, oldRows, 1)
	require.Empty(t, CompareAccountEpoch(
		"preview",
		oldEpoch,
		oldRows,
		[]DingoAccountReward{
			{StakeAddress: "stake1reward", RewardType: "member", Amount: "42"},
		},
		now,
		0,
		time.Time{},
		false,
	))
}

// TestAccountCoveragePreservesBoundedZeroRewardSummary proves historical
// lifecycle reporting retains its exact count and capped sample after the
// per-address rows are evicted.
func TestAccountCoveragePreservesBoundedZeroRewardSummary(t *testing.T) {
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	for epoch := uint64(100); epoch < 100+accountCheckpointRetentionEpochs+2; epoch++ {
		require.NoError(t, cache.SaveAccountFetchChunkProgress(
			"preview", epoch, fmt.Sprintf("chunk-%d", epoch), nil,
			[]string{"stake1zero", "stake1zero2"}, now,
		))
		require.NoError(t, cache.CommitAccountRewardsForEpoch(
			"preview", epoch, nil, 2, true, now,
		))
	}

	summary, err := cache.GetZeroRewardSummary("preview", 100)
	require.NoError(t, err)
	require.Equal(t, 2, summary.Count)
	require.Equal(t, []string{"stake1zero", "stake1zero2"}, summary.Sample)
}

// legacySeedPragmas relaxes durability for the throwaway files these tests
// hand-build in a legacy shape before OpenCache migrates them; the same
// settings as the migrations package's testDBPragmas.
const legacySeedPragmas = "_pragma=journal_mode(MEMORY)&" +
	"_pragma=synchronous(OFF)"

// TestAccountCoverageSummaryMigrationBackfillsLegacyRows proves an existing
// cache gets its bounded lifecycle summary before checkpoint eviction can
// remove the legacy per-address evidence.
func TestAccountCoverageSummaryMigrationBackfillsLegacyRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.db")
	db, err := sql.Open("sqlite", path+"?"+legacySeedPragmas)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE koios_account_coverage (
		id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL,
		epoch INTEGER NOT NULL, requested_count INTEGER NOT NULL DEFAULT 0,
		fetched_count INTEGER NOT NULL DEFAULT 0, complete INTEGER NOT NULL DEFAULT 0,
		fetched_at DATETIME NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE koios_account_checked (
		id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL,
		epoch INTEGER NOT NULL, stake_address TEXT NOT NULL,
		chunk_hash TEXT NOT NULL, reward_row_count INTEGER NOT NULL DEFAULT 0,
		checked_at DATETIME NOT NULL)`)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	_, err = db.Exec(`INSERT INTO koios_account_coverage
		(network, epoch, requested_count, fetched_count, complete, fetched_at)
		VALUES (?, ?, ?, ?, 1, ?)`, "preview", 100, 2, 0, now)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO koios_account_checked
		(network, epoch, stake_address, chunk_hash, reward_row_count, checked_at)
		VALUES (?, ?, ?, ?, 0, ?), (?, ?, ?, ?, 0, ?)`,
		"preview", 100, "stake1zero", "chunk", now,
		"preview", 100, "stake1zero2", "chunk", now)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	summary, err := cache.GetZeroRewardSummary("preview", 100)
	require.NoError(t, err)
	require.Equal(t, 2, summary.Count)
	require.Equal(t, []string{"stake1zero", "stake1zero2"}, summary.Sample)
}

// TestCommitAccountRewardsForEpochAllowsLiteralDuplicateKey proves
// idx_kar_net_epoch_addr_type's widening from unique to non-unique actually
// lets CommitAccountRewardsForEpoch insert two rows sharing the exact same
// (network, epoch, stake_address, reward_type) key without erroring — the
// real-world case is multiple pool contributions sharing the same key, plus a
// literal duplicate that CompareAccountEpoch must report (see
// CategoryAcctDuplicate's doc comment), not just two rows with different
// reward_type values for the same address (already covered by
// TestCommitAccountRewardsForEpoch). Before the index was
// widened to non-unique, this insert would have failed with a UNIQUE
// constraint violation before CompareAccountEpoch ever got a chance to flag
// the duplicate as acct_duplicate.
func TestCommitAccountRewardsForEpochAllowsLiteralDuplicateKey(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	rows := []KoiosAccountRewards{
		{
			StakeAddress: "stake_test1uqevw2xnsc0pvn9t9r9c7qryan77xqk6etza9dprr8f80qq0e8ptn",
			RewardType:   "member",
			Earned:       "1000000",
			FetchedAt:    now,
		},
		{
			// Literal duplicate: identical (network, epoch, stake_address,
			// reward_type) key as the row above.
			StakeAddress: "stake_test1uqevw2xnsc0pvn9t9r9c7qryan77xqk6etza9dprr8f80qq0e8ptn",
			RewardType:   "member",
			Earned:       "1000000",
			FetchedAt:    now,
		},
	}

	require.NoError(
		t,
		cache.CommitAccountRewardsForEpoch("preview", 100, rows, 1, true, now),
		"a literal duplicate (network, epoch, stake_address, reward_type) key must not error",
	)

	got, err := cache.GetAccountRewardsForEpoch("preview", 100)
	require.NoError(t, err)
	require.Len(
		t,
		got,
		2,
		"both duplicate rows must land so CompareAccountEpoch can later flag acct_duplicate",
	)
	require.Equal(t, "member", got[0].RewardType)
	require.Equal(t, "member", got[1].RewardType)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.Equal(t, 2, cov.FetchedCount)
	require.True(t, cov.Complete)
}

// TestAccountRewardsAdditiveColumnMigration proves OpenCache migrates an
// older koios_account_rewards table (missing reward_type/spendable_epoch/
// pool_id_bech32 — the #1875 schema-only shape) forward without errors or
// data loss, and that the widened unique index is in place afterward.
func TestAccountRewardsAdditiveColumnMigration(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cache.db")

	// Build the pre-#3097 shape directly, bypassing createCacheSchema.
	db, err := sql.Open("sqlite", path+"?"+legacySeedPragmas)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE koios_account_rewards (
		id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
		stake_address TEXT NOT NULL, earned TEXT NOT NULL, fetched_at DATETIME NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(
		`CREATE UNIQUE INDEX idx_kar_net_epoch_addr ON koios_account_rewards(network, epoch, stake_address)`,
	)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	_, err = db.Exec(
		`INSERT INTO koios_account_rewards (network, epoch, stake_address, earned, fetched_at) VALUES (?, ?, ?, ?, ?)`,
		"preview",
		50,
		"stake_test1existingrow",
		"42",
		now,
	)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Opening through the real path must migrate forward without error.
	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	got, err := cache.GetAccountRewardsForEpoch("preview", 50)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "stake_test1existingrow", got[0].StakeAddress)
	require.Equal(t, "42", got[0].Earned)
	require.Equal(t, "", got[0].RewardType) // pre-existing row defaults to ""

	// The widened unique key must now allow a second (member/leader) row for
	// the same address+epoch, which the old (network, epoch, stake_address)
	// unique index would have rejected.
	require.NoError(
		t,
		cache.CommitAccountRewardsForEpoch("preview", 50, []KoiosAccountRewards{
			{
				StakeAddress: "stake_test1existingrow",
				RewardType:   "member",
				Earned:       "1",
				FetchedAt:    now,
			},
			{
				StakeAddress: "stake_test1existingrow",
				RewardType:   "leader",
				Earned:       "2",
				FetchedAt:    now,
			},
		}, 1, true, now),
	)
	got, err = cache.GetAccountRewardsForEpoch("preview", 50)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

// TestCommitEpochMismatchesRollsBackOnFailedInsert proves #3410's fix:
// CommitEpochMismatches deletes and (re)inserts an epoch's mismatch rows in a
// single transaction, so a write failure partway through the insert rolls
// the delete back with it instead of leaving the epoch with zero evidence. A
// BEFORE INSERT trigger raises an error for one sentinel field value so the
// first row of the replacement batch inserts fine and the second aborts the
// whole transaction, without touching the production schema.
func TestCommitEpochMismatchesRollsBackOnFailedInsert(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	const network = "preview"
	const epoch = uint64(42)
	now := time.Now().UTC().Truncate(time.Second)

	prior := []CheckMismatch{{
		Network:    network,
		Epoch:      epoch,
		Field:      "prior_evidence",
		DingoValue: "1",
		KoiosValue: "2",
		Category:   CategoryDBError,
		CheckedAt:  now,
	}}
	require.NoError(t, cache.CommitEpochMismatches(network, epoch, prior))

	_, err = cache.db.Exec(`
		CREATE TRIGGER fail_on_sentinel BEFORE INSERT ON check_mismatches
		WHEN NEW.field = 'force_fail'
		BEGIN SELECT RAISE(ABORT, 'forced test failure'); END`)
	require.NoError(t, err)

	replacement := []CheckMismatch{
		{
			Network:   network,
			Epoch:     epoch,
			Field:     "ok_row",
			Category:  CategoryDBError,
			CheckedAt: now,
		},
		{
			Network:   network,
			Epoch:     epoch,
			Field:     "force_fail",
			Category:  CategoryDBError,
			CheckedAt: now,
		},
	}
	err = cache.CommitEpochMismatches(network, epoch, replacement)
	require.Error(t, err)

	got, err := cache.GetMismatches(network, epoch, "")
	require.NoError(t, err)
	require.Len(
		t,
		got,
		1,
		"a failed replacement must leave the prior evidence intact",
	)
	require.Equal(t, "prior_evidence", got[0].Field)
}

// accountMismatch and aggregateMismatch build one mismatch row in each check
// phase's scope, for the phase-isolation tests below.
func accountMismatch(network string, epoch uint64, at time.Time) CheckMismatch {
	return CheckMismatch{
		Network:      network,
		Epoch:        epoch,
		StakeAddress: "stake_test1account",
		Field:        "account_reward",
		DingoValue:   "1",
		KoiosValue:   "2",
		Category:     CategoryValueMismatch,
		CheckedAt:    at,
		Scope:        ScopeAccount,
	}
}

func aggregateMismatch(network string, epoch uint64, at time.Time) CheckMismatch {
	return CheckMismatch{
		Network:    network,
		Epoch:      epoch,
		PoolBech32: "pool1aggregate",
		Field:      "active_stake",
		DingoValue: "1",
		KoiosValue: "2",
		Category:   CategoryValueMismatch,
		CheckedAt:  at,
		Scope:      ScopeAggregate,
	}
}

// TestCheckEpochStatusPhasesClearIndependently pins the contract the two
// observer queues need from one (network, epoch) status row: the aggregate
// phase and the account phase write it at unrelated times, so neither may
// overwrite the other's verdict, and each must be able to clear its own
// failure once it passes again. Letting either phase own the single status
// column loses one of those two properties — a last-writer-wins column hides
// an account failure behind a later aggregate pass, and a column that refuses
// to leave a failure never records the recovery.
func TestCheckEpochStatusPhasesClearIndependently(t *testing.T) {
	t.Parallel()

	const network = "preview"
	const epoch = uint64(42)

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	readStatus := func() CheckEpochStatus {
		t.Helper()
		all, err := cache.GetStatusSummary(network)
		require.NoError(t, err)
		require.Len(t, all, 1)
		return all[0]
	}

	// The account phase records a failure.
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:                network,
		Epoch:                  epoch,
		LastCheckedAt:          now,
		AggregateStatus:        StatusPass,
		AggregateMismatchCount: 0,
		AccountStatus:          StatusFail,
		AccountMismatchCount:   3,
	}))
	require.Equal(t, StatusFail, readStatus().Status)

	// A later aggregate-only pass must not hide it. The aggregate phase
	// leaves AccountStatus empty because it never ran that comparison.
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:         network,
		Epoch:           epoch,
		LastCheckedAt:   now.Add(time.Minute),
		AggregateStatus: StatusPass,
	}))
	after := readStatus()
	require.Equal(t, StatusFail, after.Status,
		"an aggregate pass must not clear the account phase's failure")
	require.Equal(t, 3, after.MismatchCount)
	require.Equal(t, StatusFail, after.AccountStatus)
	require.Equal(t, now.Add(time.Minute), after.LastCheckedAt.UTC())

	// The account phase recovering must clear its own failure.
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:              network,
		Epoch:                epoch,
		LastCheckedAt:        now.Add(2 * time.Minute),
		AggregateStatus:      StatusPass,
		AccountStatus:        StatusPass,
		AccountMismatchCount: 0,
	}))
	recovered := readStatus()
	require.Equal(t, StatusPass, recovered.Status,
		"a recovered account check must clear the stored account failure")
	require.Equal(t, 0, recovered.MismatchCount)

	// The same must hold in the other direction: an aggregate failure that an
	// account-phase pass cannot clear, and that the aggregate phase's own
	// later pass does.
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:                network,
		Epoch:                  epoch,
		LastCheckedAt:          now.Add(3 * time.Minute),
		AggregateStatus:        StatusFail,
		AggregateMismatchCount: 2,
	}))
	require.Equal(t, StatusFail, readStatus().Status)

	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:         network,
		Epoch:           epoch,
		LastCheckedAt:   now.Add(4 * time.Minute),
		AggregateStatus: StatusPass,
		AccountStatus:   StatusPass,
	}))
	require.Equal(t, StatusPass, readStatus().Status,
		"a recovered aggregate check must clear the stored aggregate failure")

	// ERROR is merged by severity, not by recency: an account ERROR alongside
	// an aggregate PASS surfaces as ERROR.
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:              network,
		Epoch:                epoch,
		LastCheckedAt:        now.Add(5 * time.Minute),
		AggregateStatus:      StatusPass,
		AccountStatus:        StatusError,
		AccountMismatchCount: 1,
	}))
	require.Equal(t, StatusError, readStatus().Status)
}

// TestUpsertCheckEpochStatusDefaultsToAggregatePhase pins the reading of a
// caller that sets only Status: it is an aggregate-phase result, so it clears
// on its own next pass rather than sticking, and it never touches the account
// phase's stored verdict.
func TestUpsertCheckEpochStatusDefaultsToAggregatePhase(t *testing.T) {
	t.Parallel()

	const network = "preview"
	const epoch = uint64(7)

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:       network,
		Epoch:         epoch,
		LastCheckedAt: now,
		Status:        StatusFail,
		MismatchCount: 4,
	}))
	all, err := cache.GetStatusSummary(network)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, StatusFail, all[0].AggregateStatus)
	require.Equal(t, 4, all[0].AggregateMismatchCount)
	require.Empty(t, all[0].AccountStatus)

	require.NoError(t, cache.UpsertCheckEpochStatus(CheckEpochStatus{
		Network:       network,
		Epoch:         epoch,
		LastCheckedAt: now.Add(time.Minute),
		Status:        StatusPass,
	}))
	all, err = cache.GetStatusSummary(network)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, StatusPass, all[0].Status)
	require.Equal(t, 0, all[0].MismatchCount)
}

// TestCommitEpochMismatchesReplacesOnlyNamedScopes proves the evidence rows
// follow the status: an aggregate-only run replaces its own rows and leaves
// the account phase's in place. Deleting them would leave the account failure
// recorded on check_epoch_status with nothing behind it, and a later report
// would read a FAIL epoch with no mismatches to show for it.
func TestCommitEpochMismatchesReplacesOnlyNamedScopes(t *testing.T) {
	t.Parallel()

	const network = "preview"
	const epoch = uint64(12)

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, cache.CommitEpochMismatches(
		network,
		epoch,
		[]CheckMismatch{
			aggregateMismatch(network, epoch, now),
			accountMismatch(network, epoch, now),
		},
		AllMismatchScopes...,
	))

	// The aggregate phase passing replaces its own rows only.
	require.NoError(t, cache.CommitEpochMismatches(
		network,
		epoch,
		nil,
		ScopeAggregate,
	))
	got, err := cache.GetMismatches(network, epoch, "")
	require.NoError(t, err)
	require.Len(t, got, 1,
		"an aggregate-only commit must not delete the account phase's evidence")
	require.Equal(t, ScopeAccount, got[0].Scope)
	require.Equal(t, "account_reward", got[0].Field)

	// The account phase passing then clears its own.
	require.NoError(t, cache.CommitEpochMismatches(
		network,
		epoch,
		nil,
		ScopeAccount,
	))
	got, err = cache.GetMismatches(network, epoch, "")
	require.NoError(t, err)
	require.Empty(t, got,
		"a recovered account check must clear the account phase's evidence")
}

// TestCommitEpochMismatchesDefaultsScopeToAggregate pins the reading of a
// caller that passes no scope and untagged rows: the whole epoch is replaced
// and the rows land in the aggregate scope, which is what every caller
// predating the phase split meant.
func TestCommitEpochMismatchesDefaultsScopeToAggregate(t *testing.T) {
	t.Parallel()

	const network = "preview"
	const epoch = uint64(13)

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, cache.CommitEpochMismatches(
		network,
		epoch,
		[]CheckMismatch{accountMismatch(network, epoch, now)},
		AllMismatchScopes...,
	))
	require.NoError(t, cache.CommitEpochMismatches(
		network,
		epoch,
		[]CheckMismatch{{
			Network:   network,
			Epoch:     epoch,
			Field:     "untagged",
			Category:  CategoryValueMismatch,
			CheckedAt: now,
		}},
	))
	got, err := cache.GetMismatches(network, epoch, "")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "untagged", got[0].Field)
	require.Equal(t, ScopeAggregate, got[0].Scope)
}

// TestCheckPhaseColumnMigration proves OpenCache migrates a cache file
// written before the phase split forward without data loss, and that the
// pre-existing verdict is attributed to the aggregate phase so it can clear
// on that phase's next pass rather than outliving its cause.
func TestCheckPhaseColumnMigration(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "cache.db")

	db, err := sql.Open("sqlite", path+"?"+legacySeedPragmas)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE check_epoch_status (
		id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
		last_checked_at DATETIME NOT NULL, status TEXT NOT NULL, mismatch_count INTEGER NOT NULL,
		dingo_pool_count INTEGER NOT NULL, koios_pool_count INTEGER NOT NULL,
		only_dingo_pools TEXT NOT NULL, only_koios_pools TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE check_mismatches (
		id INTEGER PRIMARY KEY AUTOINCREMENT, network TEXT NOT NULL, epoch INTEGER NOT NULL,
		pool_bech32 TEXT NOT NULL, stake_address TEXT NOT NULL, field TEXT NOT NULL,
		dingo_value TEXT NOT NULL, koios_value TEXT NOT NULL, category TEXT NOT NULL,
		checked_at DATETIME NOT NULL)`)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	_, err = db.Exec(
		`INSERT INTO check_epoch_status (network, epoch, last_checked_at, status,
			mismatch_count, dingo_pool_count, koios_pool_count, only_dingo_pools, only_koios_pools)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"preview", 9, now, StatusFail, 5, 1, 1, "[]", "[]",
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`INSERT INTO check_mismatches (network, epoch, pool_bech32, stake_address, field,
			dingo_value, koios_value, category, checked_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"preview", 9, "pool1legacy", "", "legacy_field", "1", "2", CategoryValueMismatch, now,
	)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	statuses, err := cache.GetStatusSummary("preview")
	require.NoError(t, err)
	require.Len(t, statuses, 1)
	require.Equal(t, StatusFail, statuses[0].Status)
	require.Equal(t, StatusFail, statuses[0].AggregateStatus,
		"an unattributed stored verdict is the aggregate phase's, so it clears on that phase's next pass")
	require.Equal(t, 5, statuses[0].AggregateMismatchCount)
	require.Empty(t, statuses[0].AccountStatus)

	mismatches, err := cache.GetMismatches("preview", 9, "")
	require.NoError(t, err)
	require.Len(t, mismatches, 1)
	require.Equal(t, "legacy_field", mismatches[0].Field)
	require.Equal(t, ScopeAggregate, mismatches[0].Scope)
}

// TestTxInfoCacheRoundTripsAndScopesByNetwork proves koios_tx_info stores a
// KoiosTxInfoItem losslessly (the datum/asset/reference-script detail
// CanonicalKoiosUTxOEntry compares on, not just the hash), keys it by
// network the way every other Koios-sourced table is keyed, and answers a
// hash list longer than SQLite's 999-bound-parameter limit -- the case
// txInfoLookupChunk exists for, which a chunk-sized test would never reach.
func TestTxInfoCacheRoundTripsAndScopesByNetwork(t *testing.T) {
	t.Parallel()

	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	datumHash := "deadbeef"
	rich := KoiosTxInfoItem{
		TxHash: "rich",
		Inputs: []KoiosTxInfoUtxoRef{{TxHash: "prev", TxIndex: 3}},
		Outputs: []KoiosTxInfoOutput{{
			TxHash:          "rich",
			TxIndex:         0,
			Value:           "42",
			DatumHash:       &datumHash,
			InlineDatum:     &KoiosTxInfoInlineDatum{Bytes: "d87980"},
			ReferenceScript: &KoiosTxInfoReferenceScript{Hash: "cafe"},
			AssetList: []KoiosTxInfoAsset{
				{PolicyID: "aa", AssetName: "bb", Quantity: "9"},
			},
		}},
	}

	const bulk = 1500
	items := []KoiosTxInfoItem{rich}
	hashes := []string{"rich"}
	for i := range bulk {
		h := fmt.Sprintf("tx%04d", i)
		items = append(items, KoiosTxInfoItem{TxHash: h})
		hashes = append(hashes, h)
	}
	require.NoError(t, cache.UpsertTxInfos("preview", items, time.Now().UTC()))

	got, err := cache.GetTxInfos("preview", hashes)
	require.NoError(t, err)
	require.Len(t, got, bulk+1,
		"a hash list past SQLite's parameter limit must still be answered in full")
	require.Equal(t, rich, got["rich"],
		"a cached item must round-trip byte-identically, datum and assets included")

	// A hash nobody cached is simply absent, not an error: that is the
	// signal the caller uses to fetch exactly the misses.
	partial, err := cache.GetTxInfos("preview", []string{"rich", "never-seen"})
	require.NoError(t, err)
	require.Len(t, partial, 1)
	require.NotContains(t, partial, "never-seen")

	// Rows are network-scoped like every other Koios-sourced table, so
	// preprod never reads preview's answers.
	other, err := cache.GetTxInfos("preprod", hashes)
	require.NoError(t, err)
	require.Empty(t, other)
}
