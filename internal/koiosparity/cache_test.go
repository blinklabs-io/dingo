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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGetEpochsNeedingCheckDoesNotRequeueCheckedPreStakingEpoch(
	t *testing.T,
) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
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
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
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
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
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

// TestCommitAccountRewardsForEpochAllowsLiteralDuplicateKey proves
// idx_kar_net_epoch_addr_type's widening from unique to non-unique actually
// lets CommitAccountRewardsForEpoch insert two rows sharing the exact same
// (network, epoch, stake_address, reward_type) key without erroring — the
// real-world case is Koios itself legitimately returning a duplicate
// /account_reward_history row (see CategoryAcctDuplicate's doc comment), not
// just two rows with different reward_type values for the same address
// (already covered by TestCommitAccountRewardsForEpoch). Before the index was
// widened to non-unique, this insert would have failed with a UNIQUE
// constraint violation before CompareAccountEpoch ever got a chance to flag
// the duplicate as acct_duplicate.
func TestCommitAccountRewardsForEpochAllowsLiteralDuplicateKey(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
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
	path := filepath.Join(t.TempDir(), "cache.db")

	// Build the pre-#3097 shape directly, bypassing createCacheSchema.
	db, err := sql.Open("sqlite", path)
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
	cache, err := OpenCache(path, nil)
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
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
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
