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

package sqlstore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestGetLiveStakeInputsForPoolsMatchesPerRowPendingCredits pins the live
// stake read against the per-row formulation it replaced: total_stake plus
// pendingRewardCreditSubquery evaluated for every row. The fixture spans two
// pool chunks, credited and uncredited rounds, and outputs the subquery
// excludes (guarded, unspendable, folded), so the once-per-call totals must
// land on exactly the rows, in exactly the order, the subquery produced.
func TestGetLiveStakeInputsForPoolsMatchesPerRowPendingCredits(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	require.NoError(t, store.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{
			{SnapshotEpoch: 1, BoundarySlot: 100},
			{SnapshotEpoch: 2, BoundarySlot: 200},
		}, nil,
	))
	poolCount := store.dialect.ParameterLimit() + 2
	pools := make([][]byte, 0, poolCount)
	for i := range poolCount {
		pool := bytesRepeat(0x00, 28)
		binary.BigEndian.PutUint32(pool, uint32(i+1)) //nolint:gosec // bounded fixture index
		pools = append(pools, pool)
	}
	credited := bytesRepeat(0xa1, 28)
	bare := bytesRepeat(0xa2, 28)
	lastChunk := bytesRepeat(0xa3, 28)
	output := func(
		epoch uint64, key []byte, pool []byte, amount uint64,
		spendable, guarded bool,
	) *models.RewardAccountOutput {
		return &models.RewardAccountOutput{
			Epoch: epoch, CredentialTag: 0, StakingKey: key,
			PoolKeyHash: pool, RewardType: "member",
			Amount: types.Uint64(amount), Spendable: spendable,
			Guarded: guarded, CapturedSlot: epoch * 100, BoundarySlot: epoch * 100,
		}
	}
	require.NoError(t, store.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{
			output(1, credited, pools[0], 25, true, false),
			output(2, credited, pools[0], 10, true, false),
			// Excluded: guarded, unspendable, and a round not yet credited.
			output(2, credited, pools[1], 7, true, true),
			output(2, credited, pools[2], 9, false, false),
			output(3, credited, pools[0], 11, true, false),
			// The same credential's leader reward in the credited round.
			output(2, credited, pools[3], 4, true, false),
			output(2, lastChunk, pools[poolCount-1], 5, true, false),
			// Folded below.
			output(1, lastChunk, pools[0], 13, true, false),
		}, nil,
	))
	_, err := store.writeDB.Exec(
		`UPDATE reward_account_output SET folded = TRUE WHERE epoch = 1 AND staking_key = ?`,
		lastChunk,
	)
	require.NoError(t, err)
	insert := func(pool, key []byte, total string) {
		_, err := store.writeDB.Exec(store.dialect.Rebind(`
INSERT INTO reward_live_stake (
    pool_key_hash, staking_key, credential_tag, utxo_stake, reward_stake,
    total_stake, registered, updated_slot, calculation_version
) VALUES (?, ?, 0, ?, '0', ?, TRUE, 1, ?)`),
			pool, key, total, total, models.RewardStakeCalculationVersion,
		)
		require.NoError(t, err)
	}
	insert(pools[0], credited, "1000")
	insert(pools[0], bare, "2000")
	insert(pools[poolCount-1], lastChunk, "3000")
	for i, pool := range pools[1 : poolCount-1] {
		filler := bytesRepeat(0xb0, 28)
		binary.BigEndian.PutUint32(filler[24:], uint32(i+1)) //nolint:gosec // bounded fixture index
		insert(pool, filler, "1")
	}

	inputs, err := store.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	require.Len(t, inputs, poolCount+1)
	got := make([]string, 0, len(inputs))
	for _, input := range inputs {
		got = append(got, fmt.Sprintf("%x %d %x %d",
			input.PoolKeyHash, input.CredentialTag, input.StakingKey,
			uint64(input.Stake)))
	}

	// The per-row formulation, chunked exactly as the read is.
	var want []string
	chunkSize := store.dialect.ParameterLimit()
	for start := 0; start < len(pools); start += chunkSize {
		end := min(start+chunkSize, len(pools))
		rows, err := store.readDB.Query(`
SELECT rls.pool_key_hash, rls.staking_key, rls.credential_tag,
       CAST(rls.total_stake AS INTEGER) + `+
			store.pendingRewardCreditSubquery(
				"rls.credential_tag", "rls.staking_key",
			)+`
FROM reward_live_stake rls
WHERE rls.pool_key_hash IN (`+bindPlaceholders(end-start)+`)
  AND rls.registered = TRUE
ORDER BY rls.pool_key_hash ASC, rls.credential_tag ASC,
         rls.staking_key ASC`, byteSliceArgs(pools[start:end])...)
		require.NoError(t, err)
		for rows.Next() {
			var pool, key []byte
			var tag, stake int64
			require.NoError(t, rows.Scan(&pool, &key, &tag, &stake))
			want = append(want, fmt.Sprintf("%x %d %x %d", pool, tag, key, stake))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	require.Equal(t, want, got)
	require.Contains(t, got, fmt.Sprintf("%x 0 %x 1039", pools[0], credited),
		"credited rounds 1 and 2 add 25+10+4; guarded, unspendable and "+
			"uncredited outputs do not")
	require.Contains(t, got, fmt.Sprintf("%x 0 %x 2000", pools[0], bare))
	require.Contains(t, got,
		fmt.Sprintf("%x 0 %x 3005", pools[poolCount-1], lastChunk),
		"the second chunk receives its pending credit; the folded one does not")
}

// TestPendingRewardCreditTotalsReadTheCoveringIndex is the regression for the
// boundary stake read: totalling every credential's unfolded credits must
// visit only the credited rounds' unfolded rows, through the four columns of
// idx_reward_account_output_pending_round, with no scan of the table and no
// sort, whether or not sqlite_stat1 has been populated: a node runs ANALYZE
// only around an import, so the boundary read normally runs without current
// stats. No credential-keyed index serves it, so folding a credit moves no
// entry in one.
func TestPendingRewardCreditTotalsReadTheCoveringIndex(t *testing.T) {
	t.Parallel()
	for _, stats := range []struct {
		name    string
		analyze bool
	}{
		{name: "without_planner_stats"},
		{name: "with_planner_stats", analyze: true},
	} {
		t.Run(stats.name, func(t *testing.T) {
			t.Parallel()
			store := newManagementTestStore(t)
			require.NoError(t, store.SetPendingRewardCreditRounds(
				[]models.RewardCreditRound{{SnapshotEpoch: 1, BoundarySlot: 100}},
				nil,
			))
			outputs := make([]*models.RewardAccountOutput, 0, 64)
			for i := range 64 {
				key := bytesRepeat(0xc0, 28)
				key[27] = byte(i)
				outputs = append(outputs, &models.RewardAccountOutput{
					Epoch: 1, CredentialTag: 0, StakingKey: key,
					PoolKeyHash: bytesRepeat(0xc1, 28), RewardType: "member",
					Amount: types.Uint64(uint64(i)), Spendable: i%4 != 0,
					Guarded: i%8 == 1, CapturedSlot: 100, BoundarySlot: 100,
				})
			}
			require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
			if stats.analyze {
				analyzeStore(t, store)
			}
			plan := queryPlan(t, store.readDB, store.pendingRewardCreditRowsSQL())
			require.Contains(t, plan,
				"idx_reward_account_output_pending_round "+
					"(spendable=? AND guarded=? AND folded=? AND epoch=?)",
				"plan %s", plan)
			require.NotContains(t, plan, "SCAN reward_account_output",
				"plan %s", plan)
			require.NotContains(t, plan, "TEMP B-TREE", "plan %s", plan)
		})
	}
}

// newLiveStakeSnapshotTestStore opens a file-backed database with separate
// read and write pools. It needs WAL rather than the MEMORY journal other
// file-backed tests use: only WAL lets a write commit while a read
// transaction holds its snapshot, and the shared-cache in-memory stores lock
// tables instead.
func newLiveStakeSnapshotTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := "file:" + filepath.Join(t.TempDir(), "live_stake.db") +
		"?_pragma=synchronous(OFF)&_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)"
	writeDB, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	writeDB.SetMaxOpenConns(1)
	readDB, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	readDB.SetMaxOpenConns(2)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         writeDB,
		ReadDB:          readDB,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// seedLiveStakePools registers one delegator per pool, each with total_stake
// 1000 and one unfolded 25-lovelace credit in credited round 1.
func seedLiveStakePools(t *testing.T, store *Store, count int) [][]byte {
	t.Helper()
	require.NoError(t, store.SetPendingRewardCreditRounds(
		[]models.RewardCreditRound{{SnapshotEpoch: 1, BoundarySlot: 100}}, nil,
	))
	pools := make([][]byte, 0, count)
	outputs := make([]*models.RewardAccountOutput, 0, count)
	for i := range count {
		pool := bytesRepeat(0x00, 28)
		binary.BigEndian.PutUint32(pool, uint32(i+1)) //nolint:gosec // bounded fixture index
		key := bytesRepeat(0xd0, 28)
		binary.BigEndian.PutUint32(key[24:], uint32(i+1)) //nolint:gosec // bounded fixture index
		pools = append(pools, pool)
		outputs = append(outputs, &models.RewardAccountOutput{
			Epoch: 1, CredentialTag: 0, StakingKey: key, PoolKeyHash: pool,
			RewardType: "member", Amount: 25, Spendable: true,
			CapturedSlot: 100, BoundarySlot: 100,
		})
		_, err := store.writeDB.Exec(store.dialect.Rebind(`
INSERT INTO reward_live_stake (
    pool_key_hash, staking_key, credential_tag, utxo_stake, reward_stake,
    total_stake, registered, updated_slot, calculation_version
) VALUES (?, ?, 0, '1000', '0', '1000', TRUE, 1, ?)`),
			pool, key, models.RewardStakeCalculationVersion,
		)
		require.NoError(t, err)
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	return pools
}

// TestGetLiveStakeInputsForPoolsReadsOneSnapshotWithoutTxn pins the
// autocommit path to one snapshot. The pending totals and the pool reads are
// separate statements; a fold committing between them moves the credit into
// total_stake after the totals already counted it. Read in one snapshot the
// delegator's stake is 1025 whichever side of the fold the snapshot falls on.
func TestGetLiveStakeInputsForPoolsReadsOneSnapshotWithoutTxn(t *testing.T) {
	t.Parallel()
	store := newLiveStakeSnapshotTestStore(t)
	pools := seedLiveStakePools(t, store, liveStakeGroupedPendingMinPools)
	folds := 0
	store.liveStakePendingTotalsRead = func() {
		folds++
		_, err := store.writeDB.Exec(
			`UPDATE reward_account_output SET folded = TRUE`,
		)
		require.NoError(t, err)
		_, err = store.writeDB.Exec(
			`UPDATE reward_live_stake SET total_stake = '1025'`,
		)
		require.NoError(t, err)
	}

	inputs, err := store.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, folds, "the grouped totals pass must have run")
	require.Len(t, inputs, len(pools))
	for _, input := range inputs {
		require.EqualValues(t, 1025, input.Stake,
			"credential %x", input.StakingKey)
	}
}

// TestGetLiveStakeInputsForPoolsSmallReadSkipsGroupedTotals pins the scope of
// the grouped pass. It totals every credential's pending credits, which only
// pays for itself on a read covering most delegators; a read of a few pools,
// such as the retiring-pool capture, keeps the per-row subquery and touches
// only its own delegators' outputs. Both paths return the same stake.
func TestGetLiveStakeInputsForPoolsSmallReadSkipsGroupedTotals(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	pools := seedLiveStakePools(t, store, liveStakeGroupedPendingMinPools)
	grouped := 0
	store.liveStakePendingTotalsRead = func() { grouped++ }

	small, err := store.GetLiveStakeInputsForPools(
		pools[:liveStakeGroupedPendingMinPools-1], 0, nil,
	)
	require.NoError(t, err)
	require.Zero(t, grouped, "a small read must not total every credential")
	require.Len(t, small, liveStakeGroupedPendingMinPools-1)
	for _, input := range small {
		require.EqualValues(t, 1025, input.Stake)
	}

	full, err := store.GetLiveStakeInputsForPools(pools, 0, nil)
	require.NoError(t, err)
	require.Equal(t, 1, grouped)
	require.Len(t, full, liveStakeGroupedPendingMinPools)
	for _, input := range full {
		require.EqualValues(t, 1025, input.Stake)
	}
}
