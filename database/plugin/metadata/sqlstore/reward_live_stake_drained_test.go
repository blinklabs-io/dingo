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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func countRewardLiveStakeRows(tb testing.TB, store *Store) int {
	tb.Helper()
	var n int
	require.NoError(
		tb,
		store.writeDB.QueryRow(`SELECT COUNT(*) FROM reward_live_stake`).
			Scan(&n),
	)
	return n
}

// Once the live-stake family is drained, no path writes reward_live_stake or
// reports it needs a rebuild, and a batched rebuild refuses; with the family
// not drained, the same calls do maintain the rows.
func TestRewardLiveStakeMaintenanceStopsOnceDrained(t *testing.T) {
	t.Parallel()
	pool := bytesRepeat(0xaa, 28)
	first := models.NewStakeCredentialRef(0, credentialKeyForIndex(1))
	second := models.NewStakeCredentialRef(0, credentialKeyForIndex(2))
	store := newMigratedSQLiteStore(t)
	seedRewardLiveStakeAccount(t, store, first, pool, 10)
	seedCredentialUtxos(t, store, 1, first, []uint64{5_000_000}, nil)
	seedRewardLiveStakeAccount(t, store, second, pool, 20)
	refresh := func(ref models.StakeCredentialRef, slot uint64) {
		t.Helper()
		require.NoError(t, store.withWriteTransaction(
			nil, func(db queryer, ctx context.Context) error {
				return store.refreshRewardLiveStakeAggregate(ctx, db, ref, slot)
			},
		))
	}
	refresh(first, 100)
	utxo, reward, _, _ := readRewardLiveStake(t, store, first)
	require.Equal(t, uint64(5_000_000), utxo)
	require.Equal(t, uint64(10), reward)
	needed, err := store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(t, needed, "the second account has no row yet")

	store.SetRewardLiveStakeDrained(true)
	require.NoError(t, store.withWriteTransaction(
		nil, func(db queryer, ctx context.Context) error {
			return store.refreshRewardLiveStakeAggregateDelta(
				ctx, db, first, 101, 7,
			)
		},
	))
	refresh(second, 102)
	require.NoError(t, store.RefreshRewardLiveStakeRefs(
		[]models.StakeCredentialRef{first, second}, 103, nil,
	))
	require.NoError(t, store.RebuildRewardLiveStake(104, nil))
	require.NoError(t, store.RebuildRewardLiveStakeFromRunningTotals(104, nil))
	needed, err = store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.False(t, needed)
	require.NoError(t, store.ClearDelegationsToRetiredPool(pool, 105, nil))
	require.ErrorIs(t, store.RebuildRewardLiveStakeFromRunningTotalsInBatches(
		104, func(func(types.Txn) error) error {
			t.Fatal("a drained family ran a batched rebuild")
			return nil
		},
	), ErrRewardLiveStakeDrained)
	require.Equal(t, 1, countRewardLiveStakeRows(t, store))
	utxo, reward, _, _ = readRewardLiveStake(t, store, first)
	require.Equal(t, uint64(5_000_000), utxo, "the delta was not applied")
	require.Equal(t, uint64(10), reward)
	var slot int64
	var delegated []byte
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT updated_slot, pool_key_hash FROM reward_live_stake
		WHERE staking_key = ?`, first.Key,
	).Scan(&slot, &delegated))
	require.Equal(t, int64(100), slot)
	require.Equal(t, pool, delegated, "the retired pool's rows were left")

	store.SetRewardLiveStakeDrained(false)
	needed, err = store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(t, needed)
	refresh(second, 106)
	require.Equal(t, 2, countRewardLiveStakeRows(t, store))
}

// EachRewardLiveStake lists every row in id order across page boundaries,
// with each figure decoded.
func TestEachRewardLiveStakeListsEveryRow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	total := eachRewardLiveStakePage*2 + 3
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	for i := range total {
		_, err := tx.Exec(
			`INSERT INTO reward_live_stake (staking_key, credential_tag,
			pool_key_hash, utxo_stake, reward_stake, total_stake, registered,
			updated_slot) VALUES (?, ?, ?, ?, '2', ?, ?, 9)`,
			credentialKeyForIndex(i+1), i%2, bytesRepeat(0xd1, 28),
			fmt.Sprint(i), fmt.Sprint(i+2), i%3 == 0,
		)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	var got []*models.RewardLiveStake
	require.NoError(t, store.EachRewardLiveStake(
		nil, func(r *models.RewardLiveStake) error {
			got = append(got, r)
			return nil
		},
	))
	require.Len(t, got, total)
	for i, r := range got {
		require.Equal(t, credentialKeyForIndex(i+1), r.StakingKey)
		require.Equal(t, uint8(i%2), r.CredentialTag)
		require.Equal(t, types.Uint64(i), r.UtxoStake)
		require.Equal(t, types.Uint64(2), r.RewardStake)
		require.Equal(t, types.Uint64(i+2), r.TotalStake)
		require.Equal(t, i%3 == 0, r.Registered)
		require.Equal(t, bytesRepeat(0xd1, 28), r.PoolKeyHash)
		if i > 0 {
			require.Greater(t, r.ID, got[i-1].ID)
		}
	}
	stop := fmt.Errorf("stop")
	require.ErrorIs(t, store.EachRewardLiveStake(
		nil, func(*models.RewardLiveStake) error { return stop },
	), stop)
}
