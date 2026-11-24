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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

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
