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

//go:build dingo_extra_plugins

package mysql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

// TestGetRewardAccountOutputsByCredentialExcludesGuardedMysql pins dingo
// #3021 against mysql: a row withheld by the CIP-0163 reward-crediting guard
// (Guarded = true) must be absent from both the returned rows and the count,
// even though it remains Spendable = true.
func TestGetRewardAccountOutputsByCredentialExcludesGuardedMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	t.Cleanup(func() { _ = store.Close() })
	db := store.DB()

	stakingKey := testHash28("reward-history-guarded-cred")
	pool := testHash28("reward-history-guarded-pool")

	t.Cleanup(func() {
		_ = db.Where("staking_key = ?", stakingKey).
			Delete(&models.RewardAccountOutput{}).Error
	})

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 10, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "member", Amount: 1_000_000, Spendable: true, Guarded: false},
		{Epoch: 11, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "leader", Amount: 9_999_999, Spendable: true, Guarded: true},
	}, nil))

	count, err := store.CountRewardAccountOutputsByCredential(0, stakingKey, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the guarded row must not be counted")

	rows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 100, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the guarded row must be absent from the results")
	require.Equal(t, uint64(10), rows[0].Epoch)
	require.True(t, rows[0].Spendable)
	require.False(t, rows[0].Guarded)
}
