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

package postgres

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cleanupDRepVotingPowerTestData(t *testing.T, store *MetadataStorePostgres) {
	t.Helper()

	db := store.DB()
	db.Where("1 = 1").Delete(&models.Utxo{})
	db.Where("1 = 1").Delete(&models.Account{})
	db.Where("1 = 1").Delete(&models.Drep{})
}

func TestPostgresGetDRepVotingPowerBatchIncludesReward(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck
	cleanupDRepVotingPowerTestData(t, pgStore)

	rewardOnlyCred := []byte("drep_reward_only_123456789012345678901234")
	rewardOnlyStake := []byte("stake_reward_only_123456789012345678901")

	require.NoError(t, pgStore.SetDrep(rewardOnlyCred, 1000, "", nil, true, nil))
	require.NoError(t, pgStore.DB().Create(&models.Account{
		StakingKey: rewardOnlyStake,
		Drep:       rewardOnlyCred,
		Reward:     types.Uint64(700),
		Active:     true,
		AddedSlot:  1000,
	}).Error)

	multiUtxoCred := []byte("drep_multi_utxo_1234567890123456789012345")
	multiUtxoStake := []byte("stake_multi_utxo_123456789012345678901234")

	require.NoError(t, pgStore.SetDrep(multiUtxoCred, 1000, "", nil, true, nil))
	require.NoError(t, pgStore.DB().Create(&models.Account{
		StakingKey: multiUtxoStake,
		Drep:       multiUtxoCred,
		Reward:     types.Uint64(500),
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, pgStore.DB().Create(&models.Utxo{
		TxId:       []byte("tx_reward_123456789012345678901234567890"),
		StakingKey: multiUtxoStake,
		Amount:     300,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, pgStore.DB().Create(&models.Utxo{
		TxId:       []byte("tx_reward_223456789012345678901234567890"),
		StakingKey: multiUtxoStake,
		Amount:     200,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)

	powers, err := pgStore.GetDRepVotingPowerBatch(
		[][]byte{rewardOnlyCred, multiUtxoCred},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), powers[string(rewardOnlyCred)])
	assert.Equal(t, uint64(1000), powers[string(multiUtxoCred)])

	singlePower, err := pgStore.GetDRepVotingPower(rewardOnlyCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), singlePower)

	singlePower, err = pgStore.GetDRepVotingPower(multiUtxoCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), singlePower)
}

func TestPostgresGetDRepVotingPowerBatchDoesNotMultiplyRewardAcrossUTxOs(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck
	cleanupDRepVotingPowerTestData(t, pgStore)

	drepCred := []byte("drep_multi_reward_12345678901234567890123")
	stakeKey := []byte("stake_multi_reward_1234567890123456789012")

	require.NoError(t, pgStore.SetDrep(drepCred, 1000, "", nil, true, nil))
	require.NoError(t, pgStore.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Drep:       drepCred,
		Reward:     types.Uint64(700),
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, pgStore.DB().Create(&models.Utxo{
		TxId:       []byte("tx_multi_12345678901234567890123456789012"),
		StakingKey: stakeKey,
		Amount:     300,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, pgStore.DB().Create(&models.Utxo{
		TxId:       []byte("tx_multi_22345678901234567890123456789012"),
		StakingKey: stakeKey,
		Amount:     200,
		OutputIdx:  1,
		AddedSlot:  1000,
	}).Error)

	powers, err := pgStore.GetDRepVotingPowerBatch([][]byte{drepCred}, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1200), powers[string(drepCred)])

	singlePower, err := pgStore.GetDRepVotingPower(drepCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1200), singlePower)
}
