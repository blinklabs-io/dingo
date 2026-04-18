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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqliteGetDRepVotingPowerIncludesReward(t *testing.T) {
	store := setupTestStore(t)

	drepCred := []byte("drep_reward_only_123456789012345678901234")
	rewardOnlyStake := []byte("stake_reward_only_123456789012345678901")
	multiUtxoStake := []byte("stake_multi_utxo_123456789012345678901234")

	require.NoError(t, store.SetDrep(drepCred, 1000, "", nil, true, nil))
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: rewardOnlyStake,
		Drep:       drepCred,
		Reward:     700,
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: multiUtxoStake,
		Drep:       drepCred,
		Reward:     500,
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_reward_123456789012345678901234567890"),
		StakingKey: multiUtxoStake,
		Amount:     300,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_reward_223456789012345678901234567890"),
		StakingKey: multiUtxoStake,
		Amount:     200,
		OutputIdx:  1,
		AddedSlot:  1000,
	}).Error)

	power, err := store.GetDRepVotingPower(drepCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1700), power)
}

func TestSqliteGetDRepVotingPowerBatchIncludesReward(t *testing.T) {
	store := setupTestStore(t)

	rewardOnlyCred := []byte("drep_reward_only_22345678901234567890123")
	rewardOnlyStake := []byte("stake_reward_only_2234567890123456789012")
	multiUtxoCred := []byte("drep_multi_utxo_1234567890123456789012345")
	multiUtxoStake := []byte("stake_multi_utxo_223456789012345678901234")

	require.NoError(t, store.SetDrep(rewardOnlyCred, 1000, "", nil, true, nil))
	require.NoError(t, store.SetDrep(multiUtxoCred, 1000, "", nil, true, nil))
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: rewardOnlyStake,
		Drep:       rewardOnlyCred,
		Reward:     700,
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: multiUtxoStake,
		Drep:       multiUtxoCred,
		Reward:     500,
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_multi_12345678901234567890123456789012"),
		StakingKey: multiUtxoStake,
		Amount:     300,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_multi_22345678901234567890123456789012"),
		StakingKey: multiUtxoStake,
		Amount:     200,
		OutputIdx:  1,
		AddedSlot:  1000,
	}).Error)

	powers, err := store.GetDRepVotingPowerBatch(
		[][]byte{rewardOnlyCred, multiUtxoCred},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), powers[string(rewardOnlyCred)])
	assert.Equal(t, uint64(1000), powers[string(multiUtxoCred)])
}

func TestSqliteGetDRepVotingPowerBatchDoesNotMultiplyRewardAcrossUTxOs(t *testing.T) {
	store := setupTestStore(t)

	drepCred := []byte("drep_multi_reward_12345678901234567890123")
	stakeKey := []byte("stake_multi_reward_1234567890123456789012")

	require.NoError(t, store.SetDrep(drepCred, 1000, "", nil, true, nil))
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Drep:       drepCred,
		Reward:     700,
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_multi_32345678901234567890123456789012"),
		StakingKey: stakeKey,
		Amount:     300,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_multi_42345678901234567890123456789012"),
		StakingKey: stakeKey,
		Amount:     200,
		OutputIdx:  1,
		AddedSlot:  1000,
	}).Error)

	powers, err := store.GetDRepVotingPowerBatch([][]byte{drepCred}, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1200), powers[string(drepCred)])
}
