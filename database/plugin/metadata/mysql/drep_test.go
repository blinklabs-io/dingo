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

package mysql

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cleanupDRepVotingPowerTestData(t *testing.T, store *MetadataStoreMysql) {
	t.Helper()

	db := store.DB()
	db.Where("1 = 1").Delete(&models.Utxo{})
	db.Where("1 = 1").Delete(&models.Account{})
	db.Where("1 = 1").Delete(&models.Drep{})
}

func TestMysqlGetDRepVotingPowerBatchIncludesReward(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	cleanupDRepVotingPowerTestData(t, store)

	rewardOnlyCred := testHash28("drep_reward_only")
	rewardOnlyStake := testHash28("stake_reward_only")
	multiUtxoCred := testHash28("drep_multi_utxo")
	multiUtxoStake := testHash28("stake_multi_utxo")

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
		TxId:       testHash32("tx_reward_1"),
		StakingKey: multiUtxoStake,
		Amount:     300,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       testHash32("tx_reward_2"),
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

	singlePower, err := store.GetDRepVotingPower(rewardOnlyCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), singlePower)

	singlePower, err = store.GetDRepVotingPower(multiUtxoCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), singlePower)
}

func TestMysqlGetDRepVotingPowerByTypeIncludesReward(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	cleanupDRepVotingPowerTestData(t, store)

	abstainStake := testHash28("stake_abstain_reward")
	noConfidenceStake := testHash28("stake_no_conf_reward")

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: abstainStake,
		DrepType:   models.DrepTypeAlwaysAbstain,
		Reward:     700,
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: noConfidenceStake,
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		Reward:     500,
		Active:     true,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       testHash32("tx_type_1"),
		StakingKey: noConfidenceStake,
		Amount:     300,
		OutputIdx:  0,
		AddedSlot:  1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       testHash32("tx_type_2"),
		StakingKey: noConfidenceStake,
		Amount:     200,
		OutputIdx:  1,
		AddedSlot:  1000,
	}).Error)

	powers, err := store.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), powers[models.DrepTypeAlwaysAbstain])
	assert.Equal(t, uint64(1000), powers[models.DrepTypeAlwaysNoConfidence])
}
