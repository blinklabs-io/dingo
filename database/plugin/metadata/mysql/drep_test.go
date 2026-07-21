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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
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

	require.NoError(t, store.SetDrep(0, rewardOnlyCred, 1000, "", nil, true, nil))
	require.NoError(t, store.SetDrep(0, multiUtxoCred, 1000, "", nil, true, nil))
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:      rewardOnlyStake,
		Drep:            rewardOnlyCred,
		Reward:          700,
		Active:          true,
		AddedSlot:       1000,
		ExpirationEpoch: 4,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:      multiUtxoStake,
		Drep:            multiUtxoCred,
		Reward:          500,
		Active:          true,
		AddedSlot:       1000,
		ExpirationEpoch: 6,
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
		[]models.StakeCredentialRef{
			{Tag: 0, Key: rewardOnlyCred},
			{Tag: 0, Key: multiUtxoCred},
		},
		0,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), powers[models.StakeCredentialRef{Tag: 0, Key: rewardOnlyCred}.MapKey()])
	assert.Equal(t, uint64(1000), powers[models.StakeCredentialRef{Tag: 0, Key: multiUtxoCred}.MapKey()])

	singlePower, err := store.GetDRepVotingPower(0, rewardOnlyCred, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), singlePower)

	singlePower, err = store.GetDRepVotingPower(0, multiUtxoCred, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), singlePower)

	powers, err = store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{
			{Tag: 0, Key: rewardOnlyCred},
			{Tag: 0, Key: multiUtxoCred},
		},
		5,
		nil,
	)
	require.NoError(t, err)
	assert.NotContains(t, powers, models.StakeCredentialRef{Tag: 0, Key: rewardOnlyCred}.MapKey())
	assert.Equal(t, uint64(1000), powers[models.StakeCredentialRef{Tag: 0, Key: multiUtxoCred}.MapKey()])

	singlePower, err = store.GetDRepVotingPower(0, rewardOnlyCred, 5, nil)
	require.NoError(t, err)
	assert.Zero(t, singlePower)
	singlePower, err = store.GetDRepVotingPower(0, multiUtxoCred, 5, nil)
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
		StakingKey:      abstainStake,
		DrepType:        models.DrepTypeAlwaysAbstain,
		Reward:          700,
		Active:          true,
		AddedSlot:       1000,
		ExpirationEpoch: 4,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:      noConfidenceStake,
		DrepType:        models.DrepTypeAlwaysNoConfidence,
		Reward:          500,
		Active:          true,
		AddedSlot:       1000,
		ExpirationEpoch: 6,
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
		0,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), powers[models.DrepTypeAlwaysAbstain])
	assert.Equal(t, uint64(1000), powers[models.DrepTypeAlwaysNoConfidence])

	powers, err = store.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		5,
		nil,
	)
	require.NoError(t, err)
	assert.NotContains(t, powers, models.DrepTypeAlwaysAbstain)
	assert.Equal(t, uint64(1000), powers[models.DrepTypeAlwaysNoConfidence])
}

// TestMysqlGetDRepVotingPowerCrossTagIsolation verifies that key-hash and
// script-hash DReps sharing the same 28-byte credential hash are treated as
// independent identities. GetDRepVotingPower and GetDRepVotingPowerBatch must
// only aggregate stake delegated to the exact (tag, hash) pair requested.
func TestMysqlGetDRepVotingPowerCrossTagIsolation(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	cleanupDRepVotingPowerTestData(t, store)

	sharedHash := testHash28("shared_drep_cross_tag")

	require.NoError(t, store.SetDrep(0, sharedHash, 1000, "", nil, true, nil))
	require.NoError(t, store.SetDrep(1, sharedHash, 1000, "", nil, true, nil))

	keyStake := testHash28("key_stake_cross_tag")
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    keyStake,
		CredentialTag: 0,
		Drep:          sharedHash,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        types.Uint64(100),
		Active:        true,
		AddedSlot:     1000,
	}).Error)

	scriptStake := testHash28("script_stake_cross_tag")
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    scriptStake,
		CredentialTag: 1,
		Drep:          sharedHash,
		DrepType:      models.DrepTypeScriptHash,
		Reward:        types.Uint64(200),
		Active:        true,
		AddedSlot:     1000,
	}).Error)

	keyPower, err := store.GetDRepVotingPower(0, sharedHash, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), keyPower, "key-hash DRep power should be 100")

	scriptPower, err := store.GetDRepVotingPower(1, sharedHash, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), scriptPower, "script-hash DRep power should be 200")

	powers, err := store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{
			{Tag: 0, Key: sharedHash},
			{Tag: 1, Key: sharedHash},
		},
		0,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), powers[models.StakeCredentialRef{Tag: 0, Key: sharedHash}.MapKey()],
		"batch: key-hash DRep power should be 100")
	assert.Equal(t, uint64(200), powers[models.StakeCredentialRef{Tag: 1, Key: sharedHash}.MapKey()],
		"batch: script-hash DRep power should be 200")
}

// TestMysqlRollbackRewardDeltaIsCredentialTagAware verifies that
// DeleteAccountRewardsAfterSlot matches AccountRewardDelta rows to accounts
// by the composite (credential_tag, staking_key) key. A credit delta for
// tag=1 must adjust only the script-hash account, not the key-hash account
// that shares the same 28-byte hash.
func TestMysqlRollbackRewardDeltaIsCredentialTagAware(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck

	stakeKey := testHash28("reward_rollback_cross_tag")
	const baseReward = types.Uint64(1_000_000)
	const creditAmount = uint64(500_000)
	const slot = uint64(200)

	keyAccount := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     10,
		Reward:        baseReward,
	}
	scriptAccount := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 1,
		Active:        true,
		AddedSlot:     10,
		Reward:        baseReward,
	}
	require.NoError(t, store.DB().Create(keyAccount).Error)
	require.NoError(t, store.DB().Create(scriptAccount).Error)

	require.NoError(t, store.AddAccountRewardByCredential(1, stakeKey, creditAmount, slot, nil, nil))

	keyBefore, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward, keyBefore.Reward, "key-hash account must not be affected by script-hash credit")

	scriptBefore, err := store.GetAccountByCredential(1, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward+types.Uint64(creditAmount), scriptBefore.Reward, "script-hash account must have received credit")

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(slot-1, nil))

	keyAfter, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward, keyAfter.Reward, "key-hash account reward must be unchanged after rollback")

	scriptAfter, err := store.GetAccountByCredential(1, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward, scriptAfter.Reward, "script-hash account reward must be restored to base after rollback")
}
