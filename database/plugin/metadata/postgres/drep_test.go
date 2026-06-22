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
	"bytes"
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

	require.NoError(t, pgStore.SetDrep(0, rewardOnlyCred, 1000, "", nil, true, nil))
	require.NoError(t, pgStore.DB().Create(&models.Account{
		StakingKey: rewardOnlyStake,
		Drep:       rewardOnlyCred,
		Reward:     types.Uint64(700),
		Active:     true,
		AddedSlot:  1000,
	}).Error)

	multiUtxoCred := []byte("drep_multi_utxo_1234567890123456789012345")
	multiUtxoStake := []byte("stake_multi_utxo_123456789012345678901234")

	require.NoError(t, pgStore.SetDrep(0, multiUtxoCred, 1000, "", nil, true, nil))
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
		[]models.StakeCredentialRef{
			{Tag: 0, Key: rewardOnlyCred},
			{Tag: 0, Key: multiUtxoCred},
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), powers[models.StakeCredentialRef{Tag: 0, Key: rewardOnlyCred}.MapKey()])
	assert.Equal(t, uint64(1000), powers[models.StakeCredentialRef{Tag: 0, Key: multiUtxoCred}.MapKey()])

	singlePower, err := pgStore.GetDRepVotingPower(0, rewardOnlyCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), singlePower)

	singlePower, err = pgStore.GetDRepVotingPower(0, multiUtxoCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), singlePower)
}

func TestPostgresGetDRepVotingPowerBatchDoesNotMultiplyRewardAcrossUTxOs(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck
	cleanupDRepVotingPowerTestData(t, pgStore)

	drepCred := []byte("drep_multi_reward_12345678901234567890123")
	stakeKey := []byte("stake_multi_reward_1234567890123456789012")

	require.NoError(t, pgStore.SetDrep(0, drepCred, 1000, "", nil, true, nil))
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

	powers, err := pgStore.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{{Tag: 0, Key: drepCred}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(1200), powers[models.StakeCredentialRef{Tag: 0, Key: drepCred}.MapKey()])

	singlePower, err := pgStore.GetDRepVotingPower(0, drepCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1200), singlePower)
}

// TestPostgresGetDRepVotingPowerCrossTagIsolation verifies that key-hash and
// script-hash DReps sharing the same 28-byte credential hash are treated as
// independent identities. GetDRepVotingPower and GetDRepVotingPowerBatch must
// only aggregate stake delegated to the exact (tag, hash) pair requested.
func TestPostgresGetDRepVotingPowerCrossTagIsolation(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck
	cleanupDRepVotingPowerTestData(t, pgStore)

	sharedHash := bytes.Repeat([]byte{0x02}, 28)

	require.NoError(t, pgStore.SetDrep(0, sharedHash, 1000, "", nil, true, nil))
	require.NoError(t, pgStore.SetDrep(1, sharedHash, 1000, "", nil, true, nil))

	keyStake := bytes.Repeat([]byte{0xA1}, 28)
	require.NoError(t, pgStore.DB().Create(&models.Account{
		StakingKey:    keyStake,
		CredentialTag: 0,
		Drep:          sharedHash,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        types.Uint64(100),
		Active:        true,
		AddedSlot:     1000,
	}).Error)

	scriptStake := bytes.Repeat([]byte{0xA2}, 28)
	require.NoError(t, pgStore.DB().Create(&models.Account{
		StakingKey:    scriptStake,
		CredentialTag: 1,
		Drep:          sharedHash,
		DrepType:      models.DrepTypeScriptHash,
		Reward:        types.Uint64(200),
		Active:        true,
		AddedSlot:     1000,
	}).Error)

	keyPower, err := pgStore.GetDRepVotingPower(0, sharedHash, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), keyPower, "key-hash DRep power should be 100")

	scriptPower, err := pgStore.GetDRepVotingPower(1, sharedHash, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), scriptPower, "script-hash DRep power should be 200")

	powers, err := pgStore.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{
			{Tag: 0, Key: sharedHash},
			{Tag: 1, Key: sharedHash},
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), powers[models.StakeCredentialRef{Tag: 0, Key: sharedHash}.MapKey()],
		"batch: key-hash DRep power should be 100")
	assert.Equal(t, uint64(200), powers[models.StakeCredentialRef{Tag: 1, Key: sharedHash}.MapKey()],
		"batch: script-hash DRep power should be 200")
}

// TestPostgresRollbackRewardDeltaIsCredentialTagAware verifies that
// DeleteAccountRewardsAfterSlot matches AccountRewardDelta rows to accounts
// by the composite (credential_tag, staking_key) key. A credit delta for
// tag=1 must adjust only the script-hash account, not the key-hash account
// that shares the same 28-byte hash.
func TestPostgresRollbackRewardDeltaIsCredentialTagAware(t *testing.T) {
	pgStore := newTestPostgresStore(t)
	defer pgStore.Close() //nolint:errcheck

	stakeKey := bytes.Repeat([]byte{0xD1}, 28)
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
	require.NoError(t, pgStore.DB().Create(keyAccount).Error)
	require.NoError(t, pgStore.DB().Create(scriptAccount).Error)

	require.NoError(t, pgStore.AddAccountRewardByCredential(1, stakeKey, creditAmount, slot, nil))

	keyBefore, err := pgStore.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward, keyBefore.Reward, "key-hash account must not be affected by script-hash credit")

	scriptBefore, err := pgStore.GetAccountByCredential(1, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward+types.Uint64(creditAmount), scriptBefore.Reward, "script-hash account must have received credit")

	require.NoError(t, pgStore.DeleteAccountRewardsAfterSlot(slot-1, nil))

	keyAfter, err := pgStore.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward, keyAfter.Reward, "key-hash account reward must be unchanged after rollback")

	scriptAfter, err := pgStore.GetAccountByCredential(1, stakeKey, true, nil)
	require.NoError(t, err)
	require.Equal(t, baseReward, scriptAfter.Reward, "script-hash account reward must be restored to base after rollback")
}
