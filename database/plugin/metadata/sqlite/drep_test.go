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
	"bytes"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqliteGetDRepVotingPowerIncludesReward(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := []byte("drep_reward_only_123456789012345678901234")
	rewardOnlyStake := []byte("stake_reward_only_123456789012345678901")
	multiUtxoStake := []byte("stake_multi_utxo_123456789012345678901234")

	require.NoError(t, store.SetDrep(0, drepCred, 1000, "", nil, true, nil))
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

	power, err := store.GetDRepVotingPower(0, drepCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(1700), power)
}

func TestSqliteGetDRepVotingPowerBatchIncludesReward(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	rewardOnlyCred := []byte("drep_reward_only_22345678901234567890123")
	rewardOnlyStake := []byte("stake_reward_only_2234567890123456789012")
	multiUtxoCred := []byte("drep_multi_utxo_1234567890123456789012345")
	multiUtxoStake := []byte("stake_multi_utxo_223456789012345678901234")

	require.NoError(t, store.SetDrep(0, rewardOnlyCred, 1000, "", nil, true, nil))
	require.NoError(t, store.SetDrep(0, multiUtxoCred, 1000, "", nil, true, nil))
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
		[]models.StakeCredentialRef{
			{Tag: 0, Key: rewardOnlyCred},
			{Tag: 0, Key: multiUtxoCred},
		},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(700), powers[models.StakeCredentialRef{Tag: 0, Key: rewardOnlyCred}.MapKey()])
	assert.Equal(t, uint64(1000), powers[models.StakeCredentialRef{Tag: 0, Key: multiUtxoCred}.MapKey()])
}

func TestSqliteGetDRepVotingPowerBatchDoesNotMultiplyRewardAcrossUTxOs(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := []byte("drep_multi_reward_12345678901234567890123")
	stakeKey := []byte("stake_multi_reward_1234567890123456789012")

	require.NoError(t, store.SetDrep(0, drepCred, 1000, "", nil, true, nil))
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

	powers, err := store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{{Tag: 0, Key: drepCred}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(1200), powers[models.StakeCredentialRef{Tag: 0, Key: drepCred}.MapKey()])
}

func TestSqliteGetDRepVotingPowerBatchUsesStakeCredentialUtxoIndex(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := []byte("drep_index_plan_123456789012345678901234")
	otherDrepCred := []byte("drep_index_plan_other_123456789012345678")
	stakeKey := []byte("stake_index_plan_12345678901234567890123")

	require.NoError(t, store.SetDrep(0, drepCred, 1000, "", nil, true, nil))
	require.NoError(t, store.SetDrep(1, otherDrepCred, 1000, "", nil, true, nil))
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Drep:          drepCred,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        700,
		Active:        true,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 1,
		Drep:          otherDrepCred,
		DrepType:      models.DrepTypeScriptHash,
		Reward:        1100,
		Active:        true,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          []byte("tx_drep_index_plan_12345678901234567890"),
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Amount:        300,
		OutputIdx:     0,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          []byte("tx_drep_index_plan_other_1234567890123"),
		StakingKey:    stakeKey,
		CredentialTag: 1,
		Amount:        900,
		OutputIdx:     1,
		AddedSlot:     1000,
	}).Error)

	powers, err := store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{{Tag: 0, Key: drepCred}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		uint64(1000),
		powers[models.StakeCredentialRef{Tag: 0, Key: drepCred}.MapKey()],
	)
	require.Contains(t, getDRepVotingPowerBatchSQL, "INDEXED BY "+utxoStakingLiveAmountIndex)

	planRows, err := store.DB().
		Raw(
			"EXPLAIN QUERY PLAN "+getDRepVotingPowerBatchSQL,
			[][]byte{drepCred},
			[][]byte{drepCred},
		).Rows()
	require.NoError(t, err)
	defer planRows.Close()

	var details []string
	for planRows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, planRows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, planRows.Err())
	plan := strings.Join(details, "\n")
	assert.Contains(t, plan, utxoStakingLiveAmountIndex)
	assert.NotContains(t, plan, "idx_utxo_deleted_staking_amount")
}

// Voting power must aggregate UTxOs by the full stake credential identity.
// Same-hash key/script stake accounts must not share UTxO sums.
func TestSqliteGetDRepVotingPowerUsesStakeCredentialTag(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	drepCred := []byte("drep_same_stake_hash_123456789012345")
	stakeKey := []byte("same_stake_hash_123456789012345678")

	require.NoError(t, store.SetDrep(0, drepCred, 1000, "", nil, true, nil))
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Drep:          drepCred,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        10,
		Active:        true,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 1,
		Drep:          drepCred,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        20,
		Active:        true,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          []byte("tx_same_hash_key_12345678901234567890"),
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Amount:        100,
		OutputIdx:     0,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          []byte("tx_same_hash_script_12345678901234567"),
		StakingKey:    stakeKey,
		CredentialTag: 1,
		Amount:        200,
		OutputIdx:     1,
		AddedSlot:     1000,
	}).Error)

	power, err := store.GetDRepVotingPower(0, drepCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(330), power)

	powers, err := store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{{Tag: 0, Key: drepCred}},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(330), powers[models.StakeCredentialRef{Tag: 0, Key: drepCred}.MapKey()])
}

// Predefined DRep voting power uses account drep_type instead of DRep hash.
// It must still join UTxOs by (credential_tag, staking_key), not hash only.
func TestSqliteGetDRepVotingPowerByTypeUsesStakeCredentialTag(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	stakeKey := []byte("same_predefined_hash_1234567890123")

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		DrepType:      models.DrepTypeAlwaysAbstain,
		Reward:        10,
		Active:        true,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 1,
		DrepType:      models.DrepTypeAlwaysAbstain,
		Reward:        20,
		Active:        true,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          []byte("tx_predefined_key_123456789012345678"),
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Amount:        100,
		OutputIdx:     0,
		AddedSlot:     1000,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          []byte("tx_predefined_script_123456789012345"),
		StakingKey:    stakeKey,
		CredentialTag: 1,
		Amount:        200,
		OutputIdx:     1,
		AddedSlot:     1000,
	}).Error)

	powers, err := store.GetDRepVotingPowerByType(
		[]uint64{models.DrepTypeAlwaysAbstain},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(330), powers[models.DrepTypeAlwaysAbstain])
}

// TestSqliteGetDRepVotingPowerCrossTagIsolation verifies that key-hash and
// script-hash DReps sharing the same 28-byte credential hash are treated as
// independent identities. GetDRepVotingPower and GetDRepVotingPowerBatch must
// only aggregate stake delegated to the exact (tag, hash) pair requested.
func TestSqliteGetDRepVotingPowerCrossTagIsolation(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	// One 28-byte hash, two DRep identities.
	sharedHash := bytes.Repeat([]byte{0x01}, 28)

	// Register both DReps.
	require.NoError(t, store.SetDrep(0, sharedHash, 1000, "", nil, true, nil))
	require.NoError(t, store.SetDrep(1, sharedHash, 1000, "", nil, true, nil))

	// Account delegated to key-hash DRep (tag=0).
	keyStake := []byte("key_stake_123456789012345678901234567")
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    keyStake,
		CredentialTag: 0,
		Drep:          sharedHash,
		DrepType:      models.DrepTypeAddrKeyHash,
		Reward:        100,
		Active:        true,
		AddedSlot:     1000,
	}).Error)

	// Account delegated to script-hash DRep (tag=1).
	scriptStake := []byte("script_stake_1234567890123456789012345")
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    scriptStake,
		CredentialTag: 1,
		Drep:          sharedHash,
		DrepType:      models.DrepTypeScriptHash,
		Reward:        200,
		Active:        true,
		AddedSlot:     1000,
	}).Error)

	// GetDRepVotingPower for key-hash DRep must only see the key account (100).
	keyPower, err := store.GetDRepVotingPower(0, sharedHash, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), keyPower, "key-hash DRep power should be 100")

	// GetDRepVotingPower for script-hash DRep must only see the script account (200).
	scriptPower, err := store.GetDRepVotingPower(1, sharedHash, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), scriptPower, "script-hash DRep power should be 200")

	// GetDRepVotingPowerBatch must return correct isolated values for both.
	powers, err := store.GetDRepVotingPowerBatch(
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

func TestSqliteInsertDrepIfAbsentInsertsNewRow(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	cred := []byte("drep_insert_absent_1234567890123456789012")
	require.NoError(
		t,
		store.InsertDrepIfAbsent(0, cred, 1500, "", nil, true, nil),
	)

	drep, err := store.GetDrep(cred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.True(t, drep.Active)
	assert.Equal(t, uint64(1500), drep.AddedSlot)
	assert.Equal(t, "", drep.AnchorURL)
	assert.Nil(t, drep.AnchorHash)
}

func TestSqliteInsertDrepIfAbsentLeavesExistingRowUntouched(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	cred := []byte("drep_insert_absent_2234567890123456789012")
	anchorURL := "https://drep.example.com/metadata"
	anchorHash := []byte("anchor_hash_1234567890123456789012345678")
	require.NoError(
		t,
		store.SetDrep(0, cred, 1000, anchorURL, anchorHash, true, nil),
	)

	// Attempt repair with placeholder values at a later slot — must be a no-op.
	require.NoError(
		t,
		store.InsertDrepIfAbsent(0, cred, 9999, "", nil, true, nil),
	)

	drep, err := store.GetDrep(cred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.Equal(t, uint64(1000), drep.AddedSlot)
	assert.Equal(t, anchorURL, drep.AnchorURL)
	assert.Equal(t, anchorHash, drep.AnchorHash)
	assert.True(t, drep.Active)
}

// Import must treat credential_tag as part of the DRep identity.
// This keeps key-hash and script-hash DReps with the same hash as separate rows.
func TestSqliteImportDrepUsesCredentialTagInConflictKeys(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	cred := []byte("same_hash_drep_12345678901234")

	require.NoError(t, store.ImportDrep(
		&models.Drep{
			CredentialTag: 0,
			Credential:    cred,
			AddedSlot:     100,
			AnchorURL:     "https://key.example/drep.json",
			Active:        true,
		},
		&models.RegistrationDrep{
			CredentialTag:  0,
			DrepCredential: cred,
			AddedSlot:      100,
		},
		nil,
	))
	require.NoError(t, store.ImportDrep(
		&models.Drep{
			CredentialTag: 1,
			Credential:    cred,
			AddedSlot:     100,
			AnchorURL:     "https://script.example/drep.json",
			Active:        true,
		},
		&models.RegistrationDrep{
			CredentialTag:  1,
			DrepCredential: cred,
			AddedSlot:      100,
		},
		nil,
	))

	keyDrep, err := store.GetDrepByCredential(0, cred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, keyDrep)
	assert.Equal(t, "https://key.example/drep.json", keyDrep.AnchorURL)

	scriptDrep, err := store.GetDrepByCredential(1, cred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, scriptDrep)
	assert.Equal(t, "https://script.example/drep.json", scriptDrep.AnchorURL)

	var regCount int64
	require.NoError(t, store.DB().Model(&models.RegistrationDrep{}).
		Where("drep_credential = ? AND added_slot = ?", cred, 100).
		Count(&regCount).Error)
	assert.Equal(t, int64(2), regCount)
}
