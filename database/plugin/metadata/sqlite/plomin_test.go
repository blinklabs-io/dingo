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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

// Plomin (pv10) HARDFORK rule: cardano-ledger's
// Conway/Rules/HardFork.hs updateDRepDelegations.

// Account delegating to an active DRep has its delegation preserved; an
// account delegating to a credential that is not present in the drep
// table is cleared.
func TestClearDanglingDRepDelegations_ClearsUnregisteredCredBackedDelegation(t *testing.T) {
	store := setupTestDB(t)

	liveCred := bytes.Repeat([]byte{0x11}, 28)
	deadCred := bytes.Repeat([]byte{0x22}, 28)

	stakeKeyLive := bytes.Repeat([]byte{0xA1}, 28)
	stakeKeyDead := bytes.Repeat([]byte{0xA2}, 28)

	// Live DRep registered as active
	require.NoError(t, store.DB().Create(&models.Drep{
		Credential: liveCred,
		Active:     true,
		AddedSlot:  50,
	}).Error)

	// One account delegating to the live DRep (key cred = DrepType 0).
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKeyLive,
		Drep:       liveCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		Active:     true,
		AddedSlot:  100,
	}).Error)

	// One account delegating to a credential with NO drep row at all.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKeyDead,
		Drep:       deadCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		Active:     true,
		AddedSlot:  100,
	}).Error)

	n, err := store.ClearDanglingDRepDelegations(1_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"exactly one account has a dangling delegation and should be cleared")

	liveAcct, err := store.GetAccount(stakeKeyLive, true, nil)
	require.NoError(t, err)
	require.NotNil(t, liveAcct)
	assert.True(t, bytes.Equal(liveAcct.Drep, liveCred),
		"delegation to an active DRep must survive the rule")
	assert.Equal(t, models.DrepTypeAddrKeyHash, liveAcct.DrepType)
	assert.Equal(t, uint64(100), liveAcct.AddedSlot,
		"unchanged accounts keep their original AddedSlot")

	deadAcct, err := store.GetAccount(stakeKeyDead, true, nil)
	require.NoError(t, err)
	require.NotNil(t, deadAcct)
	assert.Nil(t, deadAcct.Drep,
		"dangling delegation credential must be cleared")
	assert.Equal(t, uint64(0), deadAcct.DrepType,
		"DrepType must be reset to the zero value")
	assert.Equal(t, uint64(1_000), deadAcct.AddedSlot,
		"AddedSlot must be bumped so rollback past the boundary re-derives the cert state")
}

// Inactive DReps (deregistered) are also not "live" and their delegators
// must have their delegations cleared.
func TestClearDanglingDRepDelegations_InactiveDRepCountsAsDangling(t *testing.T) {
	store := setupTestDB(t)

	inactiveCred := bytes.Repeat([]byte{0x33}, 28)
	stakeKey := bytes.Repeat([]byte{0xB1}, 28)

	// Note: models.Drep carries `gorm:"default:true"` on Active so a zero
	// value ("false") at Create time is silently rewritten to true by GORM.
	// Flip the column explicitly with a post-Create UPDATE.
	require.NoError(t, store.DB().Create(&models.Drep{
		Credential: inactiveCred,
		Active:     true,
		AddedSlot:  50,
	}).Error)
	require.NoError(t, store.DB().Model(&models.Drep{}).
		Where("credential = ?", inactiveCred).
		Update("active", false).Error)

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Drep:       inactiveCred,
		DrepType:   models.DrepTypeAddrKeyHash,
		Active:     true,
		AddedSlot:  100,
	}).Error)

	n, err := store.ClearDanglingDRepDelegations(2_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	acct, err := store.GetAccount(stakeKey, true, nil)
	require.NoError(t, err)
	assert.Nil(t, acct.Drep)
	assert.Equal(t, uint64(2_000), acct.AddedSlot)
}

// Script-credential delegations (DrepType 1) are treated the same way
// as key credentials.
func TestClearDanglingDRepDelegations_ScriptCredentialAlsoCleared(t *testing.T) {
	store := setupTestDB(t)

	deadScriptCred := bytes.Repeat([]byte{0x44}, 28)
	stakeKey := bytes.Repeat([]byte{0xC1}, 28)

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Drep:       deadScriptCred,
		DrepType:   models.DrepTypeScriptHash,
		Active:     true,
		AddedSlot:  100,
	}).Error)

	n, err := store.ClearDanglingDRepDelegations(1_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	acct, err := store.GetAccount(stakeKey, true, nil)
	require.NoError(t, err)
	assert.Nil(t, acct.Drep)
}

// AlwaysAbstain / AlwaysNoConfidence delegations are pseudo-DReps with no
// credential and must not be touched by the rule.
func TestClearDanglingDRepDelegations_PseudoDRepDelegationsPreserved(t *testing.T) {
	store := setupTestDB(t)

	stakeKeyA := bytes.Repeat([]byte{0xD1}, 28)
	stakeKeyB := bytes.Repeat([]byte{0xD2}, 28)

	// Pseudo-DRep delegations are stored with Drep = nil.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKeyA,
		Drep:       nil,
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     true,
		AddedSlot:  100,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKeyB,
		Drep:       nil,
		DrepType:   models.DrepTypeAlwaysNoConfidence,
		Active:     true,
		AddedSlot:  100,
	}).Error)

	n, err := store.ClearDanglingDRepDelegations(1_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n,
		"pseudo-DRep delegations must never be counted as dangling")

	a, err := store.GetAccount(stakeKeyA, true, nil)
	require.NoError(t, err)
	assert.Equal(t, models.DrepTypeAlwaysAbstain, a.DrepType)
	assert.Equal(t, uint64(100), a.AddedSlot, "AddedSlot untouched")

	b, err := store.GetAccount(stakeKeyB, true, nil)
	require.NoError(t, err)
	assert.Equal(t, models.DrepTypeAlwaysNoConfidence, b.DrepType)
	assert.Equal(t, uint64(100), b.AddedSlot)
}

// Accounts with no DRep delegation at all are not touched.
func TestClearDanglingDRepDelegations_NoDelegationUntouched(t *testing.T) {
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0xE1}, 28)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Pool:       bytes.Repeat([]byte{0xFA}, 28),
		Active:     true,
		AddedSlot:  100,
	}).Error)

	n, err := store.ClearDanglingDRepDelegations(1_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	acct, err := store.GetAccount(stakeKey, true, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), acct.AddedSlot)
}

// Idempotent: once dangling delegations have been cleared, a second call
// finds nothing to do.
func TestClearDanglingDRepDelegations_Idempotent(t *testing.T) {
	store := setupTestDB(t)

	dead := bytes.Repeat([]byte{0x55}, 28)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: bytes.Repeat([]byte{0xF1}, 28),
		Drep:       dead,
		DrepType:   models.DrepTypeAddrKeyHash,
		Active:     true,
		AddedSlot:  100,
	}).Error)

	n1, err := store.ClearDanglingDRepDelegations(1_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, n1)

	n2, err := store.ClearDanglingDRepDelegations(2_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, n2, "re-running after a clean rule-application is a no-op")
}
