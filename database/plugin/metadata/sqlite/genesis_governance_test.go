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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeCred(b byte) *lcommon.Credential {
	var h [28]byte
	for i := range h {
		h[i] = b
	}
	return &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.CredentialHash(h),
	}
}

func makePoolId(b byte) lcommon.PoolId {
	var p [28]byte
	for i := range p {
		p[i] = b
	}
	return lcommon.PoolId(p)
}

func TestSqliteSetGenesisGovernance(t *testing.T) {
	store := setupTestStore(t)

	drepCred := makeCred(0x11)
	anchor := &lcommon.GovAnchor{
		Url:      "https://example.com/drep.json",
		DataHash: [32]byte{0xab, 0xcd},
	}
	initialDReps := conway.ConwayGenesisInitialDReps{
		drepCred: conway.ConwayGenesisDRepState{
			Expiry:  100,
			Deposit: 500_000_000,
			Anchor:  anchor,
		},
	}

	stakeCred := makeCred(0x22)
	voteCred := makeCred(0x33)
	stakeVoteCred := makeCred(0x44)
	poolA := makePoolId(0xaa)
	poolB := makePoolId(0xbb)

	delegs := conway.ConwayGenesisDelegs{
		stakeCred: conway.ConwayGenesisDelegatee{
			Type:   conway.ConwayGenesisDelegateeTypeStake,
			PoolId: poolA,
		},
		voteCred: conway.ConwayGenesisDelegatee{
			Type: conway.ConwayGenesisDelegateeTypeVote,
			DRep: lcommon.Drep{
				Type:       lcommon.DrepTypeAddrKeyHash,
				Credential: drepCred.Credential[:],
			},
		},
		stakeVoteCred: conway.ConwayGenesisDelegatee{
			Type:   conway.ConwayGenesisDelegateeTypeStakeVote,
			PoolId: poolB,
			DRep: lcommon.Drep{
				Type: lcommon.DrepTypeAbstain,
			},
		},
	}

	require.NoError(t, store.SetGenesisGovernance(
		initialDReps,
		delegs,
		[]byte("genesis-hash"),
		nil,
	))

	// DRep row exists at slot 0 with anchor and expiry.
	var drep models.Drep
	require.NoError(
		t,
		store.DB().Where("credential = ?", drepCred.Credential[:]).
			First(&drep).Error,
	)
	assert.Equal(t, uint64(0), drep.AddedSlot)
	assert.Equal(t, uint64(100), drep.ExpiryEpoch)
	assert.Equal(t, "https://example.com/drep.json", drep.AnchorURL)
	assert.True(t, drep.Active)

	// Registration row recorded the deposit.
	var reg models.RegistrationDrep
	require.NoError(
		t,
		store.DB().Where("drep_credential = ?", drepCred.Credential[:]).
			First(&reg).Error,
	)
	assert.Equal(t, uint64(0), reg.AddedSlot)
	assert.Equal(t, types.Uint64(500_000_000), reg.DepositAmount)

	// Every genesis delegation account has a synthetic Registration
	// row at slot 0 so the rollback path treats it as registered.
	for _, cred := range [][]byte{
		stakeCred.Credential[:],
		voteCred.Credential[:],
		stakeVoteCred.Credential[:],
	} {
		var genReg models.Registration
		require.NoError(
			t,
			store.DB().Where(
				"staking_key = ? AND added_slot = ?", cred, uint64(0),
			).First(&genReg).Error,
			"missing genesis Registration for %x", cred,
		)
		assert.Equal(t, uint64(0), genReg.AddedSlot)
	}

	// Stake-only delegation: account.pool set, no DRep.
	var stakeAcct models.Account
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeCred.Credential[:]).
			First(&stakeAcct).Error,
	)
	assert.Equal(t, poolA[:], stakeAcct.Pool)
	assert.Nil(t, stakeAcct.Drep)
	var stakeRow models.StakeDelegation
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeCred.Credential[:]).
			First(&stakeRow).Error,
	)
	assert.Equal(t, poolA[:], stakeRow.PoolKeyHash)
	assert.Equal(t, uint64(0), stakeRow.AddedSlot)

	// Vote-only delegation: account.drep set, no pool.
	var voteAcct models.Account
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", voteCred.Credential[:]).
			First(&voteAcct).Error,
	)
	assert.Nil(t, voteAcct.Pool)
	assert.Equal(t, drepCred.Credential[:], voteAcct.Drep)
	assert.Equal(t, models.DrepTypeAddrKeyHash, voteAcct.DrepType)
	var voteRow models.VoteDelegation
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", voteCred.Credential[:]).
			First(&voteRow).Error,
	)
	assert.Equal(t, drepCred.Credential[:], voteRow.Drep)
	assert.Equal(t, uint64(0), voteRow.AddedSlot)

	// Combined stake+vote delegation, with an AlwaysAbstain predefined DRep
	// (no credential persisted).
	var stakeVoteAcct models.Account
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeVoteCred.Credential[:]).
			First(&stakeVoteAcct).Error,
	)
	assert.Equal(t, poolB[:], stakeVoteAcct.Pool)
	assert.Nil(t, stakeVoteAcct.Drep)
	assert.Equal(t, models.DrepTypeAlwaysAbstain, stakeVoteAcct.DrepType)
	var stakeVoteRow models.StakeVoteDelegation
	require.NoError(
		t,
		store.DB().Where("staking_key = ?", stakeVoteCred.Credential[:]).
			First(&stakeVoteRow).Error,
	)
	assert.Equal(t, poolB[:], stakeVoteRow.PoolKeyHash)
	assert.Nil(t, stakeVoteRow.Drep)
	assert.Equal(t, models.DrepTypeAlwaysAbstain, stakeVoteRow.DrepType)
}

func TestSqliteSetGenesisGovernanceIdempotent(t *testing.T) {
	store := setupTestStore(t)

	drepCred := makeCred(0x55)
	stakeCred := makeCred(0x66)
	poolA := makePoolId(0x77)

	initialDReps := conway.ConwayGenesisInitialDReps{
		drepCred: conway.ConwayGenesisDRepState{
			Expiry:  100,
			Deposit: 500_000_000,
		},
	}
	delegs := conway.ConwayGenesisDelegs{
		stakeCred: conway.ConwayGenesisDelegatee{
			Type:   conway.ConwayGenesisDelegateeTypeStake,
			PoolId: poolA,
		},
	}

	// Run twice — simulates a retry after partial genesis bootstrap.
	require.NoError(t,
		store.SetGenesisGovernance(initialDReps, delegs, nil, nil),
	)
	require.NoError(t,
		store.SetGenesisGovernance(initialDReps, delegs, nil, nil),
	)

	var drepCount, regDrepCount, accountCount, delegCount, regCount int64
	require.NoError(t,
		store.DB().Model(&models.Drep{}).Count(&drepCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.RegistrationDrep{}).Count(&regDrepCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.Account{}).Count(&accountCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.StakeDelegation{}).Count(&delegCount).Error,
	)
	require.NoError(t,
		store.DB().Model(&models.Registration{}).Count(&regCount).Error,
	)
	assert.Equal(t, int64(1), drepCount)
	assert.Equal(t, int64(1), regDrepCount)
	assert.Equal(t, int64(1), accountCount)
	assert.Equal(t, int64(1), delegCount)
	assert.Equal(t, int64(1), regCount)
}

func TestSqliteSetGenesisGovernanceEmpty(t *testing.T) {
	store := setupTestStore(t)
	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{},
		conway.ConwayGenesisDelegs{},
		nil,
		nil,
	))

	var count int64
	require.NoError(t, store.DB().Model(&models.Drep{}).Count(&count).Error)
	assert.Equal(t, int64(0), count)
	require.NoError(t, store.DB().Model(&models.Account{}).Count(&count).Error)
	assert.Equal(t, int64(0), count)
}

// TestSqliteRollbackPreservesGenesisDrep verifies that rolling back past
// an on-chain DRep update does not error out on a DRep that was created
// from Conway genesis. Before the fix, batchFetchDrepCerts INNER-JOINed
// the genesis registration_drep row (certificate_id=0) out of the cache,
// causing "DRep ... has no registration cert at or before slot ..." in
// Phase 2 of RestoreDrepStateAtSlot.
func TestSqliteRollbackPreservesGenesisDrep(t *testing.T) {
	store := setupTestStore(t)

	drepCred := makeCred(0x81)

	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{
			drepCred: conway.ConwayGenesisDRepState{
				Expiry:  500,
				Deposit: 500_000_000,
			},
		},
		conway.ConwayGenesisDelegs{},
		nil,
		nil,
	))

	// On-chain UpdateDrep at slot 300 mutates Drep.added_slot.
	require.NoError(t, createTestTransaction(store.DB(), 100, 300))
	updateCert := models.Certificate{
		TransactionID: 100,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeUpdateDrep),
		Slot:          300,
	}
	require.NoError(t, store.DB().Create(&updateCert).Error)
	require.NoError(t, store.DB().Create(&models.UpdateDrep{
		Credential:    drepCred.Credential[:],
		AnchorURL:     "https://example.com/updated",
		AnchorHash:    bytes.Repeat([]byte{0x99}, 32),
		AddedSlot:     300,
		CertificateID: updateCert.ID,
	}).Error)
	require.NoError(t,
		store.DB().Model(&models.Drep{}).
			Where("credential = ?", drepCred.Credential[:]).
			Updates(map[string]any{
				"anchor_url":  "https://example.com/updated",
				"anchor_hash": bytes.Repeat([]byte{0x99}, 32),
				"added_slot":  uint64(300),
			}).Error,
	)

	// Rollback to slot 200 must NOT error, and must keep the DRep
	// (with its genesis anchor restored, since the update is undone).
	require.NoError(t, store.RestoreDrepStateAtSlot(200, nil))

	restored, err := store.GetDrep(drepCred.Credential[:], true, nil)
	require.NoError(t, err)
	require.NotNil(t, restored, "genesis DRep must survive rollback")
	assert.True(t, restored.Active)
}

// TestSqliteRollbackPreservesGenesisVoteOnlyAccount verifies that an
// account created from a Conway-genesis vote delegation is NOT deleted
// when we roll back past a later on-chain VoteDelegation. Before the
// fix, batchFetchCerts INNER-JOINed the synthetic Registration row out,
// hasReg was false, and the account was deleted by Phase 1 of
// RestoreAccountStateAtSlot.
func TestSqliteRollbackPreservesGenesisVoteOnlyAccount(t *testing.T) {
	store := setupTestStore(t)

	drepCred := makeCred(0x82)
	stakeCred := makeCred(0x83)

	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{
			drepCred: conway.ConwayGenesisDRepState{Expiry: 500},
		},
		conway.ConwayGenesisDelegs{
			stakeCred: conway.ConwayGenesisDelegatee{
				Type: conway.ConwayGenesisDelegateeTypeVote,
				DRep: lcommon.Drep{
					Type:       lcommon.DrepTypeAddrKeyHash,
					Credential: drepCred.Credential[:],
				},
			},
		},
		nil,
		nil,
	))

	// On-chain VoteDelegation at slot 300 changes the account's DRep
	// to AlwaysAbstain and bumps Account.added_slot.
	require.NoError(t, createTestTransaction(store.DB(), 101, 300))
	delegCert := models.Certificate{
		TransactionID: 101,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeVoteDelegation),
		Slot:          300,
	}
	require.NoError(t, store.DB().Create(&delegCert).Error)
	require.NoError(t, store.DB().Create(&models.VoteDelegation{
		StakingKey:    stakeCred.Credential[:],
		Drep:          nil,
		DrepType:      models.DrepTypeAlwaysAbstain,
		AddedSlot:     300,
		CertificateID: delegCert.ID,
	}).Error)
	require.NoError(t,
		store.DB().Model(&models.Account{}).
			Where("staking_key = ?", stakeCred.Credential[:]).
			Updates(map[string]any{
				"drep":       nil,
				"drep_type":  models.DrepTypeAlwaysAbstain,
				"added_slot": uint64(300),
			}).Error,
	)

	// Rollback to slot 200: the genesis-rooted account must remain.
	require.NoError(t, store.RestoreAccountStateAtSlot(200, nil))

	restored, err := store.GetAccount(stakeCred.Credential[:], true, nil)
	require.NoError(t, err)
	require.NotNil(t,
		restored,
		"genesis-rooted vote account must not be deleted by rollback",
	)
	assert.Equal(t, drepCred.Credential[:], restored.Drep,
		"DRep must be restored to genesis state")
	assert.Equal(t, models.DrepTypeAddrKeyHash, restored.DrepType)
}

// TestSqliteRollbackPreservesGenesisStakeVoteAccount verifies that an
// account created from a Conway-genesis stake+vote delegation is NOT
// deleted when we roll back past a later on-chain StakeDelegation.
func TestSqliteRollbackPreservesGenesisStakeVoteAccount(t *testing.T) {
	store := setupTestStore(t)

	drepCred := makeCred(0x84)
	stakeCred := makeCred(0x85)
	poolGenesis := makePoolId(0x86)
	poolLater := makePoolId(0x87)

	require.NoError(t, store.SetGenesisGovernance(
		conway.ConwayGenesisInitialDReps{
			drepCred: conway.ConwayGenesisDRepState{Expiry: 500},
		},
		conway.ConwayGenesisDelegs{
			stakeCred: conway.ConwayGenesisDelegatee{
				Type:   conway.ConwayGenesisDelegateeTypeStakeVote,
				PoolId: poolGenesis,
				DRep: lcommon.Drep{
					Type:       lcommon.DrepTypeAddrKeyHash,
					Credential: drepCred.Credential[:],
				},
			},
		},
		nil,
		nil,
	))

	// On-chain StakeDelegation at slot 300 changes the account's pool.
	require.NoError(t, createTestTransaction(store.DB(), 102, 300))
	delegCert := models.Certificate{
		TransactionID: 102,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          300,
	}
	require.NoError(t, store.DB().Create(&delegCert).Error)
	require.NoError(t, store.DB().Create(&models.StakeDelegation{
		StakingKey:    stakeCred.Credential[:],
		PoolKeyHash:   poolLater[:],
		AddedSlot:     300,
		CertificateID: delegCert.ID,
	}).Error)
	require.NoError(t,
		store.DB().Model(&models.Account{}).
			Where("staking_key = ?", stakeCred.Credential[:]).
			Updates(map[string]any{
				"pool":       poolLater[:],
				"added_slot": uint64(300),
			}).Error,
	)

	// Rollback to slot 200: the genesis-rooted account must remain,
	// with both pool and DRep restored to their genesis values.
	require.NoError(t, store.RestoreAccountStateAtSlot(200, nil))

	restored, err := store.GetAccount(stakeCred.Credential[:], true, nil)
	require.NoError(t, err)
	require.NotNil(t,
		restored,
		"genesis-rooted stake/vote account must not be deleted",
	)
	assert.Equal(t, poolGenesis[:], restored.Pool,
		"pool must be restored to genesis value")
	assert.Equal(t, drepCred.Credential[:], restored.Drep,
		"DRep must be restored to genesis value")
	assert.Equal(t, models.DrepTypeAddrKeyHash, restored.DrepType)
}
