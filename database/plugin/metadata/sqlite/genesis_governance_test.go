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
