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

package governance

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefundProposalDepositCreditsRewardAccount(t *testing.T) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 1)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}).Error)

	err = refundProposalDeposit(db, nil, &models.GovernanceProposal{
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}, 123)
	require.NoError(t, err)

	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(12), uint64(account.Reward))
}

func TestProcessEpochExpiresProposalAndRefundsDeposit(t *testing.T) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 2)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}).Error)
	txHash := testBytes(32, 3)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch: 1,
		ExpiresEpoch:  4,
		AnchorURL:     "https://example.invalid/expired",
		AnchorHash:    testBytes(32, 4),
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
		AddedSlot:     100,
	}, nil))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(
			pparams lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return pparams, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.ExpiredCount)
	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(12), uint64(account.Reward))

	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, proposal.ExpiredEpoch)
	require.NotNil(t, proposal.ExpiredSlot)
	assert.Equal(t, uint64(5), *proposal.ExpiredEpoch)
	assert.Equal(t, uint64(500), *proposal.ExpiredSlot)
}

func TestRewardCreditsRollbackBySlot(t *testing.T) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 1)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	rewardAddrBytes, err := rewardAddr.Bytes()
	require.NoError(t, err)

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}).Error)

	err = refundProposalDeposit(db, nil, &models.GovernanceProposal{
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}, 123)
	require.NoError(t, err)

	require.NoError(t, db.DeleteAccountRewardsAfterSlot(122, nil))
	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(5), uint64(account.Reward))
}

func TestCountActiveDRepsFiltersExpiredDReps(t *testing.T) {
	db, store := newTallyTestDB(t)
	require.NoError(t, store.DB().Create(&[]models.Drep{
		{
			Credential:  testBytes(28, 1),
			ExpiryEpoch: 0,
			Active:      true,
		},
		{
			Credential:  testBytes(28, 2),
			ExpiryEpoch: 10,
			Active:      true,
		},
		{
			Credential:  testBytes(28, 3),
			ExpiryEpoch: 11,
			Active:      true,
		},
	}).Error)

	count, err := countActiveDReps(db, nil, 10)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestCommitteeNoConfidenceStateUsesEnactedCommitteeRoot(t *testing.T) {
	assert.False(t, committeeNoConfidenceState(nil))
	assert.False(t, committeeNoConfidenceState(&models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeUpdateCommittee),
	}))
	assert.True(t, committeeNoConfidenceState(&models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeNoConfidence),
	}))
}
