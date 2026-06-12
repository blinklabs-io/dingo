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
	"github.com/blinklabs-io/gouroboros/cbor"
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

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
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
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
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

func TestProcessEpochReturnsMissingRewardAccountRefundToTreasury(
	t *testing.T,
) {
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
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

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

	active, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, active, "refund must not create a reward account")
	account, err := store.GetAccountByCredential(0, stakeCred, true, nil)
	require.NoError(t, err)
	assert.Nil(t, account)
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(107), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))

	proposal, err := db.GetGovernanceProposal(txHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, proposal.ExpiredEpoch)
	require.NotNil(t, proposal.ExpiredSlot)
	assert.Equal(t, uint64(5), *proposal.ExpiredEpoch)
	assert.Equal(t, uint64(500), *proposal.ExpiredSlot)
}

func TestProcessEpochUnclaimedDepositDoesNotIncreaseWithdrawalCapacity(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	missingStakeCred := testBytes(28, 5)
	missingReturnAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		missingStakeCred,
	)
	require.NoError(t, err)
	missingReturnAddrBytes, err := missingReturnAddr.Bytes()
	require.NoError(t, err)

	withdrawStakeCred := testBytes(28, 6)
	withdrawAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		withdrawStakeCred,
	)
	require.NoError(t, err)
	withdrawAddrBytes, err := withdrawAddr.Bytes()
	require.NoError(t, err)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: withdrawStakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	constitutionAction := &lcommon.NewConstitutionGovAction{Type: 5}
	constitutionAction.Constitution.Anchor.Url =
		"https://example.invalid/constitution"
	copy(
		constitutionAction.Constitution.Anchor.DataHash[:],
		testBytes(32, 7),
	)
	constitutionCbor, err := cbor.Encode(constitutionAction)
	require.NoError(t, err)
	withdrawalCbor, err := cbor.Encode(
		&lcommon.TreasuryWithdrawalGovAction{
			Type: 2,
			Withdrawals: map[*lcommon.Address]uint64{
				&withdrawAddr: 120,
			},
		},
	)
	require.NoError(t, err)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 8),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeNewConstitution),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/proposal-a",
		AnchorHash:    testBytes(32, 9),
		Deposit:       50,
		ReturnAddress: missingReturnAddrBytes,
		GovActionCbor: constitutionCbor,
		AddedSlot:     100,
	}, nil))
	require.NoError(t, db.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        testBytes(32, 10),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: 3,
		ExpiresEpoch:  10,
		RatifiedEpoch: &ratifiedEpoch,
		RatifiedSlot:  &ratifiedSlot,
		AnchorURL:     "https://example.invalid/proposal-b",
		AnchorHash:    testBytes(32, 11),
		Deposit:       0,
		ReturnAddress: withdrawAddrBytes,
		GovActionCbor: withdrawalCbor,
		AddedSlot:     101,
	}, nil))
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	_, err = ProcessEpoch(&EpochInput{
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
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"exceeds tracked treasury withdrawal capacity 100",
	)
}

func TestRefundProposalDepositReturnsInactiveRewardAccountToTreasury(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 5)
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
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("staking_key = ?", stakeCred).
		Update("active", false).Error)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	err = refundProposalDeposit(db, nil, &models.GovernanceProposal{
		Deposit:       7,
		ReturnAddress: rewardAddrBytes,
	}, 123)
	require.NoError(t, err)

	active, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, active, "refund must not reactivate the reward account")
	account, err := store.GetAccountByCredential(0, stakeCred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.False(t, account.Active)
	assert.Equal(t, uint64(5), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(107), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
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
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
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
