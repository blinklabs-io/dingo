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

// buildInfoProposal is a test helper that creates a GovernanceProposal with
// Info action CBOR set, ready for insertion via SetGovernanceProposal.
func buildInfoProposal(
	t *testing.T,
	txHash []byte,
	actionIndex uint32,
	expiresEpoch uint64,
	deposit uint64,
	returnAddress []byte,
	addedSlot uint64,
	parentTxHash []byte,
	parentActionIdx *uint32,
	ratifiedEpoch *uint64,
	ratifiedSlot *uint64,
) *models.GovernanceProposal {
	t.Helper()
	infoCbor, err := cbor.Encode(&lcommon.InfoGovAction{
		Type: uint(lcommon.GovActionTypeInfo),
	})
	require.NoError(t, err)
	return &models.GovernanceProposal{
		TxHash:          txHash,
		ActionIndex:     actionIndex,
		ActionType:      uint8(lcommon.GovActionTypeInfo),
		ProposedEpoch:   3,
		ExpiresEpoch:    expiresEpoch,
		AnchorURL:       "https://example.invalid/proposal",
		AnchorHash:      testBytes(32, txHash[0]),
		Deposit:         deposit,
		ReturnAddress:   returnAddress,
		GovActionCbor:   infoCbor,
		AddedSlot:       addedSlot,
		ParentTxHash:    parentTxHash,
		ParentActionIdx: parentActionIdx,
		RatifiedEpoch:   ratifiedEpoch,
		RatifiedSlot:    ratifiedSlot,
	}
}

// buildRewardAddr returns a reward address byte slice for the given stake
// credential for use in proposal return-address fields.
func buildRewardAddr(t *testing.T, stakeCred []byte) []byte {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	b, err := addr.Bytes()
	require.NoError(t, err)
	return b
}

// TestProcessEpochOrphanedChildRemovedAndRefunded verifies that when a
// ratified proposal is enacted, an active proposal that references it as
// parent is orphaned: its deposit is returned to the registered reward
// account and it is marked expired at the boundary slot.
func TestProcessEpochOrphanedChildRemovedAndRefunded(t *testing.T) {
	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 50)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 51)
	childHash := testBytes(32, 52)
	parentIdx := uint32(0)

	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, parentHash, 0, 10, 30, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, 12, 15, returnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.EnactedCount)
	assert.Equal(t, 1, out.OrphanedCount)

	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, child.ExpiredEpoch)
	require.NotNil(t, child.ExpiredSlot)
	assert.Equal(t, uint64(5), *child.ExpiredEpoch)
	assert.Equal(t, uint64(500), *child.ExpiredSlot)

	// Enacted parent's deposit (30) + orphaned child's deposit (15) = 45.
	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(45), uint64(account.Reward))
}

// TestProcessEpochOrphanedChildMissingReturnAccountGoesToTreasury checks
// that when an orphaned proposal's return reward account is not registered,
// its deposit is routed to the treasury rather than credited to the account.
func TestProcessEpochOrphanedChildMissingReturnAccountGoesToTreasury(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	parentStakeCred := testBytes(28, 53)
	parentReturnAddr := buildRewardAddr(t, parentStakeCred)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: parentStakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	missingStakeCred := testBytes(28, 54)
	missingReturnAddr := buildRewardAddr(t, missingStakeCred)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 55)
	childHash := testBytes(32, 56)
	parentIdx := uint32(0)

	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, parentHash, 0, 10, 30, parentReturnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, 12, 25, missingReturnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.OrphanedCount)

	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, child.ExpiredEpoch)

	missing, err := store.GetAccount(missingStakeCred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, missing, "orphan refund must not create a reward account")

	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(125), uint64(state.Treasury))
}

// TestProcessEpochTransitiveOrphanRemoval verifies that orphan sweeps
// cascade: when the direct child of an enacted proposal is orphaned, its own
// children are also swept and refunded in the same tick.
func TestProcessEpochTransitiveOrphanRemoval(t *testing.T) {
	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 57)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 58)
	childHash := testBytes(32, 59)
	grandchildHash := testBytes(32, 60)
	parentIdx := uint32(0)

	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, parentHash, 0, 10, 10, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, 12, 20, returnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, grandchildHash, 0, 14, 30, returnAddr, 102,
			childHash, &parentIdx, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.EnactedCount)
	assert.Equal(t, 2, out.OrphanedCount)

	for _, hash := range [][]byte{childHash, grandchildHash} {
		p, err := db.GetGovernanceProposal(hash, 0, nil)
		require.NoError(t, err)
		require.NotNil(t, p.ExpiredEpoch,
			"proposal %x should be orphaned", hash)
		assert.Equal(t, uint64(5), *p.ExpiredEpoch)
	}

	// Enacted parent's deposit (10) + orphaned child (20) + grandchild (30) = 60.
	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(60), uint64(account.Reward))
}

// TestProcessEpochOrphanExcludedFromActiveProposals verifies that after
// orphan removal, GetActiveGovernanceProposals no longer returns orphaned
// proposals (their expired_epoch field filters them out).
func TestProcessEpochOrphanExcludedFromActiveProposals(t *testing.T) {
	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 61)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 62)
	childHash := testBytes(32, 63)
	parentIdx := uint32(0)

	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, parentHash, 0, 10, 5, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, 12, 5, returnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	_, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	active, err := db.GetActiveGovernanceProposals(5, nil)
	require.NoError(t, err)
	for _, p := range active {
		assert.NotEqual(t, childHash, p.TxHash,
			"orphaned proposal must not appear in active pool")
	}
}

// TestProcessEpochOrphanedChildRestoredOnRollback verifies that rolling
// back to a slot before the boundary slot restores orphaned proposals
// (clears their expired_epoch/expired_slot) and reverses the reward credit.
func TestProcessEpochOrphanedChildRestoredOnRollback(t *testing.T) {
	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 64)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	ratifiedEpoch := uint64(4)
	ratifiedSlot := uint64(400)
	parentHash := testBytes(32, 65)
	childHash := testBytes(32, 66)
	parentIdx := uint32(0)

	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, parentHash, 0, 10, 10, returnAddr, 100,
			nil, nil, &ratifiedEpoch, &ratifiedSlot),
		nil,
	))
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, 12, 20, returnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	_, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, child.ExpiredEpoch, "child must be orphaned before rollback")

	require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(499, nil))
	require.NoError(t, db.DeleteAccountRewardsAfterSlot(499, nil))

	child, err = db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	assert.Nil(t, child.ExpiredEpoch, "orphaned status must be reversed by rollback")
	assert.Nil(t, child.ExpiredSlot)

	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(0), uint64(account.Reward),
		"reward credit must be reversed by rollback")
}

// TestProcessEpochOrphanAfterExpiry verifies that a proposal whose parent
// expires naturally at this epoch boundary is also orphaned and refunded.
func TestProcessEpochOrphanAfterExpiry(t *testing.T) {
	db, store := newTallyTestDB(t)

	stakeCred := testBytes(28, 67)
	returnAddr := buildRewardAddr(t, stakeCred)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}).Error)

	parentHash := testBytes(32, 68)
	childHash := testBytes(32, 69)
	parentIdx := uint32(0)

	// Parent expires at epoch 4 (ExpiresEpoch < NewEpoch=5).
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, parentHash, 0, 4, 10, returnAddr, 100,
			nil, nil, nil, nil),
		nil,
	))
	// Child expires at epoch 15 but references parent.
	require.NoError(t, db.SetGovernanceProposal(
		buildInfoProposal(t, childHash, 0, 15, 20, returnAddr, 101,
			parentHash, &parentIdx, nil, nil),
		nil,
	))

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    4,
		NewEpoch:     5,
		BoundarySlot: 500,
		PParams:      conwayPParamsFixture(10),
		UpdateFn: func(p lcommon.ProtocolParameters, _ any) (lcommon.ProtocolParameters, error) {
			return p, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	assert.Equal(t, 1, out.ExpiredCount)
	assert.Equal(t, 1, out.OrphanedCount)

	child, err := db.GetGovernanceProposal(childHash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, child.ExpiredEpoch)
	assert.Equal(t, uint64(5), *child.ExpiredEpoch)

	account, err := store.GetAccount(stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(30), uint64(account.Reward))
}
