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
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessEpochChainedEnactmentAccountingAndRollback proves the deposit
// and treasury movements of a boundary that enacts a parent/child
// ParameterChange chain together with a TreasuryWithdrawal, and that a
// rollback restores every one of them. The reference (Conway EPOCH) enacts
// the chain and the withdrawal, removes the child's competing sibling with
// its whole subtree, and returns every removed or enacted deposit in that
// same tick.
func TestProcessEpochChainedEnactmentAccountingAndRollback(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	const (
		initialTreasury = uint64(10_000)
		reserves        = uint64(20)
		withdrawal      = uint64(1_000)
	)
	require.NoError(t, store.SetNetworkState(
		initialTreasury, reserves, 1, nil,
	))
	parentCred, parentAddr := chainTestRegisteredAccount(t, store, 0xB1)
	childCred, childAddr := chainTestRegisteredAccount(t, store, 0xB2)
	siblingCred, siblingAddr := chainTestRegisteredAccount(t, store, 0xB3)
	grandchildCred, grandchildAddr := chainTestRegisteredAccount(t, store, 0xB4)
	treasuryCred, treasuryAddr := chainTestRegisteredAccount(t, store, 0xB5)

	withdrawalAddr, err := lcommon.NewAddressFromBytes(treasuryAddr)
	require.NoError(t, err)
	withdrawalCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        uint(lcommon.GovActionTypeTreasuryWithdrawal),
		Withdrawals: map[*lcommon.Address]uint64{&withdrawalAddr: withdrawal},
	})
	require.NoError(t, err)

	parent := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x72), nil,
		400, 100, parentAddr, chainTestParameterChange(t, 61),
	)
	child := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x71), parent,
		400, 200, childAddr, chainTestParameterChange(t, 62),
	)
	sibling := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x70), parent,
		500, 400, siblingAddr, chainTestParameterChange(t, 63),
	)
	grandchild := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x6F), sibling,
		600, 800, grandchildAddr, chainTestParameterChange(t, 64),
	)
	treasury := chainTestProposal(
		lcommon.GovActionTypeTreasuryWithdrawal, testBytes(32, 0x6E), nil,
		700, 1_600, treasuryAddr, withdrawalCbor,
	)
	stored := chainTestStore(
		t, db, child, parent, sibling, grandchild, treasury,
	)
	seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)
	pparams := chainTestPParams()

	type accountingState struct {
		treasury, reserves                    uint64
		parent, child, sibling, grandchild, w uint64
	}
	read := func() accountingState {
		t.Helper()
		treasuryPot, reservesPot := chainTestTreasury(t, store)
		return accountingState{
			treasury:   treasuryPot,
			reserves:   reservesPot,
			parent:     chainTestReward(t, store, parentCred),
			child:      chainTestReward(t, store, childCred),
			sibling:    chainTestReward(t, store, siblingCred),
			grandchild: chainTestReward(t, store, grandchildCred),
			w:          chainTestReward(t, store, treasuryCred),
		}
	}
	untouched := accountingState{
		treasury: initialTreasury,
		reserves: reserves,
	}

	ratifyAndEnact := func() {
		t.Helper()
		out := chainTestRunEpoch(t, db, stabilityTestEpoch, pparams)
		require.Equal(t, 3, out.RatifiedCount)
		require.Equal(t, untouched, read(), "RATIFY moves no lovelace")
		for _, proposal := range []*models.GovernanceProposal{
			parent, child, treasury,
		} {
			require.NotNil(t, chainTestReload(t, db, proposal).RatifiedEpoch)
		}
		require.Nil(t, chainTestReload(t, db, sibling).RatifiedEpoch)
		require.Nil(t, chainTestReload(t, db, grandchild).RatifiedEpoch)

		enactOut := chainTestRunEpoch(t, db, stabilityTestEpoch+1, pparams)
		require.Equal(t, 3, enactOut.EnactedCount)
		require.Equal(t, 2, enactOut.OrphanedCount)
		require.Equal(
			t,
			newRat(62, 100),
			chainTestMotionNoConfidence(t, enactOut.UpdatedPParams),
		)
	}

	ratifyAndEnact()
	enacted := accountingState{
		treasury:   initialTreasury - withdrawal,
		reserves:   reserves,
		parent:     100,
		child:      200,
		sibling:    400,
		grandchild: 800,
		w:          1_600 + withdrawal,
	}
	assert.Equal(t, enacted, read())
	root := chainTestParameterRoot(t, db)
	require.NotNil(t, root)
	assert.Equal(t, child.TxHash, root.TxHash)
	for _, proposal := range []*models.GovernanceProposal{sibling, grandchild} {
		removed := chainTestReload(t, db, proposal)
		require.NotNil(t, removed.ExpiredEpoch)
		require.NotNil(t, removed.DroppedEpoch)
		assert.Equal(t, stabilityTestEpoch+1, *removed.DroppedEpoch)
	}

	// Roll back to just before the ENACT boundary.
	enactBoundary := (stabilityTestEpoch + 1) * 100
	require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(
		enactBoundary-1, nil,
	))
	require.NoError(t, db.DeleteAccountRewardsAfterSlot(enactBoundary-1, nil))
	require.NoError(t, db.DeleteNetworkStateAfterSlot(enactBoundary-1, nil))
	assert.Equal(t, untouched, read())
	assert.Nil(t, chainTestParameterRoot(t, db))
	for _, proposal := range []*models.GovernanceProposal{
		parent, child, treasury,
	} {
		restored := chainTestReload(t, db, proposal)
		assert.Nil(t, restored.EnactedEpoch)
		assert.NotNil(t, restored.RatifiedEpoch)
	}
	for _, proposal := range []*models.GovernanceProposal{sibling, grandchild} {
		restored := chainTestReload(t, db, proposal)
		assert.Nil(t, restored.ExpiredEpoch)
		assert.Nil(t, restored.DroppedEpoch)
	}

	// Roll back past the RATIFY boundary as well, then replay both
	// boundaries: the outcome and every lovelace movement repeat exactly.
	require.NoError(t, db.DeleteGovernanceProposalsAfterSlot(
		stabilityTestEpoch*100-1, nil,
	))
	for _, proposal := range []*models.GovernanceProposal{
		parent, child, treasury,
	} {
		assert.Nil(t, chainTestReload(t, db, proposal).RatifiedEpoch)
	}
	ratifyAndEnact()
	assert.Equal(t, enacted, read())
	root = chainTestParameterRoot(t, db)
	require.NotNil(t, root)
	assert.Equal(t, child.TxHash, root.TxHash)
}
