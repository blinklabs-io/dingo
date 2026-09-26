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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chainTestPParams returns Conway parameters under which every action in
// these tests is accepted by DReps and SPOs without votes, so the committee
// (one seeded member voting Yes) and the RATIFY ordering rules decide the
// outcome.
func chainTestPParams() *conway.ConwayProtocolParameters {
	pparams := conwayPParamsFixture(10)
	pparams.MinCommitteeSize = 1
	zero := newRat(0, 1)
	pparams.DRepVotingThresholds = conway.DRepVotingThresholds{
		MotionNoConfidence:    zero,
		CommitteeNormal:       zero,
		CommitteeNoConfidence: zero,
		UpdateToConstitution:  zero,
		HardForkInitiation:    zero,
		PpNetworkGroup:        zero,
		PpEconomicGroup:       zero,
		PpTechnicalGroup:      zero,
		PpGovGroup:            zero,
		TreasuryWithdrawal:    zero,
	}
	pparams.PoolVotingThresholds = conway.PoolVotingThresholds{
		MotionNoConfidence:    zero,
		CommitteeNormal:       zero,
		CommitteeNoConfidence: zero,
		HardForkInitiation:    zero,
		PpSecurityGroup:       zero,
	}
	return pparams
}

// chainTestParameterChange encodes a governance-group parameter change that
// sets MotionNoConfidence to marker/100, so the enacted result identifies
// which proposal was applied last.
func chainTestParameterChange(t *testing.T, marker int64) []byte {
	t.Helper()
	thresholds := chainTestPParams().DRepVotingThresholds
	thresholds.MotionNoConfidence = newRat(marker, 100)
	encoded, err := cbor.Encode(&conway.ConwayParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			DRepVotingThresholds: &thresholds,
		},
	})
	require.NoError(t, err)
	return encoded
}

func chainTestProposal(
	actionType lcommon.GovActionType,
	txHash []byte,
	parent *models.GovernanceProposal,
	addedSlot uint64,
	deposit uint64,
	returnAddress []byte,
	actionCbor []byte,
) *models.GovernanceProposal {
	proposal := &models.GovernanceProposal{
		TxHash:        txHash,
		ActionType:    uint8(actionType),
		ProposedEpoch: stabilityTestEpoch - 1,
		ExpiresEpoch:  stabilityTestEpoch + 10,
		AnchorURL:     "https://example.invalid/chain",
		AnchorHash:    testBytes(32, 0xAA),
		Deposit:       deposit,
		ReturnAddress: returnAddress,
		GovActionCbor: actionCbor,
		AddedSlot:     addedSlot,
	}
	if parent != nil {
		parentIndex := parent.ActionIndex
		proposal.ParentTxHash = parent.TxHash
		proposal.ParentActionIdx = &parentIndex
	}
	return proposal
}

func chainTestRegisteredAccount(
	t *testing.T,
	store *tallyTestStore,
	seed byte,
) ([]byte, []byte) {
	t.Helper()
	stakeCred := testBytes(28, seed)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	return stakeCred, buildRewardAddr(t, stakeCred)
}

func chainTestReward(
	t *testing.T,
	store *tallyTestStore,
	stakeCred []byte,
) uint64 {
	t.Helper()
	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	return uint64(account.Reward)
}

func chainTestTreasury(t *testing.T, store *tallyTestStore) (uint64, uint64) {
	t.Helper()
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	return uint64(state.Treasury), uint64(state.Reserves)
}

func chainTestStore(
	t *testing.T,
	db *database.Database,
	proposals ...*models.GovernanceProposal,
) []*models.GovernanceProposal {
	t.Helper()
	loaded := make([]*models.GovernanceProposal, 0, len(proposals))
	for _, proposal := range proposals {
		require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	}
	for _, proposal := range proposals {
		stored, err := db.GetGovernanceProposal(
			proposal.TxHash, proposal.ActionIndex, nil,
		)
		require.NoError(t, err)
		loaded = append(loaded, stored)
	}
	return loaded
}

func chainTestRunEpoch(
	t *testing.T,
	db *database.Database,
	newEpoch uint64,
	pparams lcommon.ProtocolParameters,
) *EpochOutput {
	t.Helper()
	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    newEpoch - 1,
		NewEpoch:     newEpoch,
		BoundarySlot: newEpoch * 100,
		PParams:      pparams,
		UpdateFn:     eras.PParamsUpdateConway,
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())
	return out
}

func chainTestReload(
	t *testing.T,
	db *database.Database,
	proposal *models.GovernanceProposal,
) *models.GovernanceProposal {
	t.Helper()
	stored, err := db.GetGovernanceProposal(
		proposal.TxHash, proposal.ActionIndex, nil,
	)
	require.NoError(t, err)
	return stored
}

func chainTestParameterRoot(
	t *testing.T,
	db *database.Database,
) *models.GovernanceProposal {
	t.Helper()
	root, err := db.GetLastEnactedGovernanceProposal(
		[]uint8{uint8(lcommon.GovActionTypeParameterChange)}, nil,
	)
	require.NoError(t, err)
	return root
}

func chainTestMotionNoConfidence(
	t *testing.T,
	pparams lcommon.ProtocolParameters,
) cbor.Rat {
	t.Helper()
	conwayPParams, ok := pparams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	return conwayPParams.DRepVotingThresholds.MotionNoConfidence
}

// TestProcessEpochRatifiesSameSlotChildBeforeLaterSibling pins the
// submission-order half of parent-before-child ordering. Conway RATIFY walks
// proposals in insertion order (stably sorted by priority), and a child
// submitted in its parent's block precedes every proposal from a later block.
// Here the child sorts before its parent by hash at slot 400 and a competing
// sibling arrives at slot 500: the child must be evaluated immediately after
// its parent, so the child ratifies and the later sibling no longer matches
// the advanced root.
func TestProcessEpochRatifiesSameSlotChildBeforeLaterSibling(t *testing.T) {
	t.Parallel()

	db, store := newTallyTestDB(t)
	require.NoError(t, store.SetNetworkState(10, 20, 1, nil))
	parent := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x72), nil,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 61),
	)
	child := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x71), parent,
		400, 0, testBytes(29, 0), chainTestParameterChange(t, 62),
	)
	sibling := chainTestProposal(
		lcommon.GovActionTypeParameterChange, testBytes(32, 0x70), parent,
		500, 0, testBytes(29, 0), chainTestParameterChange(t, 63),
	)
	stored := chainTestStore(t, db, child, parent, sibling)
	seedHardForkCommitteeAndSPOVotes(t, db, store, stored...)

	pparams := chainTestPParams()
	out := chainTestRunEpoch(t, db, stabilityTestEpoch, pparams)
	assert.Equal(t, 2, out.RatifiedCount)
	assert.NotNil(t, chainTestReload(t, db, parent).RatifiedEpoch)
	assert.NotNil(
		t,
		chainTestReload(t, db, child).RatifiedEpoch,
		"a same-slot child is evaluated right after its parent",
	)
	assert.Nil(
		t,
		chainTestReload(t, db, sibling).RatifiedEpoch,
		"the later sibling no longer extends the advanced root",
	)

	enactOut := chainTestRunEpoch(t, db, stabilityTestEpoch+1, pparams)
	assert.Equal(t, 2, enactOut.EnactedCount)
	assert.Equal(t, 1, enactOut.OrphanedCount)
	assert.Equal(
		t,
		newRat(62, 100),
		chainTestMotionNoConfidence(t, enactOut.UpdatedPParams),
	)
	root := chainTestParameterRoot(t, db)
	require.NotNil(t, root)
	assert.Equal(t, child.TxHash, root.TxHash)
}
