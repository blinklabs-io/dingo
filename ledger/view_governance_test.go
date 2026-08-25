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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func governanceTestView(
	t *testing.T,
	pparams lcommon.ProtocolParameters,
) (*LedgerView, *database.Database) {
	t.Helper()
	db := newTestDB(t)
	ls := &LedgerState{db: db, currentPParams: pparams}
	ls.publishSnapshotsLocked()
	return &LedgerView{ls: ls}, db
}

func governanceTestHash(seed byte) []byte {
	ret := make([]byte, len(lcommon.Blake2b256{}))
	ret[0] = seed
	return ret
}

func governanceTestID(seed byte, idx uint32) lcommon.GovActionId {
	var txID lcommon.Blake2b256
	copy(txID[:], governanceTestHash(seed))
	return lcommon.GovActionId{TransactionId: txID, GovActionIdx: idx}
}

func storeGovernanceTestProposal(
	t *testing.T,
	db *database.Database,
	proposal *models.GovernanceProposal,
	action lcommon.GovAction,
) {
	t.Helper()
	if proposal.AnchorHash == nil {
		proposal.AnchorHash = make([]byte, 32)
	}
	if proposal.ReturnAddress == nil {
		proposal.ReturnAddress = make([]byte, 29)
	}
	if action != nil {
		encoded, err := cbor.Encode(action)
		require.NoError(t, err)
		proposal.GovActionCbor = encoded
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
}

func hardForkGovernanceTestAction(
	t *testing.T,
	ancestor *lcommon.GovActionId,
	major uint,
	minor uint,
) *lcommon.HardForkInitiationGovAction {
	t.Helper()
	action, err := lcommon.NewHardForkInitiationGovAction(
		ancestor,
		major,
		minor,
	)
	require.NoError(t, err)
	return action
}

func governanceProposalTestTx(
	t *testing.T,
	action lcommon.GovAction,
) *conway.ConwayTransaction {
	t.Helper()
	wrapper, err := conway.NewConwayGovAction(action)
	require.NoError(t, err)
	return &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxProposalProcedures: []conway.ConwayProposalProcedure{{
				PPGovAction: wrapper,
			}},
		},
	}
}

func governanceVoteTestTx(
	id lcommon.GovActionId,
	voterType uint8,
) *conway.ConwayTransaction {
	voter := lcommon.Voter{Type: voterType}
	actionID := id
	return &conway.ConwayTransaction{
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{
				&voter: {
					&actionID: lcommon.VotingProcedure{
						Vote: lcommon.GovVoteYes,
					},
				},
			},
		},
	}
}

func TestLedgerViewGovPurposeRoots(t *testing.T) {
	lv, db := governanceTestView(t, &conway.ConwayProtocolParameters{})

	// A non-nil empty set is authoritative. Returning nil would make
	// gouroboros silently fall back to the weaker existence-only rule.
	roots, err := lv.GovPurposeRoots()
	require.NoError(t, err)
	require.NotNil(t, roots)
	require.Nil(t, roots.PParamUpdate)
	require.Nil(t, roots.HardFork)
	require.Nil(t, roots.Committee)
	require.Nil(t, roots.Constitution)

	enactedEpoch := uint64(20)
	storeRoot := func(
		seed byte,
		actionType lcommon.GovActionType,
		actionIndex uint32,
		enactedSlot uint64,
	) {
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:       governanceTestHash(seed),
			ActionIndex:  actionIndex,
			ActionType:   uint8(actionType),
			EnactedEpoch: &enactedEpoch,
			EnactedSlot:  &enactedSlot,
		}, nil)
	}
	storeRoot(0x11, lcommon.GovActionTypeParameterChange, 1, 100)
	storeRoot(0x12, lcommon.GovActionTypeHardForkInitiation, 2, 101)
	storeRoot(0x13, lcommon.GovActionTypeNoConfidence, 3, 102)
	storeRoot(0x14, lcommon.GovActionTypeUpdateCommittee, 4, 103)
	storeRoot(0x15, lcommon.GovActionTypeNewConstitution, 5, 104)

	roots, err = lv.GovPurposeRoots()
	require.NoError(t, err)
	require.NotNil(t, roots.PParamUpdate)
	require.NotNil(t, roots.HardFork)
	require.NotNil(t, roots.Committee)
	require.NotNil(t, roots.Constitution)
	require.Equal(t, governanceTestID(0x11, 1), *roots.PParamUpdate)
	require.Equal(t, governanceTestID(0x12, 2), *roots.HardFork)
	// NoConfidence and UpdateCommittee share a purpose; the latest enacted
	// member of the pair is the single committee root.
	require.Equal(t, governanceTestID(0x14, 4), *roots.Committee)
	require.Equal(t, governanceTestID(0x15, 5), *roots.Constitution)
}

func TestLedgerViewGovPurposeRootsPropagatesDatabaseError(t *testing.T) {
	lv, db := governanceTestView(t, &conway.ConwayProtocolParameters{})
	require.NoError(t, dbtest.CloseDatabase(db))

	roots, err := lv.GovPurposeRoots()
	require.Error(t, err)
	require.Nil(t, roots)
}

func TestLedgerViewGovernanceActionExpiryIsInclusive(t *testing.T) {
	pparams := &conway.ConwayProtocolParameters{}
	lv, db := governanceTestView(t, pparams)
	require.NoError(t, db.SetEpoch(
		1_000, 10, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	id := governanceTestID(0x21, 0)
	action := hardForkGovernanceTestAction(t, nil, 10, 0)
	storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
		TxHash:        id.TransactionId[:],
		ActionIndex:   id.GovActionIdx,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		ProposedEpoch: 10,
		ExpiresEpoch:  12,
	}, action)

	state, err := lv.GovActionById(id)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.True(t, lv.GovActionExists(id))
	// ExpiresEpoch is inclusive: epoch 12 ends at slot 1299.
	require.Equal(t, uint64(1_299), state.ExpirySlot)
	require.IsType(t, &lcommon.HardForkInitiationGovAction{}, state.Action)

	vote := governanceVoteTestTx(id, lcommon.VoterTypeDRepKeyHash)
	require.NoError(t, conway.UtxoValidateVotingOnExpiredGovAction(
		vote, 1_299, lv, pparams,
	))
	err = conway.UtxoValidateVotingOnExpiredGovAction(
		vote, 1_300, lv, pparams,
	)
	var expiryErr conway.VotingOnExpiredGovActionError
	require.ErrorAs(t, err, &expiryErr)
}

func TestLedgerViewGovernanceProposalAncestry(t *testing.T) {
	pparams := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionConway,
		},
	}
	lv, db := governanceTestView(t, pparams)
	require.NoError(t, db.SetEpoch(
		1_000, 10, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	enactedEpoch := uint64(10)
	rootID := governanceTestID(0x31, 0)
	oldRootID := governanceTestID(0x32, 0)
	pendingID := governanceTestID(0x33, 0)
	expiredID := governanceTestID(0x34, 0)
	for _, test := range []struct {
		id      lcommon.GovActionId
		expires uint64
		enacted *uint64
		slot    uint64
	}{
		{rootID, 12, &enactedEpoch, 200},
		{oldRootID, 12, &enactedEpoch, 100},
		{pendingID, 12, nil, 0},
		{expiredID, 10, nil, 0},
	} {
		var enactedSlot *uint64
		if test.enacted != nil {
			enactedSlot = &test.slot
		}
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:        test.id.TransactionId[:],
			ActionIndex:   test.id.GovActionIdx,
			ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
			ProposedEpoch: 10,
			ExpiresEpoch:  test.expires,
			EnactedEpoch:  test.enacted,
			EnactedSlot:   enactedSlot,
		}, hardForkGovernanceTestAction(t, nil, 10, 0))
	}

	for _, id := range []lcommon.GovActionId{rootID, pendingID} {
		tx := governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &id, 10, 1),
		)
		require.NoError(t, conway.UtxoValidateProposalAncestry(
			tx, 1_250, lv, pparams,
		))
	}
	for _, id := range []lcommon.GovActionId{oldRootID, expiredID} {
		tx := governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &id, 10, 1),
		)
		err := conway.UtxoValidateProposalAncestry(
			tx, 1_250, lv, pparams,
		)
		var ancestryErr conway.InvalidGovActionAncestorError
		require.ErrorAs(t, err, &ancestryErr)
	}
}

func TestLedgerViewGovernanceActionContentDrivesRules(t *testing.T) {
	t.Run("hard fork succession", func(t *testing.T) {
		pparams := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
		}
		lv, db := governanceTestView(t, pparams)
		require.NoError(t, db.SetEpoch(
			0, 0, nil, nil, nil, nil, 0, 1, 1_000, nil,
		))
		ancestorID := governanceTestID(0x41, 0)
		oldRootID := governanceTestID(0x40, 0)
		enactedEpoch := uint64(0)
		oldRootSlot := uint64(1)
		rootSlot := uint64(2)
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:       oldRootID.TransactionId[:],
			ActionType:   uint8(lcommon.GovActionTypeHardForkInitiation),
			EnactedEpoch: &enactedEpoch,
			EnactedSlot:  &oldRootSlot,
		}, hardForkGovernanceTestAction(t, nil, 9, 0))
		storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
			TxHash:       ancestorID.TransactionId[:],
			ActionType:   uint8(lcommon.GovActionTypeHardForkInitiation),
			EnactedEpoch: &enactedEpoch,
			EnactedSlot:  &rootSlot,
		}, hardForkGovernanceTestAction(t, nil, 10, 0))
		oldRoot, err := lv.GovActionById(oldRootID)
		require.NoError(t, err)
		require.Nil(t, oldRoot)
		root, err := lv.GovActionById(ancestorID)
		require.NoError(t, err)
		require.IsType(t, &lcommon.HardForkInitiationGovAction{}, root.Action)
		require.False(t, lv.GovActionExists(ancestorID))
		rootVote := governanceVoteTestTx(
			ancestorID,
			lcommon.VoterTypeDRepKeyHash,
		)
		err = conway.UtxoValidateUnknownGovActionIds(
			rootVote,
			1,
			lv,
			pparams,
		)
		var unknownActionErr conway.UnknownGovActionIdError
		require.ErrorAs(t, err, &unknownActionErr)

		tx := governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &ancestorID, 10, 1),
		)
		require.NoError(t, conway.UtxoValidateHardForkCanFollow(
			tx, 1, lv, pparams,
		))

		tx = governanceProposalTestTx(
			t,
			hardForkGovernanceTestAction(t, &ancestorID, 10, 2),
		)
		err = conway.UtxoValidateHardForkCanFollow(tx, 1, lv, pparams)
		var hardForkErr conway.BadHardForkProtocolVersionError
		require.ErrorAs(t, err, &hardForkErr)
	})

	t.Run("security group voting", func(t *testing.T) {
		pparams := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: lcommon.ProtocolVersionPlomin,
			},
		}
		lv, db := governanceTestView(t, pparams)
		require.NoError(t, db.SetEpoch(
			0, 0, nil, nil, nil, nil, 0, 1, 1_000, nil,
		))
		maxBlockBodySize := uint(90_112)
		keyDeposit := uint(2_000_000)
		for _, test := range []struct {
			id     lcommon.GovActionId
			update conway.ConwayProtocolParameterUpdate
			valid  bool
		}{
			{governanceTestID(0x42, 0), conway.ConwayProtocolParameterUpdate{
				MaxBlockBodySize: &maxBlockBodySize,
			}, true},
			{governanceTestID(0x43, 0), conway.ConwayProtocolParameterUpdate{
				KeyDeposit: &keyDeposit,
			}, false},
		} {
			action, err := conway.NewConwayParameterChangeGovAction(
				nil, test.update, nil,
			)
			require.NoError(t, err)
			storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
				TxHash:       test.id.TransactionId[:],
				ActionType:   uint8(lcommon.GovActionTypeParameterChange),
				ExpiresEpoch: 1,
			}, action)
			err = conway.UtxoValidateStakePoolVotingRestrictions(
				governanceVoteTestTx(
					test.id,
					lcommon.VoterTypeStakingPoolKeyHash,
				),
				1,
				lv,
				pparams,
			)
			if test.valid {
				require.NoError(t, err)
			} else {
				var restrictionErr conway.StakePoolVotingRestrictionError
				require.ErrorAs(t, err, &restrictionErr)
			}
		}
	})
}

func TestLedgerViewDecodesHistoricalParameterActionForCurrentEra(t *testing.T) {
	maxBlockBodySize := uint(90_112)
	action, err := conway.NewConwayParameterChangeGovAction(
		nil,
		conway.ConwayProtocolParameterUpdate{
			MaxBlockBodySize: &maxBlockBodySize,
		},
		nil,
	)
	require.NoError(t, err)

	for _, test := range []struct {
		name    string
		pparams lcommon.ProtocolParameters
		want    any
	}{
		{
			name:    "Conway",
			pparams: &conway.ConwayProtocolParameters{},
			want:    &conway.ConwayParameterChangeGovAction{},
		},
		{
			name: "Dijkstra",
			pparams: &gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: conway.ConwayProtocolParameters{},
			},
			want: &gdijkstra.DijkstraParameterChangeGovAction{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lv, db := governanceTestView(t, test.pparams)
			require.NoError(t, db.SetEpoch(
				0, 0, nil, nil, nil, nil, 0, 1, 100, nil,
			))
			id := governanceTestID(0x51, 0)
			storeGovernanceTestProposal(t, db, &models.GovernanceProposal{
				TxHash:       id.TransactionId[:],
				ActionType:   uint8(lcommon.GovActionTypeParameterChange),
				ExpiresEpoch: 1,
			}, action)
			state, err := lv.GovActionById(id)
			require.NoError(t, err)
			require.IsType(t, test.want, state.Action)
		})
	}
}
