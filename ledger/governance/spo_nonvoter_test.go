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
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	spoNonVoterYesStake    uint64 = 60
	spoNonVoterSilentStake uint64 = 40
)

type spoNonVoterRatificationCase struct {
	actionType           lcommon.GovActionType
	rewardDefault        uint64
	thresholdNumerator   int64
	thresholdDenominator int64
	silentVote           *uint8
}

func votePointer(vote uint8) *uint8 {
	return &vote
}

// TestProcessEpochSPONonVoterDenominators matches the same 60/40 stake
// distribution on both sides of the Conway bootstrap rule. A silent pool on a
// HardForkInitiation remains in the denominator, so 60% passes exactly 60% but
// not 61%. A silent pool on a bootstrap ParameterChange is Abstain, so the same
// explicit Yes stake passes both thresholds.
func TestProcessEpochSPONonVoterDenominators(t *testing.T) {
	testCases := []struct {
		name        string
		actionType  lcommon.GovActionType
		defaultDRep uint64
		numerator   int64
		denominator int64
		wantRatify  bool
	}{
		{
			name:        "hard fork at denominator equality",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			numerator:   3,
			denominator: 5,
			wantRatify:  true,
		},
		{
			name:        "hard fork above denominator ratio",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			numerator:   61,
			denominator: 100,
			wantRatify:  false,
		},
		{
			name:        "bootstrap parameter change at implicit-no ratio",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			numerator:   3,
			denominator: 5,
			wantRatify:  true,
		},
		{
			name:        "bootstrap parameter change above implicit-no ratio",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			numerator:   61,
			denominator: 100,
			wantRatify:  true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ratified := runSPONonVoterRatification(
				t,
				spoNonVoterRatificationCase{
					actionType:           testCase.actionType,
					rewardDefault:        testCase.defaultDRep,
					thresholdNumerator:   testCase.numerator,
					thresholdDenominator: testCase.denominator,
				},
			)
			assert.Equal(t, testCase.wantRatify, ratified)
		})
	}
}

// TestProcessEpochSPONonVoterRatification exercises the complete persisted
// RATIFY path and the explicit-vote controls. Reward-account defaults cannot
// turn a silent hard-fork pool into Abstain, while an explicit Abstain still
// does. Bootstrap makes a silent pool Abstain before reward defaults, while an
// explicit No still remains No.
func TestProcessEpochSPONonVoterRatification(t *testing.T) {
	testCases := []struct {
		name        string
		actionType  lcommon.GovActionType
		defaultDRep uint64
		silentVote  *uint8
		wantRatify  bool
	}{
		{
			name:        "hard fork silent pool is implicit no",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			wantRatify:  false,
		},
		{
			name:        "hard fork explicit abstain is excluded",
			actionType:  lcommon.GovActionTypeHardForkInitiation,
			defaultDRep: models.DrepTypeAlwaysAbstain,
			silentVote:  votePointer(models.VoteAbstain),
			wantRatify:  true,
		},
		{
			name:        "bootstrap parameter-change silent pool abstains",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			wantRatify:  true,
		},
		{
			name:        "bootstrap parameter-change explicit no remains no",
			actionType:  lcommon.GovActionTypeParameterChange,
			defaultDRep: models.DrepTypeAlwaysNoConfidence,
			silentVote:  votePointer(models.VoteNo),
			wantRatify:  false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ratified := runSPONonVoterRatification(
				t,
				spoNonVoterRatificationCase{
					actionType:           testCase.actionType,
					rewardDefault:        testCase.defaultDRep,
					thresholdNumerator:   3,
					thresholdDenominator: 4,
					silentVote:           testCase.silentVote,
				},
			)
			assert.Equal(t, testCase.wantRatify, ratified)
		})
	}
}

func runSPONonVoterRatification(
	t *testing.T,
	testCase spoNonVoterRatificationCase,
) bool {
	t.Helper()
	db, store := newTallyTestDB(t)
	proposal := seedSPONonVoterProposal(t, db, testCase.actionType)

	coldCredential := testBytes(28, 0xE1)
	hotCredential := testBytes(28, 0xE2)
	require.NoError(t, db.SetCommitteeMembers([]*models.CommitteeMember{{
		ColdCredHash: coldCredential,
		ExpiresEpoch: stabilityTestEpoch + 10,
		AddedSlot:    1,
	}}, nil))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldCredential,
		HotCredential:  hotCredential,
		CertificateID:  1,
		AddedSlot:      1,
	})

	yesPool := testBytes(28, 0xE3)
	silentPool := testBytes(28, 0xE4)
	snapshotEpoch := stakeEpochFor(stabilityTestEpoch)
	seedPoolWithStake(
		t, store, yesPool, testBytes(28, 0xE5), spoNonVoterYesStake,
		snapshotEpoch,
	)
	seedPoolWithStake(
		t, store, silentPool, testBytes(28, 0xE6), spoNonVoterSilentStake,
		snapshotEpoch,
	)
	seedRewardAccountDelegation(
		t, store, testBytes(28, 0xE6), nil, testCase.rewardDefault,
	)
	resolveSnapshotAutoVotes(t, db, snapshotEpoch)

	for _, vote := range []*models.GovernanceVote{
		{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeCC,
			VoterCredential: hotCredential,
			Vote:            models.VoteYes,
			AddedSlot:       2,
		},
		{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeSPO,
			VoterCredential: yesPool,
			Vote:            models.VoteYes,
			AddedSlot:       2,
		},
	} {
		require.NoError(t, db.SetGovernanceVote(vote, nil))
	}
	if testCase.silentVote != nil {
		require.NoError(t, db.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeSPO,
			VoterCredential: silentPool,
			Vote:            *testCase.silentVote,
			AddedSlot:       2,
		}, nil))
	}

	pparams := conwayPParamsFixture(bootstrapProtocolVersion)
	threshold := newRat(
		testCase.thresholdNumerator,
		testCase.thresholdDenominator,
	)
	switch testCase.actionType {
	case lcommon.GovActionTypeHardForkInitiation:
		pparams.PoolVotingThresholds.HardForkInitiation = threshold
	case lcommon.GovActionTypeParameterChange:
		pparams.PoolVotingThresholds.PpSecurityGroup = threshold
	default:
		t.Fatalf("unsupported governance action type %d", testCase.actionType)
	}

	txn := db.MetadataTxn(true)
	defer txn.Release()
	out, err := ProcessEpoch(&EpochInput{
		DB:           db,
		Txn:          txn,
		PrevEpoch:    stabilityTestEpoch - 1,
		NewEpoch:     stabilityTestEpoch,
		BoundarySlot: 500,
		PParams:      pparams,
		UpdateFn: func(
			parameters lcommon.ProtocolParameters,
			_ any,
		) (lcommon.ProtocolParameters, error) {
			return parameters, nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, txn.Commit())

	stored, err := db.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	ratified := out.RatifiedCount == 1
	assert.Equal(t, ratified, stored.RatifiedEpoch != nil)
	assert.Equal(t, ratified, stored.RatifiedSlot != nil)
	return ratified
}

func seedSPONonVoterProposal(
	t *testing.T,
	db *database.Database,
	actionType lcommon.GovActionType,
) *models.GovernanceProposal {
	t.Helper()
	returnAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		testBytes(28, 0xF8),
	)
	require.NoError(t, err)
	returnAddressBytes, err := returnAddress.Bytes()
	require.NoError(t, err)

	var actionCBOR []byte
	switch actionType {
	case lcommon.GovActionTypeHardForkInitiation:
		action := &lcommon.HardForkInitiationGovAction{
			Type: uint(lcommon.GovActionTypeHardForkInitiation),
		}
		action.ProtocolVersion.Major = bootstrapProtocolVersion + 1
		actionCBOR, err = cbor.Encode(action)
	case lcommon.GovActionTypeParameterChange:
		maxTxSize := uint(16_384)
		actionCBOR, err = cbor.Encode(&conway.ConwayParameterChangeGovAction{
			Type: uint(lcommon.GovActionTypeParameterChange),
			ParamUpdate: conway.ConwayProtocolParameterUpdate{
				MaxTxSize: &maxTxSize,
			},
		})
	default:
		t.Fatalf("unsupported governance action type %d", actionType)
	}
	require.NoError(t, err)

	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, byte(actionType)+0xF0),
		ActionIndex:   0,
		ActionType:    uint8(actionType),
		ProposedEpoch: stabilityTestEpoch - 1,
		ExpiresEpoch:  stabilityTestEpoch + 10,
		Deposit:       1_000,
		ReturnAddress: returnAddressBytes,
		AnchorURL:     "https://example.invalid/spo-nonvoter",
		AnchorHash:    testBytes(32, 0xF9),
		GovActionCbor: actionCBOR,
		AddedSlot:     1,
	}
	require.NoError(t, db.SetGovernanceProposal(proposal, nil))
	stored, err := db.GetGovernanceProposal(
		proposal.TxHash,
		proposal.ActionIndex,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	return stored
}
