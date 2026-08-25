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
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRat is a test helper that builds a cbor.Rat from num/denom.
func newRat(num, denom int64) cbor.Rat {
	return cbor.Rat{Rat: big.NewRat(num, denom)}
}

// conwayPParamsFixture returns a ConwayProtocolParameters with the
// threshold fields populated at realistic Conway values. Post-bootstrap
// (major >= 10) the thresholds must be met; bootstrap thresholds are 0.
func conwayPParamsFixture(major uint) *conway.ConwayProtocolParameters {
	p := &conway.ConwayProtocolParameters{}
	p.ProtocolVersion.Major = major
	p.MinCommitteeSize = 3
	p.DRepVotingThresholds = conway.DRepVotingThresholds{
		MotionNoConfidence:    newRat(67, 100),
		CommitteeNormal:       newRat(67, 100),
		CommitteeNoConfidence: newRat(60, 100),
		UpdateToConstitution:  newRat(75, 100),
		HardForkInitiation:    newRat(60, 100),
		PpNetworkGroup:        newRat(67, 100),
		PpEconomicGroup:       newRat(67, 100),
		PpTechnicalGroup:      newRat(67, 100),
		PpGovGroup:            newRat(75, 100),
		TreasuryWithdrawal:    newRat(67, 100),
	}
	p.PoolVotingThresholds = conway.PoolVotingThresholds{
		MotionNoConfidence:    newRat(51, 100),
		CommitteeNormal:       newRat(51, 100),
		CommitteeNoConfidence: newRat(51, 100),
		HardForkInitiation:    newRat(51, 100),
		PpSecurityGroup:       newRat(51, 100),
	}
	return p
}

// ratifyInputs is a small helper to reduce keyword-argument noise in the
// per-case tests below. The default MajorVersion is 10 (post-bootstrap).
func ratifyInputs(
	tally *ProposalTally,
	pparams *conway.ConwayProtocolParameters,
	activeDReps, activeCC int,
	ccQuorum *big.Rat,
	majorVersion uint,
	committeeNoConfidence bool,
) RatifyInputs {
	return RatifyInputs{
		Tally:                 tally,
		PParams:               pparams,
		ActiveDRepCount:       activeDReps,
		ActiveCCCount:         activeCC,
		CCQuorum:              ccQuorum,
		MajorVersion:          majorVersion,
		CommitteeNoConfidence: committeeNoConfidence,
	}
}

func TestShouldRatify_BootstrapDRepOnlyDoesNotSubstituteForSPO(t *testing.T) {
	pparams := conwayPParamsFixture(9)
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeHardForkInitiation),
		DRepYesStake:   1,
		DRepTotalStake: 100,
	}
	d := ShouldRatify(ratifyInputs(tally, pparams, 5, 1, big.NewRat(2, 3), 9, false))
	assert.False(t, d.Ratified)
}

func TestShouldRatify_BootstrapMissingCommitteeDoesNotRatify(t *testing.T) {
	pparams := conwayPParamsFixture(9)
	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeParameterChange),
	}
	// Non-security parameter changes have no SPO gate and bootstrap waives
	// the DRep threshold, but an absent committee still cannot approve a
	// committee-gated action.
	d := ShouldRatify(ratifyInputs(tally, pparams, 5, 0, nil, 9, false))
	assert.False(t, d.Ratified)
	assert.True(t, d.DRepApproved)
	assert.True(t, d.SPOApproved)
	assert.False(t, d.CCApproved)
}

func TestShouldRatify_BootstrapPerBodyRequirements(t *testing.T) {
	pparams := conwayPParamsFixture(9)
	quorum := big.NewRat(2, 3)
	securityValue := uint(1)
	securityUpdate := &conway.ConwayProtocolParameterUpdate{
		MaxTxSize: &securityValue,
	}

	base := func(action lcommon.GovActionType) *ProposalTally {
		return &ProposalTally{ActionType: uint8(action)}
	}
	withCC := func(tally *ProposalTally) *ProposalTally {
		tally.CCYesCount = 2
		tally.CCTotalCount = 3
		return tally
	}
	withSPO := func(tally *ProposalTally) *ProposalTally {
		tally.SPOYesStake = 51
		tally.SPOTotalStake = 100
		return tally
	}

	tests := []struct {
		name       string
		action     lcommon.GovActionType
		tally      *ProposalTally
		param      *conway.ConwayProtocolParameterUpdate
		activeCC   int
		wantRatify bool
	}{
		{
			name:       "parameter change uses CC only",
			action:     lcommon.GovActionTypeParameterChange,
			tally:      withCC(base(lcommon.GovActionTypeParameterChange)),
			activeCC:   2,
			wantRatify: true,
		},
		{
			name:       "security parameter change requires SPO",
			action:     lcommon.GovActionTypeParameterChange,
			tally:      withSPO(withCC(base(lcommon.GovActionTypeParameterChange))),
			param:      securityUpdate,
			activeCC:   2,
			wantRatify: true,
		},
		{
			name:       "hard fork requires CC and SPO",
			action:     lcommon.GovActionTypeHardForkInitiation,
			tally:      withSPO(withCC(base(lcommon.GovActionTypeHardForkInitiation))),
			activeCC:   2,
			wantRatify: true,
		},
		{
			name:       "hard fork CC only fails",
			action:     lcommon.GovActionTypeHardForkInitiation,
			tally:      withCC(base(lcommon.GovActionTypeHardForkInitiation)),
			activeCC:   2,
			wantRatify: false,
		},
		{
			name:       "hard fork SPO only fails",
			action:     lcommon.GovActionTypeHardForkInitiation,
			tally:      withSPO(base(lcommon.GovActionTypeHardForkInitiation)),
			activeCC:   2,
			wantRatify: false,
		},
		{
			name:   "hard fork DRep only fails",
			action: lcommon.GovActionTypeHardForkInitiation,
			tally: &ProposalTally{
				ActionType:     uint8(lcommon.GovActionTypeHardForkInitiation),
				DRepYesStake:   100,
				DRepTotalStake: 100,
			},
			activeCC:   2,
			wantRatify: false,
		},
		{
			name:       "info remains non-ratifiable",
			action:     lcommon.GovActionTypeInfo,
			tally:      withSPO(withCC(base(lcommon.GovActionTypeInfo))),
			activeCC:   2,
			wantRatify: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := ShouldRatify(RatifyInputs{
				Tally:           tt.tally,
				PParams:         pparams,
				ParamUpdate:     tt.param,
				ActiveCCCount:   tt.activeCC,
				CCQuorum:        quorum,
				MajorVersion:    9,
				ActiveDRepCount: 0,
			})
			assert.Equal(t, tt.wantRatify, d.Ratified)
		})
	}
}

func TestShouldRatify_BootstrapUnsupportedActionsDoNotRatify(t *testing.T) {
	pparams := conwayPParamsFixture(9)
	unsupported := []lcommon.GovActionType{
		lcommon.GovActionTypeTreasuryWithdrawal,
		lcommon.GovActionTypeNoConfidence,
		lcommon.GovActionTypeUpdateCommittee,
		lcommon.GovActionTypeNewConstitution,
	}
	for _, action := range unsupported {
		t.Run(fmt.Sprintf("action-%d", action), func(t *testing.T) {
			tally := &ProposalTally{
				ActionType:     uint8(action),
				DRepYesStake:   100,
				DRepTotalStake: 100,
				SPOYesStake:    100,
				SPOTotalStake:  100,
				CCYesCount:     3,
				CCTotalCount:   3,
			}
			d := ShouldRatify(RatifyInputs{
				Tally:         tally,
				PParams:       pparams,
				ActiveCCCount: 3,
				CCQuorum:      big.NewRat(2, 3),
				MajorVersion:  9,
			})
			assert.False(t, d.Ratified)
		})
	}
}

func TestShouldRatify_ActionBodyMatrixAcrossBootstrapBoundary(t *testing.T) {
	securityValue := uint(1)
	securityUpdate := &conway.ConwayProtocolParameterUpdate{
		MaxTxSize: &securityValue,
	}

	type body uint8
	const (
		drepBody body = 1 << iota
		spoBody
		ccBody
	)

	makeTally := func(action lcommon.GovActionType, votes body) *ProposalTally {
		tally := &ProposalTally{ActionType: uint8(action)}
		if votes&drepBody != 0 {
			tally.DRepYesStake, tally.DRepTotalStake = 100, 100
		}
		if votes&spoBody != 0 {
			tally.SPOYesStake, tally.SPOTotalStake = 100, 100
		}
		if votes&ccBody != 0 {
			tally.CCYesCount, tally.CCTotalCount = 2, 3
		}
		return tally
	}

	type actionCase struct {
		name      string
		action    lcommon.GovActionType
		param     *conway.ConwayProtocolParameterUpdate
		required  body
		bootstrap bool
	}

	bootstrapCases := []actionCase{
		{
			name:      "parameter change",
			action:    lcommon.GovActionTypeParameterChange,
			required:  ccBody,
			bootstrap: true,
		},
		{
			name:      "security parameter change",
			action:    lcommon.GovActionTypeParameterChange,
			param:     securityUpdate,
			required:  spoBody | ccBody,
			bootstrap: true,
		},
		{
			name:      "hard fork initiation",
			action:    lcommon.GovActionTypeHardForkInitiation,
			required:  spoBody | ccBody,
			bootstrap: true,
		},
		{
			name:      "info",
			action:    lcommon.GovActionTypeInfo,
			bootstrap: true,
		},
		{
			name:      "treasury withdrawal",
			action:    lcommon.GovActionTypeTreasuryWithdrawal,
			bootstrap: true,
		},
		{
			name:      "no confidence",
			action:    lcommon.GovActionTypeNoConfidence,
			bootstrap: true,
		},
		{
			name:      "update committee",
			action:    lcommon.GovActionTypeUpdateCommittee,
			bootstrap: true,
		},
		{
			name:      "new constitution",
			action:    lcommon.GovActionTypeNewConstitution,
			bootstrap: true,
		},
	}

	postBootstrapCases := []actionCase{
		{
			name:     "parameter change",
			action:   lcommon.GovActionTypeParameterChange,
			required: drepBody | ccBody,
		},
		{
			name:     "security parameter change",
			action:   lcommon.GovActionTypeParameterChange,
			param:    securityUpdate,
			required: drepBody | spoBody | ccBody,
		},
		{
			name:     "hard fork initiation",
			action:   lcommon.GovActionTypeHardForkInitiation,
			required: drepBody | spoBody | ccBody,
		},
		{
			name:     "treasury withdrawal",
			action:   lcommon.GovActionTypeTreasuryWithdrawal,
			required: drepBody | ccBody,
		},
		{
			name:     "no confidence",
			action:   lcommon.GovActionTypeNoConfidence,
			required: drepBody | spoBody,
		},
		{
			name:     "update committee",
			action:   lcommon.GovActionTypeUpdateCommittee,
			required: drepBody | spoBody,
		},
		{
			name:     "new constitution",
			action:   lcommon.GovActionTypeNewConstitution,
			required: drepBody | ccBody,
		},
		{
			name:   "info",
			action: lcommon.GovActionTypeInfo,
		},
	}

	for _, tc := range append(bootstrapCases, postBootstrapCases...) {
		major := uint(10)
		if tc.bootstrap {
			major = 9
		}
		pparams := conwayPParamsFixture(major)
		for _, votes := range []body{0, drepBody, spoBody, ccBody,
			drepBody | spoBody, drepBody | ccBody, spoBody | ccBody,
			drepBody | spoBody | ccBody} {
			name := fmt.Sprintf("PV%d/%s/votes-%d", major, tc.name, votes)
			t.Run(name, func(t *testing.T) {
				want := tc.required != 0 && votes&tc.required == tc.required
				if tc.bootstrap && tc.action != lcommon.GovActionTypeParameterChange &&
					tc.action != lcommon.GovActionTypeHardForkInitiation {
					want = false
				}
				if tc.action == lcommon.GovActionTypeInfo {
					want = false
				}
				d := ShouldRatify(RatifyInputs{
					Tally:         makeTally(tc.action, votes),
					PParams:       pparams,
					ParamUpdate:   tc.param,
					ActiveCCCount: 3,
					CCQuorum:      big.NewRat(2, 3),
					MajorVersion:  major,
				})
				assert.Equal(t, want, d.Ratified)
			})
		}
	}
}

func TestShouldRatify_BootstrapCCChecksRemainRequired(t *testing.T) {
	pparams := conwayPParamsFixture(9)
	tally := &ProposalTally{
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		SPOYesStake:   100,
		SPOTotalStake: 100,
		CCYesCount:    3,
		CCTotalCount:  3,
	}
	inputs := func(activeCC int, quorum *big.Rat) RatifyInputs {
		return RatifyInputs{
			Tally:         tally,
			PParams:       pparams,
			ActiveCCCount: activeCC,
			CCQuorum:      quorum,
			MajorVersion:  9,
		}
	}

	assert.False(t, ShouldRatify(inputs(0, nil)).Ratified,
		"bootstrap must not approve an action without a seated committee")
	assert.False(t, ShouldRatify(inputs(1, nil)).Ratified,
		"a seated committee still requires a quorum")
}

func TestShouldRatify_InfoActionCannotRatify(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeInfo),
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 5, 5, big.NewRat(1, 2), 10, false,
	))
	assert.False(t, d.Ratified)
}

func TestShouldRatify_DRepOnlyActionPasses(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// TreasuryWithdrawal: CC-gated, no SPO. threshold 67/100.
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake:   70,
		DRepNoStake:    30,
		DRepTotalStake: 100,
		CCYesCount:     4,
		CCNoCount:      1,
		CCTotalCount:   5,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 10, 5, big.NewRat(2, 3), 10, false,
	))
	assert.True(t, d.Ratified)
}

func TestShouldRatify_DRepBelowThreshold(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake:   60,
		DRepNoStake:    40,
		DRepTotalStake: 100,
		CCYesCount:     4,
		CCNoCount:      1,
		CCTotalCount:   5,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 10, 5, big.NewRat(2, 3), 10, false,
	))
	assert.False(t, d.Ratified)
	assert.False(t, d.DRepApproved)
}

func TestShouldRatify_NoActiveDRepsRejectsNonZeroThreshold(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:   uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		CCYesCount:   4,
		CCNoCount:    1,
		CCTotalCount: 5,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 0, 5, big.NewRat(2, 3), 10, false,
	))
	assert.False(t, d.DRepApproved)
	assert.False(t, d.Ratified)
}

func TestShouldRatify_NoActiveDRepsApprovesZeroThreshold(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	pparams.DRepVotingThresholds.TreasuryWithdrawal = newRat(0, 1)
	tally := &ProposalTally{
		ActionType:   uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		CCYesCount:   4,
		CCNoCount:    1,
		CCTotalCount: 5,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 0, 5, big.NewRat(2, 3), 10, false,
	))
	assert.True(t, d.DRepApproved)
	assert.True(t, d.Ratified)
}

func TestShouldRatify_NoAuthorizedCCFails(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	pparams.MinCommitteeSize = 0
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake:   70,
		DRepNoStake:    30,
		DRepTotalStake: 100,
	}
	// No active CC members (activeCC = 0).
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 10, 0, big.NewRat(2, 3), 10, false,
	))
	assert.False(t, d.Ratified)
	assert.False(t, d.CCApproved)
}

func TestShouldRatify_CCBelowMinimumSizeFails(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	pparams.MinCommitteeSize = 6
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake:   70,
		DRepNoStake:    30,
		DRepTotalStake: 100,
		CCYesCount:     5,
		CCTotalCount:   5,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 10, 5, big.NewRat(2, 3), 10, false,
	))
	assert.False(t, d.CCApproved)
	assert.False(t, d.Ratified)
	assert.Equal(t, "cc below minimum committee size", d.FailureReason)
}

func TestShouldRatify_NoConfidenceSkipsCCCheck(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeNoConfidence),
		DRepYesStake:   70,
		DRepNoStake:    30,
		DRepTotalStake: 100,
		SPOYesStake:    60,
		SPONoStake:     40,
		SPOTotalStake:  100,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 10, 0, big.NewRat(2, 3), 10, true,
	))
	assert.True(t, d.CCApproved, "no confidence skips CC check")
	assert.True(t, d.Ratified)
}

func TestShouldRatify_HardForkRequiresAllThree(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeHardForkInitiation),
		DRepYesStake:   70,
		DRepNoStake:    30,
		DRepTotalStake: 100,
		SPOYesStake:    60,
		SPONoStake:     40,
		SPOTotalStake:  100,
		CCYesCount:     4,
		CCNoCount:      1,
		CCTotalCount:   5,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 10, 5, big.NewRat(2, 3), 10, false,
	))
	assert.True(t, d.Ratified)

	// Drop SPO below threshold -> fail
	tally.SPOYesStake = 50
	tally.SPONoStake = 50
	d = ShouldRatify(ratifyInputs(
		tally, pparams, 10, 5, big.NewRat(2, 3), 10, false,
	))
	assert.False(t, d.Ratified)
	assert.False(t, d.SPOApproved)
}

func TestValidateParentChain_NoParentNoRoot(t *testing.T) {
	proposal := &models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeParameterChange),
	}
	assert.True(t, validateParentChain(proposal, nil))
}

func TestValidateParentChain_ParentRequiredButMissing(t *testing.T) {
	proposal := &models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeParameterChange),
	}
	root := &models.GovernanceProposal{
		TxHash:      []byte{1, 2, 3},
		ActionIndex: 0,
	}
	assert.False(t, validateParentChain(proposal, root))
}

func TestValidateParentChain_MatchingParent(t *testing.T) {
	idx := uint32(0)
	proposal := &models.GovernanceProposal{
		ActionType:      uint8(lcommon.GovActionTypeParameterChange),
		ParentTxHash:    []byte{1, 2, 3},
		ParentActionIdx: &idx,
	}
	root := &models.GovernanceProposal{
		TxHash:      []byte{1, 2, 3},
		ActionIndex: 0,
	}
	assert.True(t, validateParentChain(proposal, root))
}

func TestValidateParentChain_MismatchedParent(t *testing.T) {
	idx := uint32(0)
	proposal := &models.GovernanceProposal{
		ActionType:      uint8(lcommon.GovActionTypeParameterChange),
		ParentTxHash:    []byte{9, 9, 9},
		ParentActionIdx: &idx,
	}
	root := &models.GovernanceProposal{
		TxHash:      []byte{1, 2, 3},
		ActionIndex: 0,
	}
	assert.False(t, validateParentChain(proposal, root))
}

func TestValidateParentChain_TreasuryWithdrawalNoChain(t *testing.T) {
	proposal := &models.GovernanceProposal{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	}
	root := &models.GovernanceProposal{
		TxHash:      []byte{1, 2, 3},
		ActionIndex: 0,
	}
	// Treasury withdrawals don't chain, so any root state passes.
	assert.True(t, validateParentChain(proposal, root))
}

func TestProposalTally_DRepYesRatio(t *testing.T) {
	tally := &ProposalTally{
		DRepYesStake: 70,
		DRepNoStake:  30,
		// Abstain is excluded from the denominator; total includes it.
		DRepAbstainStake: 1000,
		DRepTotalStake:   1100,
	}
	ratio := tally.DRepYesRatio()
	assert.Equal(t, big.NewRat(7, 10), ratio)
}

func TestProposalTally_DRepYesRatioZeroParticipation(t *testing.T) {
	tally := &ProposalTally{}
	ratio := tally.DRepYesRatio()
	assert.Equal(t, 0, ratio.Sign())
}

func TestGetDRepThreshold_ParameterGroupSelection(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// Update touches both economic (MinFeeA) and gov (GovActionDeposit).
	fee := uint(100)
	deposit := uint64(500)
	update := &conway.ConwayProtocolParameterUpdate{
		MinFeeA:          &fee,
		GovActionDeposit: &deposit,
	}
	got := getDRepThreshold(
		lcommon.GovActionTypeParameterChange, pparams, update, false,
	)
	// Gov group (75/100) > economic (67/100), so we expect 75/100.
	assert.Equal(t, big.NewRat(75, 100), got)
}

func TestGetDRepThreshold_NilUpdateTakesMaxAcrossGroups(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// Make technical the strictest so we can observe max behaviour
	// rather than the fixture's implicit gov-is-highest ordering.
	pparams.DRepVotingThresholds.PpTechnicalGroup = newRat(90, 100)
	got := getDRepThreshold(
		lcommon.GovActionTypeParameterChange, pparams, nil, false,
	)
	assert.Equal(t, big.NewRat(90, 100), got)
}

func TestGetSPOThreshold_InfoReturnsNil(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	got := getSPOThreshold(lcommon.GovActionTypeInfo, pparams, nil, false)
	assert.Nil(t, got)
}

func TestGetSPOThreshold_NoConfidenceUsesMotionNoConfidence(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// Distinguish MotionNoConfidence from CommitteeNoConfidence so we
	// can assert the correct field is used for NoConfidence actions.
	pparams.PoolVotingThresholds.MotionNoConfidence = newRat(40, 100)
	pparams.PoolVotingThresholds.CommitteeNoConfidence = newRat(70, 100)
	got := getSPOThreshold(
		lcommon.GovActionTypeNoConfidence, pparams, nil, false,
	)
	assert.Equal(t, big.NewRat(40, 100), got)
}

func TestGetDRepThreshold_NoConfidenceUsesMotionNoConfidence(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	pparams.DRepVotingThresholds.MotionNoConfidence = newRat(40, 100)
	pparams.DRepVotingThresholds.CommitteeNoConfidence = newRat(90, 100)
	got := getDRepThreshold(
		lcommon.GovActionTypeNoConfidence, pparams, nil, false,
	)
	assert.Equal(t, big.NewRat(40, 100), got)
}

func TestGetSPOThreshold_ParameterChangeSecurityGroup(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// An update that touches only technical-group (A0) should not
	// trigger an SPO vote.
	a0 := newRat(1, 100)
	nonSecurity := &conway.ConwayProtocolParameterUpdate{A0: &a0}
	assert.Nil(t, getSPOThreshold(
		lcommon.GovActionTypeParameterChange, pparams, nonSecurity, false,
	))
	// An update that touches a security-group parameter (MaxTxSize)
	// must return the security-group threshold.
	maxTxSize := uint(16384)
	securityUpdate := &conway.ConwayProtocolParameterUpdate{
		MaxTxSize: &maxTxSize,
	}
	got := getSPOThreshold(
		lcommon.GovActionTypeParameterChange, pparams, securityUpdate, false,
	)
	assert.Equal(t, big.NewRat(51, 100), got)
	adaPerUtxoByte := uint64(4310)
	assert.Equal(t, big.NewRat(51, 100), getSPOThreshold(
		lcommon.GovActionTypeParameterChange,
		pparams,
		&conway.ConwayProtocolParameterUpdate{
			AdaPerUtxoByte: &adaPerUtxoByte,
		},
		false,
	))
	govActionDeposit := uint64(1000000000)
	assert.Equal(t, big.NewRat(51, 100), getSPOThreshold(
		lcommon.GovActionTypeParameterChange,
		pparams,
		&conway.ConwayProtocolParameterUpdate{
			GovActionDeposit: &govActionDeposit,
		},
		false,
	))
	// No update at all means the caller cannot prove security-group
	// involvement, so no SPO vote is required.
	assert.Nil(t, getSPOThreshold(
		lcommon.GovActionTypeParameterChange, pparams, nil, false,
	))
}

func TestGetDRepThreshold_UpdateCommitteeNoConfidenceState(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// Fixture sets CommitteeNormal=67/100, CommitteeNoConfidence=60/100.
	normal := getDRepThreshold(
		lcommon.GovActionTypeUpdateCommittee, pparams, nil, false,
	)
	assert.Equal(t, big.NewRat(67, 100), normal)
	noConf := getDRepThreshold(
		lcommon.GovActionTypeUpdateCommittee, pparams, nil, true,
	)
	assert.Equal(t, big.NewRat(60, 100), noConf)
}

func TestGetSPOThreshold_UpdateCommitteeNoConfidenceState(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// Distinguish the two thresholds so the switch is observable.
	pparams.PoolVotingThresholds.CommitteeNormal = newRat(51, 100)
	pparams.PoolVotingThresholds.CommitteeNoConfidence = newRat(65, 100)
	normal := getSPOThreshold(
		lcommon.GovActionTypeUpdateCommittee, pparams, nil, false,
	)
	assert.Equal(t, big.NewRat(51, 100), normal)
	noConf := getSPOThreshold(
		lcommon.GovActionTypeUpdateCommittee, pparams, nil, true,
	)
	assert.Equal(t, big.NewRat(65, 100), noConf)
}

func TestShouldRatify_CCQuorumMissingFailsSafe(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		// CC-gated action (TreasuryWithdrawal); passes DRep threshold
		// and CC has authorized members, but the caller supplied a
		// nil quorum. We must NOT silently approve.
		ActionType:     uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake:   70,
		DRepNoStake:    30,
		DRepTotalStake: 100,
		CCYesCount:     4,
		CCNoCount:      1,
		CCTotalCount:   5,
	}
	d := ShouldRatify(ratifyInputs(
		tally, pparams, 10, 5, nil, 10, false,
	))
	assert.False(t, d.CCApproved)
	assert.False(t, d.Ratified)
	assert.Equal(t, "cc quorum missing or zero", d.FailureReason)

	// Same, but with an explicit zero quorum.
	d = ShouldRatify(ratifyInputs(
		tally, pparams, 10, 5, big.NewRat(0, 1), 10, false,
	))
	assert.False(t, d.CCApproved)
	assert.False(t, d.Ratified)
}

func TestConwayRatifyQuorum_FromGenesis(t *testing.T) {
	threshold := cbor.Rat{Rat: big.NewRat(3, 5)}
	genesis := &conway.ConwayGenesis{
		Committee: conway.ConwayGenesisCommittee{
			Threshold: &threshold,
		},
	}
	got, err := conwayRatifyQuorum(nil, nil, nil, genesis)
	assert.NoError(t, err)
	assert.Equal(t, big.NewRat(3, 5), got)
}

func TestConwayRatifyQuorum_FallbackWhenGenesisNil(t *testing.T) {
	got, err := conwayRatifyQuorum(nil, nil, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, big.NewRat(2, 3), got)
}

func TestConwayRatifyQuorum_FallbackWhenThresholdMissing(t *testing.T) {
	genesis := &conway.ConwayGenesis{}
	got, err := conwayRatifyQuorum(nil, nil, nil, genesis)
	assert.NoError(t, err)
	assert.Equal(t, big.NewRat(2, 3), got)
}

func TestConwayRatifyQuorum_PrefersDBOverGenesis(t *testing.T) {
	db, _ := newTallyTestDB(t)

	// An enacted quorum must win over the Conway genesis default so
	// the ratify loop tracks on-chain state rather than the initial
	// bootstrap threshold.
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(3, 5), 1000, nil))

	threshold := cbor.Rat{Rat: big.NewRat(2, 3)}
	genesis := &conway.ConwayGenesis{
		Committee: conway.ConwayGenesisCommittee{
			Threshold: &threshold,
		},
	}
	got, err := conwayRatifyQuorum(nil, db, nil, genesis)
	require.NoError(t, err)
	assert.Equal(t, big.NewRat(3, 5), got)
}

func TestConwayRatifyQuorum_FallsBackToGenesisAfterClear(
	t *testing.T,
) {
	db, _ := newTallyTestDB(t)

	// Enact then immediately clear. Ratify must fall back to Conway
	// genesis until the next UpdateCommittee writes a new positive
	// quorum, matching the cardano-ledger ENACT semantics for
	// NoConfidence.
	require.NoError(t, db.SetCommitteeQuorum(big.NewRat(3, 5), 1000, nil))
	require.NoError(t, db.ClearCommitteeQuorum(2000, nil))

	// Pick a genesis threshold distinct from defaultCCQuorum (2/3) so
	// the assertion fails if the code path slipped to the last-resort
	// default instead of the genesis branch.
	threshold := cbor.Rat{Rat: big.NewRat(4, 7)}
	genesis := &conway.ConwayGenesis{
		Committee: conway.ConwayGenesisCommittee{
			Threshold: &threshold,
		},
	}
	got, err := conwayRatifyQuorum(nil, db, nil, genesis)
	require.NoError(t, err)
	assert.Equal(t, big.NewRat(4, 7), got)
}
