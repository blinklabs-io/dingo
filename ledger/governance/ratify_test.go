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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/assert"
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

func TestShouldRatify_BootstrapAnyYesPasses(t *testing.T) {
	pparams := conwayPParamsFixture(9)
	tally := &ProposalTally{
		ActionType:     uint8(lcommon.GovActionTypeParameterChange),
		DRepYesStake:   1,
		DRepTotalStake: 100,
	}
	d := ShouldRatify(tally, pparams, 5, 0, nil, 9, false)
	assert.True(t, d.Ratified)
}

func TestShouldRatify_BootstrapZeroYesFails(t *testing.T) {
	pparams := conwayPParamsFixture(9)
	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeParameterChange),
	}
	d := ShouldRatify(tally, pparams, 5, 0, nil, 9, false)
	assert.False(t, d.Ratified)
}

func TestShouldRatify_InfoActionCannotRatify(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeInfo),
	}
	d := ShouldRatify(tally, pparams, 5, 5, big.NewRat(1, 2), 10, false)
	assert.False(t, d.Ratified)
}

func TestShouldRatify_DRepOnlyActionPasses(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	// TreasuryWithdrawal: CC-gated, no SPO. threshold 67/100.
	tally := &ProposalTally{
		ActionType:   uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake: 70,
		DRepNoStake:  30,
		CCYesCount:   4,
		CCNoCount:    1,
	}
	d := ShouldRatify(tally, pparams, 10, 5, big.NewRat(2, 3), 10, false)
	assert.True(t, d.Ratified)
}

func TestShouldRatify_DRepBelowThreshold(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:   uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake: 60,
		DRepNoStake:  40,
		CCYesCount:   4,
		CCNoCount:    1,
	}
	d := ShouldRatify(tally, pparams, 10, 5, big.NewRat(2, 3), 10, false)
	assert.False(t, d.Ratified)
	assert.False(t, d.DRepApproved)
}

func TestShouldRatify_NoActiveDRepsApproves(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType: uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		CCYesCount: 4,
		CCNoCount:  1,
	}
	// With no DReps registered, DRep approval is trivially met.
	d := ShouldRatify(tally, pparams, 0, 5, big.NewRat(2, 3), 10, false)
	assert.True(t, d.DRepApproved)
	assert.True(t, d.Ratified)
}

func TestShouldRatify_NoAuthorizedCCFails(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:   uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		DRepYesStake: 70,
		DRepNoStake:  30,
	}
	// CC exists but no hot key is authorized.
	d := ShouldRatify(tally, pparams, 10, 0, big.NewRat(2, 3), 10, false)
	assert.False(t, d.Ratified)
	assert.False(t, d.CCApproved)
}

func TestShouldRatify_NoConfidenceSkipsCCCheck(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:   uint8(lcommon.GovActionTypeNoConfidence),
		DRepYesStake: 70,
		DRepNoStake:  30,
		SPOYesStake:  60,
		SPONoStake:   40,
	}
	d := ShouldRatify(tally, pparams, 10, 0, big.NewRat(2, 3), 10, true)
	assert.True(t, d.CCApproved, "no confidence skips CC check")
	assert.True(t, d.Ratified)
}

func TestShouldRatify_HardForkRequiresAllThree(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	tally := &ProposalTally{
		ActionType:   uint8(lcommon.GovActionTypeHardForkInitiation),
		DRepYesStake: 70,
		DRepNoStake:  30,
		SPOYesStake:  60,
		SPONoStake:   40,
		CCYesCount:   4,
		CCNoCount:    1,
	}
	d := ShouldRatify(
		tally, pparams, 10, 5, big.NewRat(2, 3), 10, false,
	)
	assert.True(t, d.Ratified)

	// Drop SPO below threshold -> fail
	tally.SPOYesStake = 50
	tally.SPONoStake = 50
	d = ShouldRatify(
		tally, pparams, 10, 5, big.NewRat(2, 3), 10, false,
	)
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
		// Abstain must not count in the denominator.
		DRepAbstainStake: 1000,
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
		lcommon.GovActionTypeParameterChange, pparams, update,
	)
	// Gov group (75/100) > economic (67/100), so we expect 75/100.
	assert.Equal(t, big.NewRat(75, 100), got)
}

func TestGetDRepThreshold_NilUpdateFallsBackToGov(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	got := getDRepThreshold(
		lcommon.GovActionTypeParameterChange, pparams, nil,
	)
	assert.Equal(t, big.NewRat(75, 100), got)
}

func TestGetSPOThreshold_InfoReturnsNil(t *testing.T) {
	pparams := conwayPParamsFixture(10)
	got := getSPOThreshold(lcommon.GovActionTypeInfo, pparams)
	assert.Nil(t, got)
}
