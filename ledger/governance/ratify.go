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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

// bootstrapProtocolVersion is the highest protocol major version during
// the Conway bootstrap phase where governance voting thresholds are not
// yet enforced and any yes vote ratifies a proposal.
const bootstrapProtocolVersion = 9

// RatifyDecision holds the outcome of evaluating a proposal's tally
// against the thresholds defined in protocol parameters.
type RatifyDecision struct {
	Ratified      bool
	DRepApproved  bool
	SPOApproved   bool
	CCApproved    bool
	FailureReason string
}

// ShouldRatify evaluates whether the proposal should be ratified given
// its current tally, the protocol parameters, and the active state of
// DReps and CC. Follows CIP-1694 bootstrap and post-bootstrap logic.
//
// majorVersion is the current protocol major version; before and
// including version 9 (bootstrap), thresholds are effectively zero.
func ShouldRatify(
	tally *ProposalTally,
	pparams *conway.ConwayProtocolParameters,
	activeDRepCount int,
	activeCCCount int,
	ccQuorum *big.Rat,
	majorVersion uint,
	committeeNoConfidence bool,
) RatifyDecision {
	decision := RatifyDecision{}
	if tally == nil || pparams == nil {
		decision.FailureReason = "missing tally or pparams"
		return decision
	}
	actionType := lcommon.GovActionType(tally.ActionType)

	// Info actions are non-binding and cannot be enacted.
	if actionType == lcommon.GovActionTypeInfo {
		decision.FailureReason = "info actions cannot be ratified"
		return decision
	}

	// Bootstrap phase: any yes vote ratifies.
	if majorVersion <= bootstrapProtocolVersion {
		// #nosec G115 -- CCYesCount is bounded by CC size (< 100).
		yes := tally.DRepYesStake + tally.SPOYesStake +
			uint64(tally.CCYesCount)
		decision.Ratified = yes > 0
		decision.DRepApproved = true
		decision.SPOApproved = true
		decision.CCApproved = true
		if !decision.Ratified {
			decision.FailureReason = "bootstrap: no yes vote"
		}
		return decision
	}

	// DRep approval: trivially met when no DReps are registered, otherwise
	// requires yesStake/(yesStake+noStake) >= threshold.
	drepThreshold := getDRepThreshold(actionType, pparams, nil)
	if activeDRepCount == 0 || drepThreshold == nil ||
		drepThreshold.Sign() == 0 {
		decision.DRepApproved = true
	} else {
		ratio := tally.DRepYesRatio()
		decision.DRepApproved = ratio.Cmp(drepThreshold) >= 0
	}

	// SPO approval: some actions do not require SPO votes; for those,
	// threshold is nil and approval is automatic. Others require
	// yesStake/(yesStake+noStake) >= threshold of the pool snapshot.
	spoThreshold := getSPOThreshold(actionType, pparams)
	if spoThreshold == nil || spoThreshold.Sign() == 0 {
		decision.SPOApproved = true
	} else {
		ratio := tally.SPOYesRatio()
		decision.SPOApproved = ratio.Cmp(spoThreshold) >= 0
	}

	// CC approval: skipped entirely for NoConfidence and UpdateCommittee.
	// For other actions, require the CC to be seated (!noConfidence),
	// have voters, and meet quorum. If CC exists but no hot keys are
	// authorized (activeCCCount == 0), approval fails.
	if !needsCCApproval(actionType) {
		decision.CCApproved = true
	} else if committeeNoConfidence {
		decision.CCApproved = false
		decision.FailureReason = "cc in no-confidence state"
	} else if activeCCCount == 0 {
		decision.CCApproved = false
		decision.FailureReason = "cc has no authorized members"
	} else if ccQuorum == nil || ccQuorum.Sign() == 0 {
		decision.CCApproved = true
	} else {
		ratio := tally.CCYesRatio()
		decision.CCApproved = ratio.Cmp(ccQuorum) >= 0
	}

	decision.Ratified = decision.DRepApproved && decision.SPOApproved &&
		decision.CCApproved
	if !decision.Ratified && decision.FailureReason == "" {
		decision.FailureReason = "threshold not met"
	}
	return decision
}

// getDRepThreshold returns the DRep yes-ratio threshold for the action
// type, or nil if DReps do not vote on this action. For ParameterChange
// the threshold depends on which parameter groups are touched; paramUpdate
// may be nil when the caller just wants the most restrictive group.
func getDRepThreshold(
	actionType lcommon.GovActionType,
	pparams *conway.ConwayProtocolParameters,
	paramUpdate *conway.ConwayProtocolParameterUpdate,
) *big.Rat {
	t := &pparams.DRepVotingThresholds
	switch actionType {
	case lcommon.GovActionTypeNoConfidence:
		return rateToRat(t.CommitteeNoConfidence)
	case lcommon.GovActionTypeUpdateCommittee:
		return rateToRat(t.CommitteeNormal)
	case lcommon.GovActionTypeNewConstitution:
		return rateToRat(t.UpdateToConstitution)
	case lcommon.GovActionTypeHardForkInitiation:
		return rateToRat(t.HardForkInitiation)
	case lcommon.GovActionTypeTreasuryWithdrawal:
		return rateToRat(t.TreasuryWithdrawal)
	case lcommon.GovActionTypeParameterChange:
		return mostRestrictiveDRepParamThreshold(t, paramUpdate)
	case lcommon.GovActionTypeInfo:
		return nil
	default:
		return nil
	}
}

// getSPOThreshold returns the SPO yes-ratio threshold for the action
// type. SPOs vote on a limited subset of actions; nil means SPOs do
// not vote and approval is automatic.
func getSPOThreshold(
	actionType lcommon.GovActionType,
	pparams *conway.ConwayProtocolParameters,
) *big.Rat {
	t := &pparams.PoolVotingThresholds
	switch actionType {
	case lcommon.GovActionTypeNoConfidence:
		return rateToRat(t.CommitteeNoConfidence)
	case lcommon.GovActionTypeUpdateCommittee:
		return rateToRat(t.CommitteeNormal)
	case lcommon.GovActionTypeHardForkInitiation:
		return rateToRat(t.HardForkInitiation)
	case lcommon.GovActionTypeParameterChange:
		// Only the security parameter group triggers SPO votes.
		// We conservatively apply the security threshold; callers
		// that know the paramUpdate should further refine this.
		return rateToRat(t.PpSecurityGroup)
	case lcommon.GovActionTypeNewConstitution,
		lcommon.GovActionTypeTreasuryWithdrawal,
		lcommon.GovActionTypeInfo:
		return nil
	default:
		return nil
	}
}

// needsCCApproval returns true if the action type requires
// constitutional committee approval.
func needsCCApproval(actionType lcommon.GovActionType) bool {
	switch actionType {
	case lcommon.GovActionTypeNewConstitution,
		lcommon.GovActionTypeHardForkInitiation,
		lcommon.GovActionTypeParameterChange,
		lcommon.GovActionTypeTreasuryWithdrawal:
		return true
	case lcommon.GovActionTypeNoConfidence,
		lcommon.GovActionTypeUpdateCommittee,
		lcommon.GovActionTypeInfo:
		return false
	default:
		return false
	}
}

// mostRestrictiveDRepParamThreshold selects the maximum DRep threshold
// across all parameter groups touched by the update. If the update is
// nil, we return the PpGovGroup threshold as a conservative default.
func mostRestrictiveDRepParamThreshold(
	t *conway.DRepVotingThresholds,
	update *conway.ConwayProtocolParameterUpdate,
) *big.Rat {
	if update == nil {
		return rateToRat(t.PpGovGroup)
	}
	var best *big.Rat
	consider := func(r cbor.Rat) {
		candidate := rateToRat(r)
		if best == nil || candidate.Cmp(best) > 0 {
			best = candidate
		}
	}
	if touchesNetworkGroup(update) {
		consider(t.PpNetworkGroup)
	}
	if touchesEconomicGroup(update) {
		consider(t.PpEconomicGroup)
	}
	if touchesTechnicalGroup(update) {
		consider(t.PpTechnicalGroup)
	}
	if touchesGovGroup(update) {
		consider(t.PpGovGroup)
	}
	if best == nil {
		// Parameter change touches no known group; require gov
		// threshold as a safe default.
		return rateToRat(t.PpGovGroup)
	}
	return best
}

// touchesNetworkGroup covers max block body/tx/header size, max value
// size, collateral percentage, ex-unit limits, max collateral inputs.
func touchesNetworkGroup(u *conway.ConwayProtocolParameterUpdate) bool {
	return u.MaxBlockBodySize != nil ||
		u.MaxTxSize != nil ||
		u.MaxBlockHeaderSize != nil ||
		u.MaxValueSize != nil ||
		u.MaxTxExUnits != nil ||
		u.MaxBlockExUnits != nil ||
		u.MaxCollateralInputs != nil
}

// touchesEconomicGroup covers fees, deposits, minting, rewards,
// min pool cost, monetary policy.
func touchesEconomicGroup(u *conway.ConwayProtocolParameterUpdate) bool {
	return u.MinFeeA != nil ||
		u.MinFeeB != nil ||
		u.KeyDeposit != nil ||
		u.PoolDeposit != nil ||
		u.Rho != nil ||
		u.Tau != nil ||
		u.MinPoolCost != nil ||
		u.AdaPerUtxoByte != nil ||
		u.ExecutionCosts != nil ||
		u.MinFeeRefScriptCostPerByte != nil
}

// touchesTechnicalGroup covers technical parameters: nopt, a0, max epoch,
// collateral percentage, cost models, protocol version minor.
func touchesTechnicalGroup(u *conway.ConwayProtocolParameterUpdate) bool {
	return u.NOpt != nil ||
		u.A0 != nil ||
		u.MaxEpoch != nil ||
		u.CollateralPercentage != nil ||
		u.CostModels != nil
}

// touchesGovGroup covers governance-era parameters: voting thresholds,
// deposits, lifetimes, committee sizes.
func touchesGovGroup(u *conway.ConwayProtocolParameterUpdate) bool {
	return u.PoolVotingThresholds != nil ||
		u.DRepVotingThresholds != nil ||
		u.MinCommitteeSize != nil ||
		u.CommitteeTermLimit != nil ||
		u.GovActionValidityPeriod != nil ||
		u.GovActionDeposit != nil ||
		u.DRepDeposit != nil ||
		u.DRepInactivityPeriod != nil
}

// rateToRat converts the zero-value of cbor.Rat (Rat == nil) into a
// zero *big.Rat. A non-nil Rat is returned directly.
func rateToRat(r cbor.Rat) *big.Rat {
	if r.Rat == nil {
		return new(big.Rat)
	}
	return r.Rat
}

// validateParentChain verifies that the proposal's parent action ID
// matches the current root for its action type. Actions with no parent
// chain (Info, TreasuryWithdrawal) always pass. Returns true if the
// proposal is eligible for ratification based on its parent chain.
func validateParentChain(
	proposal *models.GovernanceProposal,
	currentRoot *models.GovernanceProposal,
) bool {
	actionType := lcommon.GovActionType(proposal.ActionType)
	switch actionType {
	case lcommon.GovActionTypeInfo,
		lcommon.GovActionTypeTreasuryWithdrawal:
		return true
	case lcommon.GovActionTypeParameterChange,
		lcommon.GovActionTypeHardForkInitiation,
		lcommon.GovActionTypeNoConfidence,
		lcommon.GovActionTypeUpdateCommittee,
		lcommon.GovActionTypeNewConstitution:
		// These action types chain through the purpose root; fall
		// through to the root-matching logic below.
	default:
		return false
	}

	// No current root: the proposal must also have no parent.
	if currentRoot == nil {
		return proposal.ParentTxHash == nil &&
			proposal.ParentActionIdx == nil
	}
	// Current root exists: proposal must reference it exactly.
	if proposal.ParentTxHash == nil || proposal.ParentActionIdx == nil {
		return false
	}
	if !bytesEqual(proposal.ParentTxHash, currentRoot.TxHash) {
		return false
	}
	return *proposal.ParentActionIdx == currentRoot.ActionIndex
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
