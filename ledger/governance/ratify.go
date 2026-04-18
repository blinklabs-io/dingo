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
	"bytes"
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

// RatifyInputs is the decoded-action-aware shape callers pass to
// ShouldRatify. The paramUpdate pointer is consulted for ParameterChange
// proposals so DRep and SPO thresholds can be selected precisely.
type RatifyInputs struct {
	Tally                 *ProposalTally
	PParams               *conway.ConwayProtocolParameters
	ParamUpdate           *conway.ConwayProtocolParameterUpdate
	ActiveDRepCount       int // reserved for future min-DRep-count gate
	ActiveCCCount         int
	CCQuorum              *big.Rat
	MajorVersion          uint
	CommitteeNoConfidence bool
}

// ShouldRatify evaluates whether the proposal should be ratified given
// its current tally, the protocol parameters, and the active state of
// DReps and CC. Follows CIP-1694 bootstrap and post-bootstrap logic.
//
// MajorVersion is the current protocol major version; before and
// including version 9 (bootstrap), thresholds are effectively zero.
func ShouldRatify(in RatifyInputs) RatifyDecision {
	decision := RatifyDecision{}
	if in.Tally == nil || in.PParams == nil {
		decision.FailureReason = "missing tally or pparams"
		return decision
	}
	actionType := lcommon.GovActionType(in.Tally.ActionType)

	// Info actions are non-binding and cannot be enacted.
	if actionType == lcommon.GovActionTypeInfo {
		decision.FailureReason = "info actions cannot be ratified"
		return decision
	}

	// Bootstrap phase: any yes vote ratifies.
	if in.MajorVersion <= bootstrapProtocolVersion {
		// #nosec G115 -- CCYesCount is bounded by CC size (< 100).
		yes := in.Tally.DRepYesStake + in.Tally.SPOYesStake +
			uint64(in.Tally.CCYesCount)
		if yes == 0 {
			decision.FailureReason = "bootstrap: no yes vote"
			return decision
		}
		decision.Ratified = true
		decision.DRepApproved = true
		decision.SPOApproved = true
		decision.CCApproved = true
		return decision
	}

	// DRep approval: actions with no DRep gate or a zero threshold pass
	// automatically. Otherwise requires
	// yesStake/(totalStake-abstainStake) >= threshold per CIP-1694.
	drepThreshold := getDRepThreshold(
		actionType, in.PParams, in.ParamUpdate, in.CommitteeNoConfidence,
	)
	if drepThreshold == nil ||
		drepThreshold.Sign() == 0 {
		decision.DRepApproved = true
	} else {
		ratio := in.Tally.DRepYesRatio()
		decision.DRepApproved = ratio.Cmp(drepThreshold) >= 0
	}

	// SPO approval: some actions do not require SPO votes; for those,
	// threshold is nil and approval is automatic. Others require
	// yesStake/(totalStake-abstainStake) >= threshold of the pool snapshot.
	spoThreshold := getSPOThreshold(
		actionType, in.PParams, in.ParamUpdate, in.CommitteeNoConfidence,
	)
	if spoThreshold == nil || spoThreshold.Sign() == 0 {
		decision.SPOApproved = true
	} else {
		ratio := in.Tally.SPOYesRatio()
		decision.SPOApproved = ratio.Cmp(spoThreshold) >= 0
	}

	// CC approval: skipped entirely for NoConfidence and UpdateCommittee.
	// For other actions, require the CC to be seated (!noConfidence),
	// satisfy the minimum committee size, and meet quorum.
	switch {
	case !needsCCApproval(actionType):
		decision.CCApproved = true
	case in.CommitteeNoConfidence:
		decision.CCApproved = false
		decision.FailureReason = "cc in no-confidence state"
	case in.ActiveCCCount == 0:
		// Check zero-members before the min-size comparison so the
		// failure reason distinguishes "no members" from "below
		// minimum" even when MinCommitteeSize >= 1.
		decision.CCApproved = false
		decision.FailureReason = "cc has no active members"
	case in.ActiveCCCount < int(in.PParams.MinCommitteeSize): //nolint:gosec
		decision.CCApproved = false
		decision.FailureReason = "cc below minimum committee size"
	case in.CCQuorum == nil || in.CCQuorum.Sign() == 0:
		// Fail-safe: a missing or zero quorum signals a plumbing bug,
		// not "no quorum required". Auto-approving here would silently
		// ratify CC-gated actions (ParameterChange, HardForkInitiation,
		// TreasuryWithdrawal, NewConstitution) whenever the caller
		// forgot to pass a quorum.
		decision.CCApproved = false
		decision.FailureReason = "cc quorum missing or zero"
	default:
		ratio := in.Tally.CCYesRatio()
		decision.CCApproved = ratio.Cmp(in.CCQuorum) >= 0
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
// committeeNoConfidence selects CommitteeNoConfidence over CommitteeNormal
// for UpdateCommittee when the committee is in a no-confidence state.
func getDRepThreshold(
	actionType lcommon.GovActionType,
	pparams *conway.ConwayProtocolParameters,
	paramUpdate *conway.ConwayProtocolParameterUpdate,
	committeeNoConfidence bool,
) *big.Rat {
	t := &pparams.DRepVotingThresholds
	switch actionType {
	case lcommon.GovActionTypeNoConfidence:
		// CIP-1694 Motion of No Confidence uses MotionNoConfidence.
		return rateToRat(t.MotionNoConfidence)
	case lcommon.GovActionTypeUpdateCommittee:
		// Per CIP-1694: when the committee is already in an
		// explicitly enacted no-confidence state, proposals to seat a
		// new committee are evaluated against CommitteeNoConfidence;
		// otherwise CommitteeNormal applies.
		if committeeNoConfidence {
			return rateToRat(t.CommitteeNoConfidence)
		}
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
		// Fail-closed: unknown/future action types require an
		// unachievable 1/1 yes ratio so they cannot silently ratify
		// without an explicit case being added here.
		return big.NewRat(1, 1)
	}
}

// getSPOThreshold returns the SPO yes-ratio threshold for the action
// type. SPOs vote on a limited subset of actions; nil means SPOs do
// not vote and approval is automatic. For ParameterChange, the SPO
// threshold only applies when the update touches the security
// parameter group; callers without the decoded update receive nil.
// committeeNoConfidence selects CommitteeNoConfidence over CommitteeNormal
// for UpdateCommittee when the committee is in a no-confidence state.
func getSPOThreshold(
	actionType lcommon.GovActionType,
	pparams *conway.ConwayProtocolParameters,
	paramUpdate *conway.ConwayProtocolParameterUpdate,
	committeeNoConfidence bool,
) *big.Rat {
	t := &pparams.PoolVotingThresholds
	switch actionType {
	case lcommon.GovActionTypeNoConfidence:
		// CIP-1694 Motion of No Confidence uses MotionNoConfidence
		// for SPOs as well.
		return rateToRat(t.MotionNoConfidence)
	case lcommon.GovActionTypeUpdateCommittee:
		// Per CIP-1694: use CommitteeNoConfidence when the committee
		// is in a no-confidence state, CommitteeNormal otherwise.
		if committeeNoConfidence {
			return rateToRat(t.CommitteeNoConfidence)
		}
		return rateToRat(t.CommitteeNormal)
	case lcommon.GovActionTypeHardForkInitiation:
		return rateToRat(t.HardForkInitiation)
	case lcommon.GovActionTypeParameterChange:
		if paramUpdate == nil || !touchesSecurityGroup(paramUpdate) {
			return nil
		}
		return rateToRat(t.PpSecurityGroup)
	case lcommon.GovActionTypeNewConstitution,
		lcommon.GovActionTypeTreasuryWithdrawal,
		lcommon.GovActionTypeInfo:
		return nil
	default:
		// Fail-closed: unknown/future action types require an
		// unachievable 1/1 yes ratio so they cannot silently ratify
		// without an explicit case being added here.
		return big.NewRat(1, 1)
	}
}

// touchesSecurityGroup reports whether the parameter update changes any
// security-relevant parameter per CIP-1694: max block body size, max tx
// size, max block header size, max value size, max block ex-units, min
// fee coefficients, UTxO cost per byte, governance action deposit, and
// min fee ref script cost per byte.
func touchesSecurityGroup(u *conway.ConwayProtocolParameterUpdate) bool {
	return u.MaxBlockBodySize != nil ||
		u.MaxTxSize != nil ||
		u.MaxBlockHeaderSize != nil ||
		u.MaxValueSize != nil ||
		u.MaxBlockExUnits != nil ||
		u.MinFeeA != nil ||
		u.MinFeeB != nil ||
		u.AdaPerUtxoByte != nil ||
		u.GovActionDeposit != nil ||
		u.MinFeeRefScriptCostPerByte != nil
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
		// Fail-closed: require CC approval for unknown/future action
		// types so they cannot silently bypass the committee gate.
		return true
	}
}

// mostRestrictiveDRepParamThreshold selects the maximum DRep threshold
// across all parameter groups touched by the update. If the update is
// nil the caller can't tell which groups are touched, so we return the
// maximum across all four groups — the most conservative choice, which
// matches the documented intent ("most restrictive").
func mostRestrictiveDRepParamThreshold(
	t *conway.DRepVotingThresholds,
	update *conway.ConwayProtocolParameterUpdate,
) *big.Rat {
	var best *big.Rat
	consider := func(r cbor.Rat) {
		candidate := rateToRat(r)
		if best == nil || candidate.Cmp(best) > 0 {
			best = candidate
		}
	}
	if update == nil {
		consider(t.PpNetworkGroup)
		consider(t.PpEconomicGroup)
		consider(t.PpTechnicalGroup)
		consider(t.PpGovGroup)
		return best
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
// size, ex-unit limits, and max collateral inputs. Collateral percentage
// belongs to the technical group and is handled in touchesTechnicalGroup.
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
	if !bytes.Equal(proposal.ParentTxHash, currentRoot.TxHash) {
		return false
	}
	return *proposal.ParentActionIdx == currentRoot.ActionIndex
}
