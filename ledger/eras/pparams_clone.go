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

package eras

import (
	"fmt"
	"math/big"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// CloneGovernanceProtocolParameters returns an independently owned copy of
// Conway-or-later protocol parameters. Every map, slice, and rational is
// cloned so an era update cannot mutate a previously published snapshot.
func CloneGovernanceProtocolParameters(
	pparams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	switch p := pparams.(type) {
	case *conway.ConwayProtocolParameters:
		if p == nil {
			return nil, fmt.Errorf("nil Conway protocol parameters")
		}
		return cloneConwayProtocolParameters(p), nil
	case *gdijkstra.DijkstraProtocolParameters:
		if p == nil {
			return nil, fmt.Errorf("nil Dijkstra protocol parameters")
		}
		ret := *p
		ret.ConwayProtocolParameters = *cloneConwayProtocolParameters(
			&p.ConwayProtocolParameters,
		)
		ret.RefScriptCostMultiplier = cloneRat(p.RefScriptCostMultiplier)
		ret.CommitteeStakeCoverage = cloneRat(p.CommitteeStakeCoverage)
		ret.QuorumStakeThreshold = cloneRat(p.QuorumStakeThreshold)
		return &ret, nil
	default:
		return nil, fmt.Errorf(
			"unsupported governance protocol parameters type %T",
			pparams,
		)
	}
}

func cloneConwayProtocolParameters(
	pparams *conway.ConwayProtocolParameters,
) *conway.ConwayProtocolParameters {
	ret := *pparams
	ret.A0 = cloneRat(pparams.A0)
	ret.Rho = cloneRat(pparams.Rho)
	ret.Tau = cloneRat(pparams.Tau)
	ret.CostModels = cloneCostModels(pparams.CostModels)
	ret.ExecutionCosts.MemPrice = cloneRat(
		pparams.ExecutionCosts.MemPrice,
	)
	ret.ExecutionCosts.StepPrice = cloneRat(
		pparams.ExecutionCosts.StepPrice,
	)
	ret.PoolVotingThresholds.MotionNoConfidence = cloneRatValue(
		pparams.PoolVotingThresholds.MotionNoConfidence,
	)
	ret.PoolVotingThresholds.CommitteeNormal = cloneRatValue(
		pparams.PoolVotingThresholds.CommitteeNormal,
	)
	ret.PoolVotingThresholds.CommitteeNoConfidence = cloneRatValue(
		pparams.PoolVotingThresholds.CommitteeNoConfidence,
	)
	ret.PoolVotingThresholds.HardForkInitiation = cloneRatValue(
		pparams.PoolVotingThresholds.HardForkInitiation,
	)
	ret.PoolVotingThresholds.PpSecurityGroup = cloneRatValue(
		pparams.PoolVotingThresholds.PpSecurityGroup,
	)
	ret.DRepVotingThresholds.MotionNoConfidence = cloneRatValue(
		pparams.DRepVotingThresholds.MotionNoConfidence,
	)
	ret.DRepVotingThresholds.CommitteeNormal = cloneRatValue(
		pparams.DRepVotingThresholds.CommitteeNormal,
	)
	ret.DRepVotingThresholds.CommitteeNoConfidence = cloneRatValue(
		pparams.DRepVotingThresholds.CommitteeNoConfidence,
	)
	ret.DRepVotingThresholds.UpdateToConstitution = cloneRatValue(
		pparams.DRepVotingThresholds.UpdateToConstitution,
	)
	ret.DRepVotingThresholds.HardForkInitiation = cloneRatValue(
		pparams.DRepVotingThresholds.HardForkInitiation,
	)
	ret.DRepVotingThresholds.PpNetworkGroup = cloneRatValue(
		pparams.DRepVotingThresholds.PpNetworkGroup,
	)
	ret.DRepVotingThresholds.PpEconomicGroup = cloneRatValue(
		pparams.DRepVotingThresholds.PpEconomicGroup,
	)
	ret.DRepVotingThresholds.PpTechnicalGroup = cloneRatValue(
		pparams.DRepVotingThresholds.PpTechnicalGroup,
	)
	ret.DRepVotingThresholds.PpGovGroup = cloneRatValue(
		pparams.DRepVotingThresholds.PpGovGroup,
	)
	ret.DRepVotingThresholds.TreasuryWithdrawal = cloneRatValue(
		pparams.DRepVotingThresholds.TreasuryWithdrawal,
	)
	ret.MinFeeRefScriptCostPerByte = cloneRat(
		pparams.MinFeeRefScriptCostPerByte,
	)
	return &ret
}

func cloneRat(value *cbor.Rat) *cbor.Rat {
	if value == nil {
		return nil
	}
	ret := &cbor.Rat{}
	if value.Rat != nil {
		ret.Rat = new(big.Rat).Set(value.Rat)
	}
	return ret
}

func cloneRatValue(value cbor.Rat) cbor.Rat {
	if value.Rat == nil {
		return cbor.Rat{}
	}
	return cbor.Rat{Rat: new(big.Rat).Set(value.Rat)}
}
