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
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

// ValidateProtocolParameterDomains checks that complete Conway or Dijkstra
// protocol parameters fit the integer widths, cost-model language-ID range and
// rational domains of the Conway CDDL. Parameters that did not come from a
// proposal, such as an imported ledger snapshot, are decoded into Go integers
// wider than their wire types and would otherwise be accepted unchecked.
// Other eras are returned unchecked.
//
// Only domains are checked. Zero values are not rejected, because the
// non-zero rules apply to proposed updates and genesis values may be zero.
func ValidateProtocolParameterDomains(pp lcommon.ProtocolParameters) error {
	var params *conway.ConwayProtocolParameters
	switch p := pp.(type) {
	case *conway.ConwayProtocolParameters:
		params = p
	case *gdijkstra.DijkstraProtocolParameters:
		if p != nil {
			params = &p.ConwayProtocolParameters
		}
	}
	if params == nil {
		return nil
	}
	update := conway.ConwayProtocolParameterUpdate{
		MinFeeA:                    &params.MinFeeA,
		MinFeeB:                    &params.MinFeeB,
		MaxBlockBodySize:           &params.MaxBlockBodySize,
		MaxTxSize:                  &params.MaxTxSize,
		MaxBlockHeaderSize:         &params.MaxBlockHeaderSize,
		KeyDeposit:                 &params.KeyDeposit,
		PoolDeposit:                &params.PoolDeposit,
		MaxEpoch:                   &params.MaxEpoch,
		NOpt:                       &params.NOpt,
		A0:                         params.A0,
		Rho:                        params.Rho,
		Tau:                        params.Tau,
		MinPoolCost:                &params.MinPoolCost,
		AdaPerUtxoByte:             &params.AdaPerUtxoByte,
		CostModels:                 params.CostModels,
		MaxTxExUnits:               &params.MaxTxExUnits,
		MaxBlockExUnits:            &params.MaxBlockExUnits,
		MaxValueSize:               &params.MaxValueSize,
		CollateralPercentage:       &params.CollateralPercentage,
		MaxCollateralInputs:        &params.MaxCollateralInputs,
		MinCommitteeSize:           &params.MinCommitteeSize,
		CommitteeTermLimit:         &params.CommitteeTermLimit,
		GovActionValidityPeriod:    &params.GovActionValidityPeriod,
		GovActionDeposit:           &params.GovActionDeposit,
		DRepDeposit:                &params.DRepDeposit,
		DRepInactivityPeriod:       &params.DRepInactivityPeriod,
		MinFeeRefScriptCostPerByte: params.MinFeeRefScriptCostPerByte,
	}
	// A group of rationals that is entirely unset was never populated, which
	// is how parameters built in Go rather than decoded appear; a decoded
	// value always carries every member. A partly set group is still checked.
	if p := params.ExecutionCosts; p.MemPrice != nil || p.StepPrice != nil {
		update.ExecutionCosts = &params.ExecutionCosts
	}
	if poolThresholdsSet(&params.PoolVotingThresholds) {
		update.PoolVotingThresholds = &params.PoolVotingThresholds
	}
	if drepThresholdsSet(&params.DRepVotingThresholds) {
		update.DRepVotingThresholds = &params.DRepVotingThresholds
	}
	return conway.ValidateProtocolParameterUpdate(&update)
}

func poolThresholdsSet(t *conway.PoolVotingThresholds) bool {
	for _, rat := range []cbor.Rat{
		t.MotionNoConfidence,
		t.CommitteeNormal,
		t.CommitteeNoConfidence,
		t.HardForkInitiation,
		t.PpSecurityGroup,
	} {
		if rat.Rat != nil {
			return true
		}
	}
	return false
}

func drepThresholdsSet(t *conway.DRepVotingThresholds) bool {
	for _, rat := range []cbor.Rat{
		t.MotionNoConfidence,
		t.CommitteeNormal,
		t.CommitteeNoConfidence,
		t.UpdateToConstitution,
		t.HardForkInitiation,
		t.PpNetworkGroup,
		t.PpEconomicGroup,
		t.PpTechnicalGroup,
		t.PpGovGroup,
		t.TreasuryWithdrawal,
	} {
		if rat.Rat != nil {
			return true
		}
	}
	return false
}
