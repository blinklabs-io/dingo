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

package blockfrost

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

func ratPointerFloat(r *cbor.Rat) *float64 {
	if r == nil || r.Rat == nil {
		return nil
	}
	f, _ := r.Float64()
	return &f
}

func ratValueFloat(r cbor.Rat) *float64 {
	return ratPointerFloat(&r)
}

func uintPointer[T ~uint | ~uint64](v *T) *uint64 {
	if v == nil {
		return nil
	}
	out := uint64(*v)
	return &out
}

func decimalPointer[T ~uint | ~uint64 | ~int64](v *T) *string {
	if v == nil {
		return nil
	}
	out := strconv.FormatUint(uint64(*v), 10)
	return &out
}

// proposalParameters renders a stored protocol parameter update. Parameters
// the update leaves unset are null. Parameters Blockfrost lists that Conway
// has no counterpart for (decentralisation, extra entropy, minimum UTxO
// value, coins per UTxO word) stay null.
func proposalParameters(
	updateCbor []byte,
	proposedEpoch uint64,
) (ProposalParametersResponse, error) {
	var update conway.ConwayProtocolParameterUpdate
	if _, err := cbor.Decode(updateCbor, &update); err != nil {
		return ProposalParametersResponse{}, fmt.Errorf(
			"decode protocol parameter update: %w", err,
		)
	}
	ret := ProposalParametersResponse{
		Epoch:                  &proposedEpoch,
		MinFeeA:                uintPointer(update.MinFeeA),
		MinFeeB:                uintPointer(update.MinFeeB),
		MaxBlockSize:           uintPointer(update.MaxBlockBodySize),
		MaxTxSize:              uintPointer(update.MaxTxSize),
		MaxBlockHeaderSize:     uintPointer(update.MaxBlockHeaderSize),
		KeyDeposit:             decimalPointer(update.KeyDeposit),
		PoolDeposit:            decimalPointer(update.PoolDeposit),
		EMax:                   uintPointer(update.MaxEpoch),
		NOpt:                   uintPointer(update.NOpt),
		A0:                     ratPointerFloat(update.A0),
		Rho:                    ratPointerFloat(update.Rho),
		Tau:                    ratPointerFloat(update.Tau),
		MinPoolCost:            decimalPointer(update.MinPoolCost),
		MaxValSize:             decimalPointer(update.MaxValueSize),
		CollateralPercent:      uintPointer(update.CollateralPercentage),
		MaxCollateralInputs:    uintPointer(update.MaxCollateralInputs),
		CoinsPerUtxoSize:       decimalPointer(update.AdaPerUtxoByte),
		CommitteeMinSize:       decimalPointer(update.MinCommitteeSize),
		CommitteeMaxTermLength: decimalPointer(update.CommitteeTermLimit),
		GovActionLifetime: decimalPointer(
			update.GovActionValidityPeriod,
		),
		GovActionDeposit: decimalPointer(update.GovActionDeposit),
		DRepDeposit:      decimalPointer(update.DRepDeposit),
		DRepActivity:     decimalPointer(update.DRepInactivityPeriod),
		MinFeeRefScriptCostPerByte: ratPointerFloat(
			update.MinFeeRefScriptCostPerByte,
		),
	}
	if version := update.ProtocolVersion; version != nil {
		major, minor := uint64(version.Major), uint64(version.Minor)
		ret.ProtocolMajorVer, ret.ProtocolMinorVer = &major, &minor
	}
	if prices := update.ExecutionCosts; prices != nil {
		ret.PriceMem = ratPointerFloat(prices.MemPrice)
		ret.PriceStep = ratPointerFloat(prices.StepPrice)
	}
	if units := update.MaxTxExUnits; units != nil {
		mem := strconv.FormatInt(units.Memory, 10)
		steps := strconv.FormatInt(units.Steps, 10)
		ret.MaxTxExMem, ret.MaxTxExSteps = &mem, &steps
	}
	if units := update.MaxBlockExUnits; units != nil {
		mem := strconv.FormatInt(units.Memory, 10)
		steps := strconv.FormatInt(units.Steps, 10)
		ret.MaxBlockExMem, ret.MaxBlockExSteps = &mem, &steps
	}
	if thresholds := update.PoolVotingThresholds; thresholds != nil {
		ret.PvtMotionNoConfidence = ratValueFloat(thresholds.MotionNoConfidence)
		ret.PvtCommitteeNormal = ratValueFloat(thresholds.CommitteeNormal)
		ret.PvtCommitteeNoConfidence = ratValueFloat(
			thresholds.CommitteeNoConfidence,
		)
		ret.PvtHardForkInitiation = ratValueFloat(thresholds.HardForkInitiation)
		ret.PvtppSecurityGroup = ratValueFloat(thresholds.PpSecurityGroup)
		ret.PvtPPSecurityGroup = ratValueFloat(thresholds.PpSecurityGroup)
	}
	if thresholds := update.DRepVotingThresholds; thresholds != nil {
		ret.DvtMotionNoConfidence = ratValueFloat(thresholds.MotionNoConfidence)
		ret.DvtCommitteeNormal = ratValueFloat(thresholds.CommitteeNormal)
		ret.DvtCommitteeNoConfidence = ratValueFloat(
			thresholds.CommitteeNoConfidence,
		)
		ret.DvtUpdateToConstitution = ratValueFloat(
			thresholds.UpdateToConstitution,
		)
		ret.DvtHardForkInitiation = ratValueFloat(thresholds.HardForkInitiation)
		ret.DvtPPNetworkGroup = ratValueFloat(thresholds.PpNetworkGroup)
		ret.DvtPPEconomicGroup = ratValueFloat(thresholds.PpEconomicGroup)
		ret.DvtPPTechnicalGroup = ratValueFloat(thresholds.PpTechnicalGroup)
		ret.DvtPPGovGroup = ratValueFloat(thresholds.PpGovGroup)
		ret.DvtTreasuryWithdrawal = ratValueFloat(thresholds.TreasuryWithdrawal)
	}
	if update.CostModels != nil {
		names := map[uint]string{0: "PlutusV1", 1: "PlutusV2", 2: "PlutusV3"}
		models := make(map[string][]int64, len(update.CostModels))
		for language, costs := range update.CostModels {
			name, ok := names[language]
			if !ok {
				name = "PlutusV" + strconv.FormatUint(uint64(language)+1, 10)
			}
			models[name] = costs
		}
		encoded, err := json.Marshal(models)
		if err != nil {
			return ProposalParametersResponse{}, fmt.Errorf(
				"encode cost models: %w", err,
			)
		}
		ret.CostModels = encoded
	}
	return ret, nil
}
