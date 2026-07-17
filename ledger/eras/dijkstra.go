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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

var DijkstraEraDesc = EraDesc{
	Id:                      gdijkstra.EraIdDijkstra,
	Name:                    gdijkstra.EraNameDijkstra,
	MinMajorVersion:         gdijkstra.MinProtocolVersionDijkstra,
	MaxMajorVersion:         gdijkstra.MaxProtocolVersionDijkstra,
	DecodePParamsFunc:       DecodePParamsDijkstra,
	DecodePParamsUpdateFunc: DecodePParamsUpdateDijkstra,
	PParamsUpdateFunc:       PParamsUpdateDijkstra,
	HardForkFunc:            HardForkDijkstra,
	EpochLengthFunc:         EpochLengthShelley,
	CalculateEtaVFunc:       CalculateEtaVDijkstra,
	CertDepositFunc:         CertDepositDijkstra,
	ValidateTxFunc:          ValidateTxDijkstra,
	EvaluateTxFunc:          EvaluateTxDijkstra,
}

func DecodePParamsDijkstra(data []byte) (lcommon.ProtocolParameters, error) {
	var ret gdijkstra.DijkstraProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func DecodePParamsUpdateDijkstra(data []byte) (any, error) {
	var ret gdijkstra.DijkstraProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateDijkstra(
	currentPParams lcommon.ProtocolParameters,
	pparamsUpdate any,
) (lcommon.ProtocolParameters, error) {
	dijkstraPParams, ok := currentPParams.(*gdijkstra.DijkstraProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	dijkstraPParamsUpdate, ok := pparamsUpdate.(gdijkstra.DijkstraProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	if err := dijkstraPParams.ApplyUpdate(&dijkstraPParamsUpdate); err != nil {
		return nil, err
	}
	return dijkstraPParams, nil
}

func HardForkDijkstra(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	conwayPParams, ok := prevPParams.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: *conwayPParams,
	}
	ret.CostModels = cloneCostModels(ret.CostModels)
	if nodeConfig != nil {
		dijkstraGenesis := nodeConfig.DijkstraGenesis()
		if !isEmptyDijkstraGenesis(dijkstraGenesis) {
			if err := ret.UpdateFromGenesis(dijkstraGenesis); err != nil {
				return nil, err
			}
		}
	}
	if ret.ProtocolVersion.Major < gdijkstra.MinProtocolVersionDijkstra {
		ret.ProtocolVersion.Major = gdijkstra.MinProtocolVersionDijkstra
	}
	return &ret, nil
}

func isEmptyDijkstraGenesis(genesis *gdijkstra.DijkstraGenesis) bool {
	if genesis == nil {
		return true
	}
	if genesis.MaxRefScriptSizePerBlock != 0 ||
		genesis.MaxRefScriptSizePerTx != 0 ||
		genesis.RefScriptCostStride != 0 ||
		genesis.RefScriptCostMultiplier != nil ||
		genesis.CommitteeStakeCoverage != nil ||
		genesis.QuorumStakeThreshold != nil {
		return false
	}
	return isEmptyConwayGenesis(&genesis.ConwayGenesis)
}

func isEmptyConwayGenesis(genesis *conway.ConwayGenesis) bool {
	if genesis == nil {
		return true
	}
	return genesis.PoolVotingThresholds.CommitteeNormal == nil &&
		genesis.PoolVotingThresholds.CommitteeNoConfidence == nil &&
		genesis.PoolVotingThresholds.HardForkInitiation == nil &&
		genesis.PoolVotingThresholds.MotionNoConfidence == nil &&
		genesis.PoolVotingThresholds.PpSecurityGroup == nil &&
		genesis.DRepVotingThresholds.MotionNoConfidence == nil &&
		genesis.DRepVotingThresholds.CommitteeNormal == nil &&
		genesis.DRepVotingThresholds.CommitteeNoConfidence == nil &&
		genesis.DRepVotingThresholds.UpdateToConstitution == nil &&
		genesis.DRepVotingThresholds.HardForkInitiation == nil &&
		genesis.DRepVotingThresholds.PpNetworkGroup == nil &&
		genesis.DRepVotingThresholds.PpEconomicGroup == nil &&
		genesis.DRepVotingThresholds.PpTechnicalGroup == nil &&
		genesis.DRepVotingThresholds.PpGovGroup == nil &&
		genesis.DRepVotingThresholds.TreasuryWithdrawal == nil &&
		genesis.MinCommitteeSize == 0 &&
		genesis.CommitteeTermLimit == 0 &&
		genesis.GovActionValidityPeriod == 0 &&
		genesis.GovActionDeposit == 0 &&
		genesis.DRepDeposit == 0 &&
		genesis.DRepInactivityPeriod == 0 &&
		genesis.MinFeeRefScriptCostPerByte == nil &&
		len(genesis.PlutusV3CostModel) == 0 &&
		genesis.Constitution.Anchor.DataHash == "" &&
		genesis.Constitution.Anchor.Url == "" &&
		genesis.Constitution.Script == "" &&
		len(genesis.Committee.Members) == 0 &&
		genesis.Committee.Threshold == nil &&
		len(genesis.Delegs) == 0 &&
		len(genesis.InitialDReps) == 0
}

func CalculateEtaVDijkstra(
	nodeConfig *cardano.CardanoNodeConfig,
	prevBlockNonce []byte,
	block ledger.Block,
) ([]byte, error) {
	if len(prevBlockNonce) == 0 {
		tmpNonce, err := hex.DecodeString(nodeConfig.ShelleyGenesisHash)
		if err != nil {
			return nil, err
		}
		prevBlockNonce = tmpNonce
	}
	h, ok := block.Header().(*gdijkstra.DijkstraBlockHeader)
	if !ok {
		return nil, errors.New("unexpected block type")
	}
	vrfNonce := praosVRFNonceValue(h.Body.VrfResult.Output)
	tmpNonce, err := lcommon.CalculateRollingNonce(
		prevBlockNonce,
		vrfNonce,
	)
	if err != nil {
		return nil, err
	}
	return tmpNonce.Bytes(), nil
}

func CertDepositDijkstra(
	cert lcommon.Certificate,
	pp lcommon.ProtocolParameters,
) (uint64, error) {
	tmpPparams, ok := pp.(*gdijkstra.DijkstraProtocolParameters)
	if !ok {
		return 0, ErrIncompatibleProtocolParams
	}
	switch cert.(type) {
	case *lcommon.PoolRegistrationCertificate:
		return uint64(tmpPparams.PoolDeposit), nil
	case *lcommon.RegistrationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.RegistrationDrepCertificate:
		return uint64(tmpPparams.DRepDeposit), nil
	case *lcommon.StakeRegistrationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.StakeRegistrationDelegationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.StakeVoteRegistrationDelegationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	case *lcommon.VoteRegistrationDelegationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	default:
		return 0, nil
	}
}

func ValidateTxDijkstra(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	if _, ok := pp.(*gdijkstra.DijkstraProtocolParameters); !ok {
		return ErrIncompatibleProtocolParams
	}
	normalizedTx, err := normalizeScriptDataHashCbor(tx)
	if err != nil {
		return fmt.Errorf("normalize script data hash CBOR: %w", err)
	}
	tx = normalizedTx
	errs := []error{}
	for _, validationRule := range dijkstraValidationRules(ls) {
		err = validationRule.validationFunc(tx, slot, ls, pp)
		if err != nil {
			errs = append(
				errs,
				fmt.Errorf(
					"dijkstra utxo validation rule %d: %w",
					validationRule.index,
					err,
				),
			)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

var (
	dijkstraUtxoValidationRules       = buildDijkstraValidationRules(false)
	dijkstraPhase1UtxoValidationRules = buildDijkstraValidationRules(true)
)

func dijkstraValidationRules(
	ls lcommon.LedgerState,
) []indexedUtxoValidationRule {
	if shouldSkipPhase2Validation(ls) {
		return dijkstraPhase1UtxoValidationRules
	}
	return dijkstraUtxoValidationRules
}

func buildDijkstraValidationRules(
	skipPhase2 bool,
) []indexedUtxoValidationRule {
	if skipPhase2 {
		return buildIndexedUtxoValidationRules(
			gdijkstra.UtxoValidationRules,
			dijkstraUtxoValidatePlutusScriptsRuleIndex,
			gdijkstra.UtxoValidatePlutusScripts,
			"dijkstra.UtxoValidatePlutusScripts",
		)
	}
	return buildIndexedUtxoValidationRules(
		gdijkstra.UtxoValidationRules,
		noUtxoValidationRuleIndex,
		nil,
		"",
	)
}

func EvaluateTxDijkstra(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	tmpPparams, ok := pp.(*gdijkstra.DijkstraProtocolParameters)
	if !ok {
		return 0, lcommon.ExUnits{}, nil, ErrIncompatibleProtocolParams
	}
	return EvaluateTxConway(tx, ls, &tmpPparams.ConwayProtocolParameters)
}
