// Copyright 2025 Blink Labs Software
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
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
)

var ConwayEraDesc = EraDesc{
	Id:                      conway.EraIdConway,
	Name:                    conway.EraNameConway,
	MinMajorVersion:         9,
	MaxMajorVersion:         10,
	DecodePParamsFunc:       DecodePParamsConway,
	DecodePParamsUpdateFunc: DecodePParamsUpdateConway,
	PParamsUpdateFunc:       PParamsUpdateConway,
	HardForkFunc:            HardForkConway,
	EpochLengthFunc:         EpochLengthShelley,
	CalculateEtaVFunc:       CalculateEtaVConway,
	CertDepositFunc:         CertDepositConway,
	ValidateTxFunc:          ValidateTxConway,
	EvaluateTxFunc:          EvaluateTxConway,
}

func DecodePParamsConway(data []byte) (lcommon.ProtocolParameters, error) {
	var ret conway.ConwayProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func DecodePParamsUpdateConway(data []byte) (any, error) {
	var ret conway.ConwayProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateConway(
	currentPParams lcommon.ProtocolParameters,
	pparamsUpdate any,
) (lcommon.ProtocolParameters, error) {
	conwayPParams, ok := currentPParams.(*conway.ConwayProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	conwayPParamsUpdate, ok := pparamsUpdate.(conway.ConwayProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	conwayPParams.Update(&conwayPParamsUpdate)
	return conwayPParams, nil
}

func HardForkConway(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	babbagePParams, ok := prevPParams.(*babbage.BabbageProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := conway.UpgradePParams(*babbagePParams)
	conwayGenesis := nodeConfig.ConwayGenesis()
	if err := ret.UpdateFromGenesis(conwayGenesis); err != nil {
		return nil, err
	}
	return &ret, nil
}

func CalculateEtaVConway(
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
	h, ok := block.Header().(*conway.ConwayBlockHeader)
	if !ok {
		return nil, errors.New("unexpected block type")
	}
	// Praos (Babbage+) uses domain separation + double hash,
	// unlike TPraos which uses the raw VRF output directly.
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

func CertDepositConway(
	cert lcommon.Certificate,
	pp lcommon.ProtocolParameters,
) (uint64, error) {
	tmpPparams, ok := pp.(*conway.ConwayProtocolParameters)
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

func ValidateTxConway(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	if _, ok := pp.(*conway.ConwayProtocolParameters); !ok {
		return ErrIncompatibleProtocolParams
	}
	normalizedTx, err := normalizeScriptDataHashCbor(tx)
	if err != nil {
		return fmt.Errorf("normalize script data hash CBOR: %w", err)
	}
	tx = normalizedTx
	// Validate TX through ledger validation rules (Phase-1).
	// These must run even for isValid=false transactions, which still
	// require valid structure, fees, and UTxO references for collateral.
	errs := []error{}
	for idx, validationFunc := range conway.UtxoValidationRules {
		err = validationFunc(tx, slot, ls, pp)
		if err != nil {
			errs = append(
				errs,
				fmt.Errorf("conway utxo validation rule %d: %w", idx, err),
			)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	// Skip script evaluation (Phase-2) if TX is marked as not valid.
	// These transactions failed script validation on-chain; collateral
	// is consumed instead of regular inputs.
	if !tx.IsValid() {
		return nil
	}
	return nil
}

func EvaluateTxConway(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	tmpPparams, ok := pp.(*conway.ConwayProtocolParameters)
	if !ok {
		return 0, lcommon.ExUnits{}, nil, ErrIncompatibleProtocolParams
	}
	// Resolve inputs
	resolvedInputs := []lcommon.Utxo{}
	for _, tmpInput := range tx.Inputs() {
		tmpUtxo, err := ls.UtxoById(tmpInput)
		if err != nil {
			return 0, lcommon.ExUnits{}, nil, err
		}
		resolvedInputs = append(
			resolvedInputs,
			tmpUtxo,
		)
	}
	// Resolve reference inputs
	resolvedRefInputs := []lcommon.Utxo{}
	for _, tmpRefInput := range tx.ReferenceInputs() {
		tmpUtxo, err := ls.UtxoById(tmpRefInput)
		if err != nil {
			return 0, lcommon.ExUnits{}, nil, err
		}
		resolvedRefInputs = append(
			resolvedRefInputs,
			tmpUtxo,
		)
	}
	// Build TX script map
	scripts := make(map[lcommon.ScriptHash]lcommon.Script)
	// Add script refs from all resolved UTxOs (both inputs and reference inputs)
	for _, utxo := range slices.Concat(resolvedInputs, resolvedRefInputs) {
		tmpScript := utxo.Output.ScriptRef()
		if tmpScript == nil {
			continue
		}
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().PlutusV1Scripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().PlutusV2Scripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().PlutusV3Scripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().NativeScripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	// Evaluate scripts
	var retTotalExUnits lcommon.ExUnits
	retRedeemerExUnits := make(map[lcommon.RedeemerKey]lcommon.ExUnits)
	var txInfoV3 script.TxInfo
	var err error
	txInfoV3, err = script.NewTxInfoV3FromTransaction(
		ls,
		tx,
		slices.Concat(resolvedInputs, resolvedRefInputs),
	)
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	for _, redeemerPair := range txInfoV3.(script.TxInfoV3).Redeemers {
		purpose := redeemerPair.Key
		if purpose == nil {
			return 0, lcommon.ExUnits{}, nil, errors.New(
				"script purpose is nil",
			)
		}
		redeemer := redeemerPair.Value
		// Lookup script from redeemer purpose
		tmpScript := scripts[purpose.ScriptHash()]
		if tmpScript == nil {
			return 0, lcommon.ExUnits{}, nil, errors.New(
				"could not find needed script",
			)
		}
		switch s := tmpScript.(type) {
		case lcommon.PlutusV1Script:
			txInfoV1, err := script.NewTxInfoV1FromTransaction(
				ls,
				tx,
				slices.Concat(resolvedInputs, resolvedRefInputs),
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, err
			}
			// Get spent UTxO datum
			var datum data.PlutusData
			if tmp, ok := purpose.(script.ScriptPurposeSpending); ok {
				datum = tmp.Datum
			}
			sc := script.NewScriptContextV1V2(txInfoV1, purpose)
			evalContext, err := cek.NewEvalContext(
				lang.LanguageVersionV1,
				cek.ProtoVersion{
					Major: tmpPparams.ProtocolVersion.Major,
					Minor: tmpPparams.ProtocolVersion.Minor,
				},
				tmpPparams.CostModels[0],
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, fmt.Errorf("build evaluation context: %w", err)
			}
			usedBudget, err := s.Evaluate(
				datum,
				redeemer.Data,
				sc.ToPlutusData(),
				tmpPparams.MaxTxExUnits,
				evalContext,
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, err
			}
			retTotalExUnits.Steps += usedBudget.Steps
			retTotalExUnits.Memory += usedBudget.Memory
			retRedeemerExUnits[lcommon.RedeemerKey{
				Tag:   redeemer.Tag,
				Index: redeemer.Index,
			}] = usedBudget
		case lcommon.PlutusV2Script:
			txInfoV2, err := script.NewTxInfoV2FromTransaction(
				ls,
				tx,
				slices.Concat(resolvedInputs, resolvedRefInputs),
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, err
			}
			// Get spent UTxO datum
			var datum data.PlutusData
			if tmp, ok := purpose.(script.ScriptPurposeSpending); ok {
				datum = tmp.Datum
			}
			sc := script.NewScriptContextV1V2(txInfoV2, purpose)
			evalContext, err := cek.NewEvalContext(
				lang.LanguageVersionV2,
				cek.ProtoVersion{
					Major: tmpPparams.ProtocolVersion.Major,
					Minor: tmpPparams.ProtocolVersion.Minor,
				},
				tmpPparams.CostModels[1],
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, fmt.Errorf("build evaluation context: %w", err)
			}
			usedBudget, err := s.Evaluate(
				datum,
				redeemer.Data,
				sc.ToPlutusData(),
				tmpPparams.MaxTxExUnits,
				evalContext,
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, err
			}
			retTotalExUnits.Steps += usedBudget.Steps
			retTotalExUnits.Memory += usedBudget.Memory
			retRedeemerExUnits[lcommon.RedeemerKey{
				Tag:   redeemer.Tag,
				Index: redeemer.Index,
			}] = usedBudget
		case lcommon.PlutusV3Script:
			sc := script.NewScriptContextV3(txInfoV3, redeemer, purpose)
			evalContext, err := cek.NewEvalContext(
				lang.LanguageVersionV3,
				cek.ProtoVersion{
					Major: tmpPparams.ProtocolVersion.Major,
					Minor: tmpPparams.ProtocolVersion.Minor,
				},
				tmpPparams.CostModels[2],
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, fmt.Errorf("build evaluation context: %w", err)
			}
			usedBudget, err := s.Evaluate(
				sc.ToPlutusData(),
				tmpPparams.MaxTxExUnits,
				evalContext,
			)
			if err != nil {
				return 0, lcommon.ExUnits{}, nil, err
			}
			retTotalExUnits.Steps += usedBudget.Steps
			retTotalExUnits.Memory += usedBudget.Memory
			retRedeemerExUnits[lcommon.RedeemerKey{
				Tag:   redeemer.Tag,
				Index: redeemer.Index,
			}] = usedBudget
		default:
			return 0, lcommon.ExUnits{}, nil, fmt.Errorf("unimplemented script type: %T", tmpScript)
		}
	}
	// Calculate fee based on TX size and calculated ExUnits
	txSize := TxSizeForFee(tx)
	var pricesMem, pricesSteps *big.Rat
	if tmpPparams.ExecutionCosts.MemPrice != nil {
		pricesMem = tmpPparams.ExecutionCosts.MemPrice.ToBigRat()
	}
	if tmpPparams.ExecutionCosts.StepPrice != nil {
		pricesSteps = tmpPparams.ExecutionCosts.StepPrice.ToBigRat()
	}
	fee := CalculateMinFee(
		txSize,
		retTotalExUnits,
		tmpPparams.MinFeeA,
		tmpPparams.MinFeeB,
		pricesMem,
		pricesSteps,
	)
	return fee, retTotalExUnits, retRedeemerExUnits, nil
}
