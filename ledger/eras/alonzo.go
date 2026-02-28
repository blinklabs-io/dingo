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
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
)

var AlonzoEraDesc = EraDesc{
	Id:                      alonzo.EraIdAlonzo,
	Name:                    alonzo.EraNameAlonzo,
	DecodePParamsFunc:       DecodePParamsAlonzo,
	DecodePParamsUpdateFunc: DecodePParamsUpdateAlonzo,
	PParamsUpdateFunc:       PParamsUpdateAlonzo,
	HardForkFunc:            HardForkAlonzo,
	EpochLengthFunc:         EpochLengthShelley,
	CalculateEtaVFunc:       CalculateEtaVAlonzo,
	CertDepositFunc:         CertDepositAlonzo,
	ValidateTxFunc:          ValidateTxAlonzo,
	EvaluateTxFunc:          EvaluateTxAlonzo,
}

func DecodePParamsAlonzo(data []byte) (lcommon.ProtocolParameters, error) {
	var ret alonzo.AlonzoProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func DecodePParamsUpdateAlonzo(data []byte) (any, error) {
	var ret alonzo.AlonzoProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateAlonzo(
	currentPParams lcommon.ProtocolParameters,
	pparamsUpdate any,
) (lcommon.ProtocolParameters, error) {
	alonzoPParams, ok := currentPParams.(*alonzo.AlonzoProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	alonzoPParamsUpdate, ok := pparamsUpdate.(alonzo.AlonzoProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	alonzoPParams.Update(&alonzoPParamsUpdate)
	return alonzoPParams, nil
}

func HardForkAlonzo(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	maryPParams, ok := prevPParams.(*mary.MaryProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := alonzo.UpgradePParams(*maryPParams)
	alonzoGenesis := nodeConfig.AlonzoGenesis()
	if err := ret.UpdateFromGenesis(alonzoGenesis); err != nil {
		return nil, err
	}
	return &ret, nil
}

func CalculateEtaVAlonzo(
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
	h, ok := block.Header().(*alonzo.AlonzoBlockHeader)
	if !ok {
		return nil, errors.New("unexpected block type")
	}
	tmpNonce, err := lcommon.CalculateRollingNonce(
		prevBlockNonce,
		h.Body.NonceVrf.Output,
	)
	if err != nil {
		return nil, err
	}
	return tmpNonce.Bytes(), nil
}

func CertDepositAlonzo(
	cert lcommon.Certificate,
	pp lcommon.ProtocolParameters,
) (uint64, error) {
	tmpPparams, ok := pp.(*alonzo.AlonzoProtocolParameters)
	if !ok {
		return 0, ErrIncompatibleProtocolParams
	}
	switch cert.(type) {
	case *lcommon.PoolRegistrationCertificate:
		return uint64(tmpPparams.PoolDeposit), nil
	case *lcommon.StakeRegistrationCertificate:
		return uint64(tmpPparams.KeyDeposit), nil
	default:
		return 0, nil
	}
}

func ValidateTxAlonzo(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	tmpPparams, ok := pp.(*alonzo.AlonzoProtocolParameters)
	if !ok {
		return ErrIncompatibleProtocolParams
	}
	errs := []error{}
	var err error
	for _, validationFunc := range alonzo.UtxoValidationRules {
		err = validationFunc(tx, slot, ls, pp)
		if err != nil {
			errs = append(
				errs,
				err,
			)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	// Validate transaction size against protocol limit
	if err = ValidateTxSize(tx, tmpPparams.MaxTxSize); err != nil {
		return err
	}
	// Validate fee covers base cost + execution unit prices
	var pricesMem, pricesSteps *big.Rat
	if tmpPparams.ExecutionCosts.MemPrice != nil {
		pricesMem = tmpPparams.ExecutionCosts.MemPrice.ToBigRat()
	}
	if tmpPparams.ExecutionCosts.StepPrice != nil {
		pricesSteps = tmpPparams.ExecutionCosts.StepPrice.ToBigRat()
	}
	if err = ValidateTxFee(
		tx,
		tmpPparams.MinFeeA,
		tmpPparams.MinFeeB,
		pricesMem,
		pricesSteps,
	); err != nil {
		return err
	}
	// Skip script evaluation if TX is marked as not valid
	if !tx.IsValid() {
		return nil
	}
	// Resolve inputs
	resolvedInputs := []lcommon.Utxo{}
	for _, tmpInput := range tx.Inputs() {
		tmpUtxo, err := ls.UtxoById(tmpInput)
		if err != nil {
			return err
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
			return err
		}
		resolvedRefInputs = append(
			resolvedRefInputs,
			tmpUtxo,
		)
	}
	// Build TX script map
	scripts := make(map[lcommon.ScriptHash]lcommon.Script)
	for _, refInput := range resolvedRefInputs {
		tmpScript := refInput.Output.ScriptRef()
		if tmpScript == nil {
			continue
		}
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().PlutusV1Scripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().NativeScripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	// Evaluate scripts
	var txInfoV1 script.TxInfo
	txInfoV1, err = script.NewTxInfoV1FromTransaction(
		ls,
		tx,
		slices.Concat(resolvedInputs, resolvedRefInputs),
	)
	if err != nil {
		return err
	}
	for _, redeemerPair := range txInfoV1.(script.TxInfoV1).Redeemers {
		purpose := redeemerPair.Key
		if purpose == nil {
			return errors.New("script purpose is nil")
		}
		redeemer := redeemerPair.Value
		// Lookup script from redeemer purpose
		tmpScript := scripts[purpose.ScriptHash()]
		if tmpScript == nil {
			return fmt.Errorf(
				"could not find script with hash %s",
				purpose.ScriptHash().String(),
			)
		}
		switch s := tmpScript.(type) {
		case lcommon.PlutusV1Script:
			// Get spent UTxO datum
			var datum data.PlutusData
			if tmp, ok := purpose.(script.ScriptPurposeSpending); ok {
				datum = tmp.Datum
			}
			sc := script.NewScriptContextV1V2(txInfoV1, purpose)
			evalContext, err := cek.NewEvalContext(
				lang.LanguageVersionV1,
				cek.ProtoVersion{
					Major: tmpPparams.ProtocolMajor,
					Minor: tmpPparams.ProtocolMinor,
				},
				tmpPparams.CostModels[0],
			)
			if err != nil {
				return fmt.Errorf("build evaluation context: %w", err)
			}
			_, err = s.Evaluate(
				datum,
				redeemer.Data,
				sc.ToPlutusData(),
				redeemer.ExUnits,
				evalContext,
			)
			if err != nil {
				/*
					fmt.Printf("TX ID: %s\n", tx.Hash().String())
					fmt.Printf("purpose = %#v, redeemer = %#v\n", purpose, redeemer)
					scriptHash := s.Hash()
					fmt.Printf("scriptHash = %s\n", scriptHash.String())
					fmt.Printf("tx = %x\n", tx.Cbor())
					// Build inputs/outputs strings that can be plugged into Aiken script_context tests for comparison
					var tmpInputs []lcommon.TransactionInput
					var tmpOutputs []lcommon.TransactionOutput
					for _, input := range slices.Concat(resolvedInputs, resolvedRefInputs) {
						tmpInputs = append(tmpInputs, input.Id)
						tmpOutputs = append(tmpOutputs, input.Output)
					}
					tmpInputsCbor, err2 := cbor.Encode(tmpInputs)
					if err2 != nil {
						return err2
					}
					fmt.Printf("tmpInputs = %x\n", tmpInputsCbor)
					tmpOutputsCbor, err2 := cbor.Encode(tmpOutputs)
					if err2 != nil {
						return err2
					}
					fmt.Printf("tmpOutputs = %x\n", tmpOutputsCbor)
					scCbor, err2 := data.Encode(sc.ToPlutusData())
					if err2 != nil {
						return err2
					}
					fmt.Printf("scCbor = %x\n", scCbor)
				*/
				return err
			}
		default:
			return fmt.Errorf("unimplemented script type: %T", tmpScript)
		}
	}
	return nil
}

func EvaluateTxAlonzo(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	tmpPparams, ok := pp.(*alonzo.AlonzoProtocolParameters)
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
	for _, refInput := range resolvedRefInputs {
		tmpScript := refInput.Output.ScriptRef()
		if tmpScript == nil {
			continue
		}
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().PlutusV1Scripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().NativeScripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	// Evaluate scripts
	var retTotalExUnits lcommon.ExUnits
	retRedeemerExUnits := make(map[lcommon.RedeemerKey]lcommon.ExUnits)
	var txInfoV1 script.TxInfo
	var err error
	txInfoV1, err = script.NewTxInfoV1FromTransaction(
		ls,
		tx,
		slices.Concat(resolvedInputs, resolvedRefInputs),
	)
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}
	for _, redeemerPair := range txInfoV1.(script.TxInfoV1).Redeemers {
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
			// Get spent UTxO datum
			var datum data.PlutusData
			if tmp, ok := purpose.(script.ScriptPurposeSpending); ok {
				datum = tmp.Datum
			}
			sc := script.NewScriptContextV1V2(txInfoV1, purpose)
			evalContext, err := cek.NewEvalContext(
				lang.LanguageVersionV1,
				cek.ProtoVersion{
					Major: tmpPparams.ProtocolMajor,
					Minor: tmpPparams.ProtocolMinor,
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
