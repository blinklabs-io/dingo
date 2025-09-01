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
	"slices"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
)

var ConwayEraDesc = EraDesc{
	Id:                      conway.EraIdConway,
	Name:                    conway.EraNameConway,
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
	tmpNonce, err := lcommon.CalculateRollingNonce(
		prevBlockNonce,
		h.Body.VrfResult.Output,
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
		return 0, errors.New("pparams are not expected type")
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
	errs := []error{}
	for _, validationFunc := range conway.UtxoValidationRules {
		errs = append(
			errs,
			validationFunc(tx, slot, ls, pp),
		)
	}
	return errors.Join(errs...)
}

func EvaluateTxConway(
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	conwayPParams, ok := pp.(*conway.ConwayProtocolParameters)
	if !ok {
		return 0, lcommon.ExUnits{}, nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			pp,
		)
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
	for _, tmpScript := range tx.Witnesses().PlutusV2Scripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	for _, tmpScript := range tx.Witnesses().PlutusV3Scripts() {
		scripts[tmpScript.Hash()] = tmpScript
	}
	// Evaluate scripts
	var retTotalExUnits lcommon.ExUnits
	retRedeemerExUnits := make(map[lcommon.RedeemerKey]lcommon.ExUnits)
	var txInfoV3 script.TxInfo = script.NewTxInfoV3FromTransaction(tx, slices.Concat(resolvedInputs, resolvedRefInputs))
	for _, redeemerPair := range txInfoV3.(script.TxInfoV3).Redeemers {
		purpose := redeemerPair.Key
		if purpose == nil {
			// Skip unsupported redeemer tags for now
			continue
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
		case *lcommon.PlutusV3Script:
			sc := script.NewScriptContextV3(txInfoV3, redeemer, purpose)
			usedBudget, err := s.Evaluate(
				sc.ToPlutusData(),
				conwayPParams.MaxTxExUnits,
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
			/*
				if err != nil && !strings.Contains(err.Error(), "unimplemented") {
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
					tmpInputsCbor, err := cbor.Encode(tmpInputs)
					if err != nil {
						return 0, lcommon.ExUnits{}, nil, err
					}
					fmt.Printf("tmpInputs = %x\n", tmpInputsCbor)
					tmpOutputsCbor, err := cbor.Encode(tmpOutputs)
					if err != nil {
						return 0, lcommon.ExUnits{}, nil, err
					}
					fmt.Printf("tmpOutputs = %x\n", tmpOutputsCbor)
					fmt.Printf("sc = %#v\n", sc)
					scCbor, err := data.Encode(sc.ToPlutusData())
					if err != nil {
						return 0, lcommon.ExUnits{}, nil, err
					}
					fmt.Printf("scCbor = %x\n", scCbor)
				}
			*/
		default:
			return 0, lcommon.ExUnits{}, nil, fmt.Errorf("unimplemented script type: %T", tmpScript)
		}
	}
	// TODO: calculate fee based on current TX and calculated ExUnits
	return 0, retTotalExUnits, retRedeemerExUnits, nil
}
