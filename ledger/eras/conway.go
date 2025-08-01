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
	"io"
	"slices"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/scriptcontext"
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
	EvaluateScriptFunc:      EvaluateScriptConway,
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
	//fee, exUnits, redeemers, err := EvaluateTxConway(tx, slot, ls, pp)
	_, _, _, err := EvaluateTxConway(tx, slot, ls, pp)
	if err != nil {
		return err
	}
	// TODO: check exUnits against budget for each redeemer
	return nil
}

func EvaluateTxConway(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) (uint64, lcommon.ExUnits, lcommon.TransactionWitnessRedeemers, error) {
	// Phase 1 validation
	errs := []error{}
	var err error
	for _, validationFunc := range conway.UtxoValidationRules {
		err = validationFunc(tx, slot, ls, pp)
		if err != nil {
			errs = append(
				errs,
				err,
			)
		}
	}
	if len(errs) > 0 {
		return 0, lcommon.ExUnits{}, nil, errors.Join(errs...)
	}
	// Phase 2 validation
	// Resolve inputs
	var resolvedInputs []lcommon.Utxo
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
	var refInputs []lcommon.Utxo
	for _, tmpRefInput := range tx.ReferenceInputs() {
		tmpUtxo, err := ls.UtxoById(tmpRefInput)
		if err != nil {
			return 0, lcommon.ExUnits{}, nil, err
		}
		refInputs = append(
			refInputs,
			tmpUtxo,
		)
	}
	txInfo := scriptcontext.NewTxInfoV3FromTransaction(tx, resolvedInputs)
	for _, redeemerPair := range txInfo.Redeemers {
		purpose := redeemerPair.Key
		fmt.Printf("purpose = %#v\n", purpose)
		redeemer := redeemerPair.Value
		sc := scriptcontext.NewScriptContextV3(txInfo, redeemer, purpose)
		fmt.Printf("sc = %#v\n", sc)
		//var script lcommon.Script
		for _, refInput := range refInputs {
			tmpScript := refInput.Output.ScriptRef()
			if tmpScript != nil {
				switch s := tmpScript.(type) {
				case *lcommon.PlutusV3Script:
					fmt.Printf("TX ID: %s\n", tx.Hash().String())
					fmt.Printf("purpose = %#v, redeemer = %#v\n", purpose, redeemer)
					fmt.Printf("tmpScript(%T) = %x\n", s, *s)
					scriptBytes := slices.Concat([]byte{3}, []byte(*s))
					scriptHash := lcommon.Blake2b224Hash(scriptBytes)
					fmt.Printf("scriptHash = %s\n", scriptHash.String())
					// NOTE: using io.EOF to identify this particular failure upstream
					return 0, lcommon.ExUnits{}, nil, io.EOF
				}
			}
		}
		// TODO: find script for redeemer
		// TODO: evaluate script
		// TODO: build new redeemer with script consumed ExUnits
	}
	// TODO
	return 0, lcommon.ExUnits{}, nil, nil
}

func EvaluateScriptConway(
	script lcommon.Script,
	tx lcommon.Transaction,
	budget lcommon.ExUnits,
) (lcommon.ExUnits, error) {
	if script == nil {
		return lcommon.ExUnits{}, errors.New("nil script")
	}
	var exUnits lcommon.ExUnits
	switch script.(type) {
	case lcommon.PlutusV1Script:
		return exUnits, errors.New("PlutusV1 execution is not yet supported")
	case lcommon.PlutusV2Script:
		return exUnits, errors.New("PlutusV2 execution is not yet supported")
	case lcommon.PlutusV3Script:
		// TODO: create script context
		// TODO: initialize VM
		// TODO: evaluate script
	default:
		return exUnits, fmt.Errorf("unknown script type %T", script)
	}
	return exUnits, nil
}
