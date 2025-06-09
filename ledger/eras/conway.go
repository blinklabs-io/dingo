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

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
