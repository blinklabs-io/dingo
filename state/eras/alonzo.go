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
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
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
	ret.UpdateFromGenesis(alonzoGenesis)
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
		return 0, errors.New("pparams are not expected type")
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
