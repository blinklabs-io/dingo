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
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

var BabbageEraDesc = EraDesc{
	Id:                      babbage.EraIdBabbage,
	Name:                    babbage.EraNameBabbage,
	DecodePParamsFunc:       DecodePParamsBabbage,
	DecodePParamsUpdateFunc: DecodePParamsUpdateBabbage,
	PParamsUpdateFunc:       PParamsUpdateBabbage,
	HardForkFunc:            HardForkBabbage,
	EpochLengthFunc:         EpochLengthShelley,
	CalculateEtaVFunc:       CalculateEtaVBabbage,
}

func DecodePParamsBabbage(data []byte) (lcommon.ProtocolParameters, error) {
	var ret babbage.BabbageProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func DecodePParamsUpdateBabbage(data []byte) (any, error) {
	var ret babbage.BabbageProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateBabbage(currentPParams lcommon.ProtocolParameters, pparamsUpdate any) (lcommon.ProtocolParameters, error) {
	babbagePParams, ok := currentPParams.(*babbage.BabbageProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	babbagePParamsUpdate, ok := pparamsUpdate.(babbage.BabbageProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	babbagePParams.Update(&babbagePParamsUpdate)
	return babbagePParams, nil
}

func HardForkBabbage(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	alonzoPParams, ok := prevPParams.(*alonzo.AlonzoProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := babbage.UpgradePParams(*alonzoPParams)
	return &ret, nil
}

func CalculateEtaVBabbage(
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
	h, ok := block.Header().(*babbage.BabbageBlockHeader)
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
