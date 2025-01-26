// Copyright 2024 Blink Labs Software
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
}

func DecodePParamsConway(data []byte) (any, error) {
	var ret conway.ConwayProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func DecodePParamsUpdateConway(data []byte) (any, error) {
	var ret conway.ConwayProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateConway(currentPParams any, pparamsUpdate any) (any, error) {
	conwayPParams, ok := currentPParams.(conway.ConwayProtocolParameters)
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
	prevPParams any,
) (any, error) {
	babbagePParams, ok := prevPParams.(babbage.BabbageProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := conway.UpgradePParams(babbagePParams)
	conwayGenesis := nodeConfig.ConwayGenesis()
	ret.UpdateFromGenesis(conwayGenesis)
	return ret, nil
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
		return nil, fmt.Errorf("unexpected block type")
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
