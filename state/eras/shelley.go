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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

var ShelleyEraDesc = EraDesc{
	Id:                      shelley.EraIdShelley,
	Name:                    shelley.EraNameShelley,
	DecodePParamsFunc:       DecodePParamsShelley,
	DecodePParamsUpdateFunc: DecodePParamsUpdateShelley,
	PParamsUpdateFunc:       PParamsUpdateShelley,
	HardForkFunc:            HardForkShelley,
	EpochLengthFunc:         EpochLengthShelley,
	CalculateEtaVFunc:       CalculateEtaVShelley,
}

func DecodePParamsShelley(data []byte) (any, error) {
	var ret shelley.ShelleyProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func DecodePParamsUpdateShelley(data []byte) (any, error) {
	var ret shelley.ShelleyProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateShelley(currentPParams any, pparamsUpdate any) (any, error) {
	shelleyPParams, ok := currentPParams.(shelley.ShelleyProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	shelleyPParamsUpdate, ok := pparamsUpdate.(shelley.ShelleyProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	shelleyPParams.Update(&shelleyPParamsUpdate)
	return shelleyPParams, nil
}

func HardForkShelley(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams any,
) (any, error) {
	// There's no Byron protocol parameters to upgrade from, so this is mostly
	// a dummy call for consistency
	ret := shelley.UpgradePParams(nil)
	shelleyGenesis := nodeConfig.ShelleyGenesis()
	ret.UpdateFromGenesis(shelleyGenesis)
	return ret, nil
}

func EpochLengthShelley(
	nodeConfig *cardano.CardanoNodeConfig,
) (uint, uint, error) {
	shelleyGenesis := nodeConfig.ShelleyGenesis()
	return uint(shelleyGenesis.SlotLength * 1000),
		uint(shelleyGenesis.EpochLength),
		nil
}

func CalculateEtaVShelley(
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
	h, ok := block.Header().(*shelley.ShelleyBlockHeader)
	if !ok {
		return nil, fmt.Errorf("unexpected block type")
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
