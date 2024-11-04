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
	"fmt"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/node/config/cardano"
)

var AllegraEraDesc = EraDesc{
	Id:                      allegra.EraIdAllegra,
	Name:                    allegra.EraNameAllegra,
	DecodePParamsFunc:       DecodePParamsAllegra,
	DecodePParamsUpdateFunc: DecodePParamsUpdateAllegra,
	PParamsUpdateFunc:       PParamsUpdateAllegra,
	HardForkFunc:            HardForkAllegra,
	EpochLengthFunc:         EpochLengthShelley,
}

func DecodePParamsAllegra(data []byte) (any, error) {
	var ret allegra.AllegraProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func DecodePParamsUpdateAllegra(data []byte) (any, error) {
	var ret allegra.AllegraProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateAllegra(currentPParams any, pparamsUpdate any) (any, error) {
	allegraPParams, ok := currentPParams.(allegra.AllegraProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"current PParams (%T) is not expected type",
			currentPParams,
		)
	}
	allegraPParamsUpdate, ok := pparamsUpdate.(allegra.AllegraProtocolParameterUpdate)
	if !ok {
		return nil, fmt.Errorf(
			"PParams update (%T) is not expected type",
			pparamsUpdate,
		)
	}
	allegraPParams.Update(&allegraPParamsUpdate)
	return allegraPParams, nil
}

func HardForkAllegra(
	nodeConfig *cardano.CardanoNodeConfig,
	prevPParams any,
) (any, error) {
	shelleyPParams, ok := prevPParams.(shelley.ShelleyProtocolParameters)
	if !ok {
		return nil, fmt.Errorf(
			"previous PParams (%T) are not expected type",
			prevPParams,
		)
	}
	ret := allegra.UpgradePParams(shelleyPParams)
	return ret, nil
}
