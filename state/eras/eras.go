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
	"github.com/blinklabs-io/dingo/config/cardano"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type EraDesc struct {
	Id                      uint
	Name                    string
	DecodePParamsFunc       func([]byte) (lcommon.ProtocolParameters, error)
	DecodePParamsUpdateFunc func([]byte) (any, error)
	PParamsUpdateFunc       func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error)
	HardForkFunc            func(*cardano.CardanoNodeConfig, lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error)
	EpochLengthFunc         func(*cardano.CardanoNodeConfig) (uint, uint, error)
	CalculateEtaVFunc       func(*cardano.CardanoNodeConfig, []byte, ledger.Block) ([]byte, error)
}

var Eras = []EraDesc{
	ByronEraDesc,
	ShelleyEraDesc,
	AllegraEraDesc,
	MaryEraDesc,
	AlonzoEraDesc,
	BabbageEraDesc,
	ConwayEraDesc,
}
