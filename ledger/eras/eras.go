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
	DecodePParamsFunc       func([]byte) (lcommon.ProtocolParameters, error)
	DecodePParamsUpdateFunc func([]byte) (any, error)
	PParamsUpdateFunc       func(lcommon.ProtocolParameters, any) (lcommon.ProtocolParameters, error)
	HardForkFunc            func(*cardano.CardanoNodeConfig, lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error)
	EpochLengthFunc         func(*cardano.CardanoNodeConfig) (uint, uint, error)
	CalculateEtaVFunc       func(*cardano.CardanoNodeConfig, []byte, ledger.Block) ([]byte, error)
	CertDepositFunc         func(lcommon.Certificate, lcommon.ProtocolParameters) (uint64, error)
	ValidateTxFunc          func(lcommon.Transaction, uint64, lcommon.LedgerState, lcommon.ProtocolParameters) error
	EvaluateTxFunc          func(tx lcommon.Transaction, ls lcommon.LedgerState, pp lcommon.ProtocolParameters) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error)
	Name                    string
	Id                      uint
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

var ProtocolMajorVersionToEra = map[uint]EraDesc{
	0:  ByronEraDesc,
	1:  ByronEraDesc,
	2:  ShelleyEraDesc,
	3:  AllegraEraDesc,
	4:  MaryEraDesc,
	5:  AlonzoEraDesc,
	6:  AlonzoEraDesc,
	7:  BabbageEraDesc,
	8:  BabbageEraDesc,
	9:  ConwayEraDesc,
	10: ConwayEraDesc,
}
