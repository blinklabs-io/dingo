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
	"math/big"

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
	CertDepositFunc:         CertDepositShelley,
	ValidateTxFunc:          ValidateTxShelley,
}

func DecodePParamsShelley(data []byte) (lcommon.ProtocolParameters, error) {
	var ret shelley.ShelleyProtocolParameters
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func DecodePParamsUpdateShelley(data []byte) (any, error) {
	var ret shelley.ShelleyProtocolParameterUpdate
	if _, err := cbor.Decode(data, &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func PParamsUpdateShelley(
	currentPParams lcommon.ProtocolParameters,
	pparamsUpdate any,
) (lcommon.ProtocolParameters, error) {
	shelleyPParams, ok := currentPParams.(*shelley.ShelleyProtocolParameters)
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
	prevPParams lcommon.ProtocolParameters,
) (lcommon.ProtocolParameters, error) {
	// There's no Byron protocol parameters to upgrade from, so this is mostly
	// a dummy call for consistency
	ret := shelley.UpgradePParams(nil)
	shelleyGenesis := nodeConfig.ShelleyGenesis()
	if err := ret.UpdateFromGenesis(shelleyGenesis); err != nil {
		return nil, err
	}
	return &ret, nil
}

func EpochLengthShelley(
	nodeConfig *cardano.CardanoNodeConfig,
) (uint, uint, error) {
	shelleyGenesis := nodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0, 0, errors.New("unable to get shelley genesis")
	}
	// These are known to be within uint range
	// #nosec G115
	slotLengthMs := new(big.Int).Div(
		// Slot length ratio numerator times 1000 (seconds to milliseconds)
		new(big.Int).Mul(
			shelleyGenesis.SlotLength.Num(),
			big.NewInt(1_000),
		),
		// Divided by slot length ratio denominator
		shelleyGenesis.SlotLength.Denom(),
	).Uint64()
	// #nosec G115
	return uint(slotLengthMs),
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

func CertDepositShelley(
	cert lcommon.Certificate,
	pp lcommon.ProtocolParameters,
) (uint64, error) {
	tmpPparams, ok := pp.(*shelley.ShelleyProtocolParameters)
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

func ValidateTxShelley(
	tx lcommon.Transaction,
	slot uint64,
	ls lcommon.LedgerState,
	pp lcommon.ProtocolParameters,
) error {
	errs := []error{}
	for _, validationFunc := range shelley.UtxoValidationRules {
		errs = append(
			errs,
			validationFunc(tx, slot, ls, pp),
		)
	}
	return errors.Join(errs...)
}
