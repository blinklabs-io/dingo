// Copyright 2026 Blink Labs Software
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

package ledgerstate

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
)

func extractPParamsData(
	eraIndex int,
	govStateData cbor.RawMessage,
) (cbor.RawMessage, cbor.RawMessage, error) {
	if len(govStateData) == 0 {
		return nil, nil, nil
	}
	govFields, err := decodeRawElements(govStateData)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding GovState: %w", err)
	}

	currentIndex, previousIndex := pparamsFieldIndexes(eraIndex)
	current, err := protocolParametersField(
		eraIndex, govFields, currentIndex, "current",
	)
	if err != nil {
		return nil, nil, err
	}
	var previous cbor.RawMessage
	if previousIndex >= 0 && previousIndex < len(govFields) {
		previous = govFields[previousIndex]
	}
	// The previous payload is encoded in the snapshot's era even when the
	// previous epoch belongs to an earlier one; previousPParamsForEra
	// converts it once the import knows that epoch's era.
	return current, previous, nil
}

// previousPParamsForEra returns the snapshot's previous protocol parameters
// encoded for the era of the epoch they were in force over.
//
// The ledger types prevPParams by the snapshot's own era: a hard fork
// translates it together with the current parameters (translateGovState in
// Cardano.Ledger.Conway.Translation), so in the first epoch of a new era the
// payload describes a previous-era epoch in the new era's encoding. That
// epoch's row is recovered with the ledger's own downgrade, one era at a time.
// Only the downgrades the ledger defines without extra inputs are applied
// (downgradeConwayPParams, downgradeDijkstraPParams): each keeps every field
// of the older era, including the reward inputs startStep reads from
// prevPParams. Any other step needs values the snapshot no longer carries --
// downgradeBabbagePParams takes d and extraEntropy as arguments -- and is an
// error rather than a guess.
func previousPParamsForEra(
	snapshotEra int,
	payload []byte,
	era int,
) ([]byte, error) {
	if len(payload) == 0 {
		return nil, errors.New(
			"the snapshot carries no previous protocol parameters",
		)
	}
	decoded, err := decodePParamsData(snapshotEra, payload)
	if err != nil {
		return nil, fmt.Errorf("previous protocol parameters: %w", err)
	}
	if era == snapshotEra {
		return payload, nil
	}
	if era > snapshotEra {
		return nil, fmt.Errorf(
			"previous-epoch era %s is later than snapshot era %s",
			EraName(era), EraName(snapshotEra),
		)
	}
	var params any = decoded
	for from := snapshotEra; from > era; from-- {
		params, err = downgradePParams(from, params)
		if err != nil {
			return nil, err
		}
	}
	encoded, err := cbor.Encode(params)
	if err != nil {
		return nil, fmt.Errorf(
			"encoding downgraded %s protocol parameters: %w",
			EraName(era), err,
		)
	}
	if err := validatePParamsData(era, encoded); err != nil {
		return nil, fmt.Errorf(
			"downgraded previous protocol parameters: %w", err,
		)
	}
	return encoded, nil
}

func downgradePParams(from int, params any) (any, error) {
	switch {
	case from == int(eras.DijkstraEraDesc.Id):
		p, ok := params.(*dijkstra.DijkstraProtocolParameters)
		if !ok {
			return nil, fmt.Errorf(
				"downgrading Dijkstra protocol parameters: got %T", params,
			)
		}
		ret := p.ConwayProtocolParameters
		return &ret, nil
	case from == EraConway:
		p, ok := params.(*conway.ConwayProtocolParameters)
		if !ok {
			return nil, fmt.Errorf(
				"downgrading Conway protocol parameters: got %T", params,
			)
		}
		return &babbage.BabbageProtocolParameters{
			MinFeeA:              p.MinFeeA,
			MinFeeB:              p.MinFeeB,
			MaxBlockBodySize:     p.MaxBlockBodySize,
			MaxTxSize:            p.MaxTxSize,
			MaxBlockHeaderSize:   p.MaxBlockHeaderSize,
			KeyDeposit:           p.KeyDeposit,
			PoolDeposit:          p.PoolDeposit,
			MaxEpoch:             p.MaxEpoch,
			NOpt:                 p.NOpt,
			A0:                   p.A0,
			Rho:                  p.Rho,
			Tau:                  p.Tau,
			ProtocolMajor:        p.ProtocolVersion.Major,
			ProtocolMinor:        p.ProtocolVersion.Minor,
			MinPoolCost:          p.MinPoolCost,
			AdaPerUtxoByte:       p.AdaPerUtxoByte,
			CostModels:           p.CostModels,
			ExecutionCosts:       p.ExecutionCosts,
			MaxTxExUnits:         p.MaxTxExUnits,
			MaxBlockExUnits:      p.MaxBlockExUnits,
			MaxValueSize:         p.MaxValueSize,
			CollateralPercentage: p.CollateralPercentage,
			MaxCollateralInputs:  p.MaxCollateralInputs,
		}, nil
	default:
		return nil, fmt.Errorf(
			"the ledger's %s-to-%s protocol parameter downgrade needs values the snapshot does not carry",
			EraName(from), EraName(from-1),
		)
	}
}

func pparamsFieldIndexes(eraIndex int) (current int, previous int) {
	if eraIndex >= EraConway {
		return 3, 4
	}
	return 2, 3
}

func protocolParametersField(
	eraIndex int,
	fields [][]byte,
	index int,
	name string,
) (cbor.RawMessage, error) {
	if index < 0 || index >= len(fields) || len(fields[index]) == 0 {
		return nil, nil
	}
	if err := validatePParamsData(eraIndex, fields[index]); err != nil {
		return nil, fmt.Errorf(
			"validating %s %s protocol parameters in GovState field %d: %w",
			name,
			EraName(eraIndex),
			index,
			err,
		)
	}
	return fields[index], nil
}

func validatePParamsData(eraIndex int, data []byte) error {
	_, err := decodePParamsData(eraIndex, data)
	return err
}

func decodePParamsData(
	eraIndex int,
	data []byte,
) (lcommon.ProtocolParameters, error) {
	if eraIndex < 0 {
		return nil, fmt.Errorf("negative era index %d", eraIndex)
	}
	era := eras.GetEraById(
		uint(eraIndex),
	) //nolint:gosec // bounds checked above
	if era == nil {
		return nil, fmt.Errorf("unknown era %d", eraIndex)
	}
	if era.DecodePParamsFunc == nil {
		return nil, fmt.Errorf(
			"%s era does not define protocol parameters",
			era.Name,
		)
	}
	decoded, err := era.DecodePParamsFunc(data)
	if err != nil {
		return nil, fmt.Errorf(
			"decoding %s protocol parameters: %w", era.Name, err,
		)
	}
	if err := eras.ValidateProtocolParameterDomains(decoded); err != nil {
		return nil, fmt.Errorf(
			"validating %s protocol parameters: %w", era.Name, err,
		)
	}
	return decoded, nil
}
