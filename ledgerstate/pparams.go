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
	"fmt"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
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
	// Do not validate the previous payload as though it belonged to the
	// snapshot's era. At a hard fork GovState can carry a previous-epoch
	// value while the snapshot itself is already in the new era. The import
	// phase has the full era telescope and validates this raw payload against
	// the actual epoch whose row it would populate.
	return current, previous, nil
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
	if eraIndex < 0 {
		return fmt.Errorf("negative era index %d", eraIndex)
	}
	era := eras.GetEraById(
		uint(eraIndex),
	) //nolint:gosec // bounds checked above
	if era == nil {
		return fmt.Errorf("unknown era %d", eraIndex)
	}
	if era.DecodePParamsFunc == nil {
		return fmt.Errorf(
			"%s era does not define protocol parameters",
			era.Name,
		)
	}
	if _, err := era.DecodePParamsFunc(data); err != nil {
		return fmt.Errorf("decoding %s protocol parameters: %w", era.Name, err)
	}
	return nil
}
