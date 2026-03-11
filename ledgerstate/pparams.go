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
	"strings"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
)

func extractPParamsData(
	eraIndex int,
	govStateData cbor.RawMessage,
) (cbor.RawMessage, error) {
	if len(govStateData) == 0 {
		return nil, nil
	}
	govFields, err := decodeRawElements(govStateData)
	if err != nil {
		return nil, fmt.Errorf("decoding GovState: %w", err)
	}

	tried := make(map[int]struct{}, len(govFields))
	var errs []string
	for _, idx := range pparamsCandidateIndexes(eraIndex, len(govFields)) {
		if idx < 0 || idx >= len(govFields) || len(govFields[idx]) == 0 {
			continue
		}
		tried[idx] = struct{}{}
		if err := validatePParamsData(eraIndex, govFields[idx]); err == nil {
			return govFields[idx], nil
		} else {
			errs = append(errs, fmt.Sprintf("field %d: %v", idx, err))
		}
	}
	for idx, field := range govFields {
		if len(field) == 0 {
			continue
		}
		if _, ok := tried[idx]; ok {
			continue
		}
		if err := validatePParamsData(eraIndex, field); err == nil {
			return field, nil
		} else {
			errs = append(errs, fmt.Sprintf("field %d: %v", idx, err))
		}
	}
	if len(errs) == 0 {
		return nil, nil
	}
	return nil, fmt.Errorf(
		"detecting %s protocol parameters: %s",
		EraName(eraIndex),
		strings.Join(errs, "; "),
	)
}

func pparamsCandidateIndexes(eraIndex, fieldCount int) []int {
	candidates := make([]int, 0, min(fieldCount, 4))
	if eraIndex >= EraConway {
		candidates = append(candidates, 3, 4, 5, 2)
	} else {
		candidates = append(candidates, 2, 3, 4, 5)
	}
	return candidates
}

func validatePParamsData(eraIndex int, data []byte) error {
	if eraIndex < 0 {
		return fmt.Errorf("negative era index %d", eraIndex)
	}
	era := eras.GetEraById(uint(eraIndex)) //nolint:gosec // bounds checked above
	if era == nil {
		return fmt.Errorf("unknown era %d", eraIndex)
	}
	if era.DecodePParamsFunc == nil {
		return fmt.Errorf("%s era does not define protocol parameters", era.Name)
	}
	if _, err := era.DecodePParamsFunc(data); err != nil {
		return fmt.Errorf("decoding %s protocol parameters: %w", era.Name, err)
	}
	return nil
}
