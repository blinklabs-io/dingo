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

package eras_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blinklabs-io/dingo/ledger/eras"
)

func TestGetEraById(t *testing.T) {
	tests := []struct {
		name     string
		eraId    uint
		expected *eras.EraDesc
		wantNil  bool
	}{
		{
			name:     "Byron era (ID=0)",
			eraId:    0,
			expected: &eras.ByronEraDesc,
			wantNil:  false,
		},
		{
			name:     "Shelley era (ID=1)",
			eraId:    1,
			expected: &eras.ShelleyEraDesc,
			wantNil:  false,
		},
		{
			name:     "Allegra era (ID=2)",
			eraId:    2,
			expected: &eras.AllegraEraDesc,
			wantNil:  false,
		},
		{
			name:     "Mary era (ID=3)",
			eraId:    3,
			expected: &eras.MaryEraDesc,
			wantNil:  false,
		},
		{
			name:     "Alonzo era (ID=4)",
			eraId:    4,
			expected: &eras.AlonzoEraDesc,
			wantNil:  false,
		},
		{
			name:     "Babbage era (ID=5)",
			eraId:    5,
			expected: &eras.BabbageEraDesc,
			wantNil:  false,
		},
		{
			name:     "Conway era (ID=6)",
			eraId:    6,
			expected: &eras.ConwayEraDesc,
			wantNil:  false,
		},
		{
			name:    "Invalid era ID (does not exist)",
			eraId:   999,
			wantNil: true,
		},
		{
			name:    "Gap era ID (ID=7, non-existent era)",
			eraId:   7,
			wantNil: true,
		},
		{
			name:    "Gap era ID (ID=8, non-existent era)",
			eraId:   8,
			wantNil: true,
		},
		{
			name:    "Gap era ID (ID=10, non-existent era)",
			eraId:   10,
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := eras.GetEraById(tt.eraId)

			if tt.wantNil {
				if result != nil {
					t.Errorf(
						"GetEraById(%d) expected nil, got %v",
						tt.eraId,
						result,
					)
				}
				return
			}

			if result == nil {
				t.Errorf(
					"GetEraById(%d) expected era descriptor, got nil",
					tt.eraId,
				)
				return
			}

			if result.Id != tt.expected.Id {
				t.Errorf(
					"GetEraById(%d) ID mismatch: expected %d, got %d",
					tt.eraId,
					tt.expected.Id,
					result.Id,
				)
			}

			if result.Name != tt.expected.Name {
				t.Errorf(
					"GetEraById(%d) Name mismatch: expected %s, got %s",
					tt.eraId,
					tt.expected.Name,
					result.Name,
				)
			}
		})
	}
}

// TestGetEraById_HandlesGapsInEraIds tests that the function properly handles
// the gaps in era IDs (like missing 5, 6, 8) without panicking
func TestGetEraById_HandlesGapsInEraIds(t *testing.T) {
	// These era IDs don't exist and should return nil
	gapIds := []uint{7, 8, 9, 10, 100, 1000}

	for _, eraId := range gapIds {
		t.Run(t.Name(), func(t *testing.T) {
			// This should not panic and should return nil
			result := eras.GetEraById(eraId)
			if result != nil {
				t.Errorf(
					"GetEraById(%d) expected nil for gap era ID, got %v",
					eraId,
					result,
				)
			}
		})
	}
}

// TestGetEraById_AllKnownEras verifies that all eras in the Eras array
// can be retrieved by their actual ID (not array index)
func TestGetEraById_AllKnownEras(t *testing.T) {
	for i, era := range eras.Eras {
		t.Run(era.Name, func(t *testing.T) {
			result := eras.GetEraById(era.Id)

			if result == nil {
				t.Errorf(
					"GetEraById(%d) for %s era returned nil",
					era.Id,
					era.Name,
				)
				return
			}

			if result.Id != era.Id {
				t.Errorf(
					"GetEraById(%d) ID mismatch: expected %d, got %d",
					era.Id,
					era.Id,
					result.Id,
				)
			}

			if result.Name != era.Name {
				t.Errorf(
					"GetEraById(%d) Name mismatch: expected %s, got %s",
					era.Id,
					era.Name,
					result.Name,
				)
			}

			// Verify it's pointing to the same era descriptor
			if result != &eras.Eras[i] {
				t.Errorf(
					"GetEraById(%d) returned different era descriptor than expected",
					era.Id,
				)
			}
		})
	}
}

func TestIsCompatibleEra(t *testing.T) {
	tests := []struct {
		name       string
		txEraId    uint
		ledgerEra  uint
		compatible bool
	}{
		{
			name:       "same era: Byron in Byron",
			txEraId:    eras.ByronEraDesc.Id,
			ledgerEra:  eras.ByronEraDesc.Id,
			compatible: true,
		},
		{
			name:       "same era: Conway in Conway",
			txEraId:    eras.ConwayEraDesc.Id,
			ledgerEra:  eras.ConwayEraDesc.Id,
			compatible: true,
		},
		{
			name:       "same era: Babbage in Babbage",
			txEraId:    eras.BabbageEraDesc.Id,
			ledgerEra:  eras.BabbageEraDesc.Id,
			compatible: true,
		},
		{
			name:       "previous era: Babbage TX in Conway ledger",
			txEraId:    eras.BabbageEraDesc.Id,
			ledgerEra:  eras.ConwayEraDesc.Id,
			compatible: true,
		},
		{
			name:       "previous era: Alonzo TX in Babbage ledger",
			txEraId:    eras.AlonzoEraDesc.Id,
			ledgerEra:  eras.BabbageEraDesc.Id,
			compatible: true,
		},
		{
			name:       "previous era: Byron TX in Shelley ledger",
			txEraId:    eras.ByronEraDesc.Id,
			ledgerEra:  eras.ShelleyEraDesc.Id,
			compatible: true,
		},
		{
			name:       "previous era: Shelley TX in Allegra ledger",
			txEraId:    eras.ShelleyEraDesc.Id,
			ledgerEra:  eras.AllegraEraDesc.Id,
			compatible: true,
		},
		{
			name:       "two eras back: Alonzo TX in Conway ledger",
			txEraId:    eras.AlonzoEraDesc.Id,
			ledgerEra:  eras.ConwayEraDesc.Id,
			compatible: false,
		},
		{
			name:       "two eras back: Mary TX in Babbage ledger",
			txEraId:    eras.MaryEraDesc.Id,
			ledgerEra:  eras.BabbageEraDesc.Id,
			compatible: false,
		},
		{
			name:       "future era: Conway TX in Babbage ledger",
			txEraId:    eras.ConwayEraDesc.Id,
			ledgerEra:  eras.BabbageEraDesc.Id,
			compatible: false,
		},
		{
			name:       "future era: Babbage TX in Alonzo ledger",
			txEraId:    eras.BabbageEraDesc.Id,
			ledgerEra:  eras.AlonzoEraDesc.Id,
			compatible: false,
		},
		{
			name:       "no previous era for Byron",
			txEraId:    999,
			ledgerEra:  eras.ByronEraDesc.Id,
			compatible: false,
		},
		{
			name:       "unknown tx era",
			txEraId:    999,
			ledgerEra:  eras.ConwayEraDesc.Id,
			compatible: false,
		},
		{
			name:       "unknown ledger era",
			txEraId:    eras.ConwayEraDesc.Id,
			ledgerEra:  999,
			compatible: false,
		},
		{
			name:       "both unknown but equal",
			txEraId:    999,
			ledgerEra:  999,
			compatible: true,
		},
		{
			name:       "three eras back: Byron TX in Alonzo ledger",
			txEraId:    eras.ByronEraDesc.Id,
			ledgerEra:  eras.AlonzoEraDesc.Id,
			compatible: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := eras.IsCompatibleEra(
				tc.txEraId,
				tc.ledgerEra,
			)
			assert.Equal(
				t,
				tc.compatible,
				result,
				"IsCompatibleEra(%d, %d)",
				tc.txEraId,
				tc.ledgerEra,
			)
		})
	}
}

// TestIsCompatibleEra_AllAdjacentPairs verifies that
// every consecutive pair of eras is compatible (era-1
// is accepted in the next era).
func TestIsCompatibleEra_AllAdjacentPairs(t *testing.T) {
	for i := 1; i < len(eras.Eras); i++ {
		prevEra := eras.Eras[i-1]
		currEra := eras.Eras[i]
		t.Run(
			prevEra.Name+" in "+currEra.Name,
			func(t *testing.T) {
				// Previous era TX in current era ledger
				assert.True(
					t,
					eras.IsCompatibleEra(
						prevEra.Id,
						currEra.Id,
					),
					"%s TX should be compatible with %s ledger",
					prevEra.Name,
					currEra.Name,
				)
				// Current era TX in current era ledger
				assert.True(
					t,
					eras.IsCompatibleEra(
						currEra.Id,
						currEra.Id,
					),
					"%s TX should be compatible with %s ledger",
					currEra.Name,
					currEra.Name,
				)
				// Current era TX in previous era ledger
				// (future era - not compatible)
				assert.False(
					t,
					eras.IsCompatibleEra(
						currEra.Id,
						prevEra.Id,
					),
					"%s TX should NOT be compatible with %s ledger",
					currEra.Name,
					prevEra.Name,
				)
			},
		)
	}
}
