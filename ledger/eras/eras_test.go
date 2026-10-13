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
	known := []struct {
		name string
		id   uint
	}{
		{name: "Byron", id: 0},
		{name: "Shelley", id: 1},
		{name: "Allegra", id: 2},
		{name: "Mary", id: 3},
		{name: "Alonzo", id: 4},
		{name: "Babbage", id: 5},
		{name: "Conway", id: 6},
		{name: "Dijkstra", id: 7},
	}
	for i, tc := range known {
		t.Run(tc.name, func(t *testing.T) {
			got := eras.GetEraById(tc.id)
			if assert.NotNil(t, got) {
				assert.Equal(t, tc.id, got.Id)
				assert.Equal(t, tc.name, got.Name)
				assert.Same(t, &eras.ErasWithDijkstra[i], got)
			}
		})
	}
	for _, eraID := range []uint{8, 9, 999} {
		assert.Nil(t, eras.GetEraById(eraID), "unknown era ID %d", eraID)
	}
}

func TestActiveEras_DijkstraGate(t *testing.T) {
	defaultEras := eras.ActiveEras(false)
	assert.Equal(t, eras.Eras, defaultEras)
	assert.Nil(t, eras.GetEraByIdIn(defaultEras, eras.DijkstraEraDesc.Id))

	dijkstraEras := eras.ActiveEras(true)
	assert.Equal(t, eras.ErasWithDijkstra, dijkstraEras)
	result := eras.GetEraByIdIn(dijkstraEras, eras.DijkstraEraDesc.Id)
	assert.NotNil(t, result)
	assert.Equal(t, eras.DijkstraEraDesc.Name, result.Name)
}

func TestEraForVersionIn_DijkstraGate(t *testing.T) {
	_, found := eras.EraForVersionIn(eras.ActiveEras(false), 12)
	assert.False(t, found)

	result, found := eras.EraForVersionIn(eras.ActiveEras(true), 12)
	assert.True(t, found)
	assert.Equal(t, eras.DijkstraEraDesc.Id, result.Id)
}

func TestIsCompatibleEra(t *testing.T) {
	tests := []struct {
		name       string
		txEraId    uint
		ledgerEra  uint
		compatible bool
	}{
		{
			name:       "two eras back: Alonzo TX in Conway ledger",
			txEraId:    eras.AlonzoEraDesc.Id,
			ledgerEra:  eras.ConwayEraDesc.Id,
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
