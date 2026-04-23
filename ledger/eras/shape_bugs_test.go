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

package eras_test

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStabilityWindowForEra_RejectsActiveSlotsCoeffAbove1 verifies that
// StabilityWindowForEra treats ActiveSlotsCoeff > 1 as malformed.
//
// ActiveSlotsCoeff is the "active slot coefficient" f in the Praos
// stability-window formula ceil(3*k/f). The protocol requires f in (0, 1].
// An f > 1 silently shrinks the safe zone below the protocol floor — a bad
// genesis file should be rejected, not accepted with a subtle security
// regression.
func TestStabilityWindowForEra_RejectsActiveSlotsCoeffAbove1(t *testing.T) {
	shelley := `{
		"activeSlotsCoeff": 2.0,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelley)))
	_, err := eras.StabilityWindowForEra(cfg, eras.ShelleyEraDesc.Id)
	assert.Error(t, err,
		"StabilityWindowForEra must reject ActiveSlotsCoeff > 1 (would silently shrink safe zone)")
}
