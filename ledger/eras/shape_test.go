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
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Full mainnet-ish config with both genesis files present.
func newTestCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	byron := `{"blockVersionData":{"slotDuration":"20000"},"protocolConsts":{"k":432}}`
	shelley := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadByronGenesisFromReader(strings.NewReader(byron)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelley)))
	return cfg
}

// Config with only Shelley genesis loaded (no Byron).
func newShelleyOnlyCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	shelley := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelley)))
	return cfg
}

// ---------------------------------------------------------------- StabilityWindow

// Byron: window = 2k = 864 for k=432.
func TestStabilityWindowForEra_Byron(t *testing.T) {
	cfg := newTestCfg(t)
	w, err := eras.StabilityWindowForEra(cfg, eras.ByronEraDesc.Id)
	require.NoError(t, err)
	assert.Equal(t, uint64(864), w)
}

// Shelley: window = ceil(3k/f) = ceil(3*432/0.05) = 25_920.
func TestStabilityWindowForEra_Shelley(t *testing.T) {
	cfg := newTestCfg(t)
	w, err := eras.StabilityWindowForEra(cfg, eras.ShelleyEraDesc.Id)
	require.NoError(t, err)
	assert.Equal(t, uint64(25_920), w)
}

// Post-Shelley eras use the same computation.
func TestStabilityWindowForEra_Conway(t *testing.T) {
	cfg := newTestCfg(t)
	w, err := eras.StabilityWindowForEra(cfg, eras.ConwayEraDesc.Id)
	require.NoError(t, err)
	assert.Equal(t, uint64(25_920), w)
}

// Byron era but no Byron genesis in config — error.
func TestStabilityWindowForEra_Byron_MissingGenesis(t *testing.T) {
	cfg := newShelleyOnlyCfg(t)
	_, err := eras.StabilityWindowForEra(cfg, eras.ByronEraDesc.Id)
	assert.Error(t, err)
}

// Nil config — error.
func TestStabilityWindowForEra_NilConfig(t *testing.T) {
	_, err := eras.StabilityWindowForEra(nil, eras.ShelleyEraDesc.Id)
	assert.Error(t, err)
}

// ---------------------------------------------------------------- BuildEraParams

func TestBuildEraParams_Byron(t *testing.T) {
	cfg := newTestCfg(t)
	p, err := eras.BuildEraParams(cfg, eras.ByronEraDesc)
	require.NoError(t, err)
	assert.Equal(t, uint64(4320), p.EpochSize) // k=432 → 10k = 4320 slots
	assert.Equal(t, 20*time.Second, p.SlotLength)
	assert.Equal(t, uint64(864), p.SafeZoneSlots)
}

func TestBuildEraParams_Shelley(t *testing.T) {
	cfg := newTestCfg(t)
	p, err := eras.BuildEraParams(cfg, eras.ShelleyEraDesc)
	require.NoError(t, err)
	assert.Equal(t, uint64(432_000), p.EpochSize)
	assert.Equal(t, time.Second, p.SlotLength)
	assert.Equal(t, uint64(25_920), p.SafeZoneSlots)
}

// Missing genesis for the era — error.
func TestBuildEraParams_MissingGenesis(t *testing.T) {
	cfg := newShelleyOnlyCfg(t)
	_, err := eras.BuildEraParams(cfg, eras.ByronEraDesc)
	assert.Error(t, err)
}

// ---------------------------------------------------------------- BuildShape

func TestBuildShape_OK(t *testing.T) {
	cfg := newTestCfg(t)
	shape, err := eras.BuildShape(cfg)
	require.NoError(t, err)

	// SystemStart comes from Shelley genesis.
	expectedStart := time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expectedStart, shape.SystemStart)

	// All 7 Cardano eras present in declaration order.
	require.Len(t, shape.Eras, 7)
	assert.Equal(t, "Byron", shape.Eras[0].EraName)
	assert.Equal(t, "Shelley", shape.Eras[1].EraName)
	assert.Equal(t, "Conway", shape.Eras[6].EraName)

	// Version ranges derived from ProtocolMajorVersionToEra.
	type vr struct{ min, max uint }
	want := map[string]vr{
		"Byron":   {0, 1},
		"Shelley": {2, 2},
		"Allegra": {3, 3},
		"Mary":    {4, 4},
		"Alonzo":  {5, 6},
		"Babbage": {7, 8},
		"Conway":  {9, 10},
	}
	for _, e := range shape.Eras {
		w, ok := want[e.EraName]
		require.True(t, ok, "unexpected era %q", e.EraName)
		assert.Equal(t, w.min, e.MinMajorVersion, "%s MinMajorVersion", e.EraName)
		assert.Equal(t, w.max, e.MaxMajorVersion, "%s MaxMajorVersion", e.EraName)
	}

	assert.NoError(t, shape.Validate(), "BuildShape must produce a valid Shape")
}

// EraForVersion from the resulting Shape matches the legacy EraForVersion lookup.
func TestBuildShape_EraForVersionMatchesLegacy(t *testing.T) {
	cfg := newTestCfg(t)
	shape, err := eras.BuildShape(cfg)
	require.NoError(t, err)

	for v := uint(0); v <= 10; v++ {
		got, found := shape.EraForVersion(v)
		require.True(t, found, "version %d should resolve", v)
		legacy := eras.ProtocolMajorVersionToEra[v]
		assert.Equal(t, legacy.Id, got.EraID, "version %d EraID", v)
	}

	_, found := shape.EraForVersion(99)
	assert.False(t, found)
}

func TestBuildShape_MissingShelleyGenesis(t *testing.T) {
	cfg := &cardano.CardanoNodeConfig{}
	_, err := eras.BuildShape(cfg)
	assert.Error(t, err)
}
