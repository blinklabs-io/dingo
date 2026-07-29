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

package ledger

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalShelleyGenesisCfg returns the smallest test configuration that also
// provides the era shape and stability-window inputs used by HardForkSummary.
func minimalShelleyGenesisCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	return newTestEraHistoryCfg(t)
}

// TestHardForkSummary_SingleEra verifies the simple case: one era spanning
// multiple contiguous epochs, built from epochCache alone.
func TestHardForkSummary_SingleEra(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 0, StartSlot: 0, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
			{EpochId: 1, StartSlot: 100, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
			{EpochId: 2, StartSlot: 200, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
		},
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(250, []byte("tip"))},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.NotNil(t, sum)
	assert.Equal(t, time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC), sum.SystemStart)

	require.Len(t, sum.Eras, 1)
	era := sum.Eras[0]
	assert.Equal(t, uint(1), era.EraID)
	assert.Equal(t, hardfork.Bound{RelativeTime: 0, Slot: 0, Epoch: 0}, era.Start)
	require.NotNil(t, era.End, "current era should be safe-zone bounded")
	assert.Equal(t, uint64(26_200), era.End.Slot)
	assert.Equal(t, uint64(262), era.End.Epoch)
	assert.Equal(t, uint64(100), era.Params.EpochSize)
	assert.Equal(t, time.Second, era.Params.SlotLength)
	assert.Equal(t, uint64(25_920), era.Params.SafeZoneSlots)
	assert.Equal(t, uint64(25_920), era.Params.GenesisWindow)
}

// TestHardForkSummary_TwoEras verifies two contiguous eras produce a Summary
// with the first era bounded and the second (current) era safe-zone bounded.
func TestHardForkSummary_TwoEras(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			// Byron-ish: EraId=0, 20s slots, 100 slots/epoch
			{EpochId: 0, StartSlot: 0, SlotLength: 20_000, LengthInSlots: 100, EraId: 0},
			{EpochId: 1, StartSlot: 100, SlotLength: 20_000, LengthInSlots: 100, EraId: 0},
			// Shelley-ish: EraId=1, starts at slot 200, 1s slots, 432 slots/epoch
			{EpochId: 2, StartSlot: 200, SlotLength: 1000, LengthInSlots: 432, EraId: 1},
			{EpochId: 3, StartSlot: 632, SlotLength: 1000, LengthInSlots: 432, EraId: 1},
		},
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(700, []byte("tip"))},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 2)

	byron := sum.Eras[0]
	assert.Equal(t, uint(0), byron.EraID)
	assert.Equal(t, hardfork.Bound{RelativeTime: 0, Slot: 0, Epoch: 0}, byron.Start)
	require.NotNil(t, byron.End, "past era must be bounded")
	// Byron spans 2 epochs × 100 slots × 20s = 4000s, ending at slot 200, epoch 2.
	assert.Equal(t, hardfork.Bound{
		RelativeTime: 4000 * time.Second,
		Slot:         200,
		Epoch:        2,
	}, *byron.End)

	shelley := sum.Eras[1]
	assert.Equal(t, uint(1), shelley.EraID)
	// Shelley's Start must line up with Byron's End.
	assert.Equal(t, *byron.End, shelley.Start)
	require.NotNil(t, shelley.End, "current era should be safe-zone bounded")
	assert.Equal(t, uint64(26_984), shelley.End.Slot)
	assert.Equal(t, uint64(64), shelley.End.Epoch)
	assert.Equal(t, uint64(432), shelley.Params.EpochSize)
	assert.Equal(t, time.Second, shelley.Params.SlotLength)

	// End-to-end: the summary's SlotToTime should agree with the manual walk.
	// Slot 250 is 50 Shelley slots past era start (slot 200) → 4000s + 50s after SystemStart.
	got, err := sum.SlotToTime(250)
	require.NoError(t, err)
	assert.Equal(t, sum.SystemStart.Add(4000*time.Second+50*time.Second), got)
}

// TestHardForkSummary_EmptyCache errors.
func TestHardForkSummary_EmptyCache(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()
	_, err := ls.HardForkSummary()
	assert.Error(t, err)
}

// TestHardForkSummary_MissingShelleyGenesis tolerates a config without a
// Shelley genesis: SystemStart stays at the
// zero time. Callers that need wall-clock conversions must provide the
// genesis, but epoch-cache-only callers (like SlotToEpoch) can still get a
// meaningful Summary.
func TestHardForkSummary_MissingShelleyGenesis(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 0, StartSlot: 0, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
		},
		config: LedgerStateConfig{CardanoNodeConfig: &cardano.CardanoNodeConfig{}},
	}
	ls.publishSnapshotsLocked()
	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.True(t, sum.SystemStart.IsZero(),
		"missing shelley genesis ⇒ SystemStart is zero time")
	require.Len(t, sum.Eras, 1)
}

// TestHardForkSummary_CarriesTransitionInfo ensures the current transitionInfo
// is reflected in the returned Summary.
func TestHardForkSummary_CarriesTransitionInfo(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{EpochId: 5, StartSlot: 500, SlotLength: 1000, LengthInSlots: 100, EraId: 1},
		},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(7),
		currentTip:     ochainsync.Tip{Point: ocommon.NewPoint(550, []byte("tip"))},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	// A resolved zero safe zone is valid and must still flow through
	// BuildSummary so TransitionKnown can set the confirmed era boundary.
	shape := hardfork.Shape{
		SystemStart: time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC),
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     100,
				SlotLength:    time.Second,
				SafeZoneSlots: 0,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()
	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.Equal(t, hardfork.NewTransitionKnown(7), sum.Transition)
	require.Len(t, sum.Eras, 1)
	require.NotNil(t, sum.Eras[0].End)
	assert.Zero(t, sum.Eras[0].Params.SafeZoneSlots)
	assert.Equal(t, uint64(700), sum.Eras[0].End.Slot)
	assert.Equal(t, uint64(7), sum.Eras[0].End.Epoch)
}

func TestHardForkSummary_RejectsSlotPastSafeZone(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       500,
				StartSlot:     100_000,
				SlotLength:    1_000,
				LengthInSlots: 432_000,
				EraId:         eras.ConwayEraDesc.Id,
			},
		},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(200_000, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 1)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, uint64(532_000), sum.Eras[0].End.Slot)

	_, err = sum.SlotToEpoch(531_999)
	require.NoError(t, err)
	_, err = sum.SlotToEpoch(532_000)
	assert.ErrorIs(t, err, hardfork.ErrPastHorizon)
}

func TestHardForkSummary_TransitionImpossibleKeepsLiveForecastRolling(
	t *testing.T,
) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1_000,
				LengthInSlots: 500,
				EraId:         eras.ConwayEraDesc.Id,
			},
		},
		currentEra:     eras.ConwayEraDesc,
		transitionInfo: hardfork.NewTransitionImpossible(),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(455, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.Equal(t, hardfork.NewTransitionImpossible(), sum.Transition)
	require.Len(t, sum.Eras, 1)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, uint64(26_500), sum.Eras[0].End.Slot)

	_, err = sum.SlotToEpoch(500)
	require.NoError(t, err,
		"a known same-era epoch boundary must remain forecastable")
}
