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
	// BuildSummary so TransitionKnown can set the confirmed era boundary. Its
	// zero SystemStart must not replace the Shelley genesis value.
	shape := hardfork.Shape{
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
	assert.Equal(
		t,
		time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC),
		sum.SystemStart,
	)
	// A known transition bounds the current era at the announced epoch
	// boundary and appends an open successor era so the header forecast
	// horizon still covers the first post-boundary epoch (see HardForkSummary).
	require.Len(t, sum.Eras, 2)
	require.NotNil(t, sum.Eras[0].End)
	assert.Zero(t, sum.Eras[0].Params.SafeZoneSlots)
	assert.Equal(t, uint64(700), sum.Eras[0].End.Slot)
	assert.Equal(t, uint64(7), sum.Eras[0].End.Epoch)
	// The appended successor era is open (unbounded) and starts exactly at the
	// announced boundary, so SlotToEpoch resolves slots in the first
	// post-boundary epoch instead of returning ErrPastHorizon.
	assert.Equal(t, *sum.Eras[0].End, sum.Eras[1].Start)
	assert.Nil(t, sum.Eras[1].End)
	info, err := sum.SlotToEpoch(700)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), info.Epoch)
}

// TestHardForkSummary_KnownTransitionExtendsHeaderHorizon is a regression test
// for the header forecast-horizon deadlock: a pending hard-fork initiation arms
// TransitionKnown before an epoch boundary, BuildSummary bounds the current era
// at that boundary, and the header verification gate then rejected the first
// header of the post-boundary epoch as past-horizon, so the node could never
// apply the block that would consume the transition and extend era history.
// HardForkSummary must append an open successor era at the boundary so the
// first post-boundary epoch stays within the forecast horizon. Reproduces the
// musashi epoch 6 to 7 wedge in miniature.
func TestHardForkSummary_KnownTransitionExtendsHeaderHorizon(t *testing.T) {
	const (
		epochSize    = uint64(100)
		startEpoch   = uint64(4)
		startSlot    = uint64(400)
		knownEpoch   = uint64(7)
		boundarySlot = uint64(700) // first slot of epoch 7 (400 + (7-4)*100)
	)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       startEpoch,
			StartSlot:     startSlot,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         1,
		}},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(knownEpoch),
		currentTip:     ochainsync.Tip{Point: ocommon.NewPoint(688, []byte("tip"))},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	// A single modeled era: the ledger already occupies the last era, so the
	// appended successor era reuses the current era's params.
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    time.Second,
				SafeZoneSlots: 0,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)

	// The first block of the post-boundary epoch, and slots within it, must
	// resolve to that epoch rather than returning ErrPastHorizon.
	for _, slot := range []uint64{
		boundarySlot,
		boundarySlot + 10,
		boundarySlot + epochSize - 1,
	} {
		info, err := sum.SlotToEpoch(slot)
		require.NoErrorf(t, err, "slot %d must be within horizon", slot)
		assert.Equalf(t, knownEpoch, info.Epoch, "slot %d epoch", slot)
	}

	// The successor era is open and reuses the current (last modeled) era's
	// params; the bounded era still ends exactly at the announced boundary.
	require.Len(t, sum.Eras, 2)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, boundarySlot, sum.Eras[0].End.Slot)
	assert.Equal(t, *sum.Eras[0].End, sum.Eras[1].Start)
	assert.Nil(t, sum.Eras[1].End)
	assert.Equal(t, sum.Eras[0].EraID, sum.Eras[1].EraID)
	assert.Equal(t, sum.Eras[0].Params.EpochSize, sum.Eras[1].Params.EpochSize)
	assert.Equal(t, sum.Eras[0].Params.SlotLength, sum.Eras[1].Params.SlotLength)
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

func TestHardForkSummary_MainnetForecastBoundary(t *testing.T) {
	testCases := []struct {
		name          string
		tipSlot       uint64
		wantEndSlot   uint64
		nextEpochOpen bool
	}{
		{
			name:        "before nonce cutoff",
			tipSlot:     302_399,
			wantEndSlot: 432_000,
		},
		{
			name:          "at nonce cutoff",
			tipSlot:       302_400,
			wantEndSlot:   864_000,
			nextEpochOpen: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := minimalShelleyGenesisCfg(t)
			ls := &LedgerState{
				epochCache: []models.Epoch{{
					EpochId:       0,
					StartSlot:     0,
					SlotLength:    1_000,
					LengthInSlots: 432_000,
					EraId:         eras.ConwayEraDesc.Id,
				}},
				currentEra: eras.ConwayEraDesc,
				currentTip: ochainsync.Tip{
					Point: ocommon.NewPoint(
						testCase.tipSlot,
						[]byte("tip"),
					),
				},
				config: LedgerStateConfig{CardanoNodeConfig: cfg},
			}
			shape := hardfork.Shape{
				SystemStart: cfg.ShelleyGenesis().SystemStart,
				Eras: []hardfork.ShapeEntry{{
					EraID: eras.ConwayEraDesc.Id,
					Params: hardfork.EraParams{
						EpochSize:     432_000,
						SlotLength:    time.Second,
						SafeZoneSlots: 129_600,
						GenesisWindow: 129_600,
					},
				}},
			}
			ls.cachedShape.Store(&shape)
			ls.publishSnapshotsLocked()

			sum, err := ls.HardForkSummary()
			require.NoError(t, err)
			require.Len(t, sum.Eras, 1)
			require.NotNil(t, sum.Eras[0].End)
			assert.Equal(t, testCase.wantEndSlot, sum.Eras[0].End.Slot)

			_, err = ls.EpochInfo(1)
			if testCase.nextEpochOpen {
				require.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, hardfork.ErrPastHorizon)
			}

			// Arbitrary time queries stay bounded at the exclusive end.
			endTime := sum.SystemStart.Add(
				time.Duration(testCase.wantEndSlot) * time.Second,
			)
			_, err = ls.TimeToSlot(endTime)
			assert.ErrorIs(t, err, hardfork.ErrPastHorizon)

			// Operational near-now timing remains available to a node whose
			// ledger is catching up from behind the forecast.
			currentSlot, err := ls.TimeToSlot(time.Now())
			require.NoError(t, err)
			assert.Greater(t, currentSlot, testCase.wantEndSlot)
		})
	}
}

func TestEpochInfoUsesMaterializedEpochPastForecast(t *testing.T) {
	cfg := minimalShelleyGenesisCfg(t)
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1_000,
				LengthInSlots: 100,
				EraId:         eras.ShelleyEraDesc.Id,
			},
			{
				EpochId:       10,
				StartSlot:     1_000,
				SlotLength:    1_000,
				LengthInSlots: 100,
				EraId:         eras.ShelleyEraDesc.Id,
			},
		},
		currentEra: eras.ShelleyEraDesc,
		config:     LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	shape := hardfork.Shape{
		SystemStart: cfg.ShelleyGenesis().SystemStart,
		Eras: []hardfork.ShapeEntry{{
			EraID: eras.ShelleyEraDesc.Id,
			Params: hardfork.EraParams{
				EpochSize:     100,
				SlotLength:    time.Second,
				SafeZoneSlots: 1,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	_, err = sum.EpochInfo(10)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)

	info, err := ls.EpochInfo(10)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), info.StartSlot)
	assert.Equal(t, uint(100), info.LengthInSlots)
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
