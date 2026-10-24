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
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type forecastChildBlock struct {
	gledger.Block
	slot   uint64
	number uint64
	prev   lcommon.Blake2b256
	era    lcommon.Era
}

func (b *forecastChildBlock) SlotNumber() uint64 { return b.slot }

func (b *forecastChildBlock) BlockNumber() uint64 { return b.number }

func (b *forecastChildBlock) PrevHash() lcommon.Blake2b256 { return b.prev }

func (b *forecastChildBlock) Era() lcommon.Era { return b.era }

func newAppliedTipForecastState(t *testing.T) (*LedgerState, lcommon.Blake2b256) {
	t.Helper()
	cfg := minimalShelleyGenesisCfg(t)
	cfg.ShelleyGenesisHash = strings.Repeat("42", 32)
	var tipHash lcommon.Blake2b256
	copy(tipHash[:], strings.Repeat("t", 32))
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point:       ocommon.NewPoint(90, tipHash.Bytes()),
			BlockNumber: 10,
		},
		config: LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	shape := hardfork.Shape{
		SystemStart: cfg.ShelleyGenesis().SystemStart,
		Eras: []hardfork.ShapeEntry{{
			EraID: eras.ConwayEraDesc.Id,
			Params: hardfork.EraParams{
				EpochSize:     100,
				SlotLength:    time.Second,
				SafeZoneSlots: 30,
				GenesisWindow: 30,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()
	return ls, tipHash
}

func TestForecastNextEpochForAppliedTipChild(t *testing.T) {
	t.Parallel()

	ls, tipHash := newAppliedTipForecastState(t)
	child := &forecastChildBlock{slot: 120, number: 11, prev: tipHash}

	epoch, cache, ok, err := ls.forecastNextEpochForAppliedTipChild(
		t.Context(), child,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), epoch.EpochId)
	require.Equal(t, uint64(100), epoch.StartSlot)
	require.Len(t, cache, 2)
	require.NotEmpty(t, epoch.Nonce)
	require.Len(
		t, ls.loadConsensusSnapshot().epochCache, 1,
		"the forecast must not mutate the shared epoch cache",
	)

	// The deferral path must hand the same forecast to header verification.
	gotEpoch, gotCache, err := ls.headerVerificationEpochForBlockWithCache(
		t.Context(), child, false,
	)
	require.NoError(t, err)
	require.Equal(t, epoch.EpochId, gotEpoch.EpochId)
	require.Len(t, gotCache, 2)
}

func TestForecastNextEpochForAppliedTipChildAcrossHardFork(t *testing.T) {
	t.Parallel()

	ls, tipHash := newAppliedTipForecastState(t)
	shape := ls.eraShape()
	shape.Eras[0].NextEraTrigger = hardfork.NewTriggerAtEpoch(1)
	dijkstraParams, err := eras.BuildEraParams(
		ls.config.CardanoNodeConfig,
		eras.DijkstraEraDesc,
	)
	require.NoError(t, err)
	shape.Eras = append(shape.Eras, hardfork.ShapeEntry{
		EraID:          eras.DijkstraEraDesc.Id,
		EraName:        eras.DijkstraEraDesc.Name,
		Params:         dijkstraParams,
		NextEraTrigger: hardfork.NewTriggerNotDuringThisExecution(),
	})
	ls.cachedShape.Store(&shape)
	ls.config.EnableDijkstra = true
	ls.activeEras = eras.ActiveEras(true)
	ls.transitionInfo = hardfork.NewTransitionUnknown()
	ls.publishSnapshotsLocked()
	gotShape := ls.eraShape()
	entry, hasEntry := gotShape.EraForID(eras.ConwayEraDesc.Id)
	require.True(t, hasEntry)
	require.Equal(t, hardfork.TriggerAtEpoch, entry.NextEraTrigger.Kind)
	_, hasDijkstra := ls.eraById(eras.DijkstraEraDesc.Id)
	require.True(t, hasDijkstra)
	gotEraID, isBoundary := ls.forecastBoundaryEra(
		eras.ConwayEraDesc.Id, 1, ls.loadConsensusSnapshot().transitionInfo,
	)
	require.True(t, isBoundary)
	require.Equal(t, eras.DijkstraEraDesc.Id, gotEraID)

	child := &forecastChildBlock{
		slot:   120,
		number: 11,
		prev:   tipHash,
		era:    lcommon.Era{Id: uint8(eras.DijkstraEraDesc.Id)},
	}
	epoch, cache, ok, err := ls.forecastNextEpochForAppliedTipChild(
		t.Context(), child,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), epoch.EpochId)
	require.Equal(t, eras.DijkstraEraDesc.Id, epoch.EraId)
	require.Equal(t, dijkstraParams.EpochSize, uint64(epoch.LengthInSlots))
	require.Equal(t, dijkstraParams.SlotLength.Milliseconds(), int64(epoch.SlotLength))
	require.Len(t, cache, 2)
	require.Len(t, ls.loadConsensusSnapshot().epochCache, 1,
		"the boundary forecast must not publish before transactional rollover")
	gotEpoch, gotCache, err := ls.headerVerificationEpochForBlockWithCache(
		t.Context(), child, true,
	)
	require.NoError(t, err)
	require.Equal(t, epoch, gotEpoch)
	require.Len(t, gotCache, 2)
	require.Len(t, ls.loadConsensusSnapshot().epochCache, 1,
		"forging header validation must keep the boundary forecast immutable")

	child.era = lcommon.Era{Id: uint8(eras.ConwayEraDesc.Id)}
	_, _, ok, err = ls.forecastNextEpochForAppliedTipChild(t.Context(), child)
	require.NoError(t, err)
	require.False(t, ok,
		"a predecessor-era header cannot use the successor-era boundary forecast")
}

// A confirmed (TransitionKnown) hard-fork transition fixes the boundary
// even without a configured epoch trigger, so the successor era is forecast.
func TestForecastNextEpochForAppliedTipChildConfirmedTransition(t *testing.T) {
	t.Parallel()

	ls, tipHash := newAppliedTipForecastState(t)
	shape := ls.eraShape()
	// Version-triggered successor (no configured epoch trigger).
	shape.Eras[0].NextEraTrigger = hardfork.NewTriggerAtVersion(
		eras.DijkstraEraDesc.MinMajorVersion,
	)
	dijkstraParams, err := eras.BuildEraParams(
		ls.config.CardanoNodeConfig,
		eras.DijkstraEraDesc,
	)
	require.NoError(t, err)
	shape.Eras = append(shape.Eras, hardfork.ShapeEntry{
		EraID:          eras.DijkstraEraDesc.Id,
		EraName:        eras.DijkstraEraDesc.Name,
		Params:         dijkstraParams,
		NextEraTrigger: hardfork.NewTriggerNotDuringThisExecution(),
	})
	ls.cachedShape.Store(&shape)
	ls.config.EnableDijkstra = true
	ls.activeEras = eras.ActiveEras(true)
	ls.transitionInfo = hardfork.NewTransitionKnown(1)
	ls.publishSnapshotsLocked()

	gotEraID, isBoundary := ls.forecastBoundaryEra(
		eras.ConwayEraDesc.Id, 1, ls.loadConsensusSnapshot().transitionInfo,
	)
	require.True(t, isBoundary, "confirmed transition must be a boundary")
	require.Equal(t, eras.DijkstraEraDesc.Id, gotEraID)

	child := &forecastChildBlock{
		slot:   120,
		number: 11,
		prev:   tipHash,
		era:    lcommon.Era{Id: uint8(eras.DijkstraEraDesc.Id)},
	}
	epoch, cache, ok, err := ls.forecastNextEpochForAppliedTipChild(
		t.Context(), child,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), epoch.EpochId)
	require.Equal(t, eras.DijkstraEraDesc.Id, epoch.EraId)
	require.Len(t, cache, 2)
	require.Len(t, ls.loadConsensusSnapshot().epochCache, 1)
}

func TestForecastNextEpochForAppliedTipChildRejectsNonChildren(t *testing.T) {
	t.Parallel()

	ls, tipHash := newAppliedTipForecastState(t)
	var other lcommon.Blake2b256
	copy(other[:], strings.Repeat("o", 32))
	tests := map[string]*forecastChildBlock{
		"unknown parent":    {slot: 120, number: 11, prev: other},
		"skipped height":    {slot: 120, number: 12, prev: tipHash},
		"same epoch":        {slot: 95, number: 11, prev: tipHash},
		"beyond next epoch": {slot: 250, number: 11, prev: tipHash},
		"not after tip":     {slot: 90, number: 11, prev: tipHash},
	}
	for name, blk := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, ok, err := ls.forecastNextEpochForAppliedTipChild(
				t.Context(), blk,
			)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func newBoundaryForecastState(
	t *testing.T,
	trigger hardfork.TriggerHardFork,
	transition hardfork.TransitionInfo,
	enableDijkstra bool,
) (*LedgerState, lcommon.Blake2b256, hardfork.EraParams) {
	t.Helper()
	ls, tipHash := newAppliedTipForecastState(t)
	shape := ls.eraShape()
	shape.Eras[0].NextEraTrigger = trigger
	dijkstraParams, err := eras.BuildEraParams(
		ls.config.CardanoNodeConfig,
		eras.DijkstraEraDesc,
	)
	require.NoError(t, err)
	shape.Eras = append(shape.Eras, hardfork.ShapeEntry{
		EraID:          eras.DijkstraEraDesc.Id,
		EraName:        eras.DijkstraEraDesc.Name,
		Params:         dijkstraParams,
		NextEraTrigger: hardfork.NewTriggerNotDuringThisExecution(),
	})
	ls.cachedShape.Store(&shape)
	ls.config.EnableDijkstra = enableDijkstra
	ls.activeEras = eras.ActiveEras(enableDijkstra)
	ls.transitionInfo = transition
	ls.publishSnapshotsLocked()
	return ls, tipHash, dijkstraParams
}

func TestForecastNextEpochForAppliedTipChildBoundaryLimits(t *testing.T) {
	t.Parallel()

	conway := lcommon.Era{Id: uint8(eras.ConwayEraDesc.Id)}
	dijkstra := lcommon.Era{Id: uint8(eras.DijkstraEraDesc.Id)}

	t.Run("trigger outranks transition", func(t *testing.T) {
		t.Parallel()
		ls, tipHash, _ := newBoundaryForecastState(
			t,
			hardfork.NewTriggerAtEpoch(5),
			hardfork.NewTransitionKnown(1),
			true,
		)
		child := &forecastChildBlock{
			slot: 120, number: 11, prev: tipHash, era: conway,
		}
		epoch, _, ok, err := ls.forecastNextEpochForAppliedTipChild(
			t.Context(), child,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, eras.ConwayEraDesc.Id, epoch.EraId)
		require.Equal(t, uint(100), epoch.LengthInSlots)
		_, isBoundary := ls.forecastBoundaryEra(
			eras.ConwayEraDesc.Id, 1, ls.loadConsensusSnapshot().transitionInfo,
		)
		require.False(t, isBoundary,
			"the successor era must not open before the configured epoch")
	})

	t.Run("no second successor epoch", func(t *testing.T) {
		t.Parallel()
		ls, tipHash, dijkstraParams := newBoundaryForecastState(
			t,
			hardfork.NewTriggerAtEpoch(1),
			hardfork.NewTransitionUnknown(),
			true,
		)
		child := &forecastChildBlock{
			slot:   100 + dijkstraParams.EpochSize,
			number: 11,
			prev:   tipHash,
			era:    dijkstra,
		}
		_, _, ok, err := ls.forecastNextEpochForAppliedTipChild(
			t.Context(), child,
		)
		require.NoError(t, err)
		require.False(t, ok)

		child.slot = 100 + dijkstraParams.EpochSize - 1
		epoch, _, ok, err := ls.forecastNextEpochForAppliedTipChild(
			t.Context(), child,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(1), epoch.EpochId)
	})

	t.Run("inactive successor era defers", func(t *testing.T) {
		t.Parallel()
		ls, tipHash, _ := newBoundaryForecastState(
			t,
			hardfork.NewTriggerAtEpoch(1),
			hardfork.NewTransitionUnknown(),
			false,
		)
		for _, era := range []lcommon.Era{conway, dijkstra} {
			child := &forecastChildBlock{
				slot: 120, number: 11, prev: tipHash, era: era,
			}
			_, _, ok, err := ls.forecastNextEpochForAppliedTipChild(
				t.Context(), child,
			)
			require.ErrorIs(t, err, errEpochCacheForecastBoundary)
			require.False(t, ok)
			_, _, err = ls.headerVerificationEpochForBlockWithCache(
				t.Context(), child, true,
			)
			require.ErrorIs(t, err, errHeaderVerificationDeferred)
			require.Len(t, ls.loadConsensusSnapshot().epochCache, 1)
		}
	})
}
