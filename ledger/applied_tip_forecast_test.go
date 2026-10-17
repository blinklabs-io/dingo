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
}

func (b *forecastChildBlock) SlotNumber() uint64 { return b.slot }

func (b *forecastChildBlock) BlockNumber() uint64 { return b.number }

func (b *forecastChildBlock) PrevHash() lcommon.Blake2b256 { return b.prev }

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
