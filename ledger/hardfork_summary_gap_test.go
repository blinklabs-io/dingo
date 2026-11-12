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
	"context"
	"io"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func summaryForCache(
	t *testing.T,
	cfg *cardano.CardanoNodeConfig,
	cache []models.Epoch,
	current eras.EraDesc,
) *hardfork.Summary {
	t.Helper()
	ls := &LedgerState{
		epochCache: cache,
		currentEra: current,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(cache[len(cache)-1].StartSlot+1, []byte("tip")),
		},
		config: LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	ls.publishSnapshotsLocked()
	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.NoError(t, sum.Validate())
	return sum
}

// TestHardForkSummary_EraStartsAtTransitionEpochWhenItIsEmpty pins that a first
// Shelley block in the epoch after the transition leaves the Byron-to-Shelley
// boundary where the hard-fork combinator puts it, at the start of the
// transition epoch, so every Shelley slot converts to the right time.
func TestHardForkSummary_EraStartsAtTransitionEpochWhenItIsEmpty(t *testing.T) {
	t.Parallel()

	cache := []models.Epoch{
		{EpochId: 0, StartSlot: 0, SlotLength: 20_000, LengthInSlots: 100, EraId: 0},
		{EpochId: 1, StartSlot: 100, SlotLength: 20_000, LengthInSlots: 100, EraId: 0},
		// Epoch 2 is the Shelley transition epoch and holds no block.
		{EpochId: 3, StartSlot: 632, SlotLength: 1000, LengthInSlots: 432, EraId: 1},
	}
	sum := summaryForCache(t, minimalShelleyGenesisCfg(t), cache,
		eras.EraDesc{Id: 1, Name: "Shelley"})

	require.Len(t, sum.Eras, 2)
	want := hardfork.Bound{RelativeTime: 4000 * time.Second, Slot: 200, Epoch: 2}
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, want, *sum.Eras[0].End)
	assert.Equal(t, want, sum.Eras[1].Start)

	got, err := sum.SlotToTime(642)
	require.NoError(t, err)
	assert.Equal(t, sum.SystemStart.Add(4000*time.Second+442*time.Second), got)
	info, err := sum.SlotToEpoch(642)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), info.Epoch)
}

// TestHardForkSummary_ScheduledEraStartsAtItsEpochAcrossAGap covers a fork
// scheduled before the first block of the new era: the era begins at the
// scheduled epoch however many empty epochs follow it, and the old era runs up
// to it.
func TestHardForkSummary_ScheduledEraStartsAtItsEpochAcrossAGap(t *testing.T) {
	t.Parallel()

	cfg := newTestEraHistoryCfg(t)
	conwayAt := uint64(2)
	cfg.TestConwayHardForkAtEpoch = &conwayAt
	cache := []models.Epoch{
		{EpochId: 0, StartSlot: 0, SlotLength: 1000, LengthInSlots: 100, EraId: eras.BabbageEraDesc.Id},
		{EpochId: 3, StartSlot: 300, SlotLength: 1000, LengthInSlots: 100, EraId: eras.ConwayEraDesc.Id},
	}
	sum := summaryForCache(t, cfg, cache, eras.ConwayEraDesc)

	require.Len(t, sum.Eras, 2)
	want := hardfork.Bound{RelativeTime: 200 * time.Second, Slot: 200, Epoch: 2}
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, want, *sum.Eras[0].End)
	assert.Equal(t, want, sum.Eras[1].Start)
	got, err := sum.SlotToTime(350)
	require.NoError(t, err)
	assert.Equal(t, sum.SystemStart.Add(350*time.Second), got)
}

// TestHardForkSummary_VersionEraStartsAtLabelSuccessorAcrossAGap covers a
// version-triggered fork whose first new-era block is beyond an empty epoch:
// the transition epoch is the successor of the ledger epoch label.
func TestHardForkSummary_VersionEraStartsAtLabelSuccessorAcrossAGap(t *testing.T) {
	t.Parallel()

	cache := []models.Epoch{
		{EpochId: 0, StartSlot: 0, SlotLength: 1000, LengthInSlots: 100, EraId: eras.BabbageEraDesc.Id},
		{EpochId: 3, StartSlot: 300, SlotLength: 1000, LengthInSlots: 100, EraId: eras.ConwayEraDesc.Id},
	}
	sum := summaryForCache(t, newTestEraHistoryCfg(t), cache, eras.ConwayEraDesc)

	require.Len(t, sum.Eras, 2)
	want := hardfork.Bound{RelativeTime: 100 * time.Second, Slot: 100, Epoch: 1}
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, want, *sum.Eras[0].End)
	assert.Equal(t, want, sum.Eras[1].Start)
}

// TestQueryHardForkEraHistory_EraStartsAtTransitionEpochAcrossAGap pins that
// the era history a client reads agrees with the summary: a first Conway row
// beyond an empty epoch leaves Babbage ending, and Conway starting, at the
// successor of the ledger epoch label, with every bound priced across the
// epochs no row records.
func TestQueryHardForkEraHistory_EraStartsAtTransitionEpochAcrossAGap(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	// Babbage epoch 0, then Conway epoch 3 with epochs 1 and 2 empty.
	require.NoError(t, db.SetEpoch(
		0, 0, nil, nil, nil, nil, eras.BabbageEraDesc.Id, 1000, 100, nil,
	))
	require.NoError(t, db.SetEpoch(
		300, 3, nil, nil, nil, nil, eras.ConwayEraDesc.Id, 1000, 100, nil,
	))
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(310, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	result, err := ls.queryHardForkEraHistory(context.Background(), nil)
	require.NoError(t, err)
	list, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.Len(t, list, 2)
	babbage, ok := list[0].([]any)
	require.True(t, ok)
	conway, ok := list[1].([]any)
	require.True(t, ok)

	boundary := func(v any) (string, uint64, uint64) {
		b, ok := v.([]any)
		require.True(t, ok)
		rel, ok := b[0].(*big.Int)
		require.True(t, ok)
		return rel.String(), b[1].(uint64), b[2].(uint64)
	}
	// One epoch of 100 one-second slots is 100e12 picoseconds.
	rel, slot, epoch := boundary(babbage[1])
	assert.Equal(t, "100000000000000", rel)
	assert.Equal(t, uint64(100), slot)
	assert.Equal(t, uint64(1), epoch)
	relStart, slotStart, epochStart := boundary(conway[0])
	assert.Equal(t, rel, relStart)
	assert.Equal(t, slot, slotStart)
	assert.Equal(t, epoch, epochStart)
}
