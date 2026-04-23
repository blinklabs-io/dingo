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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueryHardForkEraHistory_EmitsAllKnownEras pins an invariant that
// existing tests don't: the CBOR result always contains one entry per era in
// eras.Eras (7 entries for Cardano), even when most eras have no epochs in the
// DB. Clients rely on this shape.
func TestQueryHardForkEraHistory_EmitsAllKnownEras(t *testing.T) {
	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
	)

	db := newTestDB(t)
	// Populate only Conway — every other era is empty.
	require.NoError(t, db.SetEpoch(
		epochStartSlot, epochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	list, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.Len(t, list, len(eras.Eras),
		"era history must emit one entry per era in eras.Eras")

	// Inspect each entry: [start, end, params].
	for i, entry := range list {
		era, ok := entry.([]any)
		require.True(t, ok, "entry %d is not []any", i)
		require.Len(t, era, 3, "entry %d must have [start, end, params]", i)

		start, ok := era[0].([]any)
		require.True(t, ok, "entry %d start is not []any", i)
		require.Len(t, start, 3, "entry %d start must be 3-tuple", i)

		end, ok := era[1].([]any)
		require.True(t, ok, "entry %d end is not []any", i)
		require.Len(t, end, 3, "entry %d end must be 3-tuple", i)

		params, ok := era[2].([]any)
		require.True(t, ok, "entry %d params is not []any", i)
		require.Len(t, params, 4, "entry %d params must be 4-tuple", i)
	}
}

// TestQueryHardForkEraHistory_TransitionUnknown_TipNearEpochEnd pins the
// Haskell HFC semantics: when tipSlot + safeZone crosses into a later epoch,
// the forecast boundary extends to *that* epoch's end rather than clamping
// to the current epoch's end. This is the behavior unlocked by delegating
// the safe-zone computation to hardfork.BuildSummary; dingo's legacy
// pre-HFC-adapter code clamped too conservatively to the last-in-DB epoch's
// end.
func TestQueryHardForkEraHistory_TransitionUnknown_TipNearEpochEnd(t *testing.T) {
	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		// tipSlot near the epoch end: tip + safeZone (25_920) = 545_920
		// crosses into epoch 501 (which spans [532_000, 964_000)).
		tipSlot = uint64(520_000)
	)
	const expectedEraEndSlot = uint64(964_000) // start of epoch 502
	const expectedEraEndEpoch = uint64(502)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		epochStartSlot, epochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))

	ls := &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		transitionInfo: hardfork.NewTransitionUnknown(),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	eraList := result.(cbor.IndefLengthList)
	lastEra := eraList[len(eraList)-1].([]any)
	eraEnd := lastEra[1].([]any)

	slot, ok := eraEnd[1].(uint64)
	require.True(t, ok)
	epoch, ok := eraEnd[2].(uint64)
	require.True(t, ok)

	assert.Equal(t, expectedEraEndSlot, slot,
		"Unknown: tip+safeZone crossing into next epoch should extend EraEnd to that epoch's end")
	assert.Equal(t, expectedEraEndEpoch, epoch,
		"Unknown: EraEnd epoch should be the epoch *after* the one containing tip+safeZone")
}

// TestQueryHardForkEraHistory_AdjacentErasContiguous pins that whenever two
// adjacent eras both carry real data, era[i].End bounds match era[i+1].Start
// on all three axes (relTime, slot, epoch). This is the invariant that would
// catch a timespan-accumulation bug like the one the legacy code flagged with
// a hand-written "timespan.Sub" after the transition-epoch detection.
func TestQueryHardForkEraHistory_AdjacentErasContiguous(t *testing.T) {
	const (
		byronEpoch0     = uint64(0)
		byronEpoch0Len  = uint(21_600)
		byronSlotLenMs  = uint(20_000)
		shelleyEpoch1   = uint64(1)
		shelleyStart    = uint64(21_600) // after Byron epoch 0
		shelleyEpochLen = uint(432_000)
		slotLenMs       = uint(1_000)
		tipSlot         = shelleyStart + 100
	)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		0, byronEpoch0, nil, nil, nil, nil,
		eras.ByronEraDesc.Id, byronSlotLenMs, byronEpoch0Len, nil,
	))
	require.NoError(t, db.SetEpoch(
		shelleyStart, shelleyEpoch1, nil, nil, nil, nil,
		eras.ShelleyEraDesc.Id, slotLenMs, shelleyEpochLen, nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)
	list, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Entry 0 = Byron (populated, closed), entry 1 = Shelley (populated, open).
	byronEra := list[0].([]any)
	shelleyEra := list[1].([]any)
	byronEnd := byronEra[1].([]any)
	shelleyStartBound := shelleyEra[0].([]any)

	// relTime — *big.Int picoseconds
	byronEndRel, ok := byronEnd[0].(*big.Int)
	require.True(t, ok, "byron end relTime must be *big.Int, got %T", byronEnd[0])
	shelleyStartRel, ok := shelleyStartBound[0].(*big.Int)
	require.True(t, ok, "shelley start relTime must be *big.Int, got %T", shelleyStartBound[0])
	assert.Equal(t, 0, byronEndRel.Cmp(shelleyStartRel),
		"byron end relTime (%s) must equal shelley start relTime (%s)",
		byronEndRel, shelleyStartRel,
	)

	assert.Equal(t, byronEnd[1], shelleyStartBound[1],
		"byron end slot must equal shelley start slot")
	assert.Equal(t, byronEnd[2], shelleyStartBound[2],
		"byron end epoch must equal shelley start epoch")
}
