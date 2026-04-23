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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	"io"
	"log/slog"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestEraHistoryCfg builds a CardanoNodeConfig with both Byron and Shelley
// genesis data, including slotLength and epochLength needed by EpochLengthShelley
// and the security parameters needed by calculateStabilityWindowForEra.
func newTestEraHistoryCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	byronGenesisJSON := `{
		"blockVersionData": { "slotDuration": "20000" },
		"protocolConsts": { "k": 432 }
	}`
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotLength": 1,
		"epochLength": 432000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	cfg := &cardano.CardanoNodeConfig{}
	err := cfg.LoadByronGenesisFromReader(strings.NewReader(byronGenesisJSON))
	require.NoError(t, err)
	err = cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON))
	require.NoError(t, err)
	return cfg
}

// TestQueryHardForkEraHistory_OpenEraEndBoundedBySafeZone proves that the
// current era's EraEnd is snapped to the end of the epoch that contains
// ledgerTip + safeZone.  Within a single epoch slot↔time is linear (constant
// slot length), so the epoch-end boundary is the safe forecast limit.
//
// Setup:
//   - One Conway epoch: startSlot=100_000, length=432_000 (ends at slot 532_000)
//   - ledgerTip at slot 200_000 (well inside the epoch)
//   - safeZone = ceil(3k/f) = ceil(3*432/0.05) = 25_920
//   - safeEndSlot = 225_920, which is within the epoch (< 532_000)
//
// Expected EraEnd slot: 532_000 (epoch end), epoch number: 501
func TestQueryHardForkEraHistory_OpenEraEndBoundedBySafeZone(t *testing.T) {
	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000) // 1 second in milliseconds
		epochId        = uint64(500)
	)
	// safeZone = ceil(3 * 432 / 0.05) = 25_920; safeEndSlot = 225_920 < 532_000
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := epochStartSlot + uint64(epochLen) // 532_000

	db := newTestDB(t)
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
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	// The last entry in the list is the Conway (current, open) era.
	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	require.Len(t, lastEra, 3, "era entry should be [start, end, params]")

	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	require.Len(t, eraEnd, 3, "EraEnd should be [relTime, slot, epoch]")

	actualEraEndSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")

	actualEraEndEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")

	assert.Equal(t, expectedEraEndSlot, actualEraEndSlot,
		"open era EraEnd slot should snap to epoch boundary (%d), not mid-epoch safeEndSlot (%d)",
		expectedEraEndSlot, tipSlot+expectedSafeZone,
	)
	assert.Equal(t, epochId+1, actualEraEndEpoch,
		"open era EraEnd epoch number should be epochId+1 (%d)", epochId+1,
	)
}

func TestQueryShelleyUtxoByAddress_EmptySlice(t *testing.T) {
	ls := &LedgerState{}
	result, err := ls.queryShelleyUtxoByAddress(nil)
	require.NoError(t, err)
	// Should return []any{empty map}
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	require.Empty(t, m)
}

func TestQueryShelleyUtxoByTxIn_EmptySlice(t *testing.T) {
	ls := &LedgerState{}
	result, err := ls.queryShelleyUtxoByTxIn(nil)
	require.NoError(t, err)
	// Should return []any{empty map}
	arr, ok := result.([]any)
	require.True(t, ok, "expected []any result")
	require.Len(t, arr, 1)
	m, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected UtxoId map")
	require.Empty(t, m)
}

func TestEpochPicoseconds(t *testing.T) {
	tests := []struct {
		name          string
		slotLength    uint
		lengthInSlots uint
		expected      *big.Int
	}{
		{
			// Shelley epoch: 1000ms slots, 432000 slots
			// 1000 * 432000 * 1e9 = 432_000_000_000_000_000
			name:          "shelley epoch",
			slotLength:    1000,
			lengthInSlots: 432000,
			expected: new(big.Int).SetUint64(
				432_000_000_000_000_000,
			),
		},
		{
			// Byron epoch: 20000ms slots, 21600 slots
			// 20000 * 21600 * 1e9 = 432_000_000_000_000_000
			name:          "byron epoch",
			slotLength:    20000,
			lengthInSlots: 21600,
			expected: new(big.Int).SetUint64(
				432_000_000_000_000_000,
			),
		},
		{
			name:          "zero slot length",
			slotLength:    0,
			lengthInSlots: 432000,
			expected:      big.NewInt(0),
		},
		{
			name:          "zero length in slots",
			slotLength:    1000,
			lengthInSlots: 0,
			expected:      big.NewInt(0),
		},
		{
			// Large values that would overflow uint64 in
			// naive uint multiplication:
			// MaxUint32 * MaxUint32 * 1e9 overflows uint64,
			// but big.Int handles it correctly.
			name:          "large values no overflow",
			slotLength:    math.MaxUint32,
			lengthInSlots: math.MaxUint32,
			expected: func() *big.Int {
				a := new(big.Int).SetUint64(math.MaxUint32)
				b := new(big.Int).SetUint64(math.MaxUint32)
				r := new(big.Int).Mul(a, b)
				r.Mul(r, big.NewInt(1_000_000_000))
				return r
			}(),
		},
		{
			name:          "single slot single ms",
			slotLength:    1,
			lengthInSlots: 1,
			expected:      big.NewInt(1_000_000_000),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := epochPicoseconds(
				tc.slotLength,
				tc.lengthInSlots,
			)
			// Use Cmp instead of Equal because big.Int
			// internal representation of zero varies
			// (nil abs vs empty abs).
			assert.Equal(
				t,
				0,
				tc.expected.Cmp(result),
				"picosecond calculation mismatch: "+
					"expected %s, got %s",
				tc.expected.String(), result.String(),
			)
		})
	}
}

func TestEpochPicoseconds_OverflowSafe(t *testing.T) {
	// Verify that large values that would overflow uint64
	// in naive multiplication are handled correctly by
	// big.Int arithmetic.
	//
	// MaxUint32 * MaxUint32 = 18446744065119617025
	// which is close to MaxUint64 (18446744073709551615).
	// Multiplying by 1e9 would massively overflow uint64.
	result := epochPicoseconds(
		math.MaxUint32,
		math.MaxUint32,
	)

	// The result must be larger than MaxUint64
	maxU64 := new(big.Int).SetUint64(math.MaxUint64)
	assert.Equal(
		t,
		1,
		result.Cmp(maxU64),
		"result should exceed MaxUint64",
	)

	// Verify the exact value:
	// MaxUint32^2 * 1e9 =
	// 4294967295 * 4294967295 * 1000000000 =
	// 18446744065119617025000000000
	expected, ok := new(big.Int).SetString(
		"18446744065119617025000000000",
		10,
	)
	require.True(t, ok)
	assert.Equal(
		t,
		0,
		expected.Cmp(result),
		"exact overflow value mismatch",
	)
}

// TestQueryHardForkEraHistory_TransitionKnown proves that when TransitionInfo
// is set to TransitionKnown the era history response uses the transition
// epoch's StartSlot as the exact EraEnd, rather than the safe-zone cap.
//
// Setup:
//   - Two Conway epochs: epoch 500 (startSlot=100_000, length=432_000) and
//     epoch 501 (startSlot=532_000, length=432_000) — epoch 501 is the
//     transition epoch (stored with the old era's EraId).
//   - transitionInfo = {State: TransitionKnown, KnownEpoch: 501}
//   - ledgerTip at slot 200_000 (well inside epoch 500)
//
// Expected EraEnd slot: 532_000 (epoch 501's StartSlot — the exact boundary)
// Without TransitionKnown: EraEnd would be 200_000 + 25_920 = 225_920
func TestQueryHardForkEraHistory_TransitionKnown(t *testing.T) {
	const (
		tipSlot       = uint64(200_000)
		epoch500Start = uint64(100_000)
		epoch501Start = uint64(532_000)
		epochLen      = uint(432_000)
		slotLenMs     = uint(1_000)
		epoch500Id    = uint64(500)
		epoch501Id    = uint64(501)
	)

	db := newTestDB(t)
	// Epoch 500: the active epoch in the old era
	require.NoError(t, db.SetEpoch(
		epoch500Start, epoch500Id,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	// Epoch 501: the transition epoch, still stored with Conway era's EraId
	require.NoError(t, db.SetEpoch(
		epoch501Start, epoch501Id,
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
		transitionInfo: hardfork.NewTransitionKnown(epoch501Id),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	// The last entry in the list is the Conway (current, open) era.
	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	require.Len(t, lastEra, 3, "era entry should be [start, end, params]")

	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	require.Len(t, eraEnd, 3, "EraEnd should be [relTime, slot, epoch]")

	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")
	assert.Equal(t, epoch501Start, actualSlot,
		"TransitionKnown: EraEnd slot should be transition epoch's StartSlot (%d), not safe-zone cap",
		epoch501Start,
	)

	// Verify EraEnd epoch number is the transition epoch ID
	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")
	assert.Equal(t, epoch501Id, actualEpoch,
		"TransitionKnown: EraEnd epoch should be the transition epoch ID",
	)
}

// TestQueryHardForkEraHistory_TransitionKnown_MissingEpochFallsBackToSafeZone
// verifies that TransitionKnown with a KnownEpoch absent from the DB falls back
// to the safe-zone path, which snaps to the epoch-end boundary.
//
// Setup: one Conway epoch (500), transitionInfo.KnownEpoch = 999 (not in DB).
// Expected: falls back to epoch-end snap (532_000), epoch number 501.
func TestQueryHardForkEraHistory_TransitionKnown_MissingEpochFallsBackToSafeZone(t *testing.T) {
	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		missingEpoch   = uint64(999) // deliberately absent from DB
	)
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := epochStartSlot + uint64(epochLen) // 532_000 (epoch end)

	db := newTestDB(t)
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
		transitionInfo: hardfork.NewTransitionKnown(missingEpoch),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")

	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")

	assert.Equal(t, expectedEraEndSlot, actualSlot,
		"TransitionKnown with missing KnownEpoch must fall back to epoch-end snap (%d)",
		expectedEraEndSlot,
	)
	assert.Equal(t, epochId+1, actualEpoch,
		"EraEnd epoch should be epochId+1 (%d)", epochId+1,
	)
}

// TestQueryHardForkEraHistory_TransitionUnknown_FallsBackToSafeZone confirms
// that TransitionUnknown snaps to the epoch-end boundary (not the raw
// safeEndSlot), matching Haskell's slotToEpochBound behaviour.
func TestQueryHardForkEraHistory_TransitionUnknown_FallsBackToSafeZone(t *testing.T) {
	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
	)
	// safeZone = 25_920; safeEndSlot = 225_920 which is inside the epoch (< 532_000)
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := epochStartSlot + uint64(epochLen) // 532_000

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

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok)
	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok)
	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok)
	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok)
	assert.Equal(t, expectedEraEndSlot, actualSlot,
		"TransitionUnknown: EraEnd slot should snap to epoch end (%d), not mid-epoch safeEndSlot (%d)",
		expectedEraEndSlot, tipSlot+expectedSafeZone,
	)
	assert.Equal(t, epochId+1, actualEpoch,
		"TransitionUnknown: EraEnd epoch should be epochId+1 (%d)", epochId+1,
	)
}

// TestQueryHardForkEraHistory_TransitionImpossible_ServesEpochEnd verifies
// that when TransitionImpossible is set, queryHardForkEraHistory returns the
// full epoch-end slot rather than a safe-zone cap.
//
// Setup (mirrors TestQueryHardForkEraHistory_OpenEraEndBoundedBySafeZone):
//   - Conway epoch 500: startSlot=100_000, length=432_000 (ends at 532_000)
//   - tipSlot past the safe-zone boundary (safeEnd >= epochEnd)
//   - transitionInfo = TransitionImpossible
//
// Expected EraEnd slot: 532_000 (confirmed epoch end, no cap)
func TestQueryHardForkEraHistory_TransitionImpossible_ServesEpochEnd(t *testing.T) {
	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		epochEndSlot   = uint64(532_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		// tipSlot well past the safe-zone decision point
		tipSlot = uint64(520_000)
	)

	db := newTestDB(t)
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
		transitionInfo: hardfork.NewTransitionImpossible(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)
	require.NotEmpty(t, eraList)

	lastEra, ok := eraList[len(eraList)-1].([]any)
	require.True(t, ok, "era entry should be []any")
	eraEnd, ok := lastEra[1].([]any)
	require.True(t, ok, "EraEnd should be []any")
	actualSlot, ok := eraEnd[1].(uint64)
	require.True(t, ok, "EraEnd slot should be uint64")

	assert.Equal(t, epochEndSlot, actualSlot,
		"TransitionImpossible: EraEnd slot should be the confirmed epoch end (%d), not a safeZone cap",
		epochEndSlot,
	)
}

// TestQueryHardForkEraHistory_TransitionImpossible_EpochNumberIsNextEpoch
// verifies that the EraEnd epoch number is epochId+1 when TransitionImpossible
// is set (the epoch-loop sets tmpEnd with epochId+1 for the last epoch).
func TestQueryHardForkEraHistory_TransitionImpossible_EpochNumberIsNextEpoch(t *testing.T) {
	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		tipSlot        = uint64(520_000)
	)

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
		currentTip:     ochainsync.Tip{Point: ocommon.NewPoint(tipSlot, []byte("tip"))},
		transitionInfo: hardfork.NewTransitionImpossible(),
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

	actualEpoch, ok := eraEnd[2].(uint64)
	require.True(t, ok, "EraEnd epoch should be uint64")
	assert.Equal(t, epochId+1, actualEpoch,
		"TransitionImpossible: EraEnd epoch should be epochId+1 (%d)", epochId+1)
}

// TestQueryHardForkEraHistory_TransitionImpossible_vs_Unknown_Comparison
// confirms that TransitionImpossible and TransitionUnknown converge on the
// same epoch-end boundary when tipSlot + safeZone still lies within the
// current epoch — the common steady-state early-in-epoch case.
//
// Divergence at late-in-epoch tips (tip + safeZone crossing into the next
// epoch) is covered by TransitionUnknown_FallsBackToSafeZone and matches
// Haskell HFC's slotToEpochBound semantics.
func TestQueryHardForkEraHistory_TransitionImpossible_vs_Unknown_Comparison(t *testing.T) {
	const (
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
		// tipSlot well inside the epoch so tip + safeZone (25_920) stays in
		// the same epoch — both states snap to the same epoch-end boundary.
		tipSlot = uint64(200_000)
	)

	setupLS := func(state hardfork.TransitionState) *LedgerState {
		db := newTestDB(t)
		require.NoError(t, db.SetEpoch(
			epochStartSlot, epochId,
			nil, nil, nil, nil,
			eras.ConwayEraDesc.Id, slotLenMs, epochLen,
			nil,
		))
		return &LedgerState{
			db:             db,
			currentEra:     eras.ConwayEraDesc,
			currentTip:     ochainsync.Tip{Point: ocommon.NewPoint(tipSlot, []byte("tip"))},
			transitionInfo: hardfork.TransitionInfo{State: state},
			config: LedgerStateConfig{
				CardanoNodeConfig: newTestEraHistoryCfg(t),
				Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
			},
		}
	}

	eraEndSlot := func(ls *LedgerState) uint64 {
		result, err := ls.queryHardForkEraHistory()
		require.NoError(t, err)
		eraList := result.(cbor.IndefLengthList)
		lastEra := eraList[len(eraList)-1].([]any)
		eraEnd := lastEra[1].([]any)
		slot, ok := eraEnd[1].(uint64)
		require.True(t, ok)
		return slot
	}

	impossibleSlot := eraEndSlot(setupLS(hardfork.TransitionImpossible))
	unknownSlot := eraEndSlot(setupLS(hardfork.TransitionUnknown))

	assert.Equal(t, uint64(532_000), impossibleSlot,
		"TransitionImpossible must serve the epoch end")
	assert.Equal(t, uint64(532_000), unknownSlot,
		"TransitionUnknown snaps to epoch end when tip+safeZone stays in the same epoch")
	assert.Equal(t, impossibleSlot, unknownSlot,
		"both states return the same epoch-end slot")
}

func TestCheckedSlotAdd(t *testing.T) {
	tests := []struct {
		name      string
		startSlot uint64
		length    uint64
		expected  uint64
		expectErr bool
	}{
		{
			name:      "normal addition",
			startSlot: 100,
			length:    200,
			expected:  300,
		},
		{
			name:      "zero plus zero",
			startSlot: 0,
			length:    0,
			expected:  0,
		},
		{
			name:      "zero plus value",
			startSlot: 0,
			length:    1000,
			expected:  1000,
		},
		{
			name:      "max minus one plus one",
			startSlot: math.MaxUint64 - 1,
			length:    1,
			expected:  math.MaxUint64,
		},
		{
			name:      "max plus zero",
			startSlot: math.MaxUint64,
			length:    0,
			expected:  math.MaxUint64,
		},
		{
			name:      "overflow max plus one",
			startSlot: math.MaxUint64,
			length:    1,
			expectErr: true,
		},
		{
			name:      "overflow large values",
			startSlot: math.MaxUint64 / 2,
			length:    math.MaxUint64/2 + 2,
			expectErr: true,
		},
		{
			name:      "realistic shelley epoch end",
			startSlot: 86400000,
			length:    432000,
			expected:  86832000,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := checkedSlotAdd(
				tc.startSlot,
				tc.length,
			)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					"era history overflow",
				)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestReconstructTransitionInfo verifies that reconstructTransitionInfo sets
// TransitionKnown when the loaded pparams carry a protocol version that maps
// to a later era than the current epoch's stored EraId — i.e., the node was
// stopped in the window between an epoch-rollover version bump and the first
// block of the new era.
func TestReconstructTransitionInfo(t *testing.T) {
	tests := []struct {
		name           string
		currentEra     eras.EraDesc
		currentEpoch   models.Epoch
		currentPParams lcommon.ProtocolParameters
		expectedState  hardfork.TransitionState
		expectedEpoch  uint64
	}{
		{
			// Babbage pparams with Conway major version (9): TransitionKnown.
			// This is the pre-Conway restart window scenario.
			name:       "babbage era pparams with conway version → TransitionKnown",
			currentEra: *eras.GetEraById(eras.BabbageEraDesc.Id),
			currentEpoch: models.Epoch{
				EpochId: 500,
				EraId:   eras.BabbageEraDesc.Id,
			},
			currentPParams: &babbage.BabbageProtocolParameters{
				ProtocolMajor: 9, // Conway major version, stored under Babbage era
			},
			expectedState: hardfork.TransitionKnown,
			expectedEpoch: 500,
		},
		{
			// Babbage pparams with normal Babbage version: no transition.
			name:       "babbage era pparams with babbage version → TransitionUnknown",
			currentEra: *eras.GetEraById(eras.BabbageEraDesc.Id),
			currentEpoch: models.Epoch{
				EpochId: 499,
				EraId:   eras.BabbageEraDesc.Id,
			},
			currentPParams: &babbage.BabbageProtocolParameters{
				ProtocolMajor: 8,
			},
			expectedState: hardfork.TransitionUnknown,
		},
		{
			// Conway pparams in Conway era: pparamsEra == currentEra, no transition.
			name:       "conway era pparams with conway version → TransitionUnknown",
			currentEra: *eras.GetEraById(eras.ConwayEraDesc.Id),
			currentEpoch: models.Epoch{
				EpochId: 600,
				EraId:   eras.ConwayEraDesc.Id,
			},
			currentPParams: &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: 9,
				},
			},
			expectedState: hardfork.TransitionUnknown,
		},
		{
			// Nil pparams: must not panic, leave TransitionUnknown.
			name:           "nil pparams → TransitionUnknown",
			currentEra:     *eras.GetEraById(eras.BabbageEraDesc.Id),
			currentEpoch:   models.Epoch{EpochId: 400, EraId: eras.BabbageEraDesc.Id},
			currentPParams: nil,
			expectedState:  hardfork.TransitionUnknown,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				currentEra:     tc.currentEra,
				currentEpoch:   tc.currentEpoch,
				currentPParams: tc.currentPParams,
				// transitionInfo starts at zero value (TransitionUnknown)
			}
			ls.reconstructTransitionInfo()

			assert.Equal(t, tc.expectedState, ls.transitionInfo.State)
			if tc.expectedState == hardfork.TransitionKnown {
				assert.Equal(t, tc.expectedEpoch, ls.transitionInfo.KnownEpoch)
			}
		})
	}
}

// TestQueryHardForkEraHistory_PastEra_NormalEpochEnd verifies that a closed
// past era whose last epoch carries the same-era protocol version produces an
// EraEnd equal to lastEp.StartSlot + lastEp.LengthInSlots (the raw boundary),
// not some alternative.
//
// Setup:
//   - currentEra = Conway (era 9)
//   - Babbage epoch 499: startSlot=64_000_000, length=432_000 → raw end=64_432_000
//   - Babbage pparams for epoch 499: ProtocolMajor=8 (Babbage version)
//
// Expected Babbage EraEnd slot: 64_432_000
func TestQueryHardForkEraHistory_PastEra_NormalEpochEnd(t *testing.T) {
	const (
		epochId    = uint64(499)
		epochStart = uint64(64_000_000)
		epochLen   = uint(432_000)
		slotLenMs  = uint(1_000)
		rawEraEnd  = epochStart + uint64(epochLen) // 64_432_000
	)

	db := newTestDB(t)
	// Store the Babbage epoch.
	require.NoError(t, db.SetEpoch(
		epochStart, epochId,
		nil, nil, nil, nil,
		eras.BabbageEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	// Store Babbage pparams for epoch 499 with Babbage major version (8).
	pp := &babbage.BabbageProtocolParameters{ProtocolMajor: 8}
	ppCbor, err := cbor.Encode(pp)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		ppCbor,
		epochStart, // slot
		epochId,
		eras.BabbageEraDesc.Id,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(epochStart+1, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Find the Babbage entry (second-to-last before Conway).
	var babbageEraEnd []any
	for _, entry := range eraList {
		eraEntry, ok := entry.([]any)
		if !ok || len(eraEntry) < 3 {
			continue
		}
		end, ok := eraEntry[1].([]any)
		if !ok || len(end) < 3 {
			continue
		}
		slot, ok := end[1].(uint64)
		if !ok {
			continue
		}
		// The Babbage era's EraEnd epoch number is epochId+1 in the normal case.
		epochNo, ok := end[2].(uint64)
		if !ok {
			continue
		}
		if epochNo == epochId+1 && slot == rawEraEnd {
			babbageEraEnd = end
			break
		}
	}
	require.NotNil(t, babbageEraEnd,
		"expected to find Babbage EraEnd with slot=%d, epochNo=%d in era history",
		rawEraEnd, epochId+1,
	)
	assert.Equal(t, rawEraEnd, babbageEraEnd[1].(uint64),
		"past era with normal pparams version: EraEnd slot should be raw boundary (%d)", rawEraEnd,
	)
}

// TestQueryHardForkEraHistory_PastEra_TransitionEpoch verifies that a closed
// past era whose last epoch carries the next-era protocol version produces an
// EraEnd equal to lastEp.StartSlot (the confirmed boundary), not the raw
// StartSlot+LengthInSlots which would overshoot.
//
// This is the SCENARIO 2 case: TransitionKnown was active, epoch K was created
// under OLD_ERA's EraId, but its pparams already show the new-era version.
// After the hard fork, epoch K ends up as the last epoch of OLD_ERA in the DB,
// but the actual era boundary is at epoch K's *start*, not its end.
//
// Setup:
//   - currentEra = Conway (era 9)
//   - Babbage epoch 499: startSlot=64_000_000, length=432_000 → raw end=64_432_000
//   - Babbage pparams for epoch 499: ProtocolMajor=9 (Conway version — transition epoch)
//
// Expected Babbage EraEnd slot: 64_000_000 (epoch 499's StartSlot)
func TestQueryHardForkEraHistory_PastEra_TransitionEpoch(t *testing.T) {
	const (
		epochId    = uint64(499)
		epochStart = uint64(64_000_000)
		epochLen   = uint(432_000)
		slotLenMs  = uint(1_000)
		rawEraEnd  = epochStart + uint64(epochLen) // 64_432_000 — wrong
		// correct boundary is epochStart because epoch 499 is a transition epoch
	)

	db := newTestDB(t)
	require.NoError(t, db.SetEpoch(
		epochStart, epochId,
		nil, nil, nil, nil,
		eras.BabbageEraDesc.Id, slotLenMs, epochLen,
		nil,
	))
	// Pparams carry Conway major version (9) — this is a transition epoch.
	pp := &babbage.BabbageProtocolParameters{ProtocolMajor: 9}
	ppCbor, err := cbor.Encode(pp)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		ppCbor,
		epochStart,
		epochId,
		eras.BabbageEraDesc.Id,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(epochStart+1, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Find the Babbage entry: for a transition epoch, EraEnd slot = epochStart
	// and EraEnd epochNo = epochId (not epochId+1).
	var babbageEraEnd []any
	for _, entry := range eraList {
		eraEntry, ok := entry.([]any)
		if !ok || len(eraEntry) < 3 {
			continue
		}
		end, ok := eraEntry[1].([]any)
		if !ok || len(end) < 3 {
			continue
		}
		slot, ok := end[1].(uint64)
		if !ok {
			continue
		}
		epochNo, ok := end[2].(uint64)
		if !ok {
			continue
		}
		// Both the raw boundary and the corrected boundary are non-zero;
		// distinguish by era params or just search for the corrected slot.
		if slot == epochStart && epochNo == epochId {
			babbageEraEnd = end
			break
		}
	}

	require.NotNil(t, babbageEraEnd,
		"expected Babbage EraEnd with slot=%d epochNo=%d; "+
			"got raw boundary %d instead — transition epoch not detected",
		epochStart, epochId, rawEraEnd,
	)
	assert.Equal(t, epochStart, babbageEraEnd[1].(uint64),
		"past era with transition-epoch pparams: EraEnd slot should be "+
			"confirmed boundary (%d = lastEp.StartSlot), not raw end (%d)",
		epochStart, rawEraEnd,
	)
	assert.Equal(t, epochId, babbageEraEnd[2].(uint64),
		"past era with transition-epoch pparams: EraEnd epochNo should be "+
			"the transition epoch's own ID (%d), not next epoch (%d)",
		epochId, epochId+1,
	)
	// Sanity: the raw boundary must NOT appear as the EraEnd slot.
	assert.NotEqual(t, rawEraEnd, babbageEraEnd[1].(uint64),
		"raw EraEnd slot (%d) must not be used for a transition epoch", rawEraEnd,
	)
}

// TestQueryHardForkEraHistory_PastEra_TransitionEpoch_Contiguity verifies that
// when a past era ends on a transition epoch the rolled-back timespan produces
// contiguous era boundaries: the closed era's EraEnd.relTime must equal the
// open era's EraStart.relTime.
//
// Without the timespan roll-back fix, there would be a gap of
// epochPicoseconds(lastEp) between the two, because timespan was not decremented
// after correcting tmpEnd.
//
// Setup:
//   - currentEra = Conway (era 9)
//   - Babbage epoch 499: startSlot=64_000_000, slotLen=1_000ms, length=432_000
//     → picoseconds = 1_000 * 432_000 * 1e9 = 432_000_000_000_000_000
//     → raw end slot = 64_432_000  (transition epoch — pparams ProtocolMajor=9)
//   - Conway epoch 500: startSlot=64_000_000 (same as Babbage 499's StartSlot),
//     slotLen=1_000ms, length=432_000
//
// Expected: babbageEraEnd.relTime == conwayEraStart.relTime
func TestQueryHardForkEraHistory_PastEra_TransitionEpoch_Contiguity(t *testing.T) {
	const (
		babbageEpochId    = uint64(499)
		babbageEpochStart = uint64(64_000_000)
		babbageEpochLen   = uint(432_000)
		slotLenMs         = uint(1_000)
		conwayEpochId     = uint64(500)
		// The Conway epoch starts at the Babbage transition epoch's StartSlot
		// (the confirmed era boundary).
		conwayEpochStart = babbageEpochStart
		conwayEpochLen   = uint(432_000)
	)

	db := newTestDB(t)
	// Babbage transition epoch (pparams carry Conway major version).
	require.NoError(t, db.SetEpoch(
		babbageEpochStart, babbageEpochId,
		nil, nil, nil, nil,
		eras.BabbageEraDesc.Id, slotLenMs, babbageEpochLen,
		nil,
	))
	pp := &babbage.BabbageProtocolParameters{ProtocolMajor: 9}
	ppCbor, err := cbor.Encode(pp)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		ppCbor,
		babbageEpochStart,
		babbageEpochId,
		eras.BabbageEraDesc.Id,
		nil,
	))
	// Conway epoch starts at the confirmed boundary.
	require.NoError(t, db.SetEpoch(
		conwayEpochStart, conwayEpochId,
		nil, nil, nil, nil,
		eras.ConwayEraDesc.Id, slotLenMs, conwayEpochLen,
		nil,
	))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(conwayEpochStart+1, []byte("tip")),
		},
		transitionInfo: hardfork.NewTransitionUnknown(),
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	result, err := ls.queryHardForkEraHistory()
	require.NoError(t, err)

	eraList, ok := result.(cbor.IndefLengthList)
	require.True(t, ok)

	// Extract relTime (big.Int) from an era's start or end tuple.
	relTime := func(tuple []any) *big.Int {
		require.GreaterOrEqual(t, len(tuple), 1)
		v, ok := tuple[0].(*big.Int)
		require.True(t, ok, "relTime should be *big.Int, got %T", tuple[0])
		return v
	}

	// Locate Babbage and Conway entries in the list by matching their EraEnd
	// epoch numbers.
	var babbageEnd, conwayStart []any
	for _, entry := range eraList {
		eraEntry, ok := entry.([]any)
		if !ok || len(eraEntry) < 3 {
			continue
		}
		start, ok := eraEntry[0].([]any)
		if !ok {
			continue
		}
		end, ok := eraEntry[1].([]any)
		if !ok {
			continue
		}
		if len(end) < 3 {
			continue
		}
		endEpoch, ok := end[2].(uint64)
		if !ok {
			continue
		}
		// Babbage's corrected EraEnd has epochNo = babbageEpochId.
		if endEpoch == babbageEpochId {
			babbageEnd = end
		}
		// Conway's EraStart has epochNo = conwayEpochId.
		if len(start) >= 3 {
			startEpoch, ok := start[2].(uint64)
			if ok && startEpoch == conwayEpochId {
				conwayStart = start
			}
		}
	}

	require.NotNil(t, babbageEnd,
		"Babbage EraEnd with epochNo=%d not found in era history", babbageEpochId)
	require.NotNil(t, conwayStart,
		"Conway EraStart with epochNo=%d not found in era history", conwayEpochId)

	babbageEndTime := relTime(babbageEnd)
	conwayStartTime := relTime(conwayStart)

	assert.Equal(t, 0, babbageEndTime.Cmp(conwayStartTime),
		"era boundaries must be contiguous: Babbage EraEnd.relTime (%s) != Conway EraStart.relTime (%s); "+
			"timespan was not rolled back after correcting the transition-epoch EraEnd",
		babbageEndTime.String(), conwayStartTime.String(),
	)
}
