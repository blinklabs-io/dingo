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
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
// current era's EraEnd must be capped at ledgerTip + safeZone, not left as the
// end of the last committed epoch.  The Haskell node uses StandardSafeZone for
// this purpose; without it, clients can attempt slot↔time conversions beyond
// the safe forecast horizon.
//
// Setup:
//   - One Conway epoch: startSlot=100_000, length=432_000 (ends at slot 532_000)
//   - ledgerTip at slot 200_000 (well inside the epoch)
//   - safeZone = ceil(3k/f) = ceil(3*432/0.05) = 25_920
//
// Expected EraEnd slot: 200_000 + 25_920 = 225_920
// Current (broken) EraEnd slot: 100_000 + 432_000 = 532_000
func TestQueryHardForkEraHistory_OpenEraEndBoundedBySafeZone(t *testing.T) {
	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000) // 1 second in milliseconds
		epochId        = uint64(500)
	)
	// safeZone = ceil(3 * 432 / 0.05) = 25_920
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := tipSlot + expectedSafeZone // 225_920

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

	assert.Equal(t, expectedEraEndSlot, actualEraEndSlot,
		"open era EraEnd slot should be ledgerTip(%d) + safeZone(%d) = %d, "+
			"not the epoch boundary at slot %d",
		tipSlot, expectedSafeZone, expectedEraEndSlot,
		epochStartSlot+uint64(epochLen),
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
		tipSlot          = uint64(200_000)
		epoch500Start    = uint64(100_000)
		epoch501Start    = uint64(532_000)
		epochLen         = uint(432_000)
		slotLenMs        = uint(1_000)
		epoch500Id       = uint64(500)
		epoch501Id       = uint64(501)
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
		transitionInfo: TransitionInfo{
			State:      TransitionKnown,
			KnownEpoch: epoch501Id,
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

// TestQueryHardForkEraHistory_TransitionUnknown_FallsBackToSafeZone confirms
// that TransitionUnknown state still produces the safe-zone-capped EraEnd,
// i.e. the existing behaviour is preserved.
func TestQueryHardForkEraHistory_TransitionUnknown_FallsBackToSafeZone(t *testing.T) {
	const (
		tipSlot        = uint64(200_000)
		epochStartSlot = uint64(100_000)
		epochLen       = uint(432_000)
		slotLenMs      = uint(1_000)
		epochId        = uint64(500)
	)
	const expectedSafeZone = uint64(25_920)
	expectedEraEndSlot := tipSlot + expectedSafeZone

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
		transitionInfo: TransitionInfo{State: TransitionUnknown},
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
	assert.Equal(t, expectedEraEndSlot, actualSlot,
		"TransitionUnknown: EraEnd slot should be tipSlot(%d)+safeZone(%d)=%d",
		tipSlot, expectedSafeZone, expectedEraEndSlot,
	)
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
		expectedState  TransitionState
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
			expectedState: TransitionKnown,
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
			expectedState: TransitionUnknown,
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
			expectedState: TransitionUnknown,
		},
		{
			// Nil pparams: must not panic, leave TransitionUnknown.
			name:          "nil pparams → TransitionUnknown",
			currentEra:    *eras.GetEraById(eras.BabbageEraDesc.Id),
			currentEpoch:  models.Epoch{EpochId: 400, EraId: eras.BabbageEraDesc.Id},
			currentPParams: nil,
			expectedState: TransitionUnknown,
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
			if tc.expectedState == TransitionKnown {
				assert.Equal(t, tc.expectedEpoch, ls.transitionInfo.KnownEpoch)
			}
		})
	}
}
