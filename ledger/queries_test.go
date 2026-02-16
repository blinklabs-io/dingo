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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
