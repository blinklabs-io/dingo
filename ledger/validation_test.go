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
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTransaction implements lcommon.Transaction for
// testing. Only the Cbor() method is needed for our
// validation functions.
type mockTransaction struct {
	lcommon.Transaction
	cbor []byte
}

func (m *mockTransaction) Cbor() []byte {
	return m.cbor
}

func TestTxBodySize(t *testing.T) {
	tests := []struct {
		name     string
		cbor     []byte
		expected uint64
	}{
		{
			name:     "empty cbor",
			cbor:     []byte{},
			expected: 0,
		},
		{
			name:     "small transaction",
			cbor:     make([]byte, 256),
			expected: 256,
		},
		{
			name:     "typical transaction",
			cbor:     make([]byte, 4096),
			expected: 4096,
		},
		{
			name:     "large transaction",
			cbor:     make([]byte, 16384),
			expected: 16384,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTransaction{cbor: tc.cbor}
			size := TxBodySize(tx)
			assert.Equal(
				t,
				tc.expected,
				size,
			)
		})
	}
}

func TestValidateTxSize(t *testing.T) {
	tests := []struct {
		name      string
		txSize    int
		maxSize   uint
		expectErr bool
	}{
		{
			name:      "within limit",
			txSize:    1000,
			maxSize:   16384,
			expectErr: false,
		},
		{
			name:      "exactly at limit",
			txSize:    16384,
			maxSize:   16384,
			expectErr: false,
		},
		{
			name:      "one byte over limit",
			txSize:    16385,
			maxSize:   16384,
			expectErr: true,
		},
		{
			name:      "well over limit",
			txSize:    32768,
			maxSize:   16384,
			expectErr: true,
		},
		{
			name:      "zero size transaction",
			txSize:    0,
			maxSize:   16384,
			expectErr: false,
		},
		{
			name:      "zero max size",
			txSize:    1,
			maxSize:   0,
			expectErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTransaction{
				cbor: make([]byte, tc.txSize),
			}
			err := ValidateTxSize(tx, tc.maxSize)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					"exceeds maximum",
				)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTxExUnits(t *testing.T) {
	tests := []struct {
		name      string
		total     lcommon.ExUnits
		max       lcommon.ExUnits
		expectErr bool
		errMsg    string
	}{
		{
			name: "within both limits",
			total: lcommon.ExUnits{
				Memory: 100,
				Steps:  200,
			},
			max: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2000,
			},
			expectErr: false,
		},
		{
			name: "exactly at both limits",
			total: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2000,
			},
			max: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2000,
			},
			expectErr: false,
		},
		{
			name: "memory exceeds limit",
			total: lcommon.ExUnits{
				Memory: 1001,
				Steps:  2000,
			},
			max: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2000,
			},
			expectErr: true,
			errMsg:    "memory",
		},
		{
			name: "steps exceeds limit",
			total: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2001,
			},
			max: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2000,
			},
			expectErr: true,
			errMsg:    "steps",
		},
		{
			name: "both exceed limits",
			total: lcommon.ExUnits{
				Memory: 1001,
				Steps:  2001,
			},
			max: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2000,
			},
			expectErr: true,
			errMsg:    "memory",
		},
		{
			name: "zero usage",
			total: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			max: lcommon.ExUnits{
				Memory: 1000,
				Steps:  2000,
			},
			expectErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTxExUnits(tc.total, tc.max)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					tc.errMsg,
				)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCalculateMinFee(t *testing.T) {
	tests := []struct {
		name        string
		txSize      uint64
		exUnits     lcommon.ExUnits
		minFeeA     uint
		minFeeB     uint
		pricesMem   *big.Rat
		pricesSteps *big.Rat
		expected    uint64
	}{
		{
			name:   "base fee only, no scripts",
			txSize: 200,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   nil,
			pricesSteps: nil,
			expected:    44*200 + 155381,
		},
		{
			name:   "base fee with zero exunits",
			txSize: 300,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    44*300 + 155381,
		},
		{
			// Mainnet-like parameters:
			// minFeeA = 44, minFeeB = 155381
			// pricesMem = 577/10000 = 0.0577
			// pricesSteps = 721/10000000 = 0.0000721
			// txSize = 300, mem = 1000000, steps = 200000000
			// baseFee = 44*300 + 155381 = 13200 + 155381 = 168581
			// memFee = ceil(577/10000 * 1000000)
			//        = ceil(577000000/10000) = ceil(57700) = 57700
			// stepFee = ceil(721/10000000 * 200000000)
			//         = ceil(144200000000/10000000)
			//         = ceil(14420) = 14420
			// scriptFee = 57700 + 14420 = 72120
			// total = 168581 + 72120 = 240701
			name:   "mainnet-like parameters",
			txSize: 300,
			exUnits: lcommon.ExUnits{
				Memory: 1000000,
				Steps:  200000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    240701,
		},
		{
			// Test ceiling behavior:
			// pricesMem = 1/3, mem = 1
			// ceil(1/3 * 1) = ceil(1/3) = 1
			// pricesSteps = 1/3, steps = 1
			// ceil(1/3 * 1) = ceil(1/3) = 1
			// scriptFee = 1 + 1 = 2
			// baseFee = 0*0 + 0 = 0
			// total = 2
			name:   "ceiling rounding",
			txSize: 0,
			exUnits: lcommon.ExUnits{
				Memory: 1,
				Steps:  1,
			},
			minFeeA:     0,
			minFeeB:     0,
			pricesMem:   big.NewRat(1, 3),
			pricesSteps: big.NewRat(1, 3),
			expected:    2,
		},
		{
			// Exact division, no ceiling needed:
			// pricesMem = 1/2, mem = 4
			// ceil(1/2 * 4) = ceil(2) = 2
			// pricesSteps = 1/4, steps = 8
			// ceil(1/4 * 8) = ceil(2) = 2
			// scriptFee = 2 + 2 = 4
			// baseFee = 10*100 + 500 = 1500
			// total = 1504
			name:   "exact division",
			txSize: 100,
			exUnits: lcommon.ExUnits{
				Memory: 4,
				Steps:  8,
			},
			minFeeA:     10,
			minFeeB:     500,
			pricesMem:   big.NewRat(1, 2),
			pricesSteps: big.NewRat(1, 4),
			expected:    1504,
		},
		{
			// Test with zero minFeeA:
			// baseFee = 0*300 + 155381 = 155381
			name:   "zero minFeeA",
			txSize: 300,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			minFeeA:     0,
			minFeeB:     155381,
			pricesMem:   nil,
			pricesSteps: nil,
			expected:    155381,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fee := CalculateMinFee(
				tc.txSize,
				tc.exUnits,
				tc.minFeeA,
				tc.minFeeB,
				tc.pricesMem,
				tc.pricesSteps,
			)
			assert.Equal(
				t,
				tc.expected,
				fee,
				"fee mismatch",
			)
		})
	}
}
