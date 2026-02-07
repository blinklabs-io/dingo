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

package eras

import (
	"iter"
	"math"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTransaction implements lcommon.Transaction for
// testing. Only the Cbor() method is needed.
type mockTransaction struct {
	lcommon.Transaction
	cbor []byte
}

func (m *mockTransaction) Cbor() []byte {
	return m.cbor
}

// mockFeeTx extends mockTransaction with Fee() and
// Witnesses() support for fee validation tests.
type mockFeeTx struct {
	lcommon.Transaction
	cbor      []byte
	fee       *big.Int
	witnesses lcommon.TransactionWitnessSet
}

func (m *mockFeeTx) Cbor() []byte {
	return m.cbor
}

func (m *mockFeeTx) Fee() *big.Int {
	return m.fee
}

func (m *mockFeeTx) Witnesses() lcommon.TransactionWitnessSet {
	return m.witnesses
}

// mockWitnessSet implements TransactionWitnessSet for
// testing, returning only redeemers.
type mockWitnessSet struct {
	redeemers lcommon.TransactionWitnessRedeemers
}

func (m *mockWitnessSet) Vkey() []lcommon.VkeyWitness {
	return nil
}

func (m *mockWitnessSet) NativeScripts() []lcommon.NativeScript {
	return nil
}

func (m *mockWitnessSet) Bootstrap() []lcommon.BootstrapWitness {
	return nil
}

func (m *mockWitnessSet) PlutusData() []lcommon.Datum {
	return nil
}

func (m *mockWitnessSet) PlutusV1Scripts() []lcommon.PlutusV1Script {
	return nil
}

func (m *mockWitnessSet) PlutusV2Scripts() []lcommon.PlutusV2Script {
	return nil
}

func (m *mockWitnessSet) PlutusV3Scripts() []lcommon.PlutusV3Script {
	return nil
}

func (m *mockWitnessSet) Redeemers() lcommon.TransactionWitnessRedeemers {
	return m.redeemers
}

// mockRedeemers implements TransactionWitnessRedeemers
// for testing.
type mockRedeemers struct {
	entries []struct {
		key lcommon.RedeemerKey
		val lcommon.RedeemerValue
	}
}

func (m *mockRedeemers) Indexes(
	_ lcommon.RedeemerTag,
) []uint {
	return nil
}

func (m *mockRedeemers) Value(
	_ uint,
	_ lcommon.RedeemerTag,
) lcommon.RedeemerValue {
	return lcommon.RedeemerValue{}
}

func (m *mockRedeemers) Iter() iter.Seq2[lcommon.RedeemerKey, lcommon.RedeemerValue] {
	return func(
		yield func(lcommon.RedeemerKey, lcommon.RedeemerValue) bool,
	) {
		for _, e := range m.entries {
			if !yield(e.key, e.val) {
				return
			}
		}
	}
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
			name:     "single byte",
			cbor:     []byte{0x00},
			expected: 1,
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
			assert.Equal(t, tc.expected, size)
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
			name:      "zero max size with non-zero tx",
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
			name: "both exceed limits returns memory error first",
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

func TestCeilMul(t *testing.T) {
	tests := []struct {
		name     string
		num      *big.Int
		denom    *big.Int
		value    *big.Int
		expected uint64
	}{
		{
			name:     "exact division",
			num:      big.NewInt(1),
			denom:    big.NewInt(2),
			value:    big.NewInt(10),
			expected: 5,
		},
		{
			name:     "ceiling rounds up",
			num:      big.NewInt(1),
			denom:    big.NewInt(3),
			value:    big.NewInt(1),
			expected: 1,
		},
		{
			name:     "zero value",
			num:      big.NewInt(577),
			denom:    big.NewInt(10000),
			value:    big.NewInt(0),
			expected: 0,
		},
		{
			name:     "zero numerator",
			num:      big.NewInt(0),
			denom:    big.NewInt(10000),
			value:    big.NewInt(1000000),
			expected: 0,
		},
		{
			name:     "mainnet memory price",
			num:      big.NewInt(577),
			denom:    big.NewInt(10000),
			value:    big.NewInt(1000000),
			expected: 57700,
		},
		{
			name:     "mainnet step price",
			num:      big.NewInt(721),
			denom:    big.NewInt(10000000),
			value:    big.NewInt(200000000),
			expected: 14420,
		},
		{
			name:     "ceiling with remainder 1",
			num:      big.NewInt(1),
			denom:    big.NewInt(3),
			value:    big.NewInt(2),
			expected: 1,
		},
		{
			name:     "ceiling with remainder 2",
			num:      big.NewInt(1),
			denom:    big.NewInt(3),
			value:    big.NewInt(4),
			expected: 2,
		},
		{
			name:     "large values",
			num:      big.NewInt(577),
			denom:    big.NewInt(10000),
			value:    big.NewInt(14000000),
			expected: 807800,
		},
		{
			// ceil(577/10000 * 14000001)
			// = ceil(8078005777/10000) = ceil(807800.5777) = 807801
			name:     "large values with ceiling",
			num:      big.NewInt(577),
			denom:    big.NewInt(10000),
			value:    big.NewInt(14000001),
			expected: 807801,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := CeilMul(
				tc.num,
				tc.denom,
				tc.value,
			)
			assert.Equal(
				t,
				tc.expected,
				result,
			)
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
			name:   "no scripts - nil prices",
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
			name:   "no scripts - zero exunits with prices set",
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
			// Single script with mainnet-like parameters:
			// minFeeA=44, minFeeB=155381
			// pricesMem=577/10000, pricesSteps=721/10000000
			// txSize=300, mem=1000000, steps=200000000
			// baseFee = 44*300+155381 = 168581
			// memFee = ceil(577*1000000/10000) = 57700
			// stepFee = ceil(721*200000000/10000000) = 14420
			// total = 168581 + 57700 + 14420 = 240701
			name:   "single script mainnet-like",
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
			// Multiple scripts - the exUnits represent the
			// sum of all script execution units.
			// Two scripts: script1(mem=500000, steps=100000000)
			//              script2(mem=500000, steps=100000000)
			// Total: mem=1000000, steps=200000000
			// Same as single script test above.
			name:   "multiple scripts summed exunits",
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
			// Three scripts with different costs summed:
			// script1(mem=300000, steps=50000000)
			// script2(mem=200000, steps=80000000)
			// script3(mem=100000, steps=70000000)
			// Total: mem=600000, steps=200000000
			// baseFee = 44*400 + 155381 = 172981
			// memFee = ceil(577*600000/10000) = ceil(34620) = 34620
			// stepFee = ceil(721*200000000/10000000) = 14420
			// total = 172981 + 34620 + 14420 = 222021
			name:   "three scripts summed",
			txSize: 400,
			exUnits: lcommon.ExUnits{
				Memory: 600000,
				Steps:  200000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    222021,
		},
		{
			// Ceiling behavior: fractions round up.
			// pricesMem=1/3, mem=1 => ceil(1/3) = 1
			// pricesSteps=1/3, steps=1 => ceil(1/3) = 1
			// scriptFee = 1 + 1 = 2
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
			// Exact division: no ceiling needed.
			// pricesMem=1/2, mem=4 => ceil(2) = 2
			// pricesSteps=1/4, steps=8 => ceil(2) = 2
			// baseFee = 10*100 + 500 = 1500
			// total = 1500 + 4 = 1504
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
		{
			name:   "zero minFeeB",
			txSize: 200,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			minFeeA:     44,
			minFeeB:     0,
			pricesMem:   nil,
			pricesSteps: nil,
			expected:    44 * 200,
		},
		{
			name:   "zero everything",
			txSize: 0,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			minFeeA:     0,
			minFeeB:     0,
			pricesMem:   nil,
			pricesSteps: nil,
			expected:    0,
		},
		{
			// Large ExUnits to test big number arithmetic.
			// mem=14000000 (14M), steps=10000000000 (10B)
			// pricesMem=577/10000
			//   memFee = ceil(577*14000000/10000) = 807800
			// pricesSteps=721/10000000
			//   stepFee = ceil(721*10000000000/10000000) = 721000
			// baseFee = 44*500 + 155381 = 177381
			// total = 177381 + 807800 + 721000 = 1706181
			name:   "large exunits",
			txSize: 500,
			exUnits: lcommon.ExUnits{
				Memory: 14000000,
				Steps:  10000000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    1706181,
		},
		{
			// Max ExUnits for mainnet (as of Conway era).
			// maxTxMem=14000000, maxTxSteps=10000000000
			// Same values as previous test.
			name:   "max exunits mainnet",
			txSize: 16384,
			exUnits: lcommon.ExUnits{
				Memory: 14000000,
				Steps:  10000000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			// baseFee = 44*16384 + 155381 = 720896 + 155381 = 876277
			// memFee = 807800
			// stepFee = 721000
			// total = 876277 + 807800 + 721000 = 2405077
			expected: 2405077,
		},
		{
			// Only memory exunits, zero steps.
			// memFee = ceil(577*1000000/10000) = 57700
			// stepFee = ceil(721*0/10000000) = 0
			// baseFee = 44*200 + 155381 = 164181
			// total = 164181 + 57700 = 221881
			name:   "memory only no steps",
			txSize: 200,
			exUnits: lcommon.ExUnits{
				Memory: 1000000,
				Steps:  0,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    221881,
		},
		{
			// Only step exunits, zero memory.
			// memFee = ceil(577*0/10000) = 0
			// stepFee = ceil(721*200000000/10000000) = 14420
			// baseFee = 44*200 + 155381 = 164181
			// total = 164181 + 14420 = 178601
			name:   "steps only no memory",
			txSize: 200,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  200000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    178601,
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

func TestCalculateMinFee_ScriptFeeAddsCorrectly(t *testing.T) {
	// Verify that a transaction with scripts costs more
	// than the same transaction without scripts.
	txSize := uint64(300)
	minFeeA := uint(44)
	minFeeB := uint(155381)
	pricesMem := big.NewRat(577, 10000)
	pricesSteps := big.NewRat(721, 10000000)

	// Fee with no scripts
	feeNoScripts := CalculateMinFee(
		txSize,
		lcommon.ExUnits{Memory: 0, Steps: 0},
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)

	// Fee with scripts
	feeWithScripts := CalculateMinFee(
		txSize,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)

	assert.Greater(
		t,
		feeWithScripts,
		feeNoScripts,
		"fee with scripts should be greater than base fee",
	)

	// The difference should equal the script execution fee
	scriptFee := feeWithScripts - feeNoScripts
	// memFee = ceil(577*1000000/10000) = 57700
	// stepFee = ceil(721*200000000/10000000) = 14420
	assert.Equal(
		t,
		uint64(72120),
		scriptFee,
		"script fee component mismatch",
	)
}

func TestCalculateMinFee_MultipleScriptsSum(t *testing.T) {
	// Verify that running N scripts with individual
	// ExUnits that sum to a total produces the same
	// fee as the total ExUnits directly.
	minFeeA := uint(44)
	minFeeB := uint(155381)
	pricesMem := big.NewRat(577, 10000)
	pricesSteps := big.NewRat(721, 10000000)
	txSize := uint64(400)

	// Three individual scripts
	scripts := []lcommon.ExUnits{
		{Memory: 300000, Steps: 50000000},
		{Memory: 200000, Steps: 80000000},
		{Memory: 100000, Steps: 70000000},
	}

	// Sum them up (simulating what EvaluateTx does)
	var totalExUnits lcommon.ExUnits
	for _, s := range scripts {
		totalExUnits.Memory += s.Memory
		totalExUnits.Steps += s.Steps
	}

	require.Equal(t, int64(600000), totalExUnits.Memory)
	require.Equal(t, int64(200000000), totalExUnits.Steps)

	fee := CalculateMinFee(
		txSize,
		totalExUnits,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)

	// baseFee = 44*400 + 155381 = 172981
	// memFee = ceil(577*600000/10000) = 34620
	// stepFee = ceil(721*200000000/10000000) = 14420
	// total = 172981 + 34620 + 14420 = 222021
	assert.Equal(t, uint64(222021), fee)
}

func TestCalculateMinFee_NilPricesIgnoresExUnits(t *testing.T) {
	// When prices are nil, even non-zero ExUnits should
	// not contribute to the fee. This can happen in
	// pre-Alonzo eras where there are no execution costs.
	fee := CalculateMinFee(
		200,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		nil,
		nil,
	)
	baseFee := uint64(44*200 + 155381)
	assert.Equal(
		t,
		baseFee,
		fee,
		"nil prices should result in base fee only",
	)
}

func TestCalculateMinFee_OnePriceNilIgnoresExUnits(t *testing.T) {
	// When only one price is nil, both should be
	// ignored (the function requires both to be non-nil).
	fee1 := CalculateMinFee(
		200,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		big.NewRat(577, 10000),
		nil,
	)
	fee2 := CalculateMinFee(
		200,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		nil,
		big.NewRat(721, 10000000),
	)
	baseFee := uint64(44*200 + 155381)
	assert.Equal(t, baseFee, fee1,
		"nil step price should ignore script fees",
	)
	assert.Equal(t, baseFee, fee2,
		"nil mem price should ignore script fees",
	)
}

func TestCeilMul_ExactMultiples(t *testing.T) {
	// When num*value is exactly divisible by denom,
	// ceiling should equal the exact quotient.
	result := CeilMul(
		big.NewInt(3),
		big.NewInt(1),
		big.NewInt(5),
	)
	assert.Equal(t, uint64(15), result)
}

func TestCeilMul_LargeValues(t *testing.T) {
	// Test with values near the practical maximum for
	// Cardano mainnet. Max memory is 14M, max steps
	// is 10B. Price rationals are small fractions.
	memResult := CeilMul(
		big.NewInt(577),
		big.NewInt(10000),
		big.NewInt(14000000),
	)
	assert.Equal(t, uint64(807800), memResult)

	stepResult := CeilMul(
		big.NewInt(721),
		big.NewInt(10000000),
		big.NewInt(10000000000),
	)
	assert.Equal(t, uint64(721000), stepResult)
}

func TestCeilMul_MaxInt64Values(t *testing.T) {
	// Test with max int64 to ensure no overflow in
	// big.Int arithmetic.
	maxVal := big.NewInt(math.MaxInt64)
	result := CeilMul(
		big.NewInt(1),
		big.NewInt(1),
		maxVal,
	)
	assert.Equal(t, uint64(math.MaxInt64), result)
}

func TestDeclaredExUnits(t *testing.T) {
	tests := []struct {
		name     string
		tx       lcommon.Transaction
		expected lcommon.ExUnits
	}{
		{
			name: "no witnesses",
			tx: &mockFeeTx{
				cbor:      make([]byte, 100),
				fee:       big.NewInt(200000),
				witnesses: nil,
			},
			expected: lcommon.ExUnits{},
		},
		{
			name: "no redeemers",
			tx: &mockFeeTx{
				cbor: make([]byte, 100),
				fee:  big.NewInt(200000),
				witnesses: &mockWitnessSet{
					redeemers: nil,
				},
			},
			expected: lcommon.ExUnits{},
		},
		{
			name: "single redeemer",
			tx: &mockFeeTx{
				cbor: make([]byte, 100),
				fee:  big.NewInt(200000),
				witnesses: &mockWitnessSet{
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 500000,
										Steps:  100000000,
									},
								},
							},
						},
					},
				},
			},
			expected: lcommon.ExUnits{
				Memory: 500000,
				Steps:  100000000,
			},
		},
		{
			name: "multiple redeemers",
			tx: &mockFeeTx{
				cbor: make([]byte, 100),
				fee:  big.NewInt(200000),
				witnesses: &mockWitnessSet{
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 300000,
										Steps:  50000000,
									},
								},
							},
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 200000,
										Steps:  80000000,
									},
								},
							},
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 100000,
										Steps:  70000000,
									},
								},
							},
						},
					},
				},
			},
			expected: lcommon.ExUnits{
				Memory: 600000,
				Steps:  200000000,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := DeclaredExUnits(tc.tx)
			assert.Equal(
				t,
				tc.expected.Memory,
				result.Memory,
			)
			assert.Equal(
				t,
				tc.expected.Steps,
				result.Steps,
			)
		})
	}
}

func TestValidateTxFee(t *testing.T) {
	pricesMem := big.NewRat(577, 10000)
	pricesSteps := big.NewRat(721, 10000000)

	tests := []struct {
		name      string
		tx        lcommon.Transaction
		minFeeA   uint
		minFeeB   uint
		pMem      *big.Rat
		pSteps    *big.Rat
		expectErr bool
	}{
		{
			// No scripts: baseFee = 44*300 + 155381 = 168581
			// Fee = 200000 >= 168581 => valid
			name: "sufficient fee no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 300),
				fee:       big.NewInt(200000),
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 168581 (exact) => valid
			name: "exact minimum fee no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 300),
				fee:       big.NewInt(168581),
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 168580 < 168581 => invalid
			name: "one lovelace under minimum no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 300),
				fee:       big.NewInt(168580),
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: true,
		},
		{
			// baseFee = 44*300 + 155381 = 168581
			// memFee = ceil(577*1000000/10000) = 57700
			// stepFee = ceil(721*200000000/10000000) = 14420
			// minFee = 168581 + 57700 + 14420 = 240701
			// Fee = 240701 (exact) => valid
			name: "exact minimum fee with scripts",
			tx: &mockFeeTx{
				cbor: make([]byte, 300),
				fee:  big.NewInt(240701),
				witnesses: &mockWitnessSet{
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 1000000,
										Steps:  200000000,
									},
								},
							},
						},
					},
				},
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// Same as above but fee = 240700 < 240701
			name: "one lovelace under minimum with scripts",
			tx: &mockFeeTx{
				cbor: make([]byte, 300),
				fee:  big.NewInt(240700),
				witnesses: &mockWitnessSet{
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 1000000,
										Steps:  200000000,
									},
								},
							},
						},
					},
				},
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: true,
		},
		{
			// Overpaying is fine
			name: "overpaying fee with scripts",
			tx: &mockFeeTx{
				cbor: make([]byte, 300),
				fee:  big.NewInt(500000),
				witnesses: &mockWitnessSet{
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 1000000,
										Steps:  200000000,
									},
								},
							},
						},
					},
				},
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// Three scripts summed:
			// mem=600000, steps=200000000
			// baseFee = 44*400 + 155381 = 172981
			// memFee = ceil(577*600000/10000) = 34620
			// stepFee = ceil(721*200000000/10000000) = 14420
			// minFee = 172981 + 34620 + 14420 = 222021
			name: "multiple redeemers exact fee",
			tx: &mockFeeTx{
				cbor: make([]byte, 400),
				fee:  big.NewInt(222021),
				witnesses: &mockWitnessSet{
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 300000,
										Steps:  50000000,
									},
								},
							},
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 200000,
										Steps:  80000000,
									},
								},
							},
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 100000,
										Steps:  70000000,
									},
								},
							},
						},
					},
				},
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// nil fee defaults to 0, should fail
			name: "nil fee fails",
			tx: &mockFeeTx{
				cbor:      make([]byte, 300),
				fee:       nil,
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: true,
		},
		{
			// nil prices: no script fee component
			// baseFee = 44*300 + 155381 = 168581
			name: "nil prices no script fee",
			tx: &mockFeeTx{
				cbor: make([]byte, 300),
				fee:  big.NewInt(168581),
				witnesses: &mockWitnessSet{
					redeemers: &mockRedeemers{
						entries: []struct {
							key lcommon.RedeemerKey
							val lcommon.RedeemerValue
						}{
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 1000000,
										Steps:  200000000,
									},
								},
							},
						},
					},
				},
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      nil,
			pSteps:    nil,
			expectErr: false,
		},
		{
			// zero everything: minFee = 0, fee = 0 => valid
			name: "zero everything",
			tx: &mockFeeTx{
				cbor:      []byte{},
				fee:       big.NewInt(0),
				witnesses: nil,
			},
			minFeeA:   0,
			minFeeB:   0,
			pMem:      nil,
			pSteps:    nil,
			expectErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTxFee(
				tc.tx,
				tc.minFeeA,
				tc.minFeeB,
				tc.pMem,
				tc.pSteps,
			)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					"less than the calculated minimum fee",
				)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTxFee_ErrorMessageIncludesFees(
	t *testing.T,
) {
	// Verify the error message contains both the
	// provided fee and the calculated minimum.
	tx := &mockFeeTx{
		cbor:      make([]byte, 300),
		fee:       big.NewInt(100000),
		witnesses: nil,
	}
	err := ValidateTxFee(
		tx,
		44,
		155381,
		big.NewRat(577, 10000),
		big.NewRat(721, 10000000),
	)
	require.Error(t, err)
	// Should mention both the provided fee and the min
	assert.Contains(t, err.Error(), "100000")
	assert.Contains(t, err.Error(), "168581")
}
