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
// testing.
type mockTransaction struct {
	lcommon.Transaction
	cbor   []byte
	txType int
}

func (m *mockTransaction) Cbor() []byte {
	return m.cbor
}

func (m *mockTransaction) Type() int {
	return m.txType
}

// mockFeeTx extends mockTransaction with Fee() and
// Witnesses() support for fee validation tests.
type mockFeeTx struct {
	lcommon.Transaction
	cbor      []byte
	txType    int
	fee       *big.Int
	witnesses lcommon.TransactionWitnessSet
}

func (m *mockFeeTx) Cbor() []byte {
	return m.cbor
}

func (m *mockFeeTx) Type() int {
	return m.txType
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

func TestTxSizeForFee(t *testing.T) {
	tests := []struct {
		name     string
		txType   int
		cbor     []byte
		expected uint64
	}{
		{
			name:     "empty cbor",
			cbor:     []byte{},
			expected: 0,
		},
		{
			// Pre-Alonzo: no IsValid byte, full size
			// returned unchanged.
			name:     "pre-alonzo full size",
			txType:   1, // Shelley
			cbor:     make([]byte, 256),
			expected: 256,
		},
		{
			// Alonzo+ 4-element TX: fee size excludes the
			// 1-byte IsValid boolean.
			name:     "alonzo subtracts isvalid byte",
			txType:   4, // Alonzo
			cbor:     make([]byte, 256),
			expected: 255,
		},
		{
			name:     "typical alonzo transaction",
			txType:   4,
			cbor:     make([]byte, 4096),
			expected: 4095,
		},
		{
			name:     "large alonzo transaction",
			txType:   4,
			cbor:     make([]byte, 16384),
			expected: 16383,
		},
		{
			// Mary (pre-Alonzo) TX: no subtraction.
			name:     "mary transaction full size",
			txType:   3, // Mary
			cbor:     make([]byte, 4096),
			expected: 4096,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTransaction{
				cbor:   tc.cbor,
				txType: tc.txType,
			}
			size := TxSizeForFee(tx)
			assert.Equal(t, tc.expected, size)
		})
	}
}

func TestValidateTxSize(t *testing.T) {
	tests := []struct {
		name      string
		txSize    int
		txType    int
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
			// Fee-relevant size = 16386 - 1 = 16385 > 16384
			name:      "one byte over limit",
			txSize:    16386,
			txType:    txTypeAlonzo,
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
			// Fee-relevant size = 2 - 1 = 1 > 0
			name:      "zero max size with non-zero tx",
			txSize:    2,
			txType:    txTypeAlonzo,
			maxSize:   0,
			expectErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTransaction{
				cbor:   make([]byte, tc.txSize),
				txType: tc.txType,
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
			// Ceiling behavior: single ceiling over sum.
			// Per Alonzo spec: scriptFee = ceil(prMem*mem + prSteps*steps)
			// pricesMem=1/3, mem=1 => 1/3
			// pricesSteps=1/3, steps=1 => 1/3
			// sum = 1/3 + 1/3 = 2/3
			// scriptFee = ceil(2/3) = 1
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
			expected:    1,
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

func TestDeclaredExUnits(t *testing.T) {
	tests := []struct {
		name      string
		tx        lcommon.Transaction
		expected  lcommon.ExUnits
		expectErr bool
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
		{
			name: "memory overflow",
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
										Memory: math.MaxInt64,
										Steps:  100,
									},
								},
							},
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 1,
										Steps:  100,
									},
								},
							},
						},
					},
				},
			},
			expectErr: true,
		},
		{
			name: "steps overflow",
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
										Memory: 100,
										Steps:  math.MaxInt64,
									},
								},
							},
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 100,
										Steps:  1,
									},
								},
							},
						},
					},
				},
			},
			expectErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DeclaredExUnits(tc.tx)
			if tc.expectErr {
				require.Error(t, err)
				assert.ErrorIs(
					t,
					err,
					ErrExUnitsOverflow,
				)
				return
			}
			require.NoError(t, err)
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

func TestSafeAddExUnits(t *testing.T) {
	tests := []struct {
		name      string
		a         lcommon.ExUnits
		b         lcommon.ExUnits
		expected  lcommon.ExUnits
		expectErr bool
	}{
		{
			name: "normal addition",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  200,
			},
			b: lcommon.ExUnits{
				Memory: 300,
				Steps:  400,
			},
			expected: lcommon.ExUnits{
				Memory: 400,
				Steps:  600,
			},
		},
		{
			name: "zero plus values",
			a:    lcommon.ExUnits{},
			b: lcommon.ExUnits{
				Memory: 500,
				Steps:  1000,
			},
			expected: lcommon.ExUnits{
				Memory: 500,
				Steps:  1000,
			},
		},
		{
			name: "both zero",
			a:    lcommon.ExUnits{},
			b:    lcommon.ExUnits{},
			expected: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
		},
		{
			name: "max int64 values no overflow",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
			b: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			expected: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
		},
		{
			name: "memory overflow",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: 1,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "steps overflow",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  math.MaxInt64,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  1,
			},
			expectErr: true,
		},
		{
			name: "both overflow",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
			b: lcommon.ExUnits{
				Memory: 1,
				Steps:  1,
			},
			expectErr: true,
		},
		{
			name: "large values just under max",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64 - 1,
				Steps:  math.MaxInt64 - 1,
			},
			b: lcommon.ExUnits{
				Memory: 1,
				Steps:  1,
			},
			expected: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
		},
		{
			name: "half max values",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64 / 2,
				Steps:  math.MaxInt64 / 2,
			},
			b: lcommon.ExUnits{
				Memory: math.MaxInt64 / 2,
				Steps:  math.MaxInt64 / 2,
			},
			expected: lcommon.ExUnits{
				Memory: (math.MaxInt64 / 2) * 2,
				Steps:  (math.MaxInt64 / 2) * 2,
			},
		},
		{
			name: "negative memory in a",
			a: lcommon.ExUnits{
				Memory: -1,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "negative memory in b",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: -1,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "negative steps in a",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  -1,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "negative steps in b",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  -1,
			},
			expectErr: true,
		},
		{
			name: "large negative memory wraparound attempt",
			a: lcommon.ExUnits{
				Memory: math.MinInt64,
				Steps:  0,
			},
			b: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  0,
			},
			expectErr: true,
		},
		{
			name: "large negative steps wraparound attempt",
			a: lcommon.ExUnits{
				Memory: 0,
				Steps:  math.MinInt64,
			},
			b: lcommon.ExUnits{
				Memory: 0,
				Steps:  math.MaxInt64,
			},
			expectErr: true,
		},
		{
			name: "both negative memory and steps",
			a: lcommon.ExUnits{
				Memory: -100,
				Steps:  -200,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  200,
			},
			expectErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := SafeAddExUnits(tc.a, tc.b)
			if tc.expectErr {
				require.Error(t, err)
				assert.ErrorIs(
					t,
					err,
					ErrExUnitsOverflow,
				)
				return
			}
			require.NoError(t, err)
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
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 200000 >= 168581 => valid
			name: "sufficient fee no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
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
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 168581 (exact) => valid
			name: "exact minimum fee no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
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
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 168580 < 168581 => invalid
			name: "one lovelace under minimum no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
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
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// scriptFee = ceil(577/10000*1000000 +
			//   721/10000000*200000000) = 72120
			// minFee = 168581 + 72120 = 240701
			// Fee = 240701 (exact) => valid
			name: "exact minimum fee with scripts",
			tx: &mockFeeTx{
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(240701),
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
			// Fee-relevant size = 301 - 1 = 300
			// minFee = 44*300 + 155381 + 72120 = 240701
			// Fee = 240700 < 240701 => invalid
			name: "one lovelace under minimum with scripts",
			tx: &mockFeeTx{
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(240700),
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
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(500000),
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
			// Fee-relevant size = 401 - 1 = 400
			// baseFee = 44*400 + 155381 = 172981
			// scriptFee = ceil(577/10000*600000 +
			//   721/10000000*200000000) = 49040
			// minFee = 172981 + 49040 = 222021
			name: "multiple redeemers exact fee",
			tx: &mockFeeTx{
				cbor:   make([]byte, 401),
				txType: txTypeAlonzo,
				fee:    big.NewInt(222021),
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
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
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
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			name: "nil prices no script fee",
			tx: &mockFeeTx{
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(168581),
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
		cbor:      make([]byte, 301),
		txType:    txTypeAlonzo,
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
	// Should mention both the provided fee and the min.
	// Fee-relevant TX size = 301 - 1 = 300 (excludes IsValid byte).
	// minFee = 44*300 + 155381 = 168581
	assert.Contains(t, err.Error(), "100000")
	assert.Contains(t, err.Error(), "168581")
}

func TestCalculateMinFee_NormalValuesNoOverflow(
	t *testing.T,
) {
	// Mainnet-like parameters should still work.
	fee := CalculateMinFee(
		300,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		big.NewRat(577, 10000),
		big.NewRat(721, 10000000),
	)
	assert.Equal(t, uint64(240701), fee)
}

func TestCalculateMinFee_OverflowSaturates(t *testing.T) {
	// Force an overflow: huge num/denom ratio with large ExUnits.
	fee := CalculateMinFee(
		math.MaxUint64,
		lcommon.ExUnits{
			Memory: math.MaxInt64,
			Steps:  math.MaxInt64,
		},
		44,
		155381,
		big.NewRat(math.MaxInt64, 1),
		big.NewRat(math.MaxInt64, 1),
	)
	assert.Equal(t, uint64(math.MaxUint64), fee,
		"overflow should saturate at MaxUint64",
	)
}
