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
	"iter"
	"math"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/ledger/eras"
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
			fee, err := CalculateMinFee(
				tc.txSize,
				tc.exUnits,
				tc.minFeeA,
				tc.minFeeB,
				tc.pricesMem,
				tc.pricesSteps,
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				tc.expected,
				fee,
				"fee mismatch",
			)
		})
	}
}

func TestDeclaredExUnits(t *testing.T) {
	t.Run("no redeemers", func(t *testing.T) {
		tx := &mockFeeTx{
			cbor:      make([]byte, 100),
			fee:       big.NewInt(200000),
			witnesses: nil,
		}
		eu, err := DeclaredExUnits(tx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), eu.Memory)
		assert.Equal(t, int64(0), eu.Steps)
	})

	t.Run("two redeemers", func(t *testing.T) {
		tx := &mockFeeTx{
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
					},
				},
			},
		}
		eu, err := DeclaredExUnits(tx)
		require.NoError(t, err)
		assert.Equal(t, int64(500000), eu.Memory)
		assert.Equal(t, int64(130000000), eu.Steps)
	})

	t.Run("overflow returns error", func(t *testing.T) {
		tx := &mockFeeTx{
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
		}
		_, err := DeclaredExUnits(tx)
		require.Error(t, err)
		assert.ErrorIs(
			t,
			err,
			eras.ErrExUnitsOverflow,
		)
	})
}

func TestValidateTxFee(t *testing.T) {
	pricesMem := big.NewRat(577, 10000)
	pricesSteps := big.NewRat(721, 10000000)

	t.Run("sufficient fee", func(t *testing.T) {
		tx := &mockFeeTx{
			cbor:      make([]byte, 300),
			fee:       big.NewInt(200000),
			witnesses: nil,
		}
		err := ValidateTxFee(
			tx, 44, 155381, pricesMem, pricesSteps,
		)
		require.NoError(t, err)
	})

	t.Run("insufficient fee", func(t *testing.T) {
		tx := &mockFeeTx{
			cbor:      make([]byte, 300),
			fee:       big.NewInt(100000),
			witnesses: nil,
		}
		err := ValidateTxFee(
			tx, 44, 155381, pricesMem, pricesSteps,
		)
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"less than the calculated minimum fee",
		)
	})

	t.Run("exact fee with scripts", func(t *testing.T) {
		// baseFee = 44*300 + 155381 = 168581
		// memFee = ceil(577*1000000/10000) = 57700
		// stepFee = ceil(721*200000000/10000000) = 14420
		// minFee = 168581 + 57700 + 14420 = 240701
		tx := &mockFeeTx{
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
		}
		err := ValidateTxFee(
			tx, 44, 155381, pricesMem, pricesSteps,
		)
		require.NoError(t, err)
	})

	t.Run("one under with scripts", func(t *testing.T) {
		tx := &mockFeeTx{
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
		}
		err := ValidateTxFee(
			tx, 44, 155381, pricesMem, pricesSteps,
		)
		require.Error(t, err)
	})
}

// mockEraTx extends mockTransaction with an era type.
type mockEraTx struct {
	lcommon.Transaction
	cbor    []byte
	eraType int
}

func (m *mockEraTx) Cbor() []byte  { return m.cbor }
func (m *mockEraTx) Type() int     { return m.eraType }
func (m *mockEraTx) IsValid() bool { return true }

func TestIsCompatibleEra(t *testing.T) {
	tests := []struct {
		name       string
		txEraId    uint
		ledgerEra  uint
		compatible bool
	}{
		{
			name:       "same era is always compatible",
			txEraId:    eras.ConwayEraDesc.Id,
			ledgerEra:  eras.ConwayEraDesc.Id,
			compatible: true,
		},
		{
			name:       "previous era: Babbage in Conway",
			txEraId:    eras.BabbageEraDesc.Id,
			ledgerEra:  eras.ConwayEraDesc.Id,
			compatible: true,
		},
		{
			name:       "two eras back: Alonzo in Conway",
			txEraId:    eras.AlonzoEraDesc.Id,
			ledgerEra:  eras.ConwayEraDesc.Id,
			compatible: false,
		},
		{
			name:       "future era: Conway in Babbage",
			txEraId:    eras.ConwayEraDesc.Id,
			ledgerEra:  eras.BabbageEraDesc.Id,
			compatible: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := IsCompatibleEra(
				tc.txEraId,
				tc.ledgerEra,
			)
			assert.Equal(
				t,
				tc.compatible,
				result,
			)
		})
	}
}

func TestValidateTxEra(t *testing.T) {
	tests := []struct {
		name      string
		txEra     int
		ledgerEra uint
		expectErr bool
		errMsg    string
	}{
		{
			name:      "same era passes",
			txEra:     int(eras.ConwayEraDesc.Id),
			ledgerEra: eras.ConwayEraDesc.Id,
			expectErr: false,
		},
		{
			name:      "previous era passes",
			txEra:     int(eras.BabbageEraDesc.Id),
			ledgerEra: eras.ConwayEraDesc.Id,
			expectErr: false,
		},
		{
			name:      "two eras back fails",
			txEra:     int(eras.AlonzoEraDesc.Id),
			ledgerEra: eras.ConwayEraDesc.Id,
			expectErr: true,
			errMsg:    "not compatible",
		},
		{
			name:      "future era fails",
			txEra:     int(eras.ConwayEraDesc.Id),
			ledgerEra: eras.BabbageEraDesc.Id,
			expectErr: true,
			errMsg:    "not compatible",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockEraTx{
				cbor:    make([]byte, 100),
				eraType: tc.txEra,
			}
			err := ValidateTxEra(tx, tc.ledgerEra)
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
