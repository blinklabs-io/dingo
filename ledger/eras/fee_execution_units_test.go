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
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	feeTestMinFeeA = 44
	feeTestMinFeeB = 155_381
	// Fees stay inside one CBOR integer width so that changing the declared
	// fee does not change the transaction size the minimum fee is priced on.
	feeTestBaseFee = 300_000
)

// feeTestPrices are deliberately coarse (1/2 lovelace per unit) so that
// rounding each budget separately differs from rounding their sum once.
func feeTestPrices() lcommon.ExUnitPrice {
	half := &cbor.Rat{Rat: big.NewRat(1, 2)}
	return lcommon.ExUnitPrice{MemPrice: half, StepPrice: half}
}

// feeTestExpectedMin is an independent oracle for the minimum fee: linear
// size fee plus one ceiling over the summed declared budgets.
func feeTestExpectedMin(
	t *testing.T,
	tx lcommon.Transaction,
	budgets ...lcommon.ExUnits,
) uint64 {
	t.Helper()
	size, err := lcommon.TxSizeForFee(tx)
	require.NoError(t, err)
	total := new(big.Rat)
	for _, b := range budgets {
		total.Add(total, big.NewRat(b.Memory, 2))
		total.Add(total, big.NewRat(b.Steps, 2))
	}
	exec := new(big.Int).Quo(total.Num(), total.Denom())
	if new(big.Int).Mod(total.Num(), total.Denom()).Sign() != 0 {
		exec.Add(exec, big.NewInt(1))
	}
	// #nosec G115 -- small test values
	return uint64(size)*feeTestMinFeeA + feeTestMinFeeB + exec.Uint64()
}

func hasFeeTooSmall(err error) bool {
	var fee shelley.FeeTooSmallUtxoError
	return errors.As(err, &fee)
}

func feeTestBabbageTx(
	t *testing.T,
	fee uint64,
	redeemers ...lcommon.ExUnits,
) *babbage.BabbageTransaction {
	t.Helper()
	witnessSet := map[uint]any{}
	if len(redeemers) > 0 {
		entries := []any{}
		for i, eu := range redeemers {
			entries = append(entries, []any{
				uint64(0), uint64(i), uint64(42),
				[]any{uint64(eu.Memory), uint64(eu.Steps)},
			})
		}
		witnessSet[5] = entries
	}
	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	body := map[uint]any{
		0: []any{[]any{inputHash, uint64(0)}},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1_000_000)}},
		2: fee,
	}
	txCbor, err := cbor.Encode([]any{body, witnessSet, true, nil})
	require.NoError(t, err)
	tx, err := babbage.NewBabbageTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return tx
}

func feeTestBabbagePParams() *babbage.BabbageProtocolParameters {
	return &babbage.BabbageProtocolParameters{
		MinFeeA:        feeTestMinFeeA,
		MinFeeB:        feeTestMinFeeB,
		MaxTxSize:      16_384,
		ExecutionCosts: feeTestPrices(),
	}
}

// A Babbage fee that covers the size component but not the declared execution
// budget is rejected; the exact minimum is not rejected for its fee.
func TestValidateTxBabbageFeeIncludesExecutionUnits(t *testing.T) {
	t.Parallel()
	budget := lcommon.ExUnits{Memory: 1_000_001, Steps: 2_000_000}
	pp := feeTestBabbagePParams()
	ls := newMockLedgerState()

	minFee := feeTestExpectedMin(
		t, feeTestBabbageTx(t, feeTestBaseFee, budget), budget,
	)
	sizeOnly := feeTestExpectedMin(t, feeTestBabbageTx(t, feeTestBaseFee))
	require.Less(t, sizeOnly, minFee)

	err := ValidateTxBabbage(
		feeTestBabbageTx(t, sizeOnly, budget), 0, ls, pp,
	)
	assert.True(t, hasFeeTooSmall(err), "size-only fee accepted: %v", err)

	err = ValidateTxBabbage(
		feeTestBabbageTx(t, minFee-1, budget), 0, ls, pp,
	)
	assert.True(t, hasFeeTooSmall(err), "minimum-1 accepted: %v", err)

	err = ValidateTxBabbage(feeTestBabbageTx(t, minFee, budget), 0, ls, pp)
	assert.False(t, hasFeeTooSmall(err), "exact minimum rejected: %v", err)

	// Redeemer-free pricing is unchanged: the size fee alone suffices.
	err = ValidateTxBabbage(feeTestBabbageTx(t, sizeOnly, nil...), 0, ls, pp)
	assert.False(t, hasFeeTooSmall(err), "redeemer-free rejected: %v", err)
}

func feeTestAlonzoTx(
	t *testing.T,
	fee uint64,
	redeemers ...lcommon.ExUnits,
) *alonzo.AlonzoTransaction {
	t.Helper()
	b := feeTestBabbageTx(t, fee, redeemers...)
	tx, err := alonzo.NewAlonzoTransactionFromCbor(b.Cbor())
	require.NoError(t, err)
	return tx
}

func feeTestAlonzoPParams() *alonzo.AlonzoProtocolParameters {
	return &alonzo.AlonzoProtocolParameters{
		MinFeeA:        feeTestMinFeeA,
		MinFeeB:        feeTestMinFeeB,
		MaxTxSize:      16_384,
		ExecutionCosts: feeTestPrices(),
	}
}

// Alonzo fee decisions are made by alonzo.UtxoValidateFeeTooSmallUtxo alone.
// The error must be the upstream rule's, and the decision must agree with
// alonzo.MinFeeTx at, below and above the boundary.
func TestValidateTxAlonzoFeeDecisionMatchesMinFeeTx(t *testing.T) {
	t.Parallel()
	budget := lcommon.ExUnits{Memory: 1_000_001, Steps: 2_000_000}
	pp := feeTestAlonzoPParams()
	ls := newMockLedgerState()
	minFee := feeTestExpectedMin(
		t, feeTestAlonzoTx(t, feeTestBaseFee, budget), budget,
	)
	libMin, err := alonzo.MinFeeTx(feeTestAlonzoTx(t, minFee, budget), pp)
	require.NoError(t, err)
	require.Equal(t, minFee, libMin)

	for _, fee := range []uint64{minFee - 1, minFee, minFee + 1} {
		err := ValidateTxAlonzo(feeTestAlonzoTx(t, fee, budget), 0, ls, pp)
		assert.Equal(t, fee < minFee, hasFeeTooSmall(err),
			"fee %d, min %d: %v", fee, minFee, err)
	}
}

// With the upstream rule set removed, ValidateTxAlonzo does not price the
// fee itself: fee ownership sits in alonzo.UtxoValidationRules, so a second
// local check cannot disagree with it.
func TestValidateTxAlonzoDoesNotRepriceFeeLocally(t *testing.T) {
	// Not t.Parallel: swaps the package-level alonzoUtxoValidationRules.
	withoutAlonzoUtxoValidationRules(t)
	budget := lcommon.ExUnits{Memory: 1_000_001, Steps: 2_000_000}
	ls := newMockLedgerState()
	ls.skipPhase2Validation = true
	err := ValidateTxAlonzo(
		feeTestAlonzoTx(t, 1, budget), 0, ls, feeTestAlonzoPParams(),
	)
	assert.NoError(t, err)
}

func feeTestDijkstraTx(
	t *testing.T,
	fee uint64,
	top []lcommon.ExUnits,
	children ...[]lcommon.ExUnits,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	mkRedeemers := func(
		budgets []lcommon.ExUnits,
	) gdijkstra.DijkstraRedeemers {
		m := map[lcommon.RedeemerKey]lcommon.RedeemerValue{}
		for i, eu := range budgets {
			// #nosec G115 -- small test index
			m[lcommon.RedeemerKey{Tag: lcommon.RedeemerTagSpend, Index: uint32(i)}] =
				lcommon.RedeemerValue{Data: lcommon.Datum{Data: data.NewConstr(0)}, ExUnits: eu}
		}
		return gdijkstra.DijkstraRedeemers{Redeemers: m}
	}
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxFee: fee,
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{},
		TxIsValid:  true,
	}
	if len(top) > 0 {
		tx.WitnessSet.WsRedeemers = mkRedeemers(top)
	}
	if len(children) > 0 {
		subs := []gdijkstra.DijkstraSubTransaction{}
		for i, c := range children {
			in := shelley.NewShelleyTransactionInput(
				fmt.Sprintf("%02x", i+1)+strings.Repeat("ab", 31), 0,
			)
			sub := gdijkstra.DijkstraSubTransaction{
				Body: gdijkstra.DijkstraSubTransactionBody{
					TxInputs: conway.NewConwayTransactionInputSet(
						[]shelley.ShelleyTransactionInput{in},
					),
				},
			}
			if len(c) > 0 {
				sub.WitnessSet.WsRedeemers = mkRedeemers(c)
			}
			subs = append(subs, sub)
		}
		tx.Body.TxSubTransactions = cbor.NewSetType(subs, false)
	}
	txCbor, err := cbor.Encode(tx)
	require.NoError(t, err)
	decoded, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return decoded
}

func feeTestDijkstraPParams() *gdijkstra.DijkstraProtocolParameters {
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MinFeeA:        feeTestMinFeeA,
			MinFeeB:        feeTestMinFeeB,
			MaxTxSize:      16_384,
			ExecutionCosts: feeTestPrices(),
		},
	}
}

// Child budgets fold into the single top-level fee: one ceiling over the sum
// of every budget, with no per-child rounding or per-child base fee. The two
// children below cost 0.5 lovelace each, so per-child rounding would demand
// one more lovelace than the batch does.
func TestValidateTxDijkstraFeeAggregatesChildBudgets(t *testing.T) {
	t.Parallel()
	a := lcommon.ExUnits{Memory: 1}
	b := lcommon.ExUnits{Steps: 1}
	pp := feeTestDijkstraPParams()
	cases := []struct {
		name     string
		top      []lcommon.ExUnits
		children [][]lcommon.ExUnits
		budgets  []lcommon.ExUnits
	}{
		{"child only", nil, [][]lcommon.ExUnits{{{Memory: 4_000_000, Steps: 2_000_000}}}, []lcommon.ExUnits{{Memory: 4_000_000, Steps: 2_000_000}}},
		{"multi child single rounding", nil, [][]lcommon.ExUnits{{a}, {b}}, []lcommon.ExUnits{a, b}},
		{"top and children", []lcommon.ExUnits{a}, [][]lcommon.ExUnits{{b}, {a}}, []lcommon.ExUnits{a, b, a}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			minFee := feeTestExpectedMin(
				t,
				feeTestDijkstraTx(t, feeTestBaseFee, tc.top, tc.children...),
				tc.budgets...,
			)
			libMin, err := gdijkstra.MinFeeTx(
				feeTestDijkstraTx(t, minFee, tc.top, tc.children...), pp,
			)
			require.NoError(t, err)
			require.Equal(t, minFee, libMin)

			err = ValidateTxDijkstra(
				feeTestDijkstraTx(t, minFee-1, tc.top, tc.children...),
				0, newMockLedgerState(), pp,
			)
			assert.True(t, hasFeeTooSmall(err), "minimum-1 accepted: %v", err)
			err = ValidateTxDijkstra(
				feeTestDijkstraTx(t, minFee, tc.top, tc.children...),
				0, newMockLedgerState(), pp,
			)
			assert.False(t, hasFeeTooSmall(err), "exact minimum rejected: %v", err)
		})
	}

	// A Dijkstra transaction without top-level or child redeemers pays only
	// the size component, matching the pre-execution-unit fee path.
	noRedeemers := feeTestDijkstraTx(t, feeTestBaseFee, nil)
	sizeOnly := feeTestExpectedMin(t, noRedeemers)
	err := ValidateTxDijkstra(
		feeTestDijkstraTx(t, sizeOnly, nil),
		0,
		newMockLedgerState(),
		pp,
	)
	assert.False(t, hasFeeTooSmall(err), "redeemer-free transaction rejected: %v", err)
}
