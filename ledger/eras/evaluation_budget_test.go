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

package eras

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type transactionBudgetEvaluator func(
	t *testing.T,
	tx lcommon.Transaction,
	ls lcommon.LedgerState,
	limit lcommon.ExUnits,
) (lcommon.ExUnits, error)

func newSpendBudgetFixture(
	t *testing.T,
	version lang.LanguageVersion,
	redeemerCount int,
) (*mockConwayFeeTx, *mockLedgerState) {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		// Plutus V1 and V2 both execute UPLC 1.0.0; the witness set selects
		// the Plutus language version independently of this program version.
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Constant{Con: &syn.Unit{}},
			},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)

	witnesses := &mockWitnessSet{}
	var script lcommon.Script
	switch version {
	case lang.LanguageVersionV1:
		plutusScript := lcommon.PlutusV1Script(scriptBytes)
		witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{plutusScript}
		script = plutusScript
	case lang.LanguageVersionV2:
		plutusScript := lcommon.PlutusV2Script(scriptBytes)
		witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{plutusScript}
		script = plutusScript
	default:
		t.Fatalf("unsupported Plutus version %v", version)
	}
	redeemers := &mockRedeemers{}
	for i := range redeemerCount {
		redeemers.entries = append(redeemers.entries, struct {
			key lcommon.RedeemerKey
			val lcommon.RedeemerValue
		}{
			key: lcommon.RedeemerKey{
				Tag:   lcommon.RedeemerTagSpend,
				Index: uint32(i),
			},
		})
	}
	witnesses.redeemers = redeemers

	inputs := make([]lcommon.TransactionInput, redeemerCount)
	ls := newMockLedgerState()
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	address := newTestScriptAddress(t, script)
	for i := range redeemerCount {
		input := newTestInput(byte(i+1), 0)
		inputs[i] = input
		ls.addUtxo(input, testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       address,
		})
	}
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType:    txTypeAlonzo,
			witnesses: witnesses,
		},
		inputs: inputs,
	}
	return tx, ls
}

func requireTransactionWideBudget(
	t *testing.T,
	version lang.LanguageVersion,
	eval transactionBudgetEvaluator,
) {
	t.Helper()
	largeLimit := lcommon.ExUnits{Steps: 1_000_000_000, Memory: 100_000_000}

	oneTx, oneLedger := newSpendBudgetFixture(
		t,
		version,
		1,
	)
	oneCost, err := eval(t, oneTx, oneLedger, largeLimit)
	require.NoError(t, err)

	twoTx, twoLedger := newSpendBudgetFixture(
		t,
		version,
		2,
	)
	twoCost, err := eval(t, twoTx, twoLedger, largeLimit)
	require.NoError(t, err)
	assert.Greater(t, twoCost.Steps, oneCost.Steps)

	_, err = eval(t, twoTx, twoLedger, oneCost)
	require.Error(t, err, "each redeemer fits alone, but their total exceeds MaxTxExUnits")

	got, err := eval(t, twoTx, twoLedger, twoCost)
	require.NoError(t, err, "the aggregate cost exactly at MaxTxExUnits is valid")
	assert.Equal(t, twoCost, got)
}

func TestEvaluateTxAlonzoStopsAtTransactionWideBudget(t *testing.T) {
	t.Parallel()
	requireTransactionWideBudget(t, lang.LanguageVersionV1, func(
		t *testing.T,
		tx lcommon.Transaction,
		ls lcommon.LedgerState,
		limit lcommon.ExUnits,
	) (lcommon.ExUnits, error) {
		_, total, _, err := EvaluateTxAlonzo(tx, ls, &alonzo.AlonzoProtocolParameters{
			ProtocolMajor: 6,
			MaxTxExUnits:  limit,
			CostModels: map[uint][]int64{
				0: defaultMachineCostModel(t, lang.LanguageVersionV1),
			},
		})
		return total, err
	})
}

func TestEvaluateTxBabbageStopsAtTransactionWideBudget(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		version lang.LanguageVersion
	}{
		{name: "v1", version: lang.LanguageVersionV1},
		{name: "v2", version: lang.LanguageVersionV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireTransactionWideBudget(t, tc.version, func(
				t *testing.T,
				tx lcommon.Transaction,
				ls lcommon.LedgerState,
				limit lcommon.ExUnits,
			) (lcommon.ExUnits, error) {
				_, total, _, err := EvaluateTxBabbage(
					tx,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  limit,
						CostModels: map[uint][]int64{
							0: defaultMachineCostModel(t, lang.LanguageVersionV1),
							1: defaultMachineCostModel(t, lang.LanguageVersionV2),
						},
					},
				)
				return total, err
			})
		})
	}
}
