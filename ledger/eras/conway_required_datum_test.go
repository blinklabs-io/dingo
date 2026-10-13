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
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

// newSpendingDatumTx spends one input locked by plutusScript, with the
// spending redeemer and script witness present so only the datum requirement
// can fail.
func newSpendingDatumTx(
	plutusScript lcommon.Script,
	valid bool,
	witnessDatums []lcommon.Datum,
) (*mockProducedValidityTx, testInput) {
	input := newTestInput(0xd1, 0)
	witnesses := &mockWitnessSet{
		plutusData: witnessDatums,
		redeemers: &mockRedeemers{entries: []struct {
			key lcommon.RedeemerKey
			val lcommon.RedeemerValue
		}{{
			key: lcommon.RedeemerKey{Tag: lcommon.RedeemerTagSpend, Index: 0},
			val: lcommon.RedeemerValue{
				Data:    lcommon.Datum{Data: data.NewConstr(0)},
				ExUnits: lcommon.ExUnits{Memory: 1, Steps: 1},
			},
		}}},
	}
	switch s := plutusScript.(type) {
	case lcommon.PlutusV1Script:
		witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{s}
	case lcommon.PlutusV2Script:
		witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{s}
	case lcommon.PlutusV3Script:
		witnesses.plutusV3Scripts = []lcommon.PlutusV3Script{s}
	}
	return &mockProducedValidityTx{
		mockConwayFeeTx: mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType:    txTypeAlonzo,
				fee:       big.NewInt(0),
				witnesses: witnesses,
			},
			inputs: []lcommon.TransactionInput{input},
		},
		valid: valid,
	}, input
}

// TestValidateTxConwayRequiresSpendingDatums drives the production Conway
// rule list. A Plutus V1/V2 script input locked with a datum hash needs the
// datum in the witness set, and a V1 script input needs a datum hash at all;
// both are UTXOW conditions that hold whether or not the transaction claims
// phase-2 failure. A V3 script input without a datum stays valid.
// Not t.Parallel: this test swaps the package-level Conway rule lists.
func TestValidateTxConwayRequiresSpendingDatums(t *testing.T) {
	keepConwayProductionRules(t, lcommon.UtxoValidationRuleSupplementalDatums)

	datum := lcommon.Datum{Data: data.NewConstr(0)}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
		MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
	}
	for _, tc := range []struct {
		name         string
		script       lcommon.Script
		datumHash    bool
		witnessDatum bool
		wantMissing  bool
	}{
		{name: "V1 datum hash without witness datum", script: lcommon.PlutusV1Script{0x01}, datumHash: true, wantMissing: true},
		{name: "V2 datum hash without witness datum", script: lcommon.PlutusV2Script{0x02}, datumHash: true, wantMissing: true},
		{name: "V3 datum hash without witness datum", script: lcommon.PlutusV3Script{0x03}, datumHash: true, wantMissing: true},
		{name: "V1 no datum hash", script: lcommon.PlutusV1Script{0x01}, wantMissing: true},
		{name: "V2 datum hash with witness datum", script: lcommon.PlutusV2Script{0x02}, datumHash: true, witnessDatum: true},
		{name: "V1 datum hash with witness datum", script: lcommon.PlutusV1Script{0x01}, datumHash: true, witnessDatum: true},
		{name: "V3 no datum hash", script: lcommon.PlutusV3Script{0x03}},
	} {
		for _, valid := range []bool{true, false} {
			name := tc.name + " isValid=false"
			if valid {
				name = tc.name + " isValid=true"
			}
			t.Run(name, func(t *testing.T) {
				var witnessDatums []lcommon.Datum
				if tc.witnessDatum {
					witnessDatums = []lcommon.Datum{datum}
				}
				tx, input := newSpendingDatumTx(tc.script, valid, witnessDatums)
				scriptAddr := newTestScriptAddress(t, tc.script)
				var output lcommon.TransactionOutput = testAddressScriptOutput{
					testOutput: newTestOutput(10_000_000),
					addr:       scriptAddr,
				}
				if tc.datumHash {
					output = testDatumHashOutput{
						testOutput: newTestOutput(10_000_000),
						addr:       scriptAddr,
						datumHash:  datum.Hash(),
					}
				}
				ls := newMockLedgerState()
				ls.skipPhase2Validation = true
				ls.addUtxo(input, output)

				err := ValidateTxConway(tx, 0, ls, pp)
				if !tc.wantMissing {
					require.NoError(t, err)
					return
				}
				var missing lcommon.MissingDatumForSpendingScriptError
				require.ErrorAs(t, err, &missing)
				require.Equal(t, tc.script.Hash(), missing.ScriptHash)
			})
		}
	}
}
