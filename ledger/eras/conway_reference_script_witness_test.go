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

// TestValidateTxConwayRejectsRedundantScriptWitness drives the production
// Conway rule list. The reference removes every script a reference input
// supplies from the needed set before comparing it with the witness set, so an
// explicit witness for a script a reference input already provides is
// extraneous, while either supply alone is accepted.
// Not t.Parallel: this test swaps the package-level Conway rule lists.
func TestValidateTxConwayRejectsRedundantScriptWitness(t *testing.T) {
	keepConwayProductionRules(t, lcommon.UtxoValidationRuleScriptWitnesses)

	pp := &conway.ConwayProtocolParameters{}
	for _, tc := range []struct {
		name   string
		script lcommon.Script
	}{
		{name: "native", script: lcommon.NativeScript{}},
		{name: "plutus v1", script: lcommon.PlutusV1Script{0x01}},
		{name: "plutus v2", script: lcommon.PlutusV2Script{0x02}},
		{name: "plutus v3", script: lcommon.PlutusV3Script{0x03}},
	} {
		for _, supply := range []struct {
			name     string
			explicit bool
			byRef    bool
			wantErr  bool
		}{
			{name: "explicit only", explicit: true},
			{name: "reference only", byRef: true},
			{name: "explicit and reference", explicit: true, byRef: true, wantErr: true},
		} {
			t.Run(tc.name+"/"+supply.name, func(t *testing.T) {
				spend := newTestInput(0xe1, 0)
				refInput := newTestInput(0xe2, 0)
				witnesses := &mockWitnessSet{}
				if _, native := tc.script.(lcommon.NativeScript); !native {
					witnesses.redeemers = &mockRedeemers{entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{{
						key: lcommon.RedeemerKey{
							Tag:   lcommon.RedeemerTagSpend,
							Index: 0,
						},
						val: lcommon.RedeemerValue{
							Data:    lcommon.Datum{Data: data.NewConstr(0)},
							ExUnits: lcommon.ExUnits{Memory: 1, Steps: 1},
						},
					}}}
				}
				if supply.explicit {
					switch s := tc.script.(type) {
					case lcommon.NativeScript:
						witnesses.nativeScripts = []lcommon.NativeScript{s}
					case lcommon.PlutusV1Script:
						witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{s}
					case lcommon.PlutusV2Script:
						witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{s}
					case lcommon.PlutusV3Script:
						witnesses.plutusV3Scripts = []lcommon.PlutusV3Script{s}
					}
				}
				tx := &mockConwayFeeTx{
					mockFeeTx: mockFeeTx{
						txType:    txTypeAlonzo,
						fee:       big.NewInt(0),
						witnesses: witnesses,
					},
					inputs: []lcommon.TransactionInput{spend},
				}
				ls := newMockLedgerState()
				ls.skipPhase2Validation = true
				ls.addUtxo(spend, testAddressScriptOutput{
					testOutput: newTestOutput(2_000_000),
					addr:       newTestScriptAddress(t, tc.script),
				})
				if supply.byRef {
					tx.referenceInputs = []lcommon.TransactionInput{refInput}
					ls.addUtxo(refInput, testAddressScriptOutput{
						testOutput: newTestOutput(1_000_000),
						addr:       newTestKeyAddress(t),
						scriptRef:  tc.script,
					})
				}

				err := ValidateTxConway(tx, 0, ls, pp)
				if !supply.wantErr {
					require.NoError(t, err)
					return
				}
				var extraneous lcommon.ExtraneousScriptWitnessesError
				require.ErrorAs(t, err, &extraneous)
				require.Equal(t, tc.script.Hash(), extraneous.ScriptHash)
			})
		}
	}
}
