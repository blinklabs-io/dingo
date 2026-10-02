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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
)

// TestValidateTxDijkstraRequiresSpendingDatums drives the production Dijkstra
// rule list: a PlutusV1 script input locked with a datum hash needs the datum
// in the witness set, whether or not the transaction claims phase-2 failure.
// Not t.Parallel: this test swaps the package-level Dijkstra rule list.
func TestValidateTxDijkstraRequiresSpendingDatums(t *testing.T) {
	original := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = retainProductionRules(
		t,
		original,
		gdijkstra.UtxoValidationRuleDescriptors(),
		lcommon.UtxoValidationRuleSupplementalDatums,
	)
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = original })

	pp := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}
	plutusScript := lcommon.PlutusV1Script{0x01}
	datum := lcommon.Datum{Data: data.NewConstr(0)}
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee66",
		0,
	)
	for _, tc := range []struct {
		name         string
		witnessDatum bool
	}{
		{name: "missing witness datum"},
		{name: "witness datum present", witnessDatum: true},
	} {
		for _, valid := range []bool{true, false} {
			name := tc.name + " isValid=false"
			if valid {
				name = tc.name + " isValid=true"
			}
			t.Run(name, func(t *testing.T) {
				witnessSet := gdijkstra.DijkstraTransactionWitnessSet{
					WsPlutusV1Scripts: cbor.NewSetType(
						[]lcommon.PlutusV1Script{plutusScript}, false,
					),
				}
				if tc.witnessDatum {
					witnessSet.WsPlutusData = cbor.NewSetType(
						[]lcommon.Datum{datum}, false,
					)
				}
				tx := &gdijkstra.DijkstraTransaction{
					Body: gdijkstra.DijkstraTransactionBody{
						TxInputs: conway.NewConwayTransactionInputSet(
							[]shelley.ShelleyTransactionInput{input},
						),
					},
					WitnessSet: witnessSet,
					TxIsValid:  valid,
				}
				ls := newMockLedgerState()
				ls.skipPhase2Validation = true
				ls.addUtxo(input, testDatumHashOutput{
					testOutput: newTestOutput(10_000_000),
					addr:       newTestScriptAddress(t, plutusScript),
					datumHash:  datum.Hash(),
				})
				err := ValidateTxDijkstra(tx, 0, ls, pp)
				if tc.witnessDatum {
					require.NoError(t, err)
					return
				}
				var missing lcommon.MissingDatumForSpendingScriptError
				require.ErrorAs(t, err, &missing)
				require.Equal(t, plutusScript.Hash(), missing.ScriptHash)
			})
		}
	}
}

// TestValidateTxDijkstraRequiresSubtransactionSpendingDatums checks that the
// required-datum rule reaches a child subtransaction: a V1 script input in the
// child needs the datum in the child's own witness set.
// Not t.Parallel: this test swaps the package-level Dijkstra rule list.
func TestValidateTxDijkstraRequiresSubtransactionSpendingDatums(t *testing.T) {
	original := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = retainProductionRules(
		t,
		original,
		gdijkstra.UtxoValidationRuleDescriptors(),
		lcommon.UtxoValidationRuleSupplementalDatums,
	)
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = original })

	pp := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}
	plutusScript := lcommon.PlutusV1Script{0x01}
	datum := lcommon.Datum{Data: data.NewConstr(0)}
	input := shelley.NewShelleyTransactionInput(
		"e228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee77",
		0,
	)
	for _, witnessDatum := range []bool{false, true} {
		for _, valid := range []bool{true, false} {
			name := "missing child datum"
			if witnessDatum {
				name = "child datum present"
			}
			if valid {
				name += " isValid=true"
			} else {
				name += " isValid=false"
			}
			t.Run(name, func(t *testing.T) {
				childWitnessSet := gdijkstra.DijkstraTransactionWitnessSet{
					WsPlutusV1Scripts: cbor.NewSetType(
						[]lcommon.PlutusV1Script{plutusScript}, false,
					),
				}
				if witnessDatum {
					childWitnessSet.WsPlutusData = cbor.NewSetType(
						[]lcommon.Datum{datum}, false,
					)
				}
				tx := &gdijkstra.DijkstraTransaction{
					Body: gdijkstra.DijkstraTransactionBody{
						TxSubTransactions: cbor.NewSetType(
							[]gdijkstra.DijkstraSubTransaction{{
								Body: gdijkstra.DijkstraSubTransactionBody{
									TxInputs: conway.NewConwayTransactionInputSet(
										[]shelley.ShelleyTransactionInput{input},
									),
								},
								WitnessSet: childWitnessSet,
							}},
							false,
						),
					},
					TxIsValid: valid,
				}
				ls := newMockLedgerState()
				ls.skipPhase2Validation = true
				ls.addUtxo(input, testDatumHashOutput{
					testOutput: newTestOutput(10_000_000),
					addr:       newTestScriptAddress(t, plutusScript),
					datumHash:  datum.Hash(),
				})
				err := ValidateTxDijkstra(tx, 0, ls, pp)
				if witnessDatum {
					require.NoError(t, err)
					return
				}
				var missing lcommon.MissingDatumForSpendingScriptError
				require.ErrorAs(t, err, &missing)
				require.Equal(t, plutusScript.Hash(), missing.ScriptHash)
			})
		}
	}
}
