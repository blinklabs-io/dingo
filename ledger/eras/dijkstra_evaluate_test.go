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
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/stretchr/testify/require"
)

func dijkstraEvaluateTestParams(
	t *testing.T,
) *gdijkstra.DijkstraProtocolParameters {
	t.Helper()
	params := dijkstraV4TestParams(
		t, defaultMachineCostModel(t, lang.LanguageVersionV4),
	)
	params.MaxTxExUnits = v4TestBudget
	return params
}

// newDijkstraTopLevelMintFromSubTx builds a transaction whose top-level mint
// redeemer runs a script that only a sub-transaction makes available.
func newDijkstraTopLevelMintFromSubTx(
	script lcommon.Script,
	sub gdijkstra.DijkstraSubTransaction,
) *gdijkstra.DijkstraTransaction {
	mint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			script.Hash(): {cbor.NewByteString([]byte("t")): big.NewInt(1)},
		},
	)
	return &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxMint: &mint,
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{sub},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagMint, Index: 0}: {
						ExUnits: v4TestBudget,
					},
				},
			},
		},
		TxIsValid: true,
	}
}

func dijkstraEvaluateTestRefInput(index uint32) shelley.ShelleyTransactionInput {
	return shelley.NewShelleyTransactionInput(
		"b228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee22",
		int(index),
	)
}

func addDijkstraRefScriptUtxo(
	t *testing.T,
	ls *mockLedgerState,
	input shelley.ShelleyTransactionInput,
	script lcommon.Script,
) {
	t.Helper()
	ls.addUtxo(input, testAddressScriptOutput{
		testOutput: newTestOutput(2_000_000),
		addr:       newTestKeyAddress(t),
		scriptRef:  script,
	})
}

// declareDijkstraRedeemerUnits sets the declared units of the transaction's
// only redeemer, at whichever level it sits.
func declareDijkstraRedeemerUnits(
	t *testing.T,
	tx *gdijkstra.DijkstraTransaction,
	units lcommon.ExUnits,
) {
	t.Helper()
	redeemers := tx.WitnessSet.WsRedeemers.Redeemers
	if len(redeemers) == 0 {
		subTxs := tx.Body.TxSubTransactions.Items()
		require.Len(t, subTxs, 1)
		redeemers = subTxs[0].WitnessSet.WsRedeemers.Redeemers
	}
	require.Len(t, redeemers, 1)
	for key, value := range redeemers {
		value.ExUnits = units
		redeemers[key] = value
	}
}

func TestEvaluateTxDijkstraEvaluatesEveryLevel(t *testing.T) {
	// Not t.Parallel: withoutDijkstraPhase1 replaces a package-level rule list.
	withoutDijkstraPhase1(t)
	params := dijkstraEvaluateTestParams(t)
	plain := plutusProgramBytes(t, v4PlainProgram)
	for _, tc := range []struct {
		name    string
		level   v4TestLevel
		version lang.LanguageVersion
		key     *lcommon.RedeemerKey
	}{
		{
			name:    "sub-transaction Plutus V4 mint",
			level:   v4ChildLevel,
			version: lang.LanguageVersionV4,
		},
		{
			name:    "top-level Plutus V4 guard",
			level:   v4TopLevel,
			version: lang.LanguageVersionV4,
			key:     &lcommon.RedeemerKey{Tag: lcommon.RedeemerTagGuarding},
		},
		{
			name:    "top-level Plutus V4 mint",
			level:   v4TopLevelMint,
			version: lang.LanguageVersionV4,
			key:     &lcommon.RedeemerKey{Tag: lcommon.RedeemerTagMint},
		},
		{
			name:    "top-level Plutus V3 guard",
			level:   v4TopLevel,
			version: lang.LanguageVersionV3,
			key:     &lcommon.RedeemerKey{Tag: lcommon.RedeemerTagGuarding},
		},
		{
			name:    "top-level Plutus V3 mint",
			level:   v4TopLevelMint,
			version: lang.LanguageVersionV3,
			key:     &lcommon.RedeemerKey{Tag: lcommon.RedeemerTagMint},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := newDijkstraPlutusLevelTx(
				t,
				tc.level,
				plutusScriptForVersion(tc.version, plain),
				lcommon.ExUnits{},
			)
			ls := newMockLedgerState()
			_, total, redeemers, err := EvaluateTxDijkstra(tx, ls, params)
			require.NoError(t, err)
			require.Positive(t, total.Steps)
			require.Positive(t, total.Memory)
			if tc.key == nil {
				// A sub-transaction redeemer counts towards the total, but
				// the (tag, index) map names top-level redeemers only.
				require.Empty(t, redeemers)
			} else {
				require.Equal(
					t,
					map[lcommon.RedeemerKey]lcommon.ExUnits{*tc.key: total},
					redeemers,
				)
			}

			declareDijkstraRedeemerUnits(t, tx, total)
			require.NoError(t, ValidateTxDijkstra(tx, 0, ls, params))
			short := total
			short.Steps--
			declareDijkstraRedeemerUnits(t, tx, short)
			var failed conway.PlutusScriptFailedError
			require.ErrorAs(t, ValidateTxDijkstra(tx, 0, ls, params), &failed)
		})
	}
}

func TestEvaluateTxDijkstraSharesScriptsAcrossLevels(t *testing.T) {
	t.Parallel()
	params := dijkstraEvaluateTestParams(t)
	plain := plutusProgramBytes(t, v4PlainProgram)
	v3 := lcommon.PlutusV3Script(plain)
	v4 := lcommon.PlutusV4Script(plain)
	mintKey := lcommon.RedeemerKey{Tag: lcommon.RedeemerTagMint}
	for _, tc := range []struct {
		name  string
		build func(t *testing.T, ls *mockLedgerState) *gdijkstra.DijkstraTransaction
	}{
		{
			name: "unrelated Plutus V4 reference script at the top level",
			build: func(t *testing.T, ls *mockLedgerState) *gdijkstra.DijkstraTransaction {
				tx := newDijkstraPlutusLevelTx(t, v4TopLevelMint, v3, v4TestBudget)
				input := dijkstraEvaluateTestRefInput(0)
				tx.Body.TxReferenceInputs = cbor.NewSetType(
					[]shelley.ShelleyTransactionInput{input}, false,
				)
				addDijkstraRefScriptUtxo(t, ls, input, v4)
				return tx
			},
		},
		{
			name: "Plutus V4 reference script resolved by a sub-transaction",
			build: func(t *testing.T, ls *mockLedgerState) *gdijkstra.DijkstraTransaction {
				input := dijkstraEvaluateTestRefInput(1)
				addDijkstraRefScriptUtxo(t, ls, input, v4)
				return newDijkstraTopLevelMintFromSubTx(
					v4,
					gdijkstra.DijkstraSubTransaction{
						Body: gdijkstra.DijkstraSubTransactionBody{
							TxReferenceInputs: cbor.NewSetType(
								[]shelley.ShelleyTransactionInput{input},
								false,
							),
						},
					},
				)
			},
		},
		{
			name: "Plutus V3 script witnessed only in a sub-transaction",
			build: func(t *testing.T, ls *mockLedgerState) *gdijkstra.DijkstraTransaction {
				return newDijkstraTopLevelMintFromSubTx(
					v3,
					gdijkstra.DijkstraSubTransaction{
						WitnessSet: witnessSetWithScript(v3, nil),
					},
				)
			},
		},
		{
			name: "unrelated Plutus V3 reference script in a sub-transaction",
			build: func(t *testing.T, ls *mockLedgerState) *gdijkstra.DijkstraTransaction {
				unrelated := lcommon.PlutusV3Script(plutusProgramBytes(
					t, `(program 1.1.0 (lam ctx (con integer 7)))`,
				))
				input := dijkstraEvaluateTestRefInput(2)
				addDijkstraRefScriptUtxo(t, ls, input, unrelated)
				tx := newDijkstraTopLevelMintFromSubTx(
					v3,
					gdijkstra.DijkstraSubTransaction{
						Body: gdijkstra.DijkstraSubTransactionBody{
							TxReferenceInputs: cbor.NewSetType(
								[]shelley.ShelleyTransactionInput{input},
								false,
							),
						},
					},
				)
				tx.WitnessSet.WsPlutusV3Scripts = cbor.NewSetType(
					[]lcommon.PlutusV3Script{v3}, false,
				)
				return tx
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ls := newMockLedgerState()
			tx := tc.build(t, ls)
			_, total, redeemers, err := EvaluateTxDijkstra(tx, ls, params)
			require.NoError(t, err)
			require.Positive(t, total.Steps)
			require.Equal(
				t,
				map[lcommon.RedeemerKey]lcommon.ExUnits{mintKey: total},
				redeemers,
			)
		})
	}
}

func TestEvaluateTxDijkstraPlutusV4CostModel(t *testing.T) {
	t.Parallel()
	baseModel := defaultMachineCostModel(t, lang.LanguageVersionV4)
	plain := lcommon.PlutusV4Script(plutusProgramBytes(t, v4PlainProgram))
	for _, level := range []struct {
		name  string
		level v4TestLevel
	}{
		{"top level", v4TopLevel},
		{"child", v4ChildLevel},
	} {
		t.Run(level.name, func(t *testing.T) {
			t.Parallel()
			tx := newDijkstraPlutusLevelTx(t, level.level, plain, v4TestBudget)
			evaluate := func(model []int64) lcommon.ExUnits {
				params := dijkstraV4TestParams(t, model)
				params.MaxTxExUnits = v4TestBudget
				_, total, _, err := EvaluateTxDijkstra(
					tx, newMockLedgerState(), params,
				)
				require.NoError(t, err)
				return total
			}
			base := evaluate(baseModel)
			raised := evaluate(scaledCostModel(baseModel, 3))
			require.Equal(t, 3*base.Steps, raised.Steps,
				"V4 machine costs come from cost model key 3")

			for name, model := range map[string][]int64{
				"absent": nil,
				"empty":  {},
			} {
				params := dijkstraV4TestParams(t, model)
				params.MaxTxExUnits = v4TestBudget
				_, _, _, err := EvaluateTxDijkstra(
					tx, newMockLedgerState(), params,
				)
				var missing lcommon.MissingCostModelError
				require.ErrorAs(t, err, &missing, name)
				require.Equal(t, uint(3), missing.Version, name)
			}
		})
	}
}

func TestEvaluateTxDijkstraPlutusV4OnlyBuiltin(t *testing.T) {
	t.Parallel()
	params := dijkstraEvaluateTestParams(t)
	program := plutusProgramBytes(t, v4OnlyBuiltinProgram)
	wrongLength := plutusProgramBytes(
		t,
		strings.Replace(v4OnlyBuiltinProgram, "(con integer 3)", "(con integer 4)", 1),
	)
	for _, level := range []struct {
		name  string
		level v4TestLevel
	}{
		{"top level", v4TopLevel},
		{"child", v4ChildLevel},
	} {
		t.Run(level.name, func(t *testing.T) {
			t.Parallel()
			tx := newDijkstraPlutusLevelTx(
				t, level.level, lcommon.PlutusV4Script(program), v4TestBudget,
			)
			_, total, _, err := EvaluateTxDijkstra(
				tx, newMockLedgerState(), params,
			)
			require.NoError(t, err)
			require.Positive(t, total.Steps)

			wrong := newDijkstraPlutusLevelTx(
				t, level.level, lcommon.PlutusV4Script(wrongLength), v4TestBudget,
			)
			_, _, _, err = EvaluateTxDijkstra(
				wrong, newMockLedgerState(), params,
			)
			var failed conway.PlutusScriptFailedError
			require.ErrorAs(t, err, &failed)
		})
	}
}

func TestEvaluateTxDijkstraFeeUsesRefScriptCostParameters(t *testing.T) {
	t.Parallel()
	params := dijkstraEvaluateTestParams(t)
	params.MinFeeRefScriptCostPerByte = &cbor.Rat{Rat: big.NewRat(1, 1)}
	params.RefScriptCostStride = 2
	params.RefScriptCostMultiplier = &cbor.Rat{Rat: big.NewRat(3, 1)}
	v3 := lcommon.PlutusV3Script(plutusProgramBytes(t, v4PlainProgram))
	mint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			v3.Hash(): {cbor.NewByteString([]byte("t")): big.NewInt(1)},
		},
	)
	topInput := dijkstraEvaluateTestRefInput(3)
	subInput := dijkstraEvaluateTestRefInput(4)
	ls := newMockLedgerState()
	addDijkstraRefScriptUtxo(t, ls, topInput, v3)
	addDijkstraRefScriptUtxo(
		t, ls, subInput, lcommon.PlutusV4Script(plutusProgramBytes(
			t, v4OnlyBuiltinProgram,
		)),
	)
	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxMint: &mint,
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{topInput}, false,
			),
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					Body: gdijkstra.DijkstraSubTransactionBody{
						TxReferenceInputs: cbor.NewSetType(
							[]shelley.ShelleyTransactionInput{subInput},
							false,
						),
					},
				}},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagMint}: {},
				},
			},
		},
		TxIsValid: true,
	}

	fee, _, _, err := EvaluateTxDijkstra(tx, ls, params)
	require.NoError(t, err)

	// Only the top-level reference script is charged, as in the Dijkstra
	// minimum-fee rule; the sub-transaction's counts towards size limits only.
	size := uint64(len(v3))
	require.Greater(t, size, 2*uint64(params.RefScriptCostStride))
	want := calculateTieredRefScriptFee(
		size, big.NewRat(1, 1), 2, big.NewRat(3, 1),
	)
	require.Equal(t, want, fee)
	require.NotEqual(
		t,
		CalculateConwayRefScriptFee(size, big.NewRat(1, 1)),
		fee,
		"the fee must follow the Dijkstra stride and multiplier",
	)
}
