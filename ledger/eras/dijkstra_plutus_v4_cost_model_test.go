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
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// v4OnlyBuiltinProgram succeeds only when lengthOfArray, a builtin introduced
// in Plutus V4, is available and returns the array's length.
const v4OnlyBuiltinProgram = `(program 1.1.0
  (lam ctx
    (force
      [(force (builtin ifThenElse))
        [(builtin equalsInteger)
          [(force (builtin lengthOfArray)) (con (array integer) [1, 2, 3])]
          (con integer 3)]
        (delay (con unit ()))
        (delay (error))])))`

// v4PlainProgram evaluates to unit using only machine steps.
const v4PlainProgram = `(program 1.1.0 (lam ctx (con unit ())))`

func plutusProgramBytes(t *testing.T, source string) []byte {
	t.Helper()
	named, err := syn.Parse(source)
	require.NoError(t, err)
	program, err := syn.NameToDeBruijn(named)
	require.NoError(t, err)
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	encoded, err := cbor.Encode(flat)
	require.NoError(t, err)
	return encoded
}

type v4TestLevel int

const (
	v4TopLevel v4TestLevel = iota
	v4ChildLevel
	// v4TopLevelMint places a mint redeemer on the top-level transaction.
	v4TopLevelMint
)

func plutusScriptForVersion(
	version lang.LanguageVersion,
	source []byte,
) lcommon.Script {
	if version == lang.LanguageVersionV4 {
		return lcommon.PlutusV4Script(source)
	}
	return lcommon.PlutusV3Script(source)
}

func witnessSetWithScript(
	script lcommon.Script,
	redeemers map[lcommon.RedeemerKey]lcommon.RedeemerValue,
) gdijkstra.DijkstraTransactionWitnessSet {
	ws := gdijkstra.DijkstraTransactionWitnessSet{
		WsRedeemers: gdijkstra.DijkstraRedeemers{Redeemers: redeemers},
	}
	switch s := script.(type) {
	case lcommon.PlutusV4Script:
		ws.WsPlutusV4Scripts = cbor.NewSetType(
			[]lcommon.PlutusV4Script{s},
			false,
		)
	case lcommon.PlutusV3Script:
		ws.WsPlutusV3Scripts = cbor.NewSetType(
			[]lcommon.PlutusV3Script{s},
			false,
		)
	}
	return ws
}

// newDijkstraPlutusLevelTx builds a transaction whose single Plutus redeemer
// sits at the requested level: a guarding redeemer at the top level, or a mint
// redeemer inside a sub-transaction.
func newDijkstraPlutusLevelTx(
	t *testing.T,
	level v4TestLevel,
	script lcommon.Script,
	exUnits lcommon.ExUnits,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	mint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			script.Hash(): {cbor.NewByteString([]byte("t")): big.NewInt(1)},
		},
	)
	if level == v4TopLevel {
		return &gdijkstra.DijkstraTransaction{
			Body: gdijkstra.DijkstraTransactionBody{
				TxGuards: &gdijkstra.DijkstraGuards{
					Credentials: []lcommon.Credential{{
						CredType:   lcommon.CredentialTypeScriptHash,
						Credential: script.Hash(),
					}},
				},
			},
			WitnessSet: witnessSetWithScript(
				script,
				map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagGuarding, Index: 0}: {
						ExUnits: exUnits,
					},
				},
			),
			TxIsValid: true,
		}
	}
	if level == v4TopLevelMint {
		return &gdijkstra.DijkstraTransaction{
			Body: gdijkstra.DijkstraTransactionBody{TxMint: &mint},
			WitnessSet: witnessSetWithScript(
				script,
				map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagMint, Index: 0}: {
						ExUnits: exUnits,
					},
				},
			),
			TxIsValid: true,
		}
	}
	return &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					Body: gdijkstra.DijkstraSubTransactionBody{TxMint: &mint},
					WitnessSet: witnessSetWithScript(
						script,
						map[lcommon.RedeemerKey]lcommon.RedeemerValue{
							{Tag: lcommon.RedeemerTagMint, Index: 0}: {
								ExUnits: exUnits,
							},
						},
					),
				}},
				false,
			),
		},
		TxIsValid: true,
	}
}

// dijkstraV4TestParams carries machine-step costs for every language and the
// supplied V4 model under cost model key 3.
func dijkstraV4TestParams(
	t *testing.T,
	v4Model []int64,
) *gdijkstra.DijkstraProtocolParameters {
	t.Helper()
	costModels := map[uint][]int64{
		0: defaultMachineCostModel(t, lang.LanguageVersionV1),
		1: defaultMachineCostModel(t, lang.LanguageVersionV2),
		2: defaultMachineCostModel(t, lang.LanguageVersionV3),
	}
	if v4Model != nil {
		costModels[3] = v4Model
	}
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: costModels,
		},
	}
}

func scaledCostModel(model []int64, factor int64) []int64 {
	ret := make([]int64, len(model))
	for i, v := range model {
		ret[i] = v * factor
	}
	return ret
}

// withoutDijkstraPhase1 swaps the package-level phase-1 rule list so a minimal
// transaction reaches phase 2.
func withoutDijkstraPhase1(t *testing.T) {
	t.Helper()
	original := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = nil
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = original })
}

var v4TestBudget = lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000}

func TestValidateTxDijkstraPlutusV4CostModel(t *testing.T) {
	// Not t.Parallel: withoutDijkstraPhase1 replaces a package-level rule list.
	withoutDijkstraPhase1(t)
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
			tx := newDijkstraPlutusLevelTx(t, level.level, plain, v4TestBudget)
			ls := newMockLedgerState()

			require.NoError(t, ValidateTxDijkstra(
				tx, 0, ls, dijkstraV4TestParams(t, baseModel),
			), "budget covers the script under the base V4 model")

			err := ValidateTxDijkstra(
				tx, 0, ls, dijkstraV4TestParams(
					t, scaledCostModel(baseModel, 1_000_000),
				),
			)
			var failed conway.PlutusScriptFailedError
			require.ErrorAs(t, err, &failed,
				"the same budget must be exhausted under a raised V4 model")
		})
	}
}

func TestValidateTxDijkstraPlutusV4RequiresCostModelKey3(t *testing.T) {
	t.Parallel()
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
			for name, params := range map[string]*gdijkstra.DijkstraProtocolParameters{
				"absent": dijkstraV4TestParams(t, nil),
				"empty":  dijkstraV4TestParams(t, []int64{}),
			} {
				t.Run(name, func(t *testing.T) {
					err := ValidateTxDijkstra(tx, 0, newMockLedgerState(), params)
					var missing lcommon.MissingCostModelError
					require.ErrorAs(t, err, &missing)
					require.Equal(t, uint(3), missing.Version)
				})
			}
		})
	}
}

func TestValidateTxDijkstraPlutusV4OnlyBuiltin(t *testing.T) {
	// Not t.Parallel: withoutDijkstraPhase1 replaces a package-level rule list.
	withoutDijkstraPhase1(t)
	program := plutusProgramBytes(t, v4OnlyBuiltinProgram)
	wrongLength := plutusProgramBytes(
		t,
		strings.Replace(v4OnlyBuiltinProgram, "(con integer 3)", "(con integer 4)", 1),
	)
	params := dijkstraV4TestParams(
		t, defaultMachineCostModel(t, lang.LanguageVersionV4),
	)
	for _, level := range []struct {
		name  string
		level v4TestLevel
	}{
		{"top level", v4TopLevel},
		{"child", v4ChildLevel},
	} {
		t.Run(level.name, func(t *testing.T) {
			v4 := newDijkstraPlutusLevelTx(
				t, level.level,
				plutusScriptForVersion(lang.LanguageVersionV4, program),
				v4TestBudget,
			)
			require.NoError(t, ValidateTxDijkstra(
				v4, 0, newMockLedgerState(), params,
			))
			// A program that expects a wrong length fails, so the success
			// above comes from the builtin's result and not from the
			// script ignoring it.
			wrong := newDijkstraPlutusLevelTx(
				t, level.level,
				plutusScriptForVersion(lang.LanguageVersionV4, wrongLength),
				v4TestBudget,
			)
			var failed conway.PlutusScriptFailedError
			require.ErrorAs(t, ValidateTxDijkstra(
				wrong, 0, newMockLedgerState(), params,
			), &failed)
		})
	}
}

func TestDijkstraParameterChangeOfCostModelKey3ChangesOutcome(t *testing.T) {
	// Not t.Parallel: withoutDijkstraPhase1 replaces a package-level rule list.
	withoutDijkstraPhase1(t)
	baseModel := defaultMachineCostModel(t, lang.LanguageVersionV4)
	raised := scaledCostModel(baseModel, 1_000_000)
	plain := lcommon.PlutusV4Script(plutusProgramBytes(t, v4PlainProgram))
	tx := newDijkstraPlutusLevelTx(t, v4TopLevel, plain, v4TestBudget)
	params := dijkstraV4TestParams(t, baseModel)
	require.NoError(t, ValidateTxDijkstra(tx, 0, newMockLedgerState(), params))

	updated, err := PParamsUpdateDijkstra(
		params,
		gdijkstra.DijkstraProtocolParameterUpdate{
			CostModels: map[uint][]int64{3: raised},
		},
	)
	require.NoError(t, err)

	var failed conway.PlutusScriptFailedError
	require.ErrorAs(
		t,
		ValidateTxDijkstra(tx, 0, newMockLedgerState(), updated),
		&failed,
	)
}
