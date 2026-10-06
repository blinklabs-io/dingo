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
	"bytes"
	"crypto/ed25519"
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

// guardrailsLedgerState is the mock ledger state with a constitution whose
// guardrails script is policy.
type guardrailsLedgerState struct {
	*mockLedgerState
	policy []byte
}

func (s guardrailsLedgerState) Constitution() (*lcommon.Constitution, error) {
	return &lcommon.Constitution{ScriptHash: s.policy}, nil
}

// plutusDataField renders UPLC that selects field n of the Constr that the
// Data-typed term d evaluates to.
func plutusDataField(d string, n int) string {
	fields := "[(force (force (builtin sndPair))) [(builtin unConstrData) " + d + "]]"
	for range n {
		fields = "[(force (builtin tailList)) " + fields + "]"
	}
	return "[(force (builtin headList)) " + fields + "]"
}

// changedParametersGuardrailsScript compiles a guardrails script that
// succeeds only when the ChangedParameters of the proposal it guards equal
// expected, the Plutus Data text of a map. It reads them from ScriptInfo
// (ProposingScript index proposal), ProposalProcedure (deposit, return account,
// action) and ParameterChange (previous action, changes, policy).
func changedParametersGuardrailsScript(
	t *testing.T,
	version lang.LanguageVersion,
	expected string,
) lcommon.Script {
	t.Helper()
	scriptInfo := plutusDataField("ctx", 2)
	procedure := plutusDataField(scriptInfo, 1)
	action := plutusDataField(procedure, 2)
	changed := plutusDataField(action, 1)
	program, err := syn.Parse(fmt.Sprintf(
		`(program 1.1.0 (lam ctx (force [(force (builtin ifThenElse))
			[(builtin equalsData) %s (con data (%s))]
			(delay (con unit ())) (delay (error))])))`,
		changed, expected,
	))
	require.NoError(t, err)
	deBruijn, err := syn.NameToDeBruijn(program)
	require.NoError(t, err)
	flatProgram, err := syn.Encode(deBruijn)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	if version == lang.LanguageVersionV4 {
		return lcommon.PlutusV4Script(scriptBytes)
	}
	return lcommon.PlutusV3Script(scriptBytes)
}

// newDijkstraGuardrailsTx builds a valid Dijkstra transaction proposing the
// given parameter update under a constitution guarded by script. A V3 script is
// carried in the witness set; a V4 script is supplied by a reference input,
// which is the only way Dijkstra carries one.
func newDijkstraGuardrailsTx(
	t *testing.T,
	script lcommon.Script,
	update []byte,
) (*gdijkstra.DijkstraTransaction, lcommon.LedgerState, *gdijkstra.DijkstraProtocolParameters) {
	t.Helper()
	const (
		deposit = uint64(1_000_000)
		fee     = uint64(1_000_000)
	)
	input := shelley.NewShelleyTransactionInput(
		"d228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee66", 0)
	collateralInput := shelley.NewShelleyTransactionInput(
		"e228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee77", 0)
	referenceInput := shelley.NewShelleyTransactionInput(
		"f228b482a1aae768e4a796380f49e021d9c21f70d3c12cb186b188dedfc0ee88", 0)

	privateKey := ed25519.NewKeyFromSeed(
		bytes.Repeat([]byte{0x77}, ed25519.SeedSize),
	)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	paymentHash := lcommon.Blake2b224Hash(publicKey)
	keyAddress, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		paymentHash[:],
		nil,
	)
	require.NoError(t, err)
	rewardAddress, err := lcommon.NewAddressFromBytes(
		append([]byte{0xe0}, paymentHash[:]...))
	require.NoError(t, err)

	state := newMockLedgerState()
	state.networkId = uint(lcommon.AddressNetworkTestnet)
	state.stakeRegistered = map[lcommon.Blake2b224]bool{
		lcommon.Blake2b224(paymentHash): true,
	}
	state.addUtxo(input, newTestOutputWithAddress(10_000_000, keyAddress))
	state.addUtxo(
		collateralInput,
		newTestOutputWithAddress(5_000_000, keyAddress),
	)

	var updateValue gdijkstra.DijkstraProtocolParameterUpdate
	_, err = cbor.Decode(update, &updateValue)
	require.NoError(t, err)
	policy := script.Hash().Bytes()
	redeemerData, _ := referenceOverlapDatum(t)
	witnessSet := gdijkstra.DijkstraTransactionWitnessSet{
		WsRedeemers: gdijkstra.DijkstraRedeemers{
			Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
				{Tag: lcommon.RedeemerTagProposing, Index: 0}: {
					Data: redeemerData,
					ExUnits: lcommon.ExUnits{
						Steps:  10_000_000,
						Memory: 1_000_000,
					},
				},
			},
		},
	}
	languageID := uint(2)
	body := gdijkstra.DijkstraTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{input}),
		TxOutputs: []gdijkstra.DijkstraTransactionOutput{{
			Output: babbage.BabbageTransactionOutput{
				OutputAddress: keyAddress,
				OutputAmount: mary.MaryTransactionOutputValue{
					Amount: 10_000_000 - fee - deposit,
				},
			},
		}},
		TxFee: fee,
		TxCollateral: cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{collateralInput}, false),
		TxProposalProcedures: []gdijkstra.DijkstraProposalProcedure{{
			PPDeposit:       deposit,
			PPRewardAccount: rewardAddress,
			PPGovAction: gdijkstra.DijkstraGovAction{
				Type: uint(lcommon.GovActionTypeParameterChange),
				Action: &gdijkstra.DijkstraParameterChangeGovAction{
					Type:        uint(lcommon.GovActionTypeParameterChange),
					ParamUpdate: updateValue,
					PolicyHash:  policy,
				},
			},
			PPAnchor: lcommon.GovAnchor{
				Url: "https://example.invalid/proposal",
			},
		}},
	}
	switch s := script.(type) {
	case lcommon.PlutusV3Script:
		witnessSet.WsPlutusV3Scripts = cbor.NewSetType([]lcommon.PlutusV3Script{s}, false)
	case lcommon.PlutusV4Script:
		languageID = 3
		body.TxReferenceInputs = cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{referenceInput}, false)
		state.addUtxo(referenceInput, testAddressScriptOutput{
			testOutput: newTestOutput(2_000_000),
			addr:       keyAddress,
			scriptRef:  s,
		})
	default:
		t.Fatalf("unsupported guardrails script %T", script)
	}
	params := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			MaxTxSize:                  16_384,
			MaxValueSize:               5_000,
			MinFeeRefScriptCostPerByte: &cbor.Rat{Rat: big.NewRat(1, 1)},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 1_000_000,
			},
			CollateralPercentage: 150,
			MaxCollateralInputs:  3,
			GovActionDeposit:     deposit,
			ExecutionCosts: lcommon.ExUnitPrice{
				MemPrice:  &cbor.Rat{Rat: big.NewRat(0, 1)},
				StepPrice: &cbor.Rat{Rat: big.NewRat(0, 1)},
			},
			CostModels: map[uint][]int64{
				2: defaultMachineCostModel(t, lang.LanguageVersionV3),
				3: defaultMachineCostModel(t, lang.LanguageVersionV4),
			},
		},
		MaxRefScriptSizePerTx:   100_000,
		RefScriptCostStride:     25_600,
		RefScriptCostMultiplier: &cbor.Rat{Rat: big.NewRat(1, 1)},
	}
	redeemersCbor, err := cbor.Encode(witnessSet.WsRedeemers.Redeemers)
	require.NoError(t, err)
	langViewsCbor, err := lcommon.EncodeLangViews(
		map[uint]struct{}{languageID: {}}, params.CostModels)
	require.NoError(t, err)
	scriptDataHash := lcommon.Blake2b256Hash(
		append(redeemersCbor, langViewsCbor...),
	)
	body.TxScriptDataHash = &scriptDataHash
	tx := &gdijkstra.DijkstraTransaction{
		Body:       body,
		WitnessSet: witnessSet,
		TxIsValid:  true,
	}
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	tx, err = gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	tx.WitnessSet.VkeyWitnesses = cbor.NewSetType(
		[]lcommon.VkeyWitness{{
			Vkey:      publicKey,
			Signature: ed25519.Sign(privateKey, tx.Hash().Bytes()),
		}}, false)
	tx.WitnessSet.SetCbor(nil)
	tx.SetCbor(nil)
	return tx, guardrailsLedgerState{
		mockLedgerState: state,
		policy:          policy,
	}, params
}

func TestValidateTxDijkstraGuardrailsScriptSeesMaxPledgeLeverage(t *testing.T) {
	t.Parallel()

	for _, version := range []struct {
		name string
		lang lang.LanguageVersion
	}{
		{"V3", lang.LanguageVersionV3},
		{"V4", lang.LanguageVersionV4},
	} {
		for _, tc := range []struct {
			name     string
			update   map[uint]any
			expected string
		}{
			{"clear", map[uint]any{38: nil}, `Map [(I 38, Constr 1 [])]`},
			{
				"clear beside another field",
				map[uint]any{0: uint64(44), 38: nil},
				`Map [(I 0, I 44), (I 38, Constr 1 [])]`,
			},
			{
				"ratio",
				map[uint]any{38: cbor.Rat{Rat: big.NewRat(5, 2)}},
				`Map [(I 38, Constr 0 [List [I 5, I 2]])]`,
			},
		} {
			t.Run(version.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				update, err := cbor.Encode(tc.update)
				require.NoError(t, err)
				script := changedParametersGuardrailsScript(
					t,
					version.lang,
					tc.expected,
				)
				tx, ls, pp := newDijkstraGuardrailsTx(t, script, update)
				require.NoError(t, ValidateTxDijkstra(tx, 0, ls, pp))

				// A script expecting any other rendering must fail, which shows
				// the script above inspected the data it was given.
				other := `Map []`
				wrong := changedParametersGuardrailsScript(
					t,
					version.lang,
					other,
				)
				tx, ls, pp = newDijkstraGuardrailsTx(t, wrong, update)
				var failed conway.PlutusScriptFailedError
				require.ErrorAs(t, ValidateTxDijkstra(tx, 0, ls, pp), &failed)
				require.Equal(t, lcommon.RedeemerTagProposing, failed.Tag)
			})
		}
	}
}
