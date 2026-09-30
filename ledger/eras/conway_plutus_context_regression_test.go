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

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/plutigo/builtin"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/require"
)

type mockProducedValidityTx struct {
	mockConwayFeeTx
	produced []lcommon.Utxo
	valid    bool
}

func (m *mockProducedValidityTx) Produced() []lcommon.Utxo {
	return m.produced
}

func (m *mockProducedValidityTx) IsValid() bool {
	return m.valid
}

// TestInvalidPlutusTxInfoUsesBodyOutputs exercises production phase-2
// validation for invalid Babbage and Conway transactions. The scripts fail
// when they see the ordinary body outputs, so an invalid transaction passes
// only when TxInfo is built from Transaction.Outputs rather than Produced.
// Not t.Parallel: this test temporarily removes package-level rule slices.
func TestInvalidPlutusTxInfoUsesBodyOutputs(t *testing.T) {
	originalBabbageRules := babbageUtxoValidationRules
	babbageUtxoValidationRules = nil
	t.Cleanup(func() { babbageUtxoValidationRules = originalBabbageRules })
	withoutConwayUtxoValidationRules(t)

	for _, outputCase := range []struct {
		name        string
		bodyOutputs []lcommon.TransactionOutput
		produced    []lcommon.Utxo
		minOutputs  int
	}{
		{
			name: "different collateral return",
			bodyOutputs: []lcommon.TransactionOutput{
				newTestOutput(1_000_000),
				newTestOutput(2_000_000),
			},
			produced:   []lcommon.Utxo{{Output: newTestOutput(3_000_000)}},
			minOutputs: 2,
		},
		{
			name: "no collateral return",
			bodyOutputs: []lcommon.TransactionOutput{
				newTestOutput(1_000_000),
			},
			minOutputs: 1,
		},
	} {
		t.Run(outputCase.name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				version lang.LanguageVersion
			}{
				{name: "PlutusV1", version: lang.LanguageVersionV1},
				{name: "PlutusV2", version: lang.LanguageVersionV2},
				{name: "PlutusV3", version: lang.LanguageVersionV3},
			} {
				t.Run("Babbage/"+tc.name, func(t *testing.T) {
					if tc.version == lang.LanguageVersionV3 {
						t.Skip("Plutus V3 is unavailable in Babbage")
					}
					tx, input := newContextMintTx(
						t,
						txInfoOutputCountScript(
							t, tc.version, outputCase.minOutputs, true,
						),
						false,
						outputCase.bodyOutputs,
						outputCase.produced,
						nil,
					)
					ls := newMockLedgerState()
					ls.addUtxo(input, newTestOutput(10_000_000))
					pp := &babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						CostModels: map[uint][]int64{
							0: defaultMachineCostModel(t, lang.LanguageVersionV1),
							1: defaultMachineCostModel(t, lang.LanguageVersionV2),
						},
						MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
					}
					require.NoError(t, ValidateTxBabbage(tx, 0, ls, pp))
				})

				t.Run("Conway/"+tc.name, func(t *testing.T) {
					tx, input := newContextMintTx(
						t,
						txInfoOutputCountScript(
							t, tc.version, outputCase.minOutputs, true,
						),
						false,
						outputCase.bodyOutputs,
						outputCase.produced,
						nil,
					)
					ls := newMockLedgerState()
					ls.addUtxo(input, newTestOutput(10_000_000))
					modelIndex := uint(0)
					if tc.version == lang.LanguageVersionV2 {
						modelIndex = 1
					} else if tc.version == lang.LanguageVersionV3 {
						modelIndex = 2
					}
					pp := &conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: lcommon.ProtocolVersionVanRossem,
						},
						CostModels: map[uint][]int64{
							modelIndex: defaultMachineCostModel(t, tc.version),
						},
						MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
					}
					require.NoError(t, ValidateTxConway(tx, 0, ls, pp))
				})
			}
		})
	}

	t.Run("PassedUnexpectedly is rejected", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			version lang.LanguageVersion
		}{
			{name: "PlutusV1", version: lang.LanguageVersionV1},
			{name: "PlutusV2", version: lang.LanguageVersionV2},
			{name: "PlutusV3", version: lang.LanguageVersionV3},
		} {
			if tc.version != lang.LanguageVersionV3 {
				t.Run("Babbage/"+tc.name, func(t *testing.T) {
					tx, input := newContextMintTx(
						t,
						txInfoOutputCountScript(t, tc.version, 1, false),
						false,
						[]lcommon.TransactionOutput{newTestOutput(1_000_000)},
						nil,
						nil,
					)
					ls := newMockLedgerState()
					ls.addUtxo(input, newTestOutput(10_000_000))
					pp := &babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						CostModels: map[uint][]int64{
							0: defaultMachineCostModel(t, lang.LanguageVersionV1),
							1: defaultMachineCostModel(t, lang.LanguageVersionV2),
						},
						MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
					}
					require.ErrorContains(
						t,
						ValidateTxBabbage(tx, 0, ls, pp),
						"transaction declared invalid but Plutus scripts succeeded",
					)
				})
			}

			t.Run("Conway/"+tc.name, func(t *testing.T) {
				tx, input := newContextMintTx(
					t,
					txInfoOutputCountScript(t, tc.version, 1, false),
					false,
					[]lcommon.TransactionOutput{newTestOutput(1_000_000)},
					nil,
					nil,
				)
				ls := newMockLedgerState()
				ls.addUtxo(input, newTestOutput(10_000_000))
				modelIndex := uint(0)
				if tc.version == lang.LanguageVersionV2 {
					modelIndex = 1
				} else if tc.version == lang.LanguageVersionV3 {
					modelIndex = 2
				}
				pp := &conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: lcommon.ProtocolVersionVanRossem,
					},
					CostModels: map[uint][]int64{
						modelIndex: defaultMachineCostModel(t, tc.version),
					},
					MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
				}
				require.ErrorContains(
					t,
					ValidateTxConway(tx, 0, ls, pp),
					"transaction declared invalid but Plutus scripts succeeded",
				)
			})
		}
	})
}

func txInfoOutputCountScript(
	t *testing.T,
	version lang.LanguageVersion,
	minOutputs int,
	failWhenPresent bool,
) lcommon.Script {
	t.Helper()
	uplcVersion := lang.LanguageVersionV1
	argumentCount := 2
	contextIndex := syn.DeBruijn(1)
	if version == lang.LanguageVersionV3 {
		// Plutus V3 uses UPLC 1.1; Plutus V1 and V2 use UPLC 1.0.
		uplcVersion = lang.LanguageVersionV2
		argumentCount = 1
	}
	context := syn.Term[syn.DeBruijn](
		&syn.Var[syn.DeBruijn]{Name: contextIndex},
	)
	contextFields := conwayV3SndPair(conwayV3UnConstrData(context))
	txInfo := conwayV3HeadList(contextFields)
	txInfoFields := conwayV3SndPair(conwayV3UnConstrData(txInfo))
	outputsFieldIndex := 1
	if version != lang.LanguageVersionV1 {
		outputsFieldIndex = 2
	}
	outputsData := conwayV3HeadList(
		conwayV3TailList(txInfoFields, outputsFieldIndex),
	)
	outputs := conwayV3UnListData(outputsData)
	shorterThanExpected := conwayV3Apply(
		builtin.NullList,
		outputs,
	)
	if minOutputs > 1 {
		shorterThanExpected = conwayV3Apply(
			builtin.NullList,
			conwayV3Apply(builtin.TailList, outputs),
		)
	}
	shortResult := syn.Term[syn.DeBruijn](
		&syn.Constant{Con: &syn.Unit{}},
	)
	presentResult := syn.Term[syn.DeBruijn](&syn.Error{})
	if !failWhenPresent {
		shortResult = &syn.Error{}
		presentResult = &syn.Constant{Con: &syn.Unit{}}
	}
	result := conwayV3Apply(
		builtin.IfThenElse,
		shorterThanExpected,
		&syn.Delay[syn.DeBruijn]{Term: shortResult},
		&syn.Delay[syn.DeBruijn]{Term: presentResult},
	)
	result = &syn.Force[syn.DeBruijn]{Term: result}
	for range argumentCount {
		result = &syn.Lambda[syn.DeBruijn]{Body: result}
	}
	program := &syn.Program[syn.DeBruijn]{Version: uplcVersion, Term: result}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	switch version {
	case lang.LanguageVersionV1:
		return lcommon.PlutusV1Script(scriptBytes)
	case lang.LanguageVersionV2:
		return lcommon.PlutusV2Script(scriptBytes)
	default:
		return lcommon.PlutusV3Script(scriptBytes)
	}
}

func newContextMintTx(
	t *testing.T,
	plutusScript lcommon.Script,
	valid bool,
	outputs []lcommon.TransactionOutput,
	produced []lcommon.Utxo,
	proposals []lcommon.ProposalProcedure,
) (*mockProducedValidityTx, testInput) {
	t.Helper()
	redeemerValue := lcommon.RedeemerValue{
		Data:    lcommon.Datum{Data: data.NewConstr(0)},
		ExUnits: lcommon.ExUnits{Memory: 5_000_000, Steps: 50_000_000},
	}
	witnesses := &mockWitnessSet{
		// valueOverride makes Value agree with the iterated entry: the
		// Babbage phase-2 path reads the declared budget through the TxInfo
		// redeemers, which look the value up by key.
		redeemers: &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{Tag: lcommon.RedeemerTagMint, Index: 0},
					val: redeemerValue,
				},
			},
			valueOverride: &redeemerValue,
		},
	}
	switch tmpScript := plutusScript.(type) {
	case lcommon.PlutusV1Script:
		witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{tmpScript}
	case lcommon.PlutusV2Script:
		witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{tmpScript}
	case lcommon.PlutusV3Script:
		witnesses.plutusV3Scripts = []lcommon.PlutusV3Script{tmpScript}
	default:
		t.Fatalf("unsupported script type %T", plutusScript)
	}
	hash := plutusScript.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(hash): {
				cbor.NewByteString([]byte("txinfo")): big.NewInt(1),
			},
		},
	)
	input := newTestInput(0xf1, 0)
	tx := &mockProducedValidityTx{
		mockConwayFeeTx: mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType:    txTypeAlonzo,
				fee:       big.NewInt(0),
				witnesses: witnesses,
			},
			inputs:             []lcommon.TransactionInput{input},
			assetMint:          &assetMint,
			outputs:            outputs,
			proposalProcedures: proposals,
		},
		produced: produced,
		valid:    valid,
	}
	return tx, input
}

// TestConwayV3TreasuryWithdrawalsSerializeInReferenceOrder compares the V3
// proposal context with the ledger's network, credential-kind, and hash order,
// then exercises the same action from a Plutus script through ValidateTxConway.
func TestConwayV3TreasuryWithdrawalsSerializeInReferenceOrder(t *testing.T) {
	withoutConwayUtxoValidationRules(t)

	addresses := []*lcommon.Address{
		newConwayRewardAddressOnNetwork(t, lcommon.AddressTypeNoneScript, lcommon.AddressNetworkTestnet, 0x02),
		newConwayRewardAddressOnNetwork(t, lcommon.AddressTypeNoneKey, lcommon.AddressNetworkMainnet, 0x02),
		newConwayRewardAddressOnNetwork(t, lcommon.AddressTypeNoneKey, lcommon.AddressNetworkTestnet, 0x01),
		newConwayRewardAddressOnNetwork(t, lcommon.AddressTypeNoneScript, lcommon.AddressNetworkMainnet, 0x01),
		newConwayRewardAddressOnNetwork(t, lcommon.AddressTypeNoneScript, lcommon.AddressNetworkTestnet, 0x01),
	}
	withdrawalAmounts := map[*lcommon.Address]uint64{
		addresses[0]: 5,
		addresses[1]: 4,
		addresses[2]: 3,
		addresses[3]: 2,
		addresses[4]: 1,
	}
	orderedAddresses := []*lcommon.Address{
		addresses[4],
		addresses[0],
		addresses[2],
		addresses[3],
		addresses[1],
	}
	withdrawalPairs := make([][2]data.PlutusData, 0, len(orderedAddresses))
	for _, address := range orderedAddresses {
		withdrawalPairs = append(withdrawalPairs, [2]data.PlutusData{
			address.ToPlutusData(),
			data.NewInteger(new(big.Int).SetUint64(withdrawalAmounts[address])),
		})
	}
	deposit := uint64(10)
	rewardAccount := newConwayRewardAddressOnNetwork(
		t,
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		0x91,
	)
	action := &lcommon.TreasuryWithdrawalGovAction{
		Type:        uint(lcommon.GovActionTypeTreasuryWithdrawal),
		Withdrawals: withdrawalAmounts,
	}
	proposal := &conway.ConwayProposalProcedure{
		PPDeposit:       deposit,
		PPRewardAccount: *rewardAccount,
		PPGovAction: conway.ConwayGovAction{
			Type:   uint(lcommon.GovActionTypeTreasuryWithdrawal),
			Action: action,
		},
	}
	expectedAction := data.NewConstr(
		2,
		data.NewMap(withdrawalPairs),
		data.NewConstr(1),
	)
	expectedProposal := data.NewConstr(
		0,
		data.NewInteger(new(big.Int).SetUint64(deposit)),
		rewardAccount.ToPlutusData(),
		expectedAction,
	)
	expectedBytes, err := data.Encode(expectedProposal)
	require.NoError(t, err)

	scriptBytes := conwayV3SerializedProposalObserver(t, expectedBytes)
	plutusScript := lcommon.PlutusV3Script(scriptBytes)
	tx, input := newContextMintTx(
		t,
		plutusScript,
		true,
		nil,
		nil,
		[]lcommon.ProposalProcedure{proposal},
	)
	ls := newMockLedgerState()
	ls.addUtxo(input, newTestOutput(10_000_000))
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
		CostModels: map[uint][]int64{
			2: defaultMachineCostModel(t, lang.LanguageVersion{1, 1, 0}),
		},
		MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
	}

	for range 24 {
		txInfo, err := script.NewTxInfoV3FromTransaction(
			ls,
			tx,
			[]lcommon.Utxo{{Id: input, Output: newTestOutput(10_000_000)}},
			lcommon.ProtocolVersionVanRossem,
		)
		require.NoError(t, err)
		serialized, err := data.Encode(txInfo.ProposalProcedures[0].ToPlutusData())
		require.NoError(t, err)
		require.Equal(t, expectedBytes, serialized)
		require.NoError(t, ValidateTxConway(tx, 0, ls, pp))
	}
}

func newConwayRewardAddressOnNetwork(
	t *testing.T,
	addressType uint8,
	network uint8,
	hashByte byte,
) *lcommon.Address {
	t.Helper()
	hash := make([]byte, lcommon.AddressHashSize)
	hash[0] = hashByte
	address, err := lcommon.NewAddressFromParts(addressType, network, nil, hash)
	require.NoError(t, err)
	return &address
}

func conwayV3SerializedProposalObserver(
	t *testing.T,
	expected []byte,
) []byte {
	t.Helper()
	context := syn.Term[syn.DeBruijn](&syn.Var[syn.DeBruijn]{Name: 1})
	contextFields := conwayV3SndPair(conwayV3UnConstrData(context))
	txInfo := conwayV3HeadList(contextFields)
	txInfoFields := conwayV3SndPair(conwayV3UnConstrData(txInfo))
	proposalsData := conwayV3HeadList(conwayV3TailList(txInfoFields, 13))
	proposal := conwayV3HeadList(conwayV3UnListData(proposalsData))
	serializedProposal := conwayV3Apply(builtin.SerialiseData, proposal)
	equal := conwayV3Apply(
		builtin.EqualsByteString,
		serializedProposal,
		&syn.Constant{Con: &syn.ByteString{Inner: expected}},
	)
	result := conwayV3Apply(
		builtin.IfThenElse,
		equal,
		&syn.Delay[syn.DeBruijn]{Term: &syn.Constant{Con: &syn.Unit{}}},
		&syn.Delay[syn.DeBruijn]{Term: &syn.Error{}},
	)
	result = &syn.Force[syn.DeBruijn]{Term: result}
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    &syn.Lambda[syn.DeBruijn]{Body: result},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	return scriptBytes
}

// Not t.Parallel: this test temporarily replaces package-level rule slices.
func TestUnusedPlutusScriptsDoNotRequireCostModels(t *testing.T) {
	installAlonzoRule(t, lcommon.UtxoValidationRuleCostModelsPresent)
	installBabbageRule(t, lcommon.UtxoValidationRuleCostModelsPresent)
	installConwayRule(t, lcommon.UtxoValidationRuleCostModelsPresent)

	t.Run("Alonzo unused V1 witness", func(t *testing.T) {
		tx := &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType:    txTypeAlonzo,
				fee:       big.NewInt(0),
				witnesses: &mockWitnessSet{plutusV1Scripts: []lcommon.PlutusV1Script{{0x01}}},
			},
		}
		pp := &alonzo.AlonzoProtocolParameters{MaxTxSize: 16_384}
		require.NoError(t, ValidateTxAlonzo(tx, 0, newMockLedgerState(), pp))

		usedScript, _ := newConwayOverlapMintScript(t, lang.LanguageVersionV1)
		usedTx, input := newContextMintTx(t, usedScript, true, nil, nil, nil)
		ls := newMockLedgerState()
		ls.addUtxo(input, newTestOutput(10_000_000))
		require.ErrorContains(
			t,
			ValidateTxAlonzo(usedTx, 0, ls, pp),
			"missing cost model for Plutus v1",
		)
	})

	t.Run("Babbage unused witness and reference scripts", func(t *testing.T) {
		for _, tc := range []struct {
			name             string
			version          lang.LanguageVersion
			missingCostModel string
		}{
			{
				name:             "PlutusV1",
				version:          lang.LanguageVersionV1,
				missingCostModel: "missing cost model for Plutus v1",
			},
			{
				name:             "PlutusV2",
				version:          lang.LanguageVersionV2,
				missingCostModel: "missing cost model for Plutus v2",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				unusedScript := plutusScriptVersionFixture(tc.version)
				for _, source := range []string{"witness", "reference input", "spent input"} {
					t.Run(source, func(t *testing.T) {
						tx := &mockConwayFeeTx{
							mockFeeTx: mockFeeTx{
								txType: txTypeAlonzo,
								fee:    big.NewInt(0),
								witnesses: &mockWitnessSet{
									plutusV1Scripts: unusedPlutusV1Scripts(unusedScript),
									plutusV2Scripts: unusedPlutusV2Scripts(unusedScript),
								},
							},
						}
						ls := newMockLedgerState()
						switch source {
						case "reference input":
							input := newTestInput(0xe1, 0)
							tx.referenceInputs = []lcommon.TransactionInput{input}
							ls.addUtxo(input, testAddressScriptOutput{
								testOutput: newTestOutput(1_000_000),
								addr:       newTestKeyAddress(t),
								scriptRef:  unusedScript,
							})
						case "spent input":
							input := newTestInput(0xe2, 0)
							tx.inputs = []lcommon.TransactionInput{input}
							ls.addUtxo(input, testAddressScriptOutput{
								testOutput: newTestOutput(1_000_000),
								addr:       newTestKeyAddress(t),
								scriptRef:  unusedScript,
							})
						}
						pp := &babbage.BabbageProtocolParameters{ProtocolMajor: 7}
						require.NoError(t, ValidateTxBabbage(tx, 0, ls, pp))
					})
				}

				usedScript, _ := newConwayOverlapMintScript(t, tc.version)
				usedTx, input := newContextMintTx(t, usedScript, true, nil, nil, nil)
				ls := newMockLedgerState()
				ls.addUtxo(input, newTestOutput(10_000_000))
				require.ErrorContains(
					t,
					ValidateTxBabbage(
						usedTx,
						0,
						ls,
						&babbage.BabbageProtocolParameters{ProtocolMajor: 7},
					),
					tc.missingCostModel,
				)
			})
		}
	})

	t.Run("Conway unused V3 reference and needed V3", func(t *testing.T) {
		refInput := newTestInput(0xe3, 0)
		tx := &mockConwayFeeTx{
			mockFeeTx:       mockFeeTx{txType: txTypeAlonzo, fee: big.NewInt(0)},
			referenceInputs: []lcommon.TransactionInput{refInput},
		}
		ls := newMockLedgerState()
		ls.addUtxo(refInput, testAddressScriptOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       newTestKeyAddress(t),
			scriptRef:  lcommon.PlutusV3Script{0x03},
		})
		pp := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: lcommon.ProtocolVersionVanRossem,
			},
		}
		require.NoError(t, ValidateTxConway(tx, 0, ls, pp))

		usedScript, _ := newConwayOverlapMintScript(t, lang.LanguageVersionV3)
		usedTx, input := newContextMintTx(t, usedScript, true, nil, nil, nil)
		usedState := newMockLedgerState()
		usedState.addUtxo(input, newTestOutput(10_000_000))
		require.ErrorContains(
			t,
			ValidateTxConway(usedTx, 0, usedState, pp),
			"missing cost model for Plutus v3",
		)
	})
}

// Not t.Parallel: this test temporarily replaces package-level rule slices.
func TestConwayTreasuryDonationUsesNeededPlutusScripts(t *testing.T) {
	installConwayRule(t, lcommon.UtxoValidationRuleValueNotConserved)

	for _, tc := range []struct {
		name   string
		script lcommon.Script
	}{
		{name: "PlutusV1", script: lcommon.PlutusV1Script{0x04}},
		{name: "PlutusV2", script: lcommon.PlutusV2Script{0x04}},
	} {
		t.Run("unrelated "+tc.name+" reference script is allowed", func(t *testing.T) {
			input := newTestInput(0xe4, 0)
			refInput := newTestInput(0xe5, 0)
			tx := &mockTreasuryDonationTx{
				mockConwayFeeTx: mockConwayFeeTx{
					mockFeeTx: mockFeeTx{txType: txTypeAlonzo, fee: big.NewInt(0)},
					inputs:    []lcommon.TransactionInput{input},
					referenceInputs: []lcommon.TransactionInput{
						refInput,
					},
					outputs: []lcommon.TransactionOutput{newTestOutput(1_000_000)},
				},
				donation: big.NewInt(1_000_000),
			}
			ls := newMockLedgerState()
			ls.addUtxo(input, newTestOutput(2_000_000))
			ls.addUtxo(refInput, testAddressScriptOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       newTestKeyAddress(t),
				scriptRef:  tc.script,
			})
			pp := &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: lcommon.ProtocolVersionVanRossem,
				},
			}
			require.NoError(t, ValidateTxConway(tx, 0, ls, pp))
		})
	}

	for _, tc := range []struct {
		name    string
		version lang.LanguageVersion
	}{
		{name: "PlutusV1", version: lang.LanguageVersionV1},
		{name: "PlutusV2", version: lang.LanguageVersionV2},
	} {
		t.Run(tc.name+" execution blocks donation", func(t *testing.T) {
			plutusScript, _ := newConwayOverlapMintScript(t, tc.version)
			witnesses := &mockWitnessSet{}
			switch scriptValue := plutusScript.(type) {
			case lcommon.PlutusV1Script:
				witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{scriptValue}
			case lcommon.PlutusV2Script:
				witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{scriptValue}
			}
			witnesses.redeemers = &mockRedeemers{entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{Tag: lcommon.RedeemerTagReward, Index: 0},
					val: lcommon.RedeemerValue{
						Data:    lcommon.Datum{Data: data.NewConstr(0)},
						ExUnits: lcommon.ExUnits{Memory: 5_000_000, Steps: 50_000_000},
					},
				},
			}}
			rewardAddress := newConwayRewardAddress(
				t,
				lcommon.AddressTypeNoneScript,
				plutusScript.Hash().Bytes(),
			)
			input := newTestInput(0xe6, 0)
			tx := &mockTreasuryDonationTx{
				mockConwayFeeTx: mockConwayFeeTx{
					mockFeeTx: mockFeeTx{
						txType:    txTypeAlonzo,
						fee:       big.NewInt(0),
						witnesses: witnesses,
					},
					inputs: []lcommon.TransactionInput{input},
					withdrawals: map[*lcommon.Address]*big.Int{
						rewardAddress: big.NewInt(1_000_000),
					},
					outputs: []lcommon.TransactionOutput{newTestOutput(1_000_000)},
				},
				donation: big.NewInt(1_000_000),
			}
			ls := newMockLedgerState()
			ls.addUtxo(input, newTestOutput(1_000_000))
			pp := &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: lcommon.ProtocolVersionVanRossem,
				},
				CostModels: map[uint][]int64{
					uint(tc.version[1]): defaultMachineCostModel(t, tc.version),
				},
				MaxTxExUnits: lcommon.ExUnits{Memory: 10_000_000, Steps: 100_000_000},
			}
			require.ErrorContains(t, ValidateTxConway(tx, 0, ls, pp), "treasury donation")
		})
	}
}

type mockTreasuryDonationTx struct {
	mockConwayFeeTx
	donation *big.Int
}

func (m *mockTreasuryDonationTx) Donation() *big.Int {
	return m.donation
}

func plutusScriptVersionFixture(version lang.LanguageVersion) lcommon.Script {
	switch version {
	case lang.LanguageVersionV1:
		return lcommon.PlutusV1Script{0x01}
	case lang.LanguageVersionV2:
		return lcommon.PlutusV2Script{0x02}
	default:
		return lcommon.PlutusV3Script{0x03}
	}
}

func unusedPlutusV1Scripts(plutusScript lcommon.Script) []lcommon.PlutusV1Script {
	if scriptValue, ok := plutusScript.(lcommon.PlutusV1Script); ok {
		return []lcommon.PlutusV1Script{scriptValue}
	}
	return nil
}

func unusedPlutusV2Scripts(plutusScript lcommon.Script) []lcommon.PlutusV2Script {
	if scriptValue, ok := plutusScript.(lcommon.PlutusV2Script); ok {
		return []lcommon.PlutusV2Script{scriptValue}
	}
	return nil
}

func installAlonzoRule(t *testing.T, id lcommon.UtxoValidationRuleId) {
	t.Helper()
	original := alonzoUtxoValidationRules
	alonzoUtxoValidationRules = singleValidationRule(
		t,
		alonzo.UtxoValidationRuleDescriptors(),
		id,
	)
	t.Cleanup(func() { alonzoUtxoValidationRules = original })
}

func installBabbageRule(t *testing.T, id lcommon.UtxoValidationRuleId) {
	t.Helper()
	original := babbageUtxoValidationRules
	babbageUtxoValidationRules = singleValidationRule(
		t,
		babbage.UtxoValidationRuleDescriptors(),
		id,
	)
	t.Cleanup(func() { babbageUtxoValidationRules = original })
}

func installConwayRule(t *testing.T, id lcommon.UtxoValidationRuleId) {
	t.Helper()
	originalRules := conwayUtxoValidationRules
	originalPhase1Rules := conwayPhase1UtxoValidationRules
	rule := singleValidationRule(t, conway.UtxoValidationRuleDescriptors(), id)
	conwayUtxoValidationRules = rule
	conwayPhase1UtxoValidationRules = rule
	t.Cleanup(func() {
		conwayUtxoValidationRules = originalRules
		conwayPhase1UtxoValidationRules = originalPhase1Rules
	})
}

func singleValidationRule(
	t *testing.T,
	descriptors []lcommon.UtxoValidationRuleDescriptor,
	id lcommon.UtxoValidationRuleId,
) []indexedUtxoValidationRule {
	t.Helper()
	for index, descriptor := range descriptors {
		if descriptor.Id == id {
			return []indexedUtxoValidationRule{{
				index:          index,
				validationFunc: descriptor.Validator,
			}}
		}
	}
	t.Fatalf("validation rule %q not found", id)
	return nil
}
