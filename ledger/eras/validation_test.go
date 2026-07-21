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
	"encoding/hex"
	"iter"
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockTransaction implements lcommon.Transaction for
// testing.
type mockTransaction struct {
	lcommon.Transaction
	cbor   []byte
	txType int
}

func (m *mockTransaction) Cbor() []byte {
	return m.cbor
}

func (m *mockTransaction) Type() int {
	return m.txType
}

// mockFeeTx extends mockTransaction with Fee() and
// Witnesses() support for fee validation tests.
type mockFeeTx struct {
	lcommon.Transaction
	cbor      []byte
	txType    int
	fee       *big.Int
	witnesses lcommon.TransactionWitnessSet
}

func (m *mockFeeTx) Cbor() []byte {
	return m.cbor
}

func (m *mockFeeTx) Type() int {
	return m.txType
}

func (m *mockFeeTx) Fee() *big.Int {
	return m.fee
}

func (m *mockFeeTx) Witnesses() lcommon.TransactionWitnessSet {
	return m.witnesses
}

func (m *mockFeeTx) ScriptDataHash() *lcommon.Blake2b256 {
	return nil
}

type mockConwayFeeTx struct {
	mockFeeTx
	inputs             []lcommon.TransactionInput
	referenceInputs    []lcommon.TransactionInput
	certificates       []lcommon.Certificate
	withdrawals        map[*lcommon.Address]*big.Int
	assetMint          *lcommon.MultiAsset[lcommon.MultiAssetTypeMint]
	votingProcedures   lcommon.VotingProcedures
	proposalProcedures []lcommon.ProposalProcedure
}

func (m *mockConwayFeeTx) Inputs() []lcommon.TransactionInput {
	return m.inputs
}

func (m *mockConwayFeeTx) ReferenceInputs() []lcommon.TransactionInput {
	return m.referenceInputs
}

func (m *mockConwayFeeTx) Id() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}

func (m *mockConwayFeeTx) Produced() []lcommon.Utxo {
	return nil
}

func (m *mockConwayFeeTx) Outputs() []lcommon.TransactionOutput {
	return nil
}

func (m *mockConwayFeeTx) TTL() uint64 {
	return 0
}

func (m *mockConwayFeeTx) ValidityIntervalStart() uint64 {
	return 0
}

func (m *mockConwayFeeTx) Certificates() []lcommon.Certificate {
	return m.certificates
}

func (m *mockConwayFeeTx) Withdrawals() map[*lcommon.Address]*big.Int {
	return m.withdrawals
}

func (m *mockConwayFeeTx) RequiredSigners() []lcommon.Blake2b224 {
	return nil
}

func (m *mockConwayFeeTx) AssetMint() *lcommon.MultiAsset[lcommon.MultiAssetTypeMint] {
	return m.assetMint
}

func (m *mockConwayFeeTx) IsValid() bool {
	return true
}

func (m *mockConwayFeeTx) VotingProcedures() lcommon.VotingProcedures {
	return m.votingProcedures
}

func (m *mockConwayFeeTx) ProposalProcedures() []lcommon.ProposalProcedure {
	return m.proposalProcedures
}

type testScriptOutput struct {
	testOutput
	scriptRef lcommon.Script
}

func (o testScriptOutput) ScriptRef() lcommon.Script {
	return o.scriptRef
}

type testAddressOutput struct {
	testOutput
	addr lcommon.Address
}

func (o testAddressOutput) Address() lcommon.Address {
	return o.addr
}

type testAddressScriptOutput struct {
	testOutput
	addr      lcommon.Address
	scriptRef lcommon.Script
}

func (o testAddressScriptOutput) Address() lcommon.Address {
	return o.addr
}

func (o testAddressScriptOutput) ScriptRef() lcommon.Script {
	return o.scriptRef
}

// mockWitnessSet implements TransactionWitnessSet for
// testing, returning only redeemers.
type mockWitnessSet struct {
	redeemers       lcommon.TransactionWitnessRedeemers
	nativeScripts   []lcommon.NativeScript
	plutusV1Scripts []lcommon.PlutusV1Script
	plutusV2Scripts []lcommon.PlutusV2Script
	plutusV3Scripts []lcommon.PlutusV3Script
}

func (m *mockWitnessSet) Vkey() []lcommon.VkeyWitness {
	return nil
}

func (m *mockWitnessSet) NativeScripts() []lcommon.NativeScript {
	return m.nativeScripts
}

func (m *mockWitnessSet) Bootstrap() []lcommon.BootstrapWitness {
	return nil
}

func (m *mockWitnessSet) PlutusData() []lcommon.Datum {
	return nil
}

func (m *mockWitnessSet) PlutusV1Scripts() []lcommon.PlutusV1Script {
	return m.plutusV1Scripts
}

func (m *mockWitnessSet) PlutusV2Scripts() []lcommon.PlutusV2Script {
	return m.plutusV2Scripts
}

func (m *mockWitnessSet) PlutusV3Scripts() []lcommon.PlutusV3Script {
	return m.plutusV3Scripts
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

func TestAlonzoValidationRulesUseLocalPlutusExecution(t *testing.T) {
	requireRuleIndexResolvesToFunc(
		t,
		alonzo.UtxoValidationRules,
		alonzoUtxoValidatePlutusScriptsRuleIndex,
		alonzo.UtxoValidatePlutusScripts,
		"alonzo.UtxoValidatePlutusScripts",
	)
	require.Len(t, alonzoUtxoValidationRules, len(alonzo.UtxoValidationRules)-1)
	requireIndexedRulesExcludeFunc(
		t,
		alonzoUtxoValidationRules,
		alonzo.UtxoValidatePlutusScripts,
		"Alonzo validation must use Dingo's local Plutus execution path",
	)
}

func TestBabbageValidationRulesUseLocalPlutusExecution(t *testing.T) {
	requireRuleIndexResolvesToFunc(
		t,
		babbage.UtxoValidationRules,
		babbageUtxoValidatePlutusScriptsRuleIndex,
		babbage.UtxoValidatePlutusScripts,
		"babbage.UtxoValidatePlutusScripts",
	)
	require.Len(t, babbageUtxoValidationRules, len(babbage.UtxoValidationRules)-1)
	requireIndexedRulesExcludeFunc(
		t,
		babbageUtxoValidationRules,
		babbage.UtxoValidatePlutusScripts,
		"Babbage validation must use Dingo's local Plutus execution path",
	)
}

func TestConwayValidationRulesUseLocalPlutusExecution(t *testing.T) {
	requireRuleIndexResolvesToFunc(
		t,
		conway.UtxoValidationRules,
		conwayUtxoValidateFeeTooSmallRuleIndex,
		conway.UtxoValidateFeeTooSmallUtxo,
		"conway.UtxoValidateFeeTooSmallUtxo",
	)
	requireRuleIndexResolvesToFunc(
		t,
		conway.UtxoValidationRules,
		conwayUtxoValidatePlutusScriptsRuleIndex,
		conway.UtxoValidatePlutusScripts,
		"conway.UtxoValidatePlutusScripts",
	)
	require.Len(t, conwayUtxoValidationRules, len(conway.UtxoValidationRules)-2)
	requireIndexedRulesExcludeFunc(
		t,
		conwayUtxoValidationRules,
		conway.UtxoValidateFeeTooSmallUtxo,
		"Conway validation must use Dingo's reference-script-aware fee rule",
	)
	requireIndexedRulesExcludeFunc(
		t,
		conwayUtxoValidationRules,
		conway.UtxoValidatePlutusScripts,
		"Conway validation must use Dingo's local Plutus execution path",
	)
}

func TestConwayPhase1ValidationRulesSkipPlutusExecution(t *testing.T) {
	requireRuleIndexResolvesToFunc(
		t,
		conway.UtxoValidationRules,
		conwayUtxoValidateFeeTooSmallRuleIndex,
		conway.UtxoValidateFeeTooSmallUtxo,
		"conway.UtxoValidateFeeTooSmallUtxo",
	)
	requireRuleIndexResolvesToFunc(
		t,
		conway.UtxoValidationRules,
		conwayUtxoValidateExUnitsTooBigRuleIndex,
		conway.UtxoValidateExUnitsTooBigUtxo,
		"conway.UtxoValidateExUnitsTooBigUtxo",
	)
	requireRuleIndexResolvesToFunc(
		t,
		conway.UtxoValidationRules,
		conwayUtxoValidatePlutusScriptsRuleIndex,
		conway.UtxoValidatePlutusScripts,
		"conway.UtxoValidatePlutusScripts",
	)
	require.Len(
		t,
		conwayPhase1UtxoValidationRules,
		len(conway.UtxoValidationRules)-2,
	)
	requireIndexedRulesExcludeFunc(
		t,
		conwayPhase1UtxoValidationRules,
		conway.UtxoValidateFeeTooSmallUtxo,
		"Conway phase-1 validation must use Dingo's reference-script-aware fee rule",
	)
	requireIndexedRulesIncludeFunc(
		t,
		conwayPhase1UtxoValidationRules,
		conway.UtxoValidateExUnitsTooBigUtxo,
		"Conway phase-1 replay must still enforce ExUnits limits",
	)
	requireIndexedRulesExcludeFunc(
		t,
		conwayPhase1UtxoValidationRules,
		conway.UtxoValidatePlutusScripts,
		"Conway phase-1 replay must not execute Plutus scripts",
	)
}

func TestValidateTxPlutusConwayMissingScriptWitnessFails(t *testing.T) {
	var scriptHash lcommon.ScriptHash
	scriptHash[0] = 0xaa
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)

	spendInput := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				redeemers: &mockRedeemers{
					entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagSpend,
								Index: 0,
							},
							val: lcommon.RedeemerValue{},
						},
					},
				},
			},
		},
		inputs: []lcommon.TransactionInput{spendInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(
		spendInput,
		testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       addr,
		},
	)

	err = ValidateTxPlutusConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	)
	require.Error(t, err)
	var missing lcommon.MissingScriptWitnessesError
	require.ErrorAs(t, err, &missing)
	assert.Equal(t, scriptHash, missing.ScriptHash)
}

func TestValidateTxPlutusConwayMissingScriptWitnessWithoutRedeemerFails(t *testing.T) {
	var scriptHash lcommon.ScriptHash
	scriptHash[0] = 0xaa
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)

	spendInput := newTestInput(0x01, 0)
	ls := newMockLedgerState()
	ls.addUtxo(
		spendInput,
		testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       addr,
		},
	)

	tests := []struct {
		name      string
		witnesses lcommon.TransactionWitnessSet
	}{
		{
			name: "nil witnesses",
		},
		{
			name:      "nil redeemers",
			witnesses: &mockWitnessSet{},
		},
		{
			name: "empty redeemers",
			witnesses: &mockWitnessSet{
				redeemers: &mockRedeemers{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					txType:    txTypeAlonzo,
					witnesses: tt.witnesses,
				},
				inputs: []lcommon.TransactionInput{spendInput},
			}

			err := ValidateTxPlutusConway(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			)
			require.Error(t, err)
			var missing lcommon.MissingScriptWitnessesError
			require.ErrorAs(t, err, &missing)
			assert.Equal(t, scriptHash, missing.ScriptHash)
		})
	}
}

func TestValidateTxPlutusConwayNativeScriptWitnessWithoutRedeemerPasses(t *testing.T) {
	nativeScript := lcommon.NativeScript{}
	scriptHash := nativeScript.Hash()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)

	spendInput := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				nativeScripts: []lcommon.NativeScript{
					nativeScript,
				},
			},
		},
		inputs: []lcommon.TransactionInput{spendInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(
		spendInput,
		testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       addr,
		},
	)

	require.NoError(t, ValidateTxPlutusConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

func TestValidateTxPlutusConwayMissingRedeemerForScriptRefFails(t *testing.T) {
	plutusScript := lcommon.PlutusV2Script([]byte{0x01, 0x02})
	scriptHash := plutusScript.Hash()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)

	spendInput := newTestInput(0x01, 0)
	ls := newMockLedgerState()
	ls.addUtxo(
		spendInput,
		testAddressScriptOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       addr,
			scriptRef:  plutusScript,
		},
	)

	tests := []struct {
		name      string
		witnesses lcommon.TransactionWitnessSet
	}{
		{
			name: "nil witnesses",
		},
		{
			name:      "nil redeemers",
			witnesses: &mockWitnessSet{},
		},
		{
			name: "empty redeemers",
			witnesses: &mockWitnessSet{
				redeemers: &mockRedeemers{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					txType:    txTypeAlonzo,
					witnesses: tt.witnesses,
				},
				inputs: []lcommon.TransactionInput{spendInput},
			}

			err := ValidateTxPlutusConway(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			)
			require.Error(t, err)
			var missing conway.MissingRedeemerForScriptError
			require.ErrorAs(t, err, &missing)
			assert.Equal(t, scriptHash, missing.ScriptHash)
			assert.Equal(t, lcommon.RedeemerTagSpend, missing.Tag)
			assert.Equal(t, uint32(0), missing.Index)
		})
	}
}

func TestValidateTxPlutusConwayRegistrationCertificateMissingRedeemerFails(t *testing.T) {
	plutusScript := lcommon.PlutusV2Script([]byte{0x03, 0x04})
	scriptHash := plutusScript.Hash()
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				plutusV2Scripts: []lcommon.PlutusV2Script{
					plutusScript,
				},
				redeemers: &mockRedeemers{},
			},
		},
		certificates: []lcommon.Certificate{
			&lcommon.RegistrationCertificate{
				StakeCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeScriptHash,
					Credential: scriptHash,
				},
				Amount: 2_000_000,
			},
		},
	}

	err := ValidateTxPlutusConway(
		tx,
		0,
		newMockLedgerState(),
		&conway.ConwayProtocolParameters{},
	)
	require.Error(t, err)
	var missing conway.MissingRedeemerForScriptError
	require.ErrorAs(t, err, &missing)
	assert.Equal(t, scriptHash, missing.ScriptHash)
	assert.Equal(t, lcommon.RedeemerTagCert, missing.Tag)
	assert.Equal(t, uint32(0), missing.Index)
}

func TestValidateTxPlutusConwayNonSpendMissingScriptWitnessFails(t *testing.T) {
	var scriptHash lcommon.ScriptHash
	scriptHash[0] = 0xaa

	scriptCred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: scriptHash,
	}
	withdrawalAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneScript,
		lcommon.AddressNetworkTestnet,
		nil,
		scriptHash.Bytes(),
	)
	require.NoError(t, err)
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeDRepScriptHash,
		Hash: [28]byte(scriptHash),
	}
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	proposal := conway.ConwayProposalProcedure{
		PPGovAction: conway.ConwayGovAction{
			Action: &lcommon.TreasuryWithdrawalGovAction{
				PolicyHash: scriptHash.Bytes(),
			},
		},
	}

	tests := []struct {
		name string
		tx   *mockConwayFeeTx
	}{
		{
			name: "mint",
			tx: &mockConwayFeeTx{
				assetMint: &assetMint,
			},
		},
		{
			name: "cert",
			tx: &mockConwayFeeTx{
				certificates: []lcommon.Certificate{
					&lcommon.RegistrationCertificate{
						StakeCredential: scriptCred,
						Amount:          2_000_000,
					},
				},
			},
		},
		{
			name: "withdraw",
			tx: &mockConwayFeeTx{
				withdrawals: map[*lcommon.Address]*big.Int{
					&withdrawalAddr: big.NewInt(1_000_000),
				},
			},
		},
		{
			name: "vote",
			tx: &mockConwayFeeTx{
				votingProcedures: lcommon.VotingProcedures{
					voter: {
						&lcommon.GovActionId{}: lcommon.VotingProcedure{
							Vote: lcommon.GovVoteYes,
						},
					},
				},
			},
		},
		{
			name: "propose",
			tx: &mockConwayFeeTx{
				proposalProcedures: []lcommon.ProposalProcedure{
					proposal,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.tx.mockFeeTx = mockFeeTx{
				txType:    txTypeAlonzo,
				witnesses: &mockWitnessSet{},
			}

			err := ValidateTxPlutusConway(
				tt.tx,
				0,
				newMockLedgerState(),
				&conway.ConwayProtocolParameters{},
			)
			require.Error(t, err)
			var missing lcommon.MissingScriptWitnessesError
			require.ErrorAs(t, err, &missing)
			assert.Equal(t, scriptHash, missing.ScriptHash)
		})
	}
}

func TestValidateTxPlutusConwayUnusedReferenceScriptWithoutRedeemerPasses(t *testing.T) {
	plutusScript := lcommon.PlutusV2Script([]byte{0x01, 0x02})
	scriptHash := plutusScript.Hash()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)

	refInput := newTestInput(0x02, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				redeemers: &mockRedeemers{},
			},
		},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(
		refInput,
		testAddressScriptOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       addr,
			scriptRef:  plutusScript,
		},
	)

	require.NoError(t, ValidateTxPlutusConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestTxInfoV2ContextSortsInputs guards the canonical Plutus requirement that
// the script context lists transaction inputs sorted by TxOutRef, not in
// transaction-body order. Building the context in body order (as the reverted
// txInfoV2WithTxInputOrder wrapper did) makes validators traverse a mis-ordered
// input list and over-compute the execution budget relative to cardano-node,
// producing phase-2 "Plutus evaluation disagrees with block producer" failures.
func TestTxInfoV2ContextSortsInputs(t *testing.T) {
	// Body order is intentionally unsorted: 0x02 before 0x01.
	bodyFirst := newTestInput(0x02, 0)
	bodySecond := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			fee:       big.NewInt(0),
			witnesses: &mockWitnessSet{},
		},
		inputs: []lcommon.TransactionInput{
			bodyFirst,
			bodySecond,
		},
	}
	ls := newMockLedgerState()
	ls.addUtxo(bodyFirst, newTestOutput(1_000_000))
	ls.addUtxo(bodySecond, newTestOutput(1_000_000))
	resolved := []lcommon.Utxo{
		{Id: bodyFirst, Output: newTestOutput(1_000_000)},
		{Id: bodySecond, Output: newTestOutput(1_000_000)},
	}

	txInfo, err := script.NewTxInfoV2FromTransaction(ls, tx, resolved)

	require.NoError(t, err)
	require.Len(t, txInfo.Inputs, 2)
	// Canonical order sorts by TxOutRef, so 0x01 must come before 0x02
	// regardless of the body order or resolved-input order.
	assert.Equal(
		t,
		bodySecond.String(),
		lcommon.Utxo(txInfo.Inputs[0]).Id.String(),
	)
	assert.Equal(
		t,
		bodyFirst.String(),
		lcommon.Utxo(txInfo.Inputs[1]).Id.String(),
	)
}

func TestBuildIndexedUtxoValidationRulesPanicsForStaleSkipIndex(t *testing.T) {
	require.PanicsWithValue(
		t,
		"test.UtxoValidatePlutusScripts hardcoded rule index 25 no longer resolves to the expected function",
		func() {
			buildIndexedUtxoValidationRules(
				alonzo.UtxoValidationRules,
				alonzoUtxoValidatePlutusScriptsRuleIndex-1,
				alonzo.UtxoValidatePlutusScripts,
				"test.UtxoValidatePlutusScripts",
			)
		},
	)
}

func requireRuleIndexResolvesToFunc(
	t *testing.T,
	rules []lcommon.UtxoValidationRuleFunc,
	index int,
	want lcommon.UtxoValidationRuleFunc,
	name string,
) {
	t.Helper()
	require.GreaterOrEqual(t, index, 0, "%s rule index must be non-negative", name)
	require.Less(t, index, len(rules), "%s rule index must be within upstream rules", name)
	require.Equal(
		t,
		utxoValidationRulePtr(want),
		utxoValidationRulePtr(rules[index]),
		"%s hardcoded rule index no longer resolves to the expected function",
		name,
	)
}

func requireIndexedRulesIncludeFunc(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	want lcommon.UtxoValidationRuleFunc,
	message string,
) {
	t.Helper()
	wantPtr := utxoValidationRulePtr(want)
	for _, rule := range rules {
		if utxoValidationRulePtr(rule.validationFunc) == wantPtr {
			return
		}
	}
	require.Fail(t, message)
}

func requireIndexedRulesExcludeFunc(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	want lcommon.UtxoValidationRuleFunc,
	message string,
) {
	t.Helper()
	wantPtr := utxoValidationRulePtr(want)
	for _, rule := range rules {
		require.NotEqual(t, wantPtr, utxoValidationRulePtr(rule.validationFunc), message)
	}
}

func TestTxSizeForFee(t *testing.T) {
	tests := []struct {
		name     string
		txType   int
		cbor     []byte
		expected uint64
	}{
		{
			name:     "empty cbor",
			cbor:     []byte{},
			expected: 0,
		},
		{
			// Pre-Alonzo: no IsValid byte, full size
			// returned unchanged.
			name:     "pre-alonzo full size",
			txType:   1, // Shelley
			cbor:     make([]byte, 256),
			expected: 256,
		},
		{
			// Alonzo+ 4-element TX: fee size excludes the
			// 1-byte IsValid boolean.
			name:     "alonzo subtracts isvalid byte",
			txType:   4, // Alonzo
			cbor:     make([]byte, 256),
			expected: 255,
		},
		{
			name:     "typical alonzo transaction",
			txType:   4,
			cbor:     make([]byte, 4096),
			expected: 4095,
		},
		{
			name:     "large alonzo transaction",
			txType:   4,
			cbor:     make([]byte, 16384),
			expected: 16383,
		},
		{
			// Mary (pre-Alonzo) TX: no subtraction.
			name:     "mary transaction full size",
			txType:   3, // Mary
			cbor:     make([]byte, 4096),
			expected: 4096,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTransaction{
				cbor:   tc.cbor,
				txType: tc.txType,
			}
			size := TxSizeForFee(tx)
			assert.Equal(t, tc.expected, size)
		})
	}
}

func TestTxSizeForFee_RebuiltConwayTxPreservesCanonicalSize(t *testing.T) {
	const issue1685TxCborHex = "84a500d901028282582004d97ebdeb064082639d67c8318ce069a35983bb05782d1327b004cca330ab5b008258204430e4bc2db0ef794c70b79851eecc332d8f77fb022c0d03ad24797f390ae54f000181825839005e7faca37d22d8753db699b104cbb2586f8787e17c116ff254ef0401e669129d1393c159b9b5a84d894271b5689910cc2e364ca05771988d1b0000000487a0103c021a0002d719031a0661906704d90102818a03581c7f4a5ac4b6a0f40cf07f989238d8e623315d80cc0602255b15c01eb3582025b400987b8e6d3f2d1913f7e7179611dc6563dc6731064de6b6dbe05114006e1b00000002540be4001a1908b100d81e82151901f4581de0e669129d1393c159b9b5a84d894271b5689910cc2e364ca05771988dd9010281581ce669129d1393c159b9b5a84d894271b5689910cc2e364ca05771988d818400190bb9444017f8d6f6827668747470733a2f2f6269742e6c792f34634e34374d31582086ed8edc5e20678c124d49dd1f6f6cb0b358797b71586f8a9db36bccf313f9eea100d9010283825820e61a0ef75ebcfba9569f2ef450d50320f376c36056f09f759d0e18ebf30a5ece5840c329a870e41de8e59b3ec872ec8d06f10e19c5dc436311e409827bf5792f86e75bb2c46785991563f42a03498c9c5342957efa15b348fffbd38f4fe64aef4f01825820942aaf02196ca16a79483b5862ff3d521e4c62c24dbc6aa495a360c101249de3584071ea7ed1740fbabe61f9c73f7306ef1ade9c2cf07a9d3c75d3ca130dd7e2078ea687cc326e7e790038580fdb3d9ec8e7e0edf70f5ff47527dd5ae0de6f5eca04825820eb2dbcf867f0611ca671a3ce89ae6c89a1a2eea96d6dcba82c607d4c9dbc489e5840f7e9a45d24cfbe8a7e7bc8200d84aa914cb51448873a41e0cf80aa641dd266490a0568b3039377fc5836d94320dc5c125f56352e0ad529f518035b4c2a313102f5f6"

	txCbor, err := hex.DecodeString(issue1685TxCborHex)
	require.NoError(t, err)

	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)

	rebuilt := &conway.ConwayTransaction{
		Body:       tx.Body,
		WitnessSet: tx.WitnessSet,
		TxIsValid:  tx.TxIsValid,
		TxMetadata: tx.TxMetadata,
	}

	// 699 is the canonical Conway transaction byte length produced by
	// NewConwayTransactionFromCbor for this fixture; TxSizeForFee must preserve
	// that exact serialized size when reconstructing the transaction for min-fee
	// accounting.
	assert.Equal(t, uint64(699), TxSizeForFee(rebuilt))
}

func TestValidateTxSize(t *testing.T) {
	tests := []struct {
		name      string
		txSize    int
		txType    int
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
			// Fee-relevant size = 16386 - 1 = 16385 > 16384
			name:      "one byte over limit",
			txSize:    16386,
			txType:    txTypeAlonzo,
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
			// Fee-relevant size = 2 - 1 = 1 > 0
			name:      "zero max size with non-zero tx",
			txSize:    2,
			txType:    txTypeAlonzo,
			maxSize:   0,
			expectErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockTransaction{
				cbor:   make([]byte, tc.txSize),
				txType: tc.txType,
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
			name: "both exceed limits returns memory error first",
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
			name:   "no scripts - nil prices",
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
			name:   "no scripts - zero exunits with prices set",
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
			// Single script with mainnet-like parameters:
			// minFeeA=44, minFeeB=155381
			// pricesMem=577/10000, pricesSteps=721/10000000
			// txSize=300, mem=1000000, steps=200000000
			// baseFee = 44*300+155381 = 168581
			// memFee = ceil(577*1000000/10000) = 57700
			// stepFee = ceil(721*200000000/10000000) = 14420
			// total = 168581 + 57700 + 14420 = 240701
			name:   "single script mainnet-like",
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
			// Multiple scripts - the exUnits represent the
			// sum of all script execution units.
			// Two scripts: script1(mem=500000, steps=100000000)
			//              script2(mem=500000, steps=100000000)
			// Total: mem=1000000, steps=200000000
			// Same as single script test above.
			name:   "multiple scripts summed exunits",
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
			// Three scripts with different costs summed:
			// script1(mem=300000, steps=50000000)
			// script2(mem=200000, steps=80000000)
			// script3(mem=100000, steps=70000000)
			// Total: mem=600000, steps=200000000
			// baseFee = 44*400 + 155381 = 172981
			// memFee = ceil(577*600000/10000) = ceil(34620) = 34620
			// stepFee = ceil(721*200000000/10000000) = 14420
			// total = 172981 + 34620 + 14420 = 222021
			name:   "three scripts summed",
			txSize: 400,
			exUnits: lcommon.ExUnits{
				Memory: 600000,
				Steps:  200000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    222021,
		},
		{
			// Ceiling behavior: single ceiling over sum.
			// Per Alonzo spec: scriptFee = ceil(prMem*mem + prSteps*steps)
			// pricesMem=1/3, mem=1 => 1/3
			// pricesSteps=1/3, steps=1 => 1/3
			// sum = 1/3 + 1/3 = 2/3
			// scriptFee = ceil(2/3) = 1
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
			expected:    1,
		},
		{
			// Exact division: no ceiling needed.
			// pricesMem=1/2, mem=4 => ceil(2) = 2
			// pricesSteps=1/4, steps=8 => ceil(2) = 2
			// baseFee = 10*100 + 500 = 1500
			// total = 1500 + 4 = 1504
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
		{
			name:   "zero minFeeB",
			txSize: 200,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			minFeeA:     44,
			minFeeB:     0,
			pricesMem:   nil,
			pricesSteps: nil,
			expected:    44 * 200,
		},
		{
			name:   "zero everything",
			txSize: 0,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			minFeeA:     0,
			minFeeB:     0,
			pricesMem:   nil,
			pricesSteps: nil,
			expected:    0,
		},
		{
			// Large ExUnits to test big number arithmetic.
			// mem=14000000 (14M), steps=10000000000 (10B)
			// pricesMem=577/10000
			//   memFee = ceil(577*14000000/10000) = 807800
			// pricesSteps=721/10000000
			//   stepFee = ceil(721*10000000000/10000000) = 721000
			// baseFee = 44*500 + 155381 = 177381
			// total = 177381 + 807800 + 721000 = 1706181
			name:   "large exunits",
			txSize: 500,
			exUnits: lcommon.ExUnits{
				Memory: 14000000,
				Steps:  10000000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    1706181,
		},
		{
			// Max ExUnits for mainnet (as of Conway era).
			// maxTxMem=14000000, maxTxSteps=10000000000
			// Same values as previous test.
			name:   "max exunits mainnet",
			txSize: 16384,
			exUnits: lcommon.ExUnits{
				Memory: 14000000,
				Steps:  10000000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			// baseFee = 44*16384 + 155381 = 720896 + 155381 = 876277
			// memFee = 807800
			// stepFee = 721000
			// total = 876277 + 807800 + 721000 = 2405077
			expected: 2405077,
		},
		{
			// Only memory exunits, zero steps.
			// memFee = ceil(577*1000000/10000) = 57700
			// stepFee = ceil(721*0/10000000) = 0
			// baseFee = 44*200 + 155381 = 164181
			// total = 164181 + 57700 = 221881
			name:   "memory only no steps",
			txSize: 200,
			exUnits: lcommon.ExUnits{
				Memory: 1000000,
				Steps:  0,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    221881,
		},
		{
			// Only step exunits, zero memory.
			// memFee = ceil(577*0/10000) = 0
			// stepFee = ceil(721*200000000/10000000) = 14420
			// baseFee = 44*200 + 155381 = 164181
			// total = 164181 + 14420 = 178601
			name:   "steps only no memory",
			txSize: 200,
			exUnits: lcommon.ExUnits{
				Memory: 0,
				Steps:  200000000,
			},
			minFeeA:     44,
			minFeeB:     155381,
			pricesMem:   big.NewRat(577, 10000),
			pricesSteps: big.NewRat(721, 10000000),
			expected:    178601,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fee := CalculateMinFee(
				tc.txSize,
				tc.exUnits,
				tc.minFeeA,
				tc.minFeeB,
				tc.pricesMem,
				tc.pricesSteps,
			)
			assert.Equal(
				t,
				tc.expected,
				fee,
				"fee mismatch",
			)
		})
	}
}

func TestCalculateMinFee_ScriptFeeAddsCorrectly(t *testing.T) {
	// Verify that a transaction with scripts costs more
	// than the same transaction without scripts.
	txSize := uint64(300)
	minFeeA := uint(44)
	minFeeB := uint(155381)
	pricesMem := big.NewRat(577, 10000)
	pricesSteps := big.NewRat(721, 10000000)

	// Fee with no scripts
	feeNoScripts := CalculateMinFee(
		txSize,
		lcommon.ExUnits{Memory: 0, Steps: 0},
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)

	// Fee with scripts
	feeWithScripts := CalculateMinFee(
		txSize,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)

	assert.Greater(
		t,
		feeWithScripts,
		feeNoScripts,
		"fee with scripts should be greater than base fee",
	)

	// The difference should equal the script execution fee
	scriptFee := feeWithScripts - feeNoScripts
	// memFee = ceil(577*1000000/10000) = 57700
	// stepFee = ceil(721*200000000/10000000) = 14420
	assert.Equal(
		t,
		uint64(72120),
		scriptFee,
		"script fee component mismatch",
	)
}

func TestCalculateMinFee_MultipleScriptsSum(t *testing.T) {
	// Verify that running N scripts with individual
	// ExUnits that sum to a total produces the same
	// fee as the total ExUnits directly.
	minFeeA := uint(44)
	minFeeB := uint(155381)
	pricesMem := big.NewRat(577, 10000)
	pricesSteps := big.NewRat(721, 10000000)
	txSize := uint64(400)

	// Three individual scripts
	scripts := []lcommon.ExUnits{
		{Memory: 300000, Steps: 50000000},
		{Memory: 200000, Steps: 80000000},
		{Memory: 100000, Steps: 70000000},
	}

	// Sum them up (simulating what EvaluateTx does)
	var totalExUnits lcommon.ExUnits
	for _, s := range scripts {
		totalExUnits.Memory += s.Memory
		totalExUnits.Steps += s.Steps
	}

	require.Equal(t, int64(600000), totalExUnits.Memory)
	require.Equal(t, int64(200000000), totalExUnits.Steps)

	fee := CalculateMinFee(
		txSize,
		totalExUnits,
		minFeeA,
		minFeeB,
		pricesMem,
		pricesSteps,
	)

	// baseFee = 44*400 + 155381 = 172981
	// memFee = ceil(577*600000/10000) = 34620
	// stepFee = ceil(721*200000000/10000000) = 14420
	// total = 172981 + 34620 + 14420 = 222021
	assert.Equal(t, uint64(222021), fee)
}

func TestCalculateConwayRefScriptFee_Tiered(t *testing.T) {
	fee := CalculateConwayRefScriptFee(
		30_000,
		big.NewRat(15, 1),
	)
	// First 25,600 bytes cost 15/byte. The remaining 4,400 bytes
	// cost 18/byte after Conway's 1.2 multiplier.
	assert.Equal(t, uint64(463_200), fee)
}

func TestCalculateConwayRefScriptFee_FloorsTieredTotal(t *testing.T) {
	fee := CalculateConwayRefScriptFee(
		25_601,
		big.NewRat(1, 3),
	)
	// floor(25,600 / 3 + 1 * 2 / 5) = floor(8,533.733...)
	assert.Equal(t, uint64(8_533), fee)
}

func TestValidateTxFeeConwayIncludesReferenceScripts(t *testing.T) {
	spendInput := newTestInput(0x01, 0)
	refInput := newTestInput(0x02, 1)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			cbor:   make([]byte, 101),
			txType: txTypeAlonzo,
			fee:    big.NewInt(450),
		},
		inputs:          []lcommon.TransactionInput{spendInput},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(
		spendInput,
		testScriptOutput{
			testOutput: newTestOutput(1_000_000),
			scriptRef:  lcommon.PlutusV2Script(make([]byte, 10)),
		},
	)
	ls.addUtxo(
		refInput,
		testScriptOutput{
			testOutput: newTestOutput(1_000_000),
			scriptRef:  lcommon.PlutusV3Script(make([]byte, 20)),
		},
	)
	pp := &conway.ConwayProtocolParameters{
		MinFeeRefScriptCostPerByte: &cbor.Rat{
			Rat: big.NewRat(15, 1),
		},
	}

	require.NoError(t, ValidateTxFeeConway(tx, ls, pp))

	tx.fee = big.NewInt(449)
	err := ValidateTxFeeConway(tx, ls, pp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "minimum fee 450")
}

func TestValidateTxFeeConwayDeduplicatesOverlappingReferenceScriptInput(
	t *testing.T,
) {
	spendInput := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			cbor:   make([]byte, 101),
			txType: txTypeAlonzo,
			fee:    big.NewInt(150),
		},
		inputs:          []lcommon.TransactionInput{spendInput},
		referenceInputs: []lcommon.TransactionInput{spendInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(
		spendInput,
		testScriptOutput{
			testOutput: newTestOutput(1_000_000),
			scriptRef:  lcommon.PlutusV2Script(make([]byte, 10)),
		},
	)
	pp := &conway.ConwayProtocolParameters{
		MinFeeRefScriptCostPerByte: &cbor.Rat{
			Rat: big.NewRat(15, 1),
		},
	}

	require.NoError(t, ValidateTxFeeConway(tx, ls, pp))

	tx.fee = big.NewInt(149)
	err := ValidateTxFeeConway(tx, ls, pp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "minimum fee 150")
}

func TestValidateTxConwaySkipPhase2StillValidatesRequiredRedeemers(
	t *testing.T,
) {
	withoutConwayUtxoValidationRules(t)

	plutusScript := lcommon.PlutusV2Script([]byte{0x01, 0x02})
	scriptHash := plutusScript.Hash()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		scriptHash.Bytes(),
		nil,
	)
	require.NoError(t, err)
	spendInput := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			fee:    big.NewInt(0),
		},
		inputs: []lcommon.TransactionInput{spendInput},
	}
	ls := newMockLedgerState()
	ls.skipPhase2Validation = true
	ls.addUtxo(
		spendInput,
		testAddressScriptOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       addr,
			scriptRef:  plutusScript,
		},
	)

	err = ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	)
	require.Error(t, err)
	var missing conway.MissingRedeemerForScriptError
	require.ErrorAs(t, err, &missing)
	assert.Equal(t, scriptHash, missing.ScriptHash)
}

func TestValidateTxConwaySkipPhase2StillValidatesRequiredScriptWitnesses(
	t *testing.T,
) {
	withoutConwayUtxoValidationRules(t)

	var scriptHash lcommon.ScriptHash
	scriptHash[0] = 0xaa
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			fee:    big.NewInt(0),
		},
		certificates: []lcommon.Certificate{
			&lcommon.RegistrationCertificate{
				StakeCredential: lcommon.Credential{
					CredType:   lcommon.CredentialTypeScriptHash,
					Credential: scriptHash,
				},
				Amount: 2_000_000,
			},
		},
	}
	ls := newMockLedgerState()
	ls.skipPhase2Validation = true

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	)
	require.Error(t, err)
	var missing lcommon.MissingScriptWitnessesError
	require.ErrorAs(t, err, &missing)
	assert.Equal(t, scriptHash, missing.ScriptHash)
}

func TestValidateTxConwayReusesResolvedPlutusContext(t *testing.T) {
	withoutConwayUtxoValidationRules(t)

	spendInput := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			fee:    big.NewInt(0),
		},
		inputs: []lcommon.TransactionInput{spendInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(spendInput, newTestOutput(1_000_000))

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Equal(t, 2, ls.utxoLookups)
}

func TestValidateTxConwayMissingInputReportsBadInputNotFeeResolution(
	t *testing.T,
) {
	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number: 258,
			Content: []any{
				[]any{inputHash, uint64(0)},
			},
		},
		2: uint64(200_000),
	}
	txCbor, err := cbor.Encode([]any{bodyMap, map[uint]any{}, true, nil})
	require.NoError(t, err)

	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)

	err = ValidateTxConway(
		tx,
		0,
		newMockLedgerState(),
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: conway.MinProtocolVersionConway,
			},
			MaxTxSize:            16_384,
			MaxValueSize:         5_000,
			CollateralPercentage: 150,
			MaxCollateralInputs:  3,
		},
	)
	require.Error(t, err)

	var badInputs shelley.BadInputsUtxoError
	require.ErrorAs(t, err, &badInputs)
	require.Len(t, badInputs.Inputs, 1)
	assert.Equal(t, tx.Inputs()[0].String(), badInputs.Inputs[0].String())
	assert.NotContains(t, err.Error(), "conway fee validation")
	assert.NotContains(t, err.Error(), "calculating reference script size")
}

func withoutConwayUtxoValidationRules(t *testing.T) {
	t.Helper()

	origRules := conwayUtxoValidationRules
	origPhase1Rules := conwayPhase1UtxoValidationRules
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil
	t.Cleanup(func() {
		conwayUtxoValidationRules = origRules
		conwayPhase1UtxoValidationRules = origPhase1Rules
	})
}

func TestCalculateMinFee_NilPricesIgnoresExUnits(t *testing.T) {
	// When prices are nil, even non-zero ExUnits should
	// not contribute to the fee. This can happen in
	// pre-Alonzo eras where there are no execution costs.
	fee := CalculateMinFee(
		200,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		nil,
		nil,
	)
	baseFee := uint64(44*200 + 155381)
	assert.Equal(
		t,
		baseFee,
		fee,
		"nil prices should result in base fee only",
	)
}

func TestCalculateMinFee_OnePriceNilIgnoresExUnits(t *testing.T) {
	// When only one price is nil, both should be
	// ignored (the function requires both to be non-nil).
	fee1 := CalculateMinFee(
		200,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		big.NewRat(577, 10000),
		nil,
	)
	fee2 := CalculateMinFee(
		200,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		nil,
		big.NewRat(721, 10000000),
	)
	baseFee := uint64(44*200 + 155381)
	assert.Equal(t, baseFee, fee1,
		"nil step price should ignore script fees",
	)
	assert.Equal(t, baseFee, fee2,
		"nil mem price should ignore script fees",
	)
}

func TestDeclaredExUnits(t *testing.T) {
	tests := []struct {
		name      string
		tx        lcommon.Transaction
		expected  lcommon.ExUnits
		expectErr bool
	}{
		{
			name: "no witnesses",
			tx: &mockFeeTx{
				cbor:      make([]byte, 100),
				fee:       big.NewInt(200000),
				witnesses: nil,
			},
			expected: lcommon.ExUnits{},
		},
		{
			name: "no redeemers",
			tx: &mockFeeTx{
				cbor: make([]byte, 100),
				fee:  big.NewInt(200000),
				witnesses: &mockWitnessSet{
					redeemers: nil,
				},
			},
			expected: lcommon.ExUnits{},
		},
		{
			name: "single redeemer",
			tx: &mockFeeTx{
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
										Memory: 500000,
										Steps:  100000000,
									},
								},
							},
						},
					},
				},
			},
			expected: lcommon.ExUnits{
				Memory: 500000,
				Steps:  100000000,
			},
		},
		{
			name: "multiple redeemers",
			tx: &mockFeeTx{
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
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 100000,
										Steps:  70000000,
									},
								},
							},
						},
					},
				},
			},
			expected: lcommon.ExUnits{
				Memory: 600000,
				Steps:  200000000,
			},
		},
		{
			name: "memory overflow",
			tx: &mockFeeTx{
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
			},
			expectErr: true,
		},
		{
			name: "steps overflow",
			tx: &mockFeeTx{
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
										Memory: 100,
										Steps:  math.MaxInt64,
									},
								},
							},
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 100,
										Steps:  1,
									},
								},
							},
						},
					},
				},
			},
			expectErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := DeclaredExUnits(tc.tx)
			if tc.expectErr {
				require.Error(t, err)
				assert.ErrorIs(
					t,
					err,
					ErrExUnitsOverflow,
				)
				return
			}
			require.NoError(t, err)
			assert.Equal(
				t,
				tc.expected.Memory,
				result.Memory,
			)
			assert.Equal(
				t,
				tc.expected.Steps,
				result.Steps,
			)
		})
	}
}

func TestSafeAddExUnits(t *testing.T) {
	tests := []struct {
		name      string
		a         lcommon.ExUnits
		b         lcommon.ExUnits
		expected  lcommon.ExUnits
		expectErr bool
	}{
		{
			name: "normal addition",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  200,
			},
			b: lcommon.ExUnits{
				Memory: 300,
				Steps:  400,
			},
			expected: lcommon.ExUnits{
				Memory: 400,
				Steps:  600,
			},
		},
		{
			name: "zero plus values",
			a:    lcommon.ExUnits{},
			b: lcommon.ExUnits{
				Memory: 500,
				Steps:  1000,
			},
			expected: lcommon.ExUnits{
				Memory: 500,
				Steps:  1000,
			},
		},
		{
			name: "both zero",
			a:    lcommon.ExUnits{},
			b:    lcommon.ExUnits{},
			expected: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
		},
		{
			name: "max int64 values no overflow",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
			b: lcommon.ExUnits{
				Memory: 0,
				Steps:  0,
			},
			expected: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
		},
		{
			name: "memory overflow",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: 1,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "steps overflow",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  math.MaxInt64,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  1,
			},
			expectErr: true,
		},
		{
			name: "both overflow",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
			b: lcommon.ExUnits{
				Memory: 1,
				Steps:  1,
			},
			expectErr: true,
		},
		{
			name: "large values just under max",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64 - 1,
				Steps:  math.MaxInt64 - 1,
			},
			b: lcommon.ExUnits{
				Memory: 1,
				Steps:  1,
			},
			expected: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  math.MaxInt64,
			},
		},
		{
			name: "half max values",
			a: lcommon.ExUnits{
				Memory: math.MaxInt64 / 2,
				Steps:  math.MaxInt64 / 2,
			},
			b: lcommon.ExUnits{
				Memory: math.MaxInt64 / 2,
				Steps:  math.MaxInt64 / 2,
			},
			expected: lcommon.ExUnits{
				Memory: (math.MaxInt64 / 2) * 2,
				Steps:  (math.MaxInt64 / 2) * 2,
			},
		},
		{
			name: "negative memory in a",
			a: lcommon.ExUnits{
				Memory: -1,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "negative memory in b",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: -1,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "negative steps in a",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  -1,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			expectErr: true,
		},
		{
			name: "negative steps in b",
			a: lcommon.ExUnits{
				Memory: 100,
				Steps:  100,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  -1,
			},
			expectErr: true,
		},
		{
			name: "large negative memory wraparound attempt",
			a: lcommon.ExUnits{
				Memory: math.MinInt64,
				Steps:  0,
			},
			b: lcommon.ExUnits{
				Memory: math.MaxInt64,
				Steps:  0,
			},
			expectErr: true,
		},
		{
			name: "large negative steps wraparound attempt",
			a: lcommon.ExUnits{
				Memory: 0,
				Steps:  math.MinInt64,
			},
			b: lcommon.ExUnits{
				Memory: 0,
				Steps:  math.MaxInt64,
			},
			expectErr: true,
		},
		{
			name: "both negative memory and steps",
			a: lcommon.ExUnits{
				Memory: -100,
				Steps:  -200,
			},
			b: lcommon.ExUnits{
				Memory: 100,
				Steps:  200,
			},
			expectErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := SafeAddExUnits(tc.a, tc.b)
			if tc.expectErr {
				require.Error(t, err)
				assert.ErrorIs(
					t,
					err,
					ErrExUnitsOverflow,
				)
				return
			}
			require.NoError(t, err)
			assert.Equal(
				t,
				tc.expected.Memory,
				result.Memory,
			)
			assert.Equal(
				t,
				tc.expected.Steps,
				result.Steps,
			)
		})
	}
}

func TestValidateTxFee(t *testing.T) {
	pricesMem := big.NewRat(577, 10000)
	pricesSteps := big.NewRat(721, 10000000)

	tests := []struct {
		name      string
		tx        lcommon.Transaction
		minFeeA   uint
		minFeeB   uint
		pMem      *big.Rat
		pSteps    *big.Rat
		expectErr bool
	}{
		{
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 200000 >= 168581 => valid
			name: "sufficient fee no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
				fee:       big.NewInt(200000),
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 168581 (exact) => valid
			name: "exact minimum fee no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
				fee:       big.NewInt(168581),
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// Fee = 168580 < 168581 => invalid
			name: "one lovelace under minimum no scripts",
			tx: &mockFeeTx{
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
				fee:       big.NewInt(168580),
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: true,
		},
		{
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			// scriptFee = ceil(577/10000*1000000 +
			//   721/10000000*200000000) = 72120
			// minFee = 168581 + 72120 = 240701
			// Fee = 240701 (exact) => valid
			name: "exact minimum fee with scripts",
			tx: &mockFeeTx{
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(240701),
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
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// Fee-relevant size = 301 - 1 = 300
			// minFee = 44*300 + 155381 + 72120 = 240701
			// Fee = 240700 < 240701 => invalid
			name: "one lovelace under minimum with scripts",
			tx: &mockFeeTx{
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(240700),
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
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: true,
		},
		{
			// Overpaying is fine
			name: "overpaying fee with scripts",
			tx: &mockFeeTx{
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(500000),
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
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// Fee-relevant size = 401 - 1 = 400
			// baseFee = 44*400 + 155381 = 172981
			// scriptFee = ceil(577/10000*600000 +
			//   721/10000000*200000000) = 49040
			// minFee = 172981 + 49040 = 222021
			name: "multiple redeemers exact fee",
			tx: &mockFeeTx{
				cbor:   make([]byte, 401),
				txType: txTypeAlonzo,
				fee:    big.NewInt(222021),
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
							{
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 100000,
										Steps:  70000000,
									},
								},
							},
						},
					},
				},
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: false,
		},
		{
			// nil fee defaults to 0, should fail
			name: "nil fee fails",
			tx: &mockFeeTx{
				cbor:      make([]byte, 301),
				txType:    txTypeAlonzo,
				fee:       nil,
				witnesses: nil,
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      pricesMem,
			pSteps:    pricesSteps,
			expectErr: true,
		},
		{
			// nil prices: no script fee component
			// Fee-relevant size = 301 - 1 = 300
			// baseFee = 44*300 + 155381 = 168581
			name: "nil prices no script fee",
			tx: &mockFeeTx{
				cbor:   make([]byte, 301),
				txType: txTypeAlonzo,
				fee:    big.NewInt(168581),
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
			},
			minFeeA:   44,
			minFeeB:   155381,
			pMem:      nil,
			pSteps:    nil,
			expectErr: false,
		},
		{
			// zero everything: minFee = 0, fee = 0 => valid
			name: "zero everything",
			tx: &mockFeeTx{
				cbor:      []byte{},
				fee:       big.NewInt(0),
				witnesses: nil,
			},
			minFeeA:   0,
			minFeeB:   0,
			pMem:      nil,
			pSteps:    nil,
			expectErr: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTxFee(
				tc.tx,
				tc.minFeeA,
				tc.minFeeB,
				tc.pMem,
				tc.pSteps,
			)
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					"less than the calculated minimum fee",
				)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateTxFee_ErrorMessageIncludesFees(
	t *testing.T,
) {
	// Verify the error message contains both the
	// provided fee and the calculated minimum.
	tx := &mockFeeTx{
		cbor:      make([]byte, 301),
		txType:    txTypeAlonzo,
		fee:       big.NewInt(100000),
		witnesses: nil,
	}
	err := ValidateTxFee(
		tx,
		44,
		155381,
		big.NewRat(577, 10000),
		big.NewRat(721, 10000000),
	)
	require.Error(t, err)
	// Should mention both the provided fee and the min.
	// Fee-relevant TX size = 301 - 1 = 300 (excludes IsValid byte).
	// minFee = 44*300 + 155381 = 168581
	assert.Contains(t, err.Error(), "100000")
	assert.Contains(t, err.Error(), "168581")
}

func TestCalculateMinFee_NormalValuesNoOverflow(
	t *testing.T,
) {
	// Mainnet-like parameters should still work.
	fee := CalculateMinFee(
		300,
		lcommon.ExUnits{
			Memory: 1000000,
			Steps:  200000000,
		},
		44,
		155381,
		big.NewRat(577, 10000),
		big.NewRat(721, 10000000),
	)
	assert.Equal(t, uint64(240701), fee)
}

func TestCalculateMinFee_OverflowSaturates(t *testing.T) {
	// Force an overflow: huge num/denom ratio with large ExUnits.
	fee := CalculateMinFee(
		math.MaxUint64,
		lcommon.ExUnits{
			Memory: math.MaxInt64,
			Steps:  math.MaxInt64,
		},
		44,
		155381,
		big.NewRat(math.MaxInt64, 1),
		big.NewRat(math.MaxInt64, 1),
	)
	assert.Equal(t, uint64(math.MaxUint64), fee,
		"overflow should saturate at MaxUint64",
	)
}

// --- CIP-23 pool-margin-floor certificate rule ---

func cip23PoolCert(num, den int64) *lcommon.PoolRegistrationCertificate {
	return &lcommon.PoolRegistrationCertificate{
		Margin: lcommon.GenesisRat{Rat: big.NewRat(num, den)},
	}
}

func TestCheckPoolMarginFloor(t *testing.T) {
	floor := big.NewRat(150, 10_000) // 1.5%

	// nil floor: no-op even for a zero-margin cert.
	require.NoError(t, checkPoolMarginFloor(
		[]lcommon.Certificate{cip23PoolCert(0, 1)}, nil))

	// below floor: rejected.
	require.Error(t, checkPoolMarginFloor(
		[]lcommon.Certificate{cip23PoolCert(1, 1000)}, floor)) // 0.1%

	// at floor: accepted.
	require.NoError(t, checkPoolMarginFloor(
		[]lcommon.Certificate{cip23PoolCert(150, 10_000)}, floor))

	// above floor: accepted.
	require.NoError(t, checkPoolMarginFloor(
		[]lcommon.Certificate{cip23PoolCert(5, 100)}, floor)) // 5%

	// nil cert margin treated as 0: rejected under a nonzero floor.
	require.Error(t, checkPoolMarginFloor(
		[]lcommon.Certificate{&lcommon.PoolRegistrationCertificate{}}, floor))

	// non-pool-registration cert ignored.
	require.NoError(t, checkPoolMarginFloor(
		[]lcommon.Certificate{&lcommon.StakeRegistrationCertificate{}}, floor))

	// multiple certs: one below floor rejects the whole set.
	require.Error(t, checkPoolMarginFloor(
		[]lcommon.Certificate{cip23PoolCert(5, 100), cip23PoolCert(1, 1000)}, floor))

	// empty cert set: accepted.
	require.NoError(t, checkPoolMarginFloor(nil, floor))

	// typed-nil *PoolRegistrationCertificate element: must not panic, treated
	// as ignorable (no error) under any floor, including nil.
	var nilReg *lcommon.PoolRegistrationCertificate
	require.NotPanics(t, func() {
		require.NoError(t, checkPoolMarginFloor(
			[]lcommon.Certificate{nilReg}, floor))
	})
	require.NotPanics(t, func() {
		require.NoError(t, checkPoolMarginFloor(
			[]lcommon.Certificate{nilReg}, nil))
	})
}
