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
	"errors"
	"iter"
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
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
	outputs            []lcommon.TransactionOutput
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
	return m.outputs
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

// newTestKeyAddress returns a testnet payment address with a key credential and
// no staking part, so no script purpose ever resolves to it.
func newTestKeyAddress(t *testing.T) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		make([]byte, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	return addr
}

// newTestScriptAddress returns the testnet payment address that locks a UTxO
// with the given script, so spending it creates a script purpose needing s.
func newTestScriptAddress(t *testing.T, s lcommon.Script) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeScriptNone,
		lcommon.AddressNetworkTestnet,
		s.Hash().Bytes(),
		nil,
	)
	require.NoError(t, err)
	return addr
}

// newTestScriptStakeAddress returns the testnet reward address whose stake
// credential is the given script, so withdrawing from it creates a rewarding
// purpose needing s.
func newTestScriptStakeAddress(t *testing.T, s lcommon.Script) lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneScript,
		lcommon.AddressNetworkTestnet,
		nil,
		s.Hash().Bytes(),
	)
	require.NoError(t, err)
	return addr
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
	valueOverride *lcommon.RedeemerValue
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
	if m.valueOverride != nil {
		return *m.valueOverride
	}
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
	descriptors := alonzo.UtxoValidationRuleDescriptors()
	plutusIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		alonzo.UtxoValidationRules,
		lcommon.UtxoValidationRulePlutusScripts,
		"alonzo.UtxoValidatePlutusScripts",
	)
	require.Len(t, alonzoUtxoValidationRules, len(alonzo.UtxoValidationRules)-1)
	requireIndexedRulesDropRuleIndex(
		t,
		alonzoUtxoValidationRules,
		plutusIndex,
		"Alonzo validation must use Dingo's local Plutus execution path",
	)
}

func TestBabbageValidationRulesUseLocalPlutusExecution(t *testing.T) {
	descriptors := babbage.UtxoValidationRuleDescriptors()
	plutusIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		babbage.UtxoValidationRules,
		lcommon.UtxoValidationRulePlutusScripts,
		"babbage.UtxoValidatePlutusScripts",
	)
	require.Len(
		t,
		babbageUtxoValidationRules,
		len(babbage.UtxoValidationRules)-1,
	)
	requireIndexedRulesDropRuleIndex(
		t,
		babbageUtxoValidationRules,
		plutusIndex,
		"Babbage validation must use Dingo's local Plutus execution path",
	)
}

func TestPlutusBudgetComparisonIncludesFinalSlippageBatch(t *testing.T) {
	// A zero declared budget is intentional: restrictive validation should
	// execute this script with the protocol transaction budget and classify the
	// resulting overage as a Plutus disagreement. The script is small enough
	// that its CEK steps remain in the trailing slippage batch. Haskell flushes
	// that batch on a successful return, producing the complete 112100 CPU / 800
	// memory cost.
	program := &syn.Program[syn.DeBruijn]{
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

	spendInput := newTestInput(0x01, 0)

	tests := []struct {
		name     string
		validate func(lcommon.Transaction, lcommon.LedgerState, lcommon.ExUnits) error
		reset    func()
	}{
		{
			name: "alonzo",
			validate: func(tx lcommon.Transaction, ls lcommon.LedgerState, maxTxExUnits lcommon.ExUnits) error {
				return ValidateTxAlonzo(
					tx,
					0,
					ls,
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 5,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
			},
			reset: func() {
				alonzoUtxoValidationRules = nil
			},
		},
		{
			name: "babbage",
			validate: func(tx lcommon.Transaction, ls lcommon.LedgerState, maxTxExUnits lcommon.ExUnits) error {
				return ValidateTxBabbage(
					tx,
					0,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
			},
			reset: func() {
				babbageUtxoValidationRules = nil
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "alonzo" {
				origRules := alonzoUtxoValidationRules
				t.Cleanup(func() { alonzoUtxoValidationRules = origRules })
			} else {
				origRules := babbageUtxoValidationRules
				t.Cleanup(func() { babbageUtxoValidationRules = origRules })
			}
			tc.reset()

			var plutusScript lcommon.Script
			witnesses := &mockWitnessSet{
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
							val: lcommon.RedeemerValue{
								ExUnits: lcommon.ExUnits{},
							},
						},
					},
				},
			}
			if tc.name == "alonzo" {
				v1Script := lcommon.PlutusV1Script(scriptBytes)
				plutusScript = v1Script
				witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{v1Script}
			} else {
				v2Script := lcommon.PlutusV2Script(scriptBytes)
				plutusScript = v2Script
				witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{v2Script}
			}
			scriptHash := plutusScript.Hash()
			addr, err := lcommon.NewAddressFromParts(
				lcommon.AddressTypeScriptNone,
				lcommon.AddressNetworkTestnet,
				scriptHash.Bytes(),
				nil,
			)
			require.NoError(t, err)
			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					txType:    txTypeAlonzo,
					witnesses: witnesses,
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
			err = tc.validate(tx, ls, lcommon.ExUnits{
				Steps:  1_000_000,
				Memory: 1_000_000,
			})
			require.Error(t, err)

			var plutusErr conway.PlutusScriptFailedError
			require.ErrorAs(t, err, &plutusErr)
			assert.Equal(t, scriptHash, plutusErr.ScriptHash)
			assert.Equal(t, lcommon.RedeemerTagSpend, plutusErr.Tag)
			assert.Equal(t, uint32(0), plutusErr.Index)
			assert.Contains(
				t,
				plutusErr.Err.Error(),
				"script exceeded declared budget: used (112100 cpu, 800 mem)",
			)

			t.Run(
				"restrictive evaluation is capped by protocol transaction budget",
				func(t *testing.T) {
					err := tc.validate(tx, ls, lcommon.ExUnits{
						Steps:  1_000,
						Memory: 100,
					})
					require.Error(t, err)
					assert.Contains(t, err.Error(), "out of budget")
				},
			)

			t.Run(
				"valid execution remains accepted within both budgets",
				func(t *testing.T) {
					value := lcommon.RedeemerValue{ExUnits: lcommon.ExUnits{
						Steps:  112_100,
						Memory: 800,
					}}
					witnesses.redeemers.(*mockRedeemers).valueOverride = &value
					require.NoError(t, tc.validate(tx, ls, lcommon.ExUnits{
						Steps:  1_000_000,
						Memory: 1_000_000,
					}))
				},
			)
		})
	}
}

func TestConwayValidationRulesUseLocalPlutusExecution(t *testing.T) {
	descriptors := conway.UtxoValidationRuleDescriptors()
	featuresIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleConwayFeaturesWithPlutusV1V2,
		"conway.UtxoValidateConwayFeaturesWithPlutusV1V2",
	)
	feeIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleFeeTooSmall,
		"conway.UtxoValidateFeeTooSmallUtxo",
	)
	plutusIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRulePlutusScripts,
		"conway.UtxoValidatePlutusScripts",
	)
	committeeIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleCommitteeCertificates,
		"conway.UtxoValidateCommitteeCertificates",
	)
	votersIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleUnknownVoters,
		"conway.UtxoValidateUnknownVoters",
	)
	require.Len(t, conwayUtxoValidationRules, len(conway.UtxoValidationRules)-2)
	requireIndexedRulesReplaceRuleIndex(
		t,
		conwayUtxoValidationRules,
		featuresIndex,
		validateConwayFeaturesWithNeededPlutusV1V2,
		"Conway validation must count only needed PlutusV1/V2 scripts",
	)
	requireIndexedRulesDropRuleIndex(
		t,
		conwayUtxoValidationRules,
		feeIndex,
		"Conway validation must use Dingo's reference-script-aware fee rule",
	)
	requireIndexedRulesDropRuleIndex(
		t,
		conwayUtxoValidationRules,
		plutusIndex,
		"Conway validation must use Dingo's local Plutus execution path",
	)
	requireIndexedRulesReplaceRuleIndex(
		t,
		conwayUtxoValidationRules,
		committeeIndex,
		validateCommitteeCertificates,
		"Conway validation must preserve committee cold credential tags",
	)
	requireIndexedRulesReplaceRuleIndex(
		t,
		conwayUtxoValidationRules,
		votersIndex,
		validateUnknownVoters,
		"Conway validation must preserve committee hot credential tags",
	)
}

func TestDijkstraValidationRulesUseCredentialAwareCommitteeState(t *testing.T) {
	descriptors := gdijkstra.UtxoValidationRuleDescriptors()
	committeeIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		gdijkstra.UtxoValidationRules,
		lcommon.UtxoValidationRuleCommitteeCertificates,
		"conway.UtxoValidateCommitteeCertificates",
	)
	votersIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		gdijkstra.UtxoValidationRules,
		lcommon.UtxoValidationRuleUnknownVoters,
		"conway.UtxoValidateUnknownVoters",
	)
	require.Len(
		t,
		dijkstraPhase1UtxoValidationRules,
		len(gdijkstra.UtxoValidationRules)-1,
	)
	requireIndexedRulesReplaceRuleIndex(
		t,
		dijkstraPhase1UtxoValidationRules,
		committeeIndex,
		validateCommitteeCertificates,
		"Dijkstra validation must preserve committee cold credential tags",
	)
	requireIndexedRulesReplaceRuleIndex(
		t,
		dijkstraPhase1UtxoValidationRules,
		votersIndex,
		validateUnknownVoters,
		"Dijkstra validation must preserve committee hot credential tags",
	)
}

type taggedCommitteeLedgerState struct {
	*mockLedgerState
	available    bool
	availableErr error
	cold         map[string]*lcommon.CommitteeMember
	hot          map[string]*lcommon.CommitteeMember
}

func (s *taggedCommitteeLedgerState) CommitteeStateAvailable() (bool, error) {
	return s.available, s.availableErr
}

func (s *taggedCommitteeLedgerState) CommitteeCredentialMember(
	credential lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	return s.cold[taggedCommitteeCredentialKey(credential)], nil
}

func (s *taggedCommitteeLedgerState) CommitteeHotCredentialMember(
	credential lcommon.Credential,
) (*lcommon.CommitteeMember, error) {
	return s.hot[taggedCommitteeCredentialKey(credential)], nil
}

func taggedCommitteeCredentialKey(credential lcommon.Credential) string {
	return string(append(
		[]byte{byte(credential.CredType)},
		credential.Credential[:]...,
	))
}

// findIndexedUtxoValidationRule returns the composed rule whose upstream
// function name matches want, and names it when absent.
func findIndexedUtxoValidationRule(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	want lcommon.UtxoValidationRuleFunc,
) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	wantName := utxoValidationRuleName(want)
	for _, candidate := range rules {
		if utxoValidationRuleName(candidate.validationFunc) == wantName {
			return candidate.validationFunc
		}
	}
	t.Fatalf("validation rule %s is not registered", wantName)
	return nil
}

func TestConwayCommitteeCertificateRulePreservesCredentialTag(t *testing.T) {
	var hash lcommon.Blake2b224
	hash[0] = 0xc1
	keyCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	scriptCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: hash,
	}
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       true,
		cold: map[string]*lcommon.CommitteeMember{
			taggedCommitteeCredentialKey(keyCredential): {ColdKey: hash},
		},
	}
	tx := &conway.ConwayTransaction{
		// Committee certificates are only inspected for a phase-2-valid
		// transaction, so the fixture must declare validity.
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{{
				Type: uint(lcommon.CertificateTypeAuthCommitteeHot),
				Certificate: &lcommon.AuthCommitteeHotCertificate{
					CertType: uint(
						lcommon.CertificateTypeAuthCommitteeHot,
					),
					ColdCredential: scriptCredential,
				},
			}},
		},
	}

	rule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateCommitteeCertificates,
	)
	err := rule(tx, 0, state, &conway.ConwayProtocolParameters{})
	var notMember conway.NotCommitteeMemberError
	require.ErrorAs(t, err, &notMember)
}

func TestConwayUnknownVoterRulePreservesCredentialTag(t *testing.T) {
	var hash lcommon.Blake2b224
	hash[0] = 0xc2
	keyCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       true,
		hot: map[string]*lcommon.CommitteeMember{
			taggedCommitteeCredentialKey(keyCredential): {ColdKey: hash},
		},
	}
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotScriptHash,
		Hash: [28]byte(hash),
	}
	tx := &conway.ConwayTransaction{
		// Votes are only inspected for a phase-2-valid transaction.
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{
				voter: {},
			},
		},
	}

	rule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateUnknownVoters,
	)
	err := rule(tx, 0, state, &conway.ConwayProtocolParameters{})
	var unknown conway.UnknownVoterError
	require.ErrorAs(t, err, &unknown)
}

func TestConwayPhase1ValidationRulesSkipPlutusExecution(t *testing.T) {
	descriptors := conway.UtxoValidationRuleDescriptors()
	feeIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleFeeTooSmall,
		"conway.UtxoValidateFeeTooSmallUtxo",
	)
	exUnitsIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleExUnitsTooBig,
		"conway.UtxoValidateExUnitsTooBigUtxo",
	)
	plutusIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRulePlutusScripts,
		"conway.UtxoValidatePlutusScripts",
	)
	require.Len(
		t,
		conwayPhase1UtxoValidationRules,
		len(conway.UtxoValidationRules)-2,
	)
	requireIndexedRulesDropRuleIndex(
		t,
		conwayPhase1UtxoValidationRules,
		feeIndex,
		"Conway phase-1 validation must use Dingo's reference-script-aware fee rule",
	)
	requireIndexedRulesRetainRuleIndex(
		t,
		conwayPhase1UtxoValidationRules,
		conway.UtxoValidationRules,
		exUnitsIndex,
		"Conway phase-1 replay must still enforce ExUnits limits",
	)
	requireIndexedRulesDropRuleIndex(
		t,
		conwayPhase1UtxoValidationRules,
		plutusIndex,
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

func TestValidateTxPlutusConwayMissingScriptWitnessWithoutRedeemerFails(
	t *testing.T,
) {
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

func TestValidateTxPlutusConwayWithdrawalRedeemerUsesStakeCredential(
	t *testing.T,
) {
	plutusScript := lcommon.PlutusV2Script([]byte{0x01, 0x02})
	scriptHash := plutusScript.Hash()
	keyHash := make([]byte, lcommon.AddressHashSize)
	keyHash[0] = 0xaa

	newAddress := func(
		t *testing.T,
		addrType uint8,
		paymentAddr []byte,
		stakingAddr []byte,
	) *lcommon.Address {
		t.Helper()
		addr, err := lcommon.NewAddressFromParts(
			addrType,
			lcommon.AddressNetworkTestnet,
			paymentAddr,
			stakingAddr,
		)
		require.NoError(t, err)
		return &addr
	}

	rewardScript := newAddress(
		t,
		lcommon.AddressTypeNoneScript,
		nil,
		scriptHash.Bytes(),
	)
	baseKeyScript := newAddress(
		t,
		lcommon.AddressTypeKeyScript,
		keyHash,
		scriptHash.Bytes(),
	)
	baseScriptKey := newAddress(
		t,
		lcommon.AddressTypeScriptKey,
		scriptHash.Bytes(),
		keyHash,
	)
	enterprise := newAddress(
		t,
		lcommon.AddressTypeKeyNone,
		keyHash,
		nil,
	)
	keyBase := newAddress(
		t,
		lcommon.AddressTypeKeyKey,
		keyHash,
		keyHash,
	)
	malformed := &lcommon.Address{}

	validate := func(
		t *testing.T,
		withdrawals map[*lcommon.Address]*big.Int,
	) error {
		t.Helper()
		tx := &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType: txTypeAlonzo,
				witnesses: &mockWitnessSet{
					plutusV2Scripts: []lcommon.PlutusV2Script{plutusScript},
				},
			},
			withdrawals: withdrawals,
		}
		return ValidateTxPlutusConway(
			tx,
			0,
			newMockLedgerState(),
			&conway.ConwayProtocolParameters{},
		)
	}

	t.Run(
		"script stake credentials require a reward redeemer",
		func(t *testing.T) {
			tests := []struct {
				name string
				addr *lcommon.Address
			}{
				{name: "reward address", addr: rewardScript},
				{name: "base address", addr: baseKeyScript},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					err := validate(t, map[*lcommon.Address]*big.Int{
						tt.addr: big.NewInt(1),
					})
					var missing conway.MissingRedeemerForScriptError
					require.ErrorAs(t, err, &missing)
					assert.Equal(t, scriptHash, missing.ScriptHash)
					assert.Equal(t, lcommon.RedeemerTagReward, missing.Tag)
					assert.Equal(t, uint32(0), missing.Index)
				})
			}
		},
	)

	t.Run(
		"key and absent stake credentials do not require a redeemer",
		func(t *testing.T) {
			for name, addr := range map[string]*lcommon.Address{
				"script payment and key stake": baseScriptKey,
				"enterprise address":           enterprise,
				"malformed address":            malformed,
			} {
				t.Run(name, func(t *testing.T) {
					require.NoError(
						t,
						validate(t, map[*lcommon.Address]*big.Int{
							addr: big.NewInt(1),
						}),
					)
				})
			}
		},
	)

	t.Run("redeemer indexes retain withdrawal ordering", func(t *testing.T) {
		err := validate(t, map[*lcommon.Address]*big.Int{
			keyBase:       big.NewInt(1),
			baseKeyScript: big.NewInt(1),
			rewardScript:  big.NewInt(1),
		})
		var missing conway.MissingRedeemerForScriptError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, scriptHash, missing.ScriptHash)
		assert.Equal(t, lcommon.RedeemerTagReward, missing.Tag)
		assert.Equal(t, uint32(1), missing.Index)
	})
}

func TestValidateTxPlutusConwayNativeScriptWitnessWithoutRedeemerPasses(
	t *testing.T,
) {
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

func TestValidateTxPlutusConwayRegistrationCertificateMissingRedeemerFails(
	t *testing.T,
) {
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

func TestValidateTxPlutusConwayUnusedReferenceScriptWithoutRedeemerPasses(
	t *testing.T,
) {
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

	txInfo, err := script.NewTxInfoV2FromTransaction(
		ls,
		tx,
		resolved,
		script.StrictValidityUpperBoundForTransaction(tx),
	)

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

// TestBuildIndexedUtxoValidationRulesResolvesByRuleId moves the plutus-scripts
// rule to the front of the upstream list and gates it behind
// common.Phase2ValidUtxoValidationRules, so the skip is only resolvable
// through the descriptor Id: a fixed index points at the wrong rule and a
// function-keyed lookup cannot see past the wrapper.
func TestBuildIndexedUtxoValidationRulesResolvesByRuleId(t *testing.T) {
	descriptors := alonzo.UtxoValidationRuleDescriptors()
	originalIndex := resolveUtxoValidationSkipIndex(
		descriptors,
		alonzo.UtxoValidationRules,
		lcommon.UtxoValidationRulePlutusScripts,
	)
	require.NotZero(t, originalIndex)
	descriptors[0], descriptors[originalIndex] = descriptors[originalIndex], descriptors[0]
	rest := make([]lcommon.UtxoValidationRuleFunc, 0, len(descriptors)-1)
	for _, descriptor := range descriptors[1:] {
		rest = append(rest, descriptor.Validator)
	}
	rules := lcommon.ComposeUtxoValidationRules(
		lcommon.Phase2ValidUtxoValidationRules(descriptors[0].Validator),
		lcommon.AlwaysUtxoValidationRules(rest...),
	)
	require.Len(t, rules, len(descriptors))
	require.NotEqual(
		t,
		utxoValidationRuleName(descriptors[0].Validator),
		utxoValidationRuleName(rules[0]),
		"the moved rule must be wrapped so no function name can match it",
	)

	require.Zero(t, resolveUtxoValidationSkipIndex(
		descriptors, rules, lcommon.UtxoValidationRulePlutusScripts,
	))
	indexed := buildIndexedUtxoValidationRules(
		descriptors,
		rules,
		lcommon.UtxoValidationRulePlutusScripts,
	)
	require.Len(t, indexed, len(rules)-1)
	requireIndexedRulesDropRuleIndex(
		t,
		indexed,
		0,
		"the resolved upstream rule must be removed",
	)
}

// requireRuleIdResolvesToFunc asserts that upstream rule id resolves to a
// single position in the era's composed rule list and that the descriptor
// there is implemented by wantFuncName. The function name is an assertion
// only; resolution keys on the Id, because upstream wraps phase-2-gated rules
// and moves shared rules between era packages.
func requireRuleIdResolvesToFunc(
	t *testing.T,
	descriptors []lcommon.UtxoValidationRuleDescriptor,
	rules []lcommon.UtxoValidationRuleFunc,
	id lcommon.UtxoValidationRuleId,
	wantFuncName string,
) int {
	t.Helper()
	index := resolveUtxoValidationSkipIndex(descriptors, rules, id)
	require.Equal(t, id, descriptors[index].Id)
	require.Equal(
		t,
		wantFuncName,
		shortUtxoValidationRuleName(descriptors[index].Validator),
		"upstream rule %s is no longer implemented by %s", id, wantFuncName,
	)
	return index
}

// requireIndexedRulesDropRuleIndex asserts the upstream rule at index is gone
// from the built list. Comparing positions rather than functions keeps the
// assertion meaningful for phase-2-gated rules, whose composed entries are
// anonymous wrappers that no function name can match.
func requireIndexedRulesDropRuleIndex(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	index int,
	message string,
) {
	t.Helper()
	for _, rule := range rules {
		require.NotEqual(t, index, rule.index, message)
	}
}

// requireIndexedRulesRetainRuleIndex asserts the upstream rule at index still
// runs, and runs the upstream implementation.
func requireIndexedRulesRetainRuleIndex(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	upstream []lcommon.UtxoValidationRuleFunc,
	index int,
	message string,
) {
	t.Helper()
	for _, rule := range rules {
		if rule.index != index {
			continue
		}
		require.Equal(
			t,
			utxoValidationRuleName(upstream[index]),
			utxoValidationRuleName(rule.validationFunc),
			message,
		)
		return
	}
	require.Fail(t, message)
}

// requireIndexedRulesReplaceRuleIndex asserts the upstream rule at index was
// swapped for Dingo's own want rule, which keeps a stable function identity.
func requireIndexedRulesReplaceRuleIndex(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	index int,
	want lcommon.UtxoValidationRuleFunc,
	message string,
) {
	t.Helper()
	for _, rule := range rules {
		if rule.index != index {
			continue
		}
		require.Equal(
			t,
			utxoValidationRuleName(want),
			utxoValidationRuleName(rule.validationFunc),
			message,
		)
		return
	}
	require.Fail(t, message)
}

func requireIndexedRulesIncludeFunc(
	t *testing.T,
	rules []indexedUtxoValidationRule,
	want lcommon.UtxoValidationRuleFunc,
	message string,
) {
	t.Helper()
	wantName := utxoValidationRuleName(want)
	for _, rule := range rules {
		if utxoValidationRuleName(rule.validationFunc) == wantName {
			return
		}
	}
	require.Fail(t, message)
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

// Preprod transaction a00696a0c2d70c381a265a845e43c55e1d00f96b27c06defc015dc92eb206240
// (epoch 4, block height 50): the Shelley protocol update that proposed
// protocol version 3.0. Its seven protocol_param_update maps each hold a
// single entry on the wire; there are no explicit null placeholders.
const preprodShelleyUpdateTxCborHex = "83a50081825820a3d6f2627a56fe7921eeda546abfe164321881d41549b7f2fbf09ea0b718d75800018182581d609e5614893238cf85e284c61ec56d5efd9f9cdc4863ba7e1bf00c2c7d1b006983fdc406aeb2021a000325a5031a00015f900682a7581c637f2e950b0fd8f8e3e811c5fbeb19e411e7a2bf37272b84b29c1a0ba10e820300581c8a4b77c4f534f8b8cc6f269e5ebb7ba77fa63a476e50e05e66d7051ca10e820300581cb00470cd193d67aac47c373602fccd4195aad3002c169b5570de1126a10e820300581cb260ffdb6eba541fcf18601923457307647dce807851b9d19da133aba10e820300581cced1599fd821a39593e00592e5292bdc1437ae0f7af388ef5257344aa10e820300581cdd2a7d71a05bed11db61555ba4c658cb1ce06c8024193d064f2a66aea10e820300581cf3b9e74f7d0f24d2314ea5dfbca94b65b2059d1ff94d97436b82d5b4a10e82030004a100888258208b0960d234bda67d52432c5d1a26aca2bfb5b9a09f966d9592a7bf0c728a1ecd584079130103d611a2b85df2de100e2d2ce6aea72128e64f1fb79e7b2cb40b4454c9f05b9142a594f975097f0f816fdf864fe26ee5579e6dc02e62105a7b3458900b825820618b625df30de53895ff29e7a3770dca56c2ff066d4aa05a6971905deecef6db5840716fa941c04771b8205a94d5f7e6fdcfe637a3375778edba0d5833d7a5e08881163be8658bc3dbdb93959642eb1a19402528b8a75cb6786cd630fd58c3dc330682582069a14b724409e0ceef671c76ec4f8bce7509b5919bb971b3855bf92ca56532225840842b04b05e906ed5c89f6bcf89415fcec9401cc6054c2391e73a21f0ad580b5d5ef66be713da2d6237b2434a29e547dab8d54b13da5492d6e08f0143cbe4140c825820d1a8de6caa8fd9b175c59862ecdd5abcd0477b84b82a0e52faecc6b3c85100a45840b634b807e001f4af4d68f773299d840a3da5e0cacb0f88ecbd45fa695eed80489bfd0092bb44f8f31b1177a3368b7f07957b69b592b5e966a45e274ddacd1a0e8258209aae625d4d15bcb3733d420e064f1cd338f386e0af049fcd42b455a69d28ad3658406955e59c61a19da7ace2ee42b90fd8ae1661a1ca98737c9ceb84e00329c4e4d5f3117a495f7c6e09570d8c4c0377f7712c409b59e357e6276c3d51e789777004825820942bb3aaab0f6442b906b65ba6ddbf7969caa662d90968926211a3d56532f11d58403b1565de7fe0ed617804b9b4ed54f026c2fa4a80627c7228a097e255c984950935ab78de08d7c31dd1ef0377cb81708330ed751a98161e3a1af0b8f3e2317f00825820d4dd69a41071bc2dc8e64a97f4bd6379524ce0c2b665728043a067e34d3e218a5840e9951169a573e3379b933f065bb2e0612fce67d110057b65de956473148dc5efdeff8ef1b4a7a01643227f844813bfd8f5be89916269fe3eccf6049f9281c1078258208ef320c2df6654a6188c45e9c639c0a686bf5a865295587d399dfeb05fe74ab65840c8818274f8e29ca6e21061494268369743dbaec05436731d29655e49ccf6f37a7a9a35072b7f70c80c709fa65497da2e8ac21c97c3150395975f8b9d4a393c02f6"

// The same transaction body re-encoded by gouroboros, whose
// ShelleyProtocolParameterUpdate fields carry no omitempty tag and therefore
// emit an explicit CBOR null for every absent optional field. This is 210
// bytes longer than the bytes that were actually on the wire and is not a
// transaction that exists on preprod.
const preprodShelleyUpdateTxReencodedCborHex = "83a50081825820a3d6f2627a56fe7921eeda546abfe164321881d41549b7f2fbf09ea0b718d75800018182581d609e5614893238cf85e284c61ec56d5efd9f9cdc4863ba7e1bf00c2c7d1b006983fdc406aeb2021a000325a5031a00015f900682a7581c637f2e950b0fd8f8e3e811c5fbeb19e411e7a2bf37272b84b29c1a0bb000f601f602f603f604f605f606f607f608f609f60af60bf60cf60df60e8203000ff6581c8a4b77c4f534f8b8cc6f269e5ebb7ba77fa63a476e50e05e66d7051cb000f601f602f603f604f605f606f607f608f609f60af60bf60cf60df60e8203000ff6581cb00470cd193d67aac47c373602fccd4195aad3002c169b5570de1126b000f601f602f603f604f605f606f607f608f609f60af60bf60cf60df60e8203000ff6581cb260ffdb6eba541fcf18601923457307647dce807851b9d19da133abb000f601f602f603f604f605f606f607f608f609f60af60bf60cf60df60e8203000ff6581cced1599fd821a39593e00592e5292bdc1437ae0f7af388ef5257344ab000f601f602f603f604f605f606f607f608f609f60af60bf60cf60df60e8203000ff6581cdd2a7d71a05bed11db61555ba4c658cb1ce06c8024193d064f2a66aeb000f601f602f603f604f605f606f607f608f609f60af60bf60cf60df60e8203000ff6581cf3b9e74f7d0f24d2314ea5dfbca94b65b2059d1ff94d97436b82d5b4b000f601f602f603f604f605f606f607f608f609f60af60bf60cf60df60e8203000ff604a100888258208b0960d234bda67d52432c5d1a26aca2bfb5b9a09f966d9592a7bf0c728a1ecd584079130103d611a2b85df2de100e2d2ce6aea72128e64f1fb79e7b2cb40b4454c9f05b9142a594f975097f0f816fdf864fe26ee5579e6dc02e62105a7b3458900b825820618b625df30de53895ff29e7a3770dca56c2ff066d4aa05a6971905deecef6db5840716fa941c04771b8205a94d5f7e6fdcfe637a3375778edba0d5833d7a5e08881163be8658bc3dbdb93959642eb1a19402528b8a75cb6786cd630fd58c3dc330682582069a14b724409e0ceef671c76ec4f8bce7509b5919bb971b3855bf92ca56532225840842b04b05e906ed5c89f6bcf89415fcec9401cc6054c2391e73a21f0ad580b5d5ef66be713da2d6237b2434a29e547dab8d54b13da5492d6e08f0143cbe4140c825820d1a8de6caa8fd9b175c59862ecdd5abcd0477b84b82a0e52faecc6b3c85100a45840b634b807e001f4af4d68f773299d840a3da5e0cacb0f88ecbd45fa695eed80489bfd0092bb44f8f31b1177a3368b7f07957b69b592b5e966a45e274ddacd1a0e8258209aae625d4d15bcb3733d420e064f1cd338f386e0af049fcd42b455a69d28ad3658406955e59c61a19da7ace2ee42b90fd8ae1661a1ca98737c9ceb84e00329c4e4d5f3117a495f7c6e09570d8c4c0377f7712c409b59e357e6276c3d51e789777004825820942bb3aaab0f6442b906b65ba6ddbf7969caa662d90968926211a3d56532f11d58403b1565de7fe0ed617804b9b4ed54f026c2fa4a80627c7228a097e255c984950935ab78de08d7c31dd1ef0377cb81708330ed751a98161e3a1af0b8f3e2317f00825820d4dd69a41071bc2dc8e64a97f4bd6379524ce0c2b665728043a067e34d3e218a5840e9951169a573e3379b933f065bb2e0612fce67d110057b65de956473148dc5efdeff8ef1b4a7a01643227f844813bfd8f5be89916269fe3eccf6049f9281c1078258208ef320c2df6654a6188c45e9c639c0a686bf5a865295587d399dfeb05fe74ab65840c8818274f8e29ca6e21061494268369743dbaec05436731d29655e49ccf6f37a7a9a35072b7f70c80c709fa65497da2e8ac21c97c3150395975f8b9d4a393c02f6"

// TestTxSizeForFee_ShelleyProtocolUpdateUsesWireBytes pins the fee-relevant
// size of a pre-Alonzo protocol-update transaction to its preserved wire
// bytes. The Haskell ledger's sizeShelleyTxF re-serializes
// [body, wits, auxiliary_data], and the body/wits encoders emit their
// memoized original bytes, so the fee size equals the on-wire length. Deriving
// the size from a re-encoded body instead would undercharge this transaction
// by 210 bytes and accept a fee that cardano-node rejects.
func TestTxSizeForFee_ShelleyProtocolUpdateUsesWireBytes(t *testing.T) {
	txCbor, err := hex.DecodeString(preprodShelleyUpdateTxCborHex)
	require.NoError(t, err)
	require.Len(t, txCbor, 1_156)

	tx, err := shelley.NewShelleyTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.Equal(
		t,
		"a00696a0c2d70c381a265a845e43c55e1d00f96b27c06defc015dc92eb206240",
		tx.Hash().String(),
	)

	// Shelley minFeeA/minFeeB were 44/155381 at preprod epoch 4, and the
	// transaction declares exactly the minimum fee for its wire size.
	const (
		minFeeA = 44
		minFeeB = 155_381
	)
	assert.Equal(t, uint64(1_156), TxSizeForFee(tx))
	assert.Equal(t, uint64(206_245), CalculateMinFee(
		TxSizeForFee(tx),
		lcommon.ExUnits{},
		minFeeA,
		minFeeB,
		nil,
		nil,
	))
	assert.Equal(t, big.NewInt(206_245), tx.Fee())
	assert.NoError(t, ValidateTxFee(tx, minFeeA, minFeeB, nil, nil))

	// Negative case: the null-expanded encoding of the same body is 1366
	// bytes, so the declared fee of 206245 is below the minimum. Sizing
	// pre-Alonzo transactions from anything other than their wire bytes must
	// not make this variant pass.
	reencodedCbor, err := hex.DecodeString(
		preprodShelleyUpdateTxReencodedCborHex,
	)
	require.NoError(t, err)
	require.Len(t, reencodedCbor, 1_366)

	reencodedTx, err := shelley.NewShelleyTransactionFromCbor(reencodedCbor)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_366), TxSizeForFee(reencodedTx))
	assert.Equal(t, uint64(215_485), CalculateMinFee(
		TxSizeForFee(reencodedTx),
		lcommon.ExUnits{},
		minFeeA,
		minFeeB,
		nil,
		nil,
	))
	assert.ErrorContains(
		t,
		ValidateTxFee(reencodedTx, minFeeA, minFeeB, nil, nil),
		"transaction fee 206245 is less than the calculated minimum fee 215485",
	)
}

func TestTxSizeForFee_ShelleyBlockTransactionUsesComponentWireBytes(
	t *testing.T,
) {
	txCbor, err := hex.DecodeString(preprodShelleyUpdateTxCborHex)
	require.NoError(t, err)
	wireTx, err := shelley.NewShelleyTransactionFromCbor(txCbor)
	require.NoError(t, err)

	// ShelleyBlock.Transactions constructs this shape from the separately
	// decoded body and witness components. The upstream transaction body now
	// preserves its original wire bytes, so rebuilding the transaction retains
	// the canonical encoding used for fee calculation.
	blockTx := &shelley.ShelleyTransaction{
		Body:       wireTx.Body,
		WitnessSet: wireTx.WitnessSet,
	}
	assert.Equal(t, wireTx.Hash(), blockTx.Hash())
	assert.Len(t, blockTx.Cbor(), 1_156)
	assert.Equal(t, uint64(1_156), TxSizeForFee(blockTx))
}

// TestTxSizeForFee_AllegraBlockTransactionUsesComponentWireBytes covers the
// same wire-byte preservation in Allegra. The preprod fixture body uses only
// fields Allegra shares with Shelley, so it decodes in both eras.
func TestTxSizeForFee_AllegraBlockTransactionUsesComponentWireBytes(
	t *testing.T,
) {
	txCbor, err := hex.DecodeString(preprodShelleyUpdateTxCborHex)
	require.NoError(t, err)
	wireTx, err := allegra.NewAllegraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.Equal(t, uint64(1_156), TxSizeForFee(wireTx))

	blockTx := &allegra.AllegraTransaction{
		Body:       wireTx.Body,
		WitnessSet: wireTx.WitnessSet,
	}
	assert.Equal(t, wireTx.Hash(), blockTx.Hash())
	assert.Len(t, blockTx.Cbor(), 1_156)
	assert.Equal(t, uint64(1_156), TxSizeForFee(blockTx))
}

// TestTxSizeForFee_MaryBlockTransactionNeedsNoCorrection pins the reason Mary
// is excluded from preAlonzoRebuiltWireSize: MaryTransactionBody also
// implements MarshalCBOR and returns its preserved bytes, so a rebuilt Mary
// transaction already encodes to its wire size. If upstream loses that method,
// this test fails and the helper's supported transaction types must be
// reconsidered.
func TestTxSizeForFee_MaryBlockTransactionNeedsNoCorrection(t *testing.T) {
	txCbor, err := hex.DecodeString(preprodShelleyUpdateTxCborHex)
	require.NoError(t, err)
	wireTx, err := mary.NewMaryTransactionFromCbor(txCbor)
	require.NoError(t, err)

	blockTx := &mary.MaryTransaction{
		Body:       wireTx.Body,
		WitnessSet: wireTx.WitnessSet,
	}
	assert.Len(t, blockTx.Cbor(), 1_156)
	assert.Equal(t, uint64(1_156), TxSizeForFee(blockTx))
	_, rebuilt := preAlonzoRebuiltWireSize(blockTx)
	assert.False(t, rebuilt)
}

func TestPreAlonzoRebuiltWireSize(t *testing.T) {
	txCbor, err := hex.DecodeString(preprodShelleyUpdateTxCborHex)
	require.NoError(t, err)
	wireTx, err := shelley.NewShelleyTransactionFromCbor(txCbor)
	require.NoError(t, err)

	t.Run("decoded from complete cbor", func(t *testing.T) {
		// Stored transaction CBOR is the encoding the node received, so it is
		// never recomputed from components.
		_, ok := preAlonzoRebuiltWireSize(wireTx)
		assert.False(t, ok)
	})
	t.Run("rebuilt from components", func(t *testing.T) {
		size, ok := preAlonzoRebuiltWireSize(&shelley.ShelleyTransaction{
			Body:       wireTx.Body,
			WitnessSet: wireTx.WitnessSet,
		})
		require.True(t, ok)
		// 1-byte array header + 343-byte body + 811-byte witness set +
		// 1-byte CBOR null auxiliary data.
		assert.Equal(t, uint64(1_156), size)
	})
	t.Run("no preserved component bytes", func(t *testing.T) {
		_, ok := preAlonzoRebuiltWireSize(&shelley.ShelleyTransaction{})
		assert.False(t, ok)
	})
	t.Run("missing witness set bytes", func(t *testing.T) {
		_, ok := preAlonzoRebuiltWireSize(&shelley.ShelleyTransaction{
			Body: wireTx.Body,
		})
		assert.False(t, ok)
	})
	t.Run("metadata without preserved auxiliary bytes", func(t *testing.T) {
		// Metadata is present but its original auxiliary-data bytes are not,
		// so the wire size cannot be rebuilt and the caller must fall back.
		_, ok := preAlonzoRebuiltWireSize(&shelley.ShelleyTransaction{
			Body:       wireTx.Body,
			WitnessSet: wireTx.WitnessSet,
			TxMetadata: &lcommon.MetaInt{},
		})
		assert.False(t, ok)
	})
	t.Run("post-alonzo transaction", func(t *testing.T) {
		_, ok := preAlonzoRebuiltWireSize(&conway.ConwayTransaction{})
		assert.False(t, ok)
	})
}

func TestPreAlonzoValidationRulesUseLocalFeeAndSizeChecks(t *testing.T) {
	shelleyDescriptors := shelley.UtxoValidationRuleDescriptors()
	shelleyFeeIndex := requireRuleIdResolvesToFunc(
		t,
		shelleyDescriptors,
		shelley.UtxoValidationRules,
		lcommon.UtxoValidationRuleFeeTooSmall,
		"shelley.UtxoValidateFeeTooSmallUtxo",
	)
	shelleySizeIndex := requireRuleIdResolvesToFunc(
		t,
		shelleyDescriptors,
		shelley.UtxoValidationRules,
		lcommon.UtxoValidationRuleMaxTxSize,
		"shelley.UtxoValidateMaxTxSizeUtxo",
	)
	require.Len(
		t,
		shelleyUtxoValidationRules,
		len(shelley.UtxoValidationRules)-2,
	)
	requireIndexedRulesDropRuleIndex(
		t,
		shelleyUtxoValidationRules,
		shelleyFeeIndex,
		"Shelley validation must size the minimum fee with TxSizeForFee",
	)
	requireIndexedRulesDropRuleIndex(
		t,
		shelleyUtxoValidationRules,
		shelleySizeIndex,
		"Shelley validation must size the max-size check with TxSizeForFee",
	)

	allegraDescriptors := allegra.UtxoValidationRuleDescriptors()
	allegraFeeIndex := requireRuleIdResolvesToFunc(
		t,
		allegraDescriptors,
		allegra.UtxoValidationRules,
		lcommon.UtxoValidationRuleFeeTooSmall,
		"allegra.UtxoValidateFeeTooSmallUtxo",
	)
	allegraSizeIndex := requireRuleIdResolvesToFunc(
		t,
		allegraDescriptors,
		allegra.UtxoValidationRules,
		lcommon.UtxoValidationRuleMaxTxSize,
		"allegra.UtxoValidateMaxTxSizeUtxo",
	)
	require.Len(
		t,
		allegraUtxoValidationRules,
		len(allegra.UtxoValidationRules)-2,
	)
	requireIndexedRulesDropRuleIndex(
		t,
		allegraUtxoValidationRules,
		allegraFeeIndex,
		"Allegra validation must size the minimum fee with TxSizeForFee",
	)
	requireIndexedRulesDropRuleIndex(
		t,
		allegraUtxoValidationRules,
		allegraSizeIndex,
		"Allegra validation must size the max-size check with TxSizeForFee",
	)
}

// TestValidateTxPreAlonzoRebuiltUpdateTxSizes drives ValidateTxShelley and
// ValidateTxAllegra with the preprod protocol-update transaction rebuilt the
// way a block delivers it. Both the upstream and Dingo fee and max-size rules
// must use the 1156 bytes that were on the wire. The empty mock ledger state
// fails other rules, so each assertion is on the fee or size message
// specifically.
func TestValidateTxPreAlonzoRebuiltUpdateTxSizes(t *testing.T) {
	const (
		preprodMinFeeA   = 44
		preprodMinFeeB   = 155_381
		preprodMaxTxSize = 16_384

		dingoFeeTooSmall  = "is less than the calculated minimum fee"
		dingoSizeTooLarge = "exceeds maximum"
	)

	txCbor, err := hex.DecodeString(preprodShelleyUpdateTxCborHex)
	require.NoError(t, err)
	shelleyWireTx, err := shelley.NewShelleyTransactionFromCbor(txCbor)
	require.NoError(t, err)
	allegraWireTx, err := allegra.NewAllegraTransactionFromCbor(txCbor)
	require.NoError(t, err)

	tests := []struct {
		name             string
		tx               lcommon.Transaction
		validateTx       lcommon.UtxoValidationRuleFunc
		upstreamFeeRule  lcommon.UtxoValidationRuleFunc
		upstreamSizeRule lcommon.UtxoValidationRuleFunc
	}{
		{
			name: "shelley",
			tx: &shelley.ShelleyTransaction{
				Body:       shelleyWireTx.Body,
				WitnessSet: shelleyWireTx.WitnessSet,
			},
			validateTx:       ValidateTxShelley,
			upstreamFeeRule:  shelley.UtxoValidateFeeTooSmallUtxo,
			upstreamSizeRule: shelley.UtxoValidateMaxTxSizeUtxo,
		},
		{
			name: "allegra",
			tx: &allegra.AllegraTransaction{
				Body:       allegraWireTx.Body,
				WitnessSet: allegraWireTx.WitnessSet,
			},
			validateTx:       ValidateTxAllegra,
			upstreamFeeRule:  allegra.UtxoValidateFeeTooSmallUtxo,
			upstreamSizeRule: allegra.UtxoValidateMaxTxSizeUtxo,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls := newMockLedgerState()
			require.Len(t, tc.tx.Cbor(), 1_156)
			require.Equal(t, uint64(1_156), TxSizeForFee(tc.tx))

			pparams := &shelley.ShelleyProtocolParameters{
				MinFeeA:   preprodMinFeeA,
				MinFeeB:   preprodMinFeeB,
				MaxTxSize: preprodMaxTxSize,
			}
			// Upstream and Dingo both size the rebuilt transaction from its
			// preserved component bytes.
			require.NoError(t, tc.upstreamFeeRule(tc.tx, 0, ls, pparams))
			err := tc.validateTx(tc.tx, 0, ls, pparams)
			require.Error(t, err, "unresolvable inputs must still fail")
			assert.NotContains(t, err.Error(), dingoFeeTooSmall)
			assert.NotContains(t, err.Error(), "fee too small")

			// Raising minFeeA by one lovelace puts the declared fee below the
			// minimum for the wire size too, so the replacement fee check is
			// running rather than silently absent.
			tighterFee := &shelley.ShelleyProtocolParameters{
				MinFeeA:   preprodMinFeeA + 1,
				MinFeeB:   preprodMinFeeB,
				MaxTxSize: preprodMaxTxSize,
			}
			require.ErrorContains(
				t,
				tc.validateTx(tc.tx, 0, ls, tighterFee),
				dingoFeeTooSmall,
			)

			// Fee and max-size must be judged against the same preserved wire
			// size. A limit between the old re-encoded size and the wire size
			// is accepted by both implementations.
			narrowSize := &shelley.ShelleyProtocolParameters{
				MinFeeA:   preprodMinFeeA,
				MinFeeB:   preprodMinFeeB,
				MaxTxSize: 1_200,
			}
			require.NoError(t, tc.upstreamSizeRule(tc.tx, 0, ls, narrowSize))
			err = tc.validateTx(tc.tx, 0, ls, narrowSize)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), dingoSizeTooLarge)
			assert.NotContains(t, err.Error(), "transaction size too large")

			// A limit below the wire size is still enforced.
			tinySize := &shelley.ShelleyProtocolParameters{
				MinFeeA:   preprodMinFeeA,
				MinFeeB:   preprodMinFeeB,
				MaxTxSize: 1_000,
			}
			require.ErrorContains(
				t,
				tc.validateTx(tc.tx, 0, ls, tinySize),
				"transaction size 1156 exceeds maximum 1000",
			)
		})
	}
}

func TestValidateTxPreAlonzoRejectsWrongProtocolParams(t *testing.T) {
	tests := []struct {
		name       string
		validateTx lcommon.UtxoValidationRuleFunc
	}{
		{name: "shelley", validateTx: ValidateTxShelley},
		{name: "allegra", validateTx: ValidateTxAllegra},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls := newMockLedgerState()
			require.ErrorIs(
				t,
				tc.validateTx(
					&shelley.ShelleyTransaction{},
					0,
					ls,
					&conway.ConwayProtocolParameters{},
				),
				ErrIncompatibleProtocolParams,
			)
			// A nil typed pointer must not be dereferenced for MinFeeA.
			var nilPparams *shelley.ShelleyProtocolParameters
			require.ErrorIs(
				t,
				tc.validateTx(&shelley.ShelleyTransaction{}, 0, ls, nilPparams),
				ErrIncompatibleProtocolParams,
			)
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
								key: lcommon.RedeemerKey{Index: 0},
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
								key: lcommon.RedeemerKey{Index: 1},
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 200000,
										Steps:  80000000,
									},
								},
							},
							{
								key: lcommon.RedeemerKey{Index: 2},
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
								key: lcommon.RedeemerKey{Index: 0},
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: math.MaxInt64,
										Steps:  100,
									},
								},
							},
							{
								key: lcommon.RedeemerKey{Index: 1},
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
								key: lcommon.RedeemerKey{Index: 0},
								val: lcommon.RedeemerValue{
									ExUnits: lcommon.ExUnits{
										Memory: 100,
										Steps:  math.MaxInt64,
									},
								},
							},
							{
								key: lcommon.RedeemerKey{Index: 1},
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
		[]lcommon.Certificate{
			cip23PoolCert(5, 100),
			cip23PoolCert(1, 1000),
		},
		floor,
	))

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

// TestConwayTxInfoCacheRendersMintIndependentOfProtocolVersion pins the
// invariant that replaced the one this test used to assert.
//
// It previously required PV9 and PV10 to render txInfoMint differently, because
// gouroboros gated the zero-lovelace ada entry on PV10. That gate was the bug:
// cardano-ledger's transMintValue takes no protocol version, so pre-Plomin
// scripts that inspect txInfoMint were costed against a mint field one entry
// short and diverged from the node that produced the block. gouroboros v0.192.0
// removed the gate, so the rendering is now identical in every era, and this
// test fails if the gate returns.
func TestConwayTxInfoCacheRendersMintIndependentOfProtocolVersion(
	t *testing.T,
) {
	input := newTestInput(0x01, 0)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			// Non-zero so the fee field cannot be mistaken for the mint field:
			// both render as a single-entry map keyed by the empty policy, and
			// a zero fee would make them identical.
			fee:       big.NewInt(311_505),
			witnesses: &mockWitnessSet{},
		},
		inputs: []lcommon.TransactionInput{input},
	}
	ls := newMockLedgerState()
	ls.addUtxo(input, newTestOutput(1_000_000))
	resolved := []lcommon.Utxo{{Id: input, Output: newTestOutput(1_000_000)}}

	// A tx that mints nothing still carries the ada entry, so no mint fixture
	// is needed to observe the rendering.
	cache := newConwayTxInfoCache(ls, tx, resolved)
	v1, err := cache.v1()
	require.NoError(t, err)
	v2, err := cache.v2()
	require.NoError(t, err)

	// txInfo field order differs by version: V1 is
	// [inputs, outputs, fee, mint, ...] while V2 inserts reference inputs
	// ahead of outputs, so its mint sits one later.
	mintOf := func(d data.PlutusData, idx int) data.PlutusData {
		constr, ok := d.(*data.Constr)
		require.True(t, ok)
		require.Greater(t, len(constr.Fields), idx)
		return constr.Fields[idx]
	}
	for _, tc := range []struct {
		name string
		mint data.PlutusData
	}{
		{"v1", mintOf(v1.ToPlutusData(), 3)},
		{"v2", mintOf(v2.ToPlutusData(), 4)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, ok := tc.mint.(*data.Map)
			require.True(t, ok, "txInfoMint must be a map")
			require.Len(t, m.Pairs, 1,
				"an empty mint must still carry the zero-lovelace ada entry")
			policy, ok := m.Pairs[0][0].(*data.ByteString)
			require.True(t, ok)
			assert.Empty(
				t,
				policy.Inner,
				"ada policy id is the empty bytestring",
			)

			// Assert the amount too. Without it the fee field, which is also a
			// single-entry map keyed by the empty policy, satisfies the checks
			// above and the test would not notice a wrong field index.
			inner, ok := m.Pairs[0][1].(*data.Map)
			require.True(t, ok, "ada entry must map asset name to amount")
			require.Len(t, inner.Pairs, 1)
			name, ok := inner.Pairs[0][0].(*data.ByteString)
			require.True(t, ok)
			assert.Empty(
				t,
				name.Inner,
				"ada asset name is the empty bytestring",
			)
			amount, ok := inner.Pairs[0][1].(*data.Integer)
			require.True(t, ok)
			assert.Zero(t, amount.Inner.Int64(),
				"the prepended ada entry mints zero")
		})
	}
}

// TestConwayPlutusRejectsNilPparams covers the typed-nil pointer case: a nil
// *conway.ConwayProtocolParameters satisfies the protocol-parameters type
// assertion, and the Conway plutus path dereferences pparams for cost models
// and the protocol version, so an unguarded nil would panic instead of
// erroring.
func TestConwayPlutusRejectsNilPparams(t *testing.T) {
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			fee:       big.NewInt(0),
			witnesses: &mockWitnessSet{},
		},
	}
	ls := newMockLedgerState()
	var nilPparams *conway.ConwayProtocolParameters

	assert.Equal(
		t,
		ErrIncompatibleProtocolParams,
		ValidateTxPlutusConway(tx, 0, ls, nilPparams),
	)
	assert.ErrorIs(
		t,
		ValidateTxConway(tx, 0, ls, nilPparams),
		ErrIncompatibleProtocolParams,
	)
	_, _, _, err := EvaluateTxConway(tx, ls, nilPparams)
	assert.ErrorIs(t, err, ErrIncompatibleProtocolParams)
	// CertDepositConway takes the same guard and reads pparams fields directly.
	_, certErr := CertDepositConway(
		&lcommon.StakeRegistrationCertificate{},
		nilPparams,
	)
	assert.ErrorIs(t, certErr, ErrIncompatibleProtocolParams)
}

// TestPreAlonzoCertDepositRejectsNilPparams completes the typed-nil guard
// across the pre-Alonzo eras.
//
// TestConwayPlutusRejectsNilPparams already pins CertDepositConway, and the
// ValidateTx path was guarded for these eras, but their CertDeposit functions
// kept an ok-only assertion and then read PoolDeposit and KeyDeposit directly.
// A typed nil satisfies the assertion, so an unguarded nil panicked here while
// the sibling era returned an error.
func TestPreAlonzoCertDepositRejectsNilPparams(t *testing.T) {
	certs := []lcommon.Certificate{
		&lcommon.StakeRegistrationCertificate{},
		&lcommon.PoolRegistrationCertificate{},
	}
	for _, cert := range certs {
		var nilShelley *shelley.ShelleyProtocolParameters
		_, err := CertDepositShelley(cert, nilShelley)
		assert.ErrorIs(t, err, ErrIncompatibleProtocolParams)

		var nilAllegra *allegra.AllegraProtocolParameters
		_, err = CertDepositAllegra(cert, nilAllegra)
		assert.ErrorIs(t, err, ErrIncompatibleProtocolParams)

		var nilMary *mary.MaryProtocolParameters
		_, err = CertDepositMary(cert, nilMary)
		assert.ErrorIs(t, err, ErrIncompatibleProtocolParams)
	}
}

// TestConwayCommitteeCertificateRuleDoesNotRejectWhenStateUnavailable proves
// the rule declines to reject on committee grounds it cannot establish.
//
// Dingo does not seed the Conway genesis committee
// (blinklabs-io/dingo#3785), so a genesis-synced node holds no committee rows
// for the whole Conway era and CommitteeStateAvailable reports false. Rejecting here would reject an authorization from a real
// genesis committee member that cardano-node accepts. The member is seated in
// the harness while availability is false, so a rejection would prove the
// authority result was ignored.
func TestConwayCommitteeCertificateRuleDoesNotRejectWhenStateUnavailable(
	t *testing.T,
) {
	var hash lcommon.Blake2b224
	hash[0] = 0xd1
	credential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       false,
		cold: map[string]*lcommon.CommitteeMember{
			taggedCommitteeCredentialKey(credential): {ColdKey: hash},
		},
	}
	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{{
				Type: uint(lcommon.CertificateTypeAuthCommitteeHot),
				Certificate: &lcommon.AuthCommitteeHotCertificate{
					CertType: uint(
						lcommon.CertificateTypeAuthCommitteeHot,
					),
					ColdCredential: credential,
				},
			}},
		},
	}

	rule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateCommitteeCertificates,
	)
	require.NoError(t, rule(tx, 0, state, &conway.ConwayProtocolParameters{}))
}

// TestConwayUnknownVoterRuleDoesNotRejectWhenStateUnavailable is the
// voter-side counterpart, for the same reason.
func TestConwayUnknownVoterRuleDoesNotRejectWhenStateUnavailable(t *testing.T) {
	var hash lcommon.Blake2b224
	hash[0] = 0xd2
	credential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       false,
		hot: map[string]*lcommon.CommitteeMember{
			taggedCommitteeCredentialKey(credential): {ColdKey: hash},
		},
	}
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hash),
	}
	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
	}

	rule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateUnknownVoters,
	)
	require.NoError(t, rule(tx, 0, state, &conway.ConwayProtocolParameters{}))
}

// TestConwayCommitteeRulesAcceptAuthoritativeEmptyCommittee is the mandatory
// counterpart to the two tests above: an authoritative empty committee must
// report available-and-empty, which for a transaction carrying no committee
// certificate and no votes means no rejection at all.
//
// This test passes both with and without the fail-closed change by design. It
// exists to pin the other side of the boundary: it fails only if fail-closed
// is over-applied to a transaction that makes no committee lookup.
func TestConwayCommitteeRulesAcceptAuthoritativeEmptyCommittee(t *testing.T) {
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       true,
	}
	tx := &conway.ConwayTransaction{TxIsValid: true}

	certRule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateCommitteeCertificates,
	)
	require.NoError(
		t,
		certRule(tx, 0, state, &conway.ConwayProtocolParameters{}),
	)

	voterRule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateUnknownVoters,
	)
	require.NoError(
		t,
		voterRule(tx, 0, state, &conway.ConwayProtocolParameters{}),
	)
}

// TestConwayCommitteeRulesSkipPhase2InvalidTransaction proves the rules do not
// inspect committee state for a phase-2-invalid transaction. Such a
// transaction applies only its collateral effects, so rejecting it here would
// diverge from the reference implementation and reject a block cardano-node
// accepts. The provider is deliberately empty and available, which would
// reject both certificates and votes if the guard were absent.
func TestConwayCommitteeRulesSkipPhase2InvalidTransaction(t *testing.T) {
	var hash lcommon.Blake2b224
	hash[0] = 0xd3
	credential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       true,
	}
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hash),
	}
	tx := &conway.ConwayTransaction{
		TxIsValid: false,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{{
				Type: uint(lcommon.CertificateTypeAuthCommitteeHot),
				Certificate: &lcommon.AuthCommitteeHotCertificate{
					CertType: uint(
						lcommon.CertificateTypeAuthCommitteeHot,
					),
					ColdCredential: credential,
				},
			}},
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
	}

	certRule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateCommitteeCertificates,
	)
	require.NoError(
		t,
		certRule(tx, 0, state, &conway.ConwayProtocolParameters{}),
	)

	voterRule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateUnknownVoters,
	)
	require.NoError(
		t,
		voterRule(tx, 0, state, &conway.ConwayProtocolParameters{}),
	)
}

// TestConwayCommitteeHotVoterTagsDoNotCrossMatch is the mandatory negative
// case for credential identity: a key-hash and a script-hash credential
// sharing the same 28 bytes are distinct voters and must not resolve to each
// other's member.
//
// This test passes both with and without the fail-closed change by design; it
// covers the tag-preservation behavior this PR adds, not the availability
// gate. It fails if the tag is ever dropped or defaulted in voter resolution.
func TestConwayCommitteeHotVoterTagsDoNotCrossMatch(t *testing.T) {
	var hash lcommon.Blake2b224
	hash[0] = 0xd4
	keyCredential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	// Only the key-hash identity is seated.
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       true,
		hot: map[string]*lcommon.CommitteeMember{
			taggedCommitteeCredentialKey(keyCredential): {ColdKey: hash},
		},
	}
	rule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateUnknownVoters,
	)
	newTx := func(voterType uint8) *conway.ConwayTransaction {
		voter := &lcommon.Voter{Type: voterType, Hash: [28]byte(hash)}
		return &conway.ConwayTransaction{
			TxIsValid: true,
			Body: conway.ConwayTransactionBody{
				TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
			},
		}
	}

	// The seated key-hash voter is accepted.
	require.NoError(t, rule(
		newTx(lcommon.VoterTypeConstitutionalCommitteeHotKeyHash),
		0, state, &conway.ConwayProtocolParameters{},
	))

	// The script-hash voter with identical bytes must not borrow it.
	var unknown conway.UnknownVoterError
	require.ErrorAs(t, rule(
		newTx(lcommon.VoterTypeConstitutionalCommitteeHotScriptHash),
		0, state, &conway.ConwayProtocolParameters{},
	), &unknown)
}

// TestConwayCommitteeRulesFailClosedOnLookupError proves a failed committee
// lookup is never treated as authorization. This is the fail-closed half of
// the contract: an availability *error* is a real failure and must reject,
// unlike an authoritative "cannot answer", which must not.
func TestConwayCommitteeRulesFailClosedOnLookupError(t *testing.T) {
	var hash lcommon.Blake2b224
	hash[0] = 0xd5
	credential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: hash,
	}
	state := &taggedCommitteeLedgerState{
		mockLedgerState: newMockLedgerState(),
		available:       true,
		availableErr:    errors.New("committee snapshot read failed"),
		cold: map[string]*lcommon.CommitteeMember{
			taggedCommitteeCredentialKey(credential): {ColdKey: hash},
		},
	}
	voter := &lcommon.Voter{
		Type: lcommon.VoterTypeConstitutionalCommitteeHotKeyHash,
		Hash: [28]byte(hash),
	}
	tx := &conway.ConwayTransaction{
		TxIsValid: true,
		Body: conway.ConwayTransactionBody{
			TxCertificates: []lcommon.CertificateWrapper{{
				Type: uint(lcommon.CertificateTypeAuthCommitteeHot),
				Certificate: &lcommon.AuthCommitteeHotCertificate{
					CertType: uint(
						lcommon.CertificateTypeAuthCommitteeHot,
					),
					ColdCredential: credential,
				},
			}},
			TxVotingProcedures: lcommon.VotingProcedures{voter: {}},
		},
	}

	var lookup conway.CommitteeMemberLookupError
	certRule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateCommitteeCertificates,
	)
	require.ErrorAs(
		t,
		certRule(tx, 0, state, &conway.ConwayProtocolParameters{}),
		&lookup,
	)

	voterRule := findIndexedUtxoValidationRule(
		t,
		conwayUtxoValidationRules,
		validateUnknownVoters,
	)
	require.ErrorAs(
		t,
		voterRule(tx, 0, state, &conway.ConwayProtocolParameters{}),
		&lookup,
	)
}
