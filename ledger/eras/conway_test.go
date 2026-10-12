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
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/common/script"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/blinklabs-io/plutigo/builtin"
	"github.com/blinklabs-io/plutigo/cek"
	"github.com/blinklabs-io/plutigo/data"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stakeRegisteredLedgerState wraps mockLedgerState so a test can report a
// stake credential as registered while leaving DRepRegistration at its
// default (unregistered) stub. mockLedgerState.IsStakeCredentialRegistered
// unconditionally returns false, so a plain mockLedgerState can never reach
// the DRep-registration check in conway.UtxoValidateDelegation: the stake
// credential check fails first.
type stakeRegisteredLedgerState struct {
	*mockLedgerState
}

func (s *stakeRegisteredLedgerState) IsStakeCredentialRegistered(
	_ lcommon.Credential,
) bool {
	return true
}

// voteDelegationTx implements just enough of lcommon.Transaction for
// conway.UtxoValidateDelegation: IsValid (it only runs its checks for
// phase-1-valid transactions) and Certificates.
type voteDelegationTx struct {
	lcommon.Transaction
	certs []lcommon.Certificate
}

func (t *voteDelegationTx) IsValid() bool { return true }

func (t *voteDelegationTx) Certificates() []lcommon.Certificate {
	return t.certs
}

func mkConwayPpMajor(major uint) *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: major,
		},
	}
}

// TestValidateDelegationConwayBootstrapAware_UnregisteredDRep is the
// regression test for the dingo live incident (Preview, epoch 646, slot
// 55847379, block_hash 8cc2e91a1fbda7d61e9781e4a71a5c6e49eb039464b93121cad
// 1208869b283ba): a vote-delegation certificate targeting a DRep credential
// that has not registered yet is valid at PV9 (the Conway bootstrap phase)
// and only becomes invalid from PV10 (Plomin) onward.
//
// Ground truth for the incident's own transaction: Koios shows DRep
// 0e4bdd698b4cc2f2e518b4b1fa5190d0bc566ac42f93c26986ecaa79 first
// registering at block_time 1740052671 (2025-02-20), roughly seven months
// after the delegation certificate at block_time 1722503379 (2024-08-01,
// epoch 646). The transaction is confirmed on the real Preview chain, so
// cardano-ledger accepted it; only dingo's check was wrong.
func TestValidateDelegationConwayBootstrapAware_UnregisteredDRep(t *testing.T) {
	unregisteredDRepCred := []byte{
		0x0e, 0x4b, 0xdd, 0x69, 0x8b, 0x4c, 0xc2, 0xf2, 0xe5, 0x18,
		0xb4, 0xb1, 0xfa, 0x51, 0x90, 0xd0, 0xbc, 0x56, 0x6a, 0xc4,
		0x2f, 0x93, 0xc2, 0x69, 0x86, 0xec, 0xaa, 0x79,
	}
	stakeCred := lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224([]byte("some-registered-stake-key-hash")),
	}
	cert := &lcommon.VoteDelegationCertificate{
		CertType:        9,
		StakeCredential: stakeCred,
		Drep: lcommon.Drep{
			Type:       lcommon.DrepTypeAddrKeyHash,
			Credential: unregisteredDRepCred,
		},
	}
	tx := &voteDelegationTx{certs: []lcommon.Certificate{cert}}
	ls := &stakeRegisteredLedgerState{mockLedgerState: newMockLedgerState()}

	t.Run("PV9 bootstrap phase accepts delegation to a not-yet-registered DRep", func(t *testing.T) {
		err := validateDelegationConwayBootstrapAware(
			tx, 55847379, ls, mkConwayPpMajor(lcommon.ProtocolVersionConway),
		)
		require.NoError(
			t,
			err,
			"PV9 must accept a vote delegation to an unregistered DRep, matching "+
				"cardano-ledger's checkDRepRegistered, which is skipped "+
				"`unless (hardforkConwayBootstrapPhase ...)`",
		)
	})

	t.Run("PV10 rejects delegation to an unregistered DRep", func(t *testing.T) {
		err := validateDelegationConwayBootstrapAware(
			tx, 55847379, ls, mkConwayPpMajor(lcommon.ProtocolVersionPlomin),
		)
		var drepErr conway.DelegateVoteToUnregisteredDRepError
		require.ErrorAs(
			t,
			err,
			&drepErr,
			"PV10 (Plomin) and later must still reject delegation to an "+
				"unregistered DRep",
		)
	})

	t.Run("unrelated delegation failures are not swallowed at PV9", func(t *testing.T) {
		unregisteredStakeCert := &lcommon.VoteDelegationCertificate{
			CertType: 9,
			StakeCredential: lcommon.Credential{
				CredType: lcommon.CredentialTypeAddrKeyHash,
				Credential: lcommon.NewBlake2b224(
					[]byte("unregistered-stake-key-hash"),
				),
			},
			Drep: lcommon.Drep{
				Type:       lcommon.DrepTypeAddrKeyHash,
				Credential: unregisteredDRepCred,
			},
		}
		badTx := &voteDelegationTx{
			certs: []lcommon.Certificate{unregisteredStakeCert},
		}
		plainLs := newMockLedgerState()
		err := validateDelegationConwayBootstrapAware(
			badTx, 55847379, plainLs, mkConwayPpMajor(lcommon.ProtocolVersionConway),
		)
		var stakeErr conway.DelegateUnregisteredStakeCredentialError
		require.ErrorAs(
			t,
			err,
			&stakeErr,
			"the bootstrap relaxation must be scoped to the DRep-registration "+
				"check only; an unregistered stake credential must still fail "+
				"at PV9",
		)
	})
}

func TestIsConwayBootstrapPhase(t *testing.T) {
	assert.True(t, isConwayBootstrapPhase(mkConwayPpMajor(lcommon.ProtocolVersionConway)))
	assert.False(t, isConwayBootstrapPhase(mkConwayPpMajor(lcommon.ProtocolVersionPlomin)))
	assert.False(t, isConwayBootstrapPhase(mkConwayPpMajor(lcommon.ProtocolVersionConway-1)))
	assert.False(t, isConwayBootstrapPhase(&conway.ConwayProtocolParameters{}))
}

// TestBuildConwayValidationRules_DelegationOverride confirms the composed
// Conway rule set replaces the upstream delegation rule (rather than
// dropping it, which would silently disable every other delegation check)
// with the bootstrap-aware wrapper.
func TestBuildConwayValidationRules_DelegationOverride(t *testing.T) {
	descriptors := conway.UtxoValidationRuleDescriptors()
	delegationIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleDelegation,
		"conway.UtxoValidateDelegation",
	)
	requireIndexedRulesReplaceRuleIndex(
		t,
		conwayUtxoValidationRules,
		delegationIndex,
		validateDelegationConwayBootstrapAware,
		"Conway validation must relax the DRep-registration check during "+
			"the PV9 bootstrap phase",
	)
}

type mockConwayFeaturesTx struct {
	mockConwayFeeTx
	currentTreasuryValue *big.Int
}

func (m *mockConwayFeaturesTx) CurrentTreasuryValue() *big.Int {
	return m.currentTreasuryValue
}

func TestConwayFeaturesRuleAllowsUnneededPlutusV1V2(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		script lcommon.Script
	}{
		{name: "PlutusV1", script: lcommon.PlutusV1Script{0x01}},
		{name: "PlutusV2", script: lcommon.PlutusV2Script{0x02}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := newTestInput(0x81, 0)
			tx := newConwayFeaturesTestTx(input)
			ls := newMockLedgerState()
			ls.addUtxo(input, testAddressScriptOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       newTestKeyAddress(t),
				scriptRef:  tc.script,
			})

			require.NoError(t, conwayFeaturesRule(t)(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			))
		})
	}
}

func TestConwayFeaturesRuleDefersPV11PlutusV3ReferenceInputCheck(
	t *testing.T,
) {
	t.Parallel()
	input := newTestInput(0x83, 0)
	tx := &mockConwayFeaturesTx{mockConwayFeeTx: mockConwayFeeTx{
		mockFeeTx: mockFeeTx{fee: big.NewInt(0)},
	}}
	tx.inputs = []lcommon.TransactionInput{input}
	tx.referenceInputs = []lcommon.TransactionInput{input}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
	}
	require.NoError(t, conwayFeaturesRule(t)(
		tx,
		0,
		newMockLedgerState(),
		pp,
	))
}

// TestValidateTxConwayPV11ReferenceInputOverlapUsesExecutedLanguage exercises
// the production validator with the relevant reference-overlap rules.
// Not t.Parallel: this test swaps the package-level Conway validation rule sets.
func TestValidateTxConwayPV11ReferenceInputOverlapUsesExecutedLanguage(
	t *testing.T,
) {
	useConwayReferenceOverlapRules(t)

	t.Run("PV10 remains transaction-wide", func(t *testing.T) {
		input := newTestInput(0x84, 0)
		tx := newConwayOverlapTx(input, nil, nil)
		ls := newMockLedgerState()
		ls.addUtxo(input, newTestOutput(2_000_000))
		pp := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: lcommon.ProtocolVersionPlomin,
			},
		}
		require.ErrorContains(
			t,
			ValidateTxConway(tx, 0, ls, pp),
			"non-disjoint reference inputs",
		)
	})

	t.Run("PV11 without Plutus is allowed", func(t *testing.T) {
		input := newTestInput(0x85, 0)
		tx := newConwayOverlapTx(input, nil, nil)
		ls := newMockLedgerState()
		ls.addUtxo(input, newTestOutput(2_000_000))
		require.NoError(t, ValidateTxConway(
			tx,
			0,
			ls,
			conwayOverlapProtocolParams(
				t,
				lcommon.ProtocolVersionVanRossem,
				0,
				lang.LanguageVersionV1,
			),
		))
		_, _, _, err := EvaluateTxConway(
			tx,
			ls,
			conwayOverlapProtocolParams(
				t,
				lcommon.ProtocolVersionVanRossem,
				0,
				lang.LanguageVersionV1,
			),
		)
		require.NoError(t, err)
	})

	for _, tc := range []struct {
		name           string
		version        lang.LanguageVersion
		costModelIndex uint
	}{
		{name: "PlutusV1", version: lang.LanguageVersionV1, costModelIndex: 0},
		{name: "PlutusV2", version: lang.LanguageVersionV2, costModelIndex: 1},
	} {
		t.Run("PV11 "+tc.name+" execution is allowed", func(t *testing.T) {
			input := newTestInput(0x86, 0)
			script, _ := newConwayOverlapMintScript(t, tc.version)
			tx := newConwayOverlapTx(input, script, nil)
			ls := newMockLedgerState()
			ls.addUtxo(input, newTestOutput(2_000_000))
			require.NoError(t, ValidateTxConway(
				tx,
				0,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					tc.costModelIndex,
					tc.version,
				),
			))
			_, _, _, err := EvaluateTxConway(
				tx,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					tc.costModelIndex,
					tc.version,
				),
			)
			require.NoError(t, err)
		})
	}

	t.Run(
		"PV11 V3 execution rejects overlap despite unused V1 and V2 scripts",
		func(t *testing.T) {
			input := newTestInput(0x87, 0)
			v3Script, _ := newConwayOverlapMintScript(t, lang.LanguageVersionV3)
			tx := newConwayOverlapTx(
				input,
				v3Script,
				[]lcommon.PlutusV1Script{{0x11}},
			)
			ls := newMockLedgerState()
			ls.addUtxo(input, testAddressScriptOutput{
				testOutput: newTestOutput(2_000_000),
				addr:       newTestKeyAddress(t),
				scriptRef:  lcommon.PlutusV2Script{0x33},
			})
			err := ValidateTxConway(
				tx,
				0,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					2,
					lang.LanguageVersionV3,
				),
			)
			require.ErrorContains(t, err, "also a regular input")
			_, _, _, err = EvaluateTxConway(
				tx,
				ls,
				conwayOverlapProtocolParams(
					t,
					lcommon.ProtocolVersionVanRossem,
					2,
					lang.LanguageVersionV3,
				),
			)
			require.ErrorContains(t, err, "also a regular input")
		},
	)
}

func useConwayReferenceOverlapRules(t *testing.T) {
	t.Helper()
	originalRules := conwayUtxoValidationRules
	originalPhase1Rules := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = originalRules
		conwayPhase1UtxoValidationRules = originalPhase1Rules
	})
	descriptors := conway.UtxoValidationRuleDescriptors()
	disjointIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleDisjointRefInputs,
		"conway.UtxoValidateDisjointRefInputs",
	)
	featuresIndex := requireRuleIdResolvesToFunc(
		t,
		descriptors,
		conway.UtxoValidationRules,
		lcommon.UtxoValidationRuleConwayFeaturesWithPlutusV1V2,
		"conway.UtxoValidateConwayFeaturesWithPlutusV1V2",
	)
	rules := make([]indexedUtxoValidationRule, 0, 2)
	for _, rule := range originalRules {
		if rule.index == disjointIndex || rule.index == featuresIndex {
			rules = append(rules, rule)
		}
	}
	require.Len(t, rules, 2)
	conwayUtxoValidationRules = rules
	conwayPhase1UtxoValidationRules = rules
}

func newConwayOverlapTx(
	input lcommon.TransactionInput,
	usedScript lcommon.Script,
	unusedV1 []lcommon.PlutusV1Script,
) *mockConwayFeeTx {
	witnesses := &mockWitnessSet{
		plutusV1Scripts: append([]lcommon.PlutusV1Script(nil), unusedV1...),
	}
	if usedScript != nil {
		switch script := usedScript.(type) {
		case lcommon.PlutusV1Script:
			witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{script}
		case lcommon.PlutusV2Script:
			witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{script}
		case lcommon.PlutusV3Script:
			witnesses.plutusV3Scripts = []lcommon.PlutusV3Script{script}
		}
		hash := usedScript.Hash()
		assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
			map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
				lcommon.Blake2b224(hash): {
					cbor.NewByteString([]byte("context-overlap")): big.NewInt(
						1,
					),
				},
			},
		)
		witnesses.redeemers = &mockRedeemers{entries: []struct {
			key lcommon.RedeemerKey
			val lcommon.RedeemerValue
		}{
			{
				key: lcommon.RedeemerKey{
					Tag:   lcommon.RedeemerTagMint,
					Index: 0,
				},
				val: lcommon.RedeemerValue{
					Data: lcommon.Datum{Data: data.NewConstr(0)},
					ExUnits: lcommon.ExUnits{
						Memory: 5_000_000,
						Steps:  50_000_000,
					},
				},
			},
		}}
		return &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{fee: big.NewInt(0), witnesses: witnesses},
			inputs:    []lcommon.TransactionInput{input},
			referenceInputs: []lcommon.TransactionInput{
				input,
			},
			assetMint: &assetMint,
		}
	}
	return &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{fee: big.NewInt(0), witnesses: witnesses},
		inputs:    []lcommon.TransactionInput{input},
		referenceInputs: []lcommon.TransactionInput{
			input,
		},
	}
}

func conwayOverlapProtocolParams(
	t *testing.T,
	major uint,
	costModelIndex uint,
	version lang.LanguageVersion,
) *conway.ConwayProtocolParameters {
	models := map[uint][]int64(nil)
	if major >= lcommon.ProtocolVersionVanRossem {
		models = map[uint][]int64{
			costModelIndex: defaultMachineCostModel(t, version),
		}
	}
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: major,
		},
		CostModels: models,
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10_000_000,
			Steps:  100_000_000,
		},
	}
}

func newConwayOverlapMintScript(
	t *testing.T,
	version lang.LanguageVersion,
) (lcommon.Script, uint) {
	t.Helper()
	uplcVersion := lang.LanguageVersionV1
	parameterCount := 2
	costModelIndex := uint(0)
	switch version {
	case lang.LanguageVersionV2:
		costModelIndex = 1
	case lang.LanguageVersionV3:
		uplcVersion = lang.LanguageVersion{1, 1, 0}
		parameterCount = 1
		costModelIndex = 2
	default:
		require.Equal(t, lang.LanguageVersionV1, version)
	}
	var term syn.Term[syn.DeBruijn] = &syn.Constant{Con: &syn.Unit{}}
	for range parameterCount {
		term = &syn.Lambda[syn.DeBruijn]{Body: term}
	}
	flatProgram, err := syn.Encode(&syn.Program[syn.DeBruijn]{
		Version: uplcVersion,
		Term:    term,
	})
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	switch version {
	case lang.LanguageVersionV1:
		return lcommon.PlutusV1Script(scriptBytes), costModelIndex
	case lang.LanguageVersionV2:
		return lcommon.PlutusV2Script(scriptBytes), costModelIndex
	default:
		return lcommon.PlutusV3Script(scriptBytes), costModelIndex
	}
}

func TestConwayScriptPurposeUsesActiveProtocolMajor(t *testing.T) {
	t.Parallel()
	certificate := &lcommon.RegistrationCertificate{
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.Blake2b224{1},
		},
		Amount: 2_000_000,
	}
	for _, tc := range []struct {
		name  string
		major uint
		want  data.PlutusData
	}{
		{
			name:  "PV9",
			major: lcommon.ProtocolVersionConway,
			want:  data.NewConstr(1),
		},
		{
			name:  "PV10",
			major: lcommon.ProtocolVersionPlomin,
			want: data.NewConstr(0,
				data.NewInteger(big.NewInt(2_000_000))),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pp := &conway.ConwayProtocolParameters{
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: tc.major,
				},
			}
			purpose, ok := buildConwayScriptPurpose(
				lcommon.RedeemerKey{Tag: lcommon.RedeemerTagCert},
				nil,
				nil,
				lcommon.MultiAsset[lcommon.MultiAssetTypeMint]{},
				[]lcommon.Certificate{certificate},
				nil,
				nil,
				nil,
				nil,
				protocolMajorVersion(pp),
			)
			require.True(t, ok)
			fields := purpose.ToPlutusData().(*data.Constr).Fields
			certificateData := fields[1].(*data.Constr).Fields
			require.Equal(t, tc.want, certificateData[1])
		})
	}
}

func TestConwayFeaturesRuleRejectsNeededPlutusV1V2(t *testing.T) {
	for _, tc := range []struct {
		name          string
		script        lcommon.Script
		plutusVersion string
	}{
		{
			name:          "PlutusV1",
			script:        lcommon.PlutusV1Script{0x03},
			plutusVersion: "PlutusV1",
		},
		{
			name:          "PlutusV2",
			script:        lcommon.PlutusV2Script{0x04},
			plutusVersion: "PlutusV2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := newTestInput(0x82, 0)
			tx := newConwayFeaturesTestTx(input)
			ls := newMockLedgerState()
			ls.addUtxo(input, testAddressScriptOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       newTestScriptAddress(t, tc.script),
				scriptRef:  tc.script,
			})

			var featureErr conway.ConwayCertificateWithPlutusV1V2Error
			require.ErrorAs(t, conwayFeaturesRule(t)(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			), &featureErr)
			assert.Equal(t, tc.plutusVersion, featureErr.PlutusVersion)
			assert.Equal(t, "VoteDelegation", featureErr.CertificateType)
		})
	}
}

func conwayFeaturesRule(t *testing.T) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	for _, rule := range conwayUtxoValidationRules {
		if utxoValidationRuleName(rule.validationFunc) ==
			utxoValidationRuleName(validateConwayFeaturesWithNeededPlutusV1V2) {
			return rule.validationFunc
		}
	}
	t.Fatal("Conway PlutusV1/V2 feature rule was not installed")
	return nil
}

func newConwayFeaturesTestTx(
	input lcommon.TransactionInput,
) *mockConwayFeaturesTx {
	return &mockConwayFeaturesTx{
		mockConwayFeeTx: mockConwayFeeTx{
			mockFeeTx: mockFeeTx{},
			inputs:    []lcommon.TransactionInput{input},
			certificates: []lcommon.Certificate{
				&lcommon.VoteDelegationCertificate{
					StakeCredential: lcommon.Credential{
						CredType: lcommon.CredentialTypeAddrKeyHash,
					},
				},
			},
		},
	}
}

// conway.UtxoValidateInlineDatumsWithPlutusV1 is the upstream Conway
// inline-datums-with-plutus-v1 UTXO rule. Dingo installs it unmodified, so
// nothing else in this repository pins its behavior. The cases below cover
// the three properties the rule has to get right, none of which
// internal/test/conformance reaches:
//
//   - an inline datum is disqualifying on a consumed input, on a reference
//     input, and on a produced output, whenever a PlutusV1 script is needed;
//   - a reference script is never disqualifying, and neither is the mere
//     presence of reference inputs;
//   - only a *needed* PlutusV1 script constrains the transaction, so a V1
//     script that is merely reachable is ignored.
//
// The needed-not-available distinction is the fix from gouroboros.

// newBabbageInlineDatumOutput builds a Babbage output carrying an inline datum
// at the given address, by round-tripping CBOR rather than asserting a concrete
// era type, so the output reports Datum() the way a decoded block output does.
func newBabbageInlineDatumOutput(
	t *testing.T,
	addr lcommon.Address,
) lcommon.TransactionOutput {
	t.Helper()
	datumCbor, err := cbor.Encode(data.NewConstr(0))
	require.NoError(t, err)
	datumOptionCbor, err := cbor.Encode([]any{
		babbage.DatumOptionTypeData,
		cbor.Tag{Number: 24, Content: datumCbor},
	})
	require.NoError(t, err)
	addressCbor, err := cbor.Encode(addr)
	require.NoError(t, err)
	amountCbor, err := cbor.Encode(uint64(1_000_000))
	require.NoError(t, err)
	outputCbor, err := cbor.Encode(map[uint]cbor.RawMessage{
		0: addressCbor,
		1: amountCbor,
		2: datumOptionCbor,
	})
	require.NoError(t, err)
	var output babbage.BabbageTransactionOutput
	_, err = cbor.Decode(outputCbor, &output)
	require.NoError(t, err)
	return &output
}

// TestConwayInlineDatumRuleRejectsUsedPlutusV1Script is the base rejection: the
// spent input is guarded by a PlutusV1 script and carries an inline datum.
func TestConwayInlineDatumRuleRejectsUsedPlutusV1Script(t *testing.T) {
	input := newTestInput(0x03, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x01, 0x02})
	scriptAddr := newTestScriptAddress(t, plutusScript)
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs: []lcommon.TransactionInput{input},
	}
	ls := newMockLedgerState()
	ls.addUtxo(input, newBabbageInlineDatumOutput(t, scriptAddr))

	var inlineDatumErr lcommon.InlineDatumsNotSupportedError
	require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	), &inlineDatumErr)
}

// TestConwayInlineDatumRuleRejectsDatumOnOutput covers an inline datum on one
// of the transaction's own outputs rather than on a consumed input. The
// PlutusV1 script context has to represent that output too, so scanning only
// the consumed inputs misses it.
func TestConwayInlineDatumRuleRejectsDatumOnOutput(t *testing.T) {
	scriptInput := newTestInput(0x21, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x03, 0x04})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs: []lcommon.TransactionInput{scriptInput},
		outputs: []lcommon.TransactionOutput{
			newBabbageInlineDatumOutput(t, newTestKeyAddress(t)),
		},
	}
	ls := newMockLedgerState()
	// The spent UTxO carries no datum; only the new output does.
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       scriptAddr,
		scriptRef:  plutusScript,
	})

	var inlineDatumErr lcommon.InlineDatumsNotSupportedError
	require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	), &inlineDatumErr)
}

// TestConwayInlineDatumRuleRejectsDatumOnReferenceInput covers an inline datum
// reachable only through a reference input.
func TestConwayInlineDatumRuleRejectsDatumOnReferenceInput(t *testing.T) {
	scriptInput := newTestInput(0x31, 0)
	refInput := newTestInput(0x32, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x05, 0x06})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs:          []lcommon.TransactionInput{scriptInput},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       scriptAddr,
		scriptRef:  plutusScript,
	})
	ls.addUtxo(refInput, newBabbageInlineDatumOutput(t, newTestKeyAddress(t)))

	var inlineDatumErr lcommon.InlineDatumsNotSupportedError
	require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	), &inlineDatumErr)
}

// TestConwayInlineDatumRuleRejectsNonSpendingPlutusV1 pins that the
// needed-script scan is not limited to spending purposes. In each case the
// PlutusV1 script is required by a non-spending purpose and the inline datum
// sits on an unrelated key-locked input. cardano-ledger maps the V1 context
// over every spent input once any V1 script runs, so these transactions are
// invalid.
func TestConwayInlineDatumRuleRejectsNonSpendingPlutusV1(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*testing.T, *mockConwayFeeTx, lcommon.PlutusV1Script)
	}{
		{name: "Minting", apply: applyMintPurpose},
		{name: "Certifying", apply: applyCertPurpose},
		{name: "Rewarding", apply: applyWithdrawalPurpose},
		{name: "Voting", apply: applyVotingPurpose},
		{name: "Proposing", apply: applyProposalPurpose},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keyInput := newTestInput(0x41, 0)
			plutusScript := lcommon.PlutusV1Script([]byte{0x07, 0x08})
			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					witnesses: &mockWitnessSet{
						plutusV1Scripts: []lcommon.PlutusV1Script{
							plutusScript,
						},
					},
				},
				inputs: []lcommon.TransactionInput{keyInput},
			}
			tc.apply(t, tx, plutusScript)
			ls := newMockLedgerState()
			// Key-locked input, so no spending purpose needs a script.
			ls.addUtxo(
				keyInput,
				newBabbageInlineDatumOutput(t, newTestKeyAddress(t)),
			)

			var inlineDatumErr lcommon.InlineDatumsNotSupportedError
			require.ErrorAs(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			), &inlineDatumErr)
		})
	}
}

func applyMintPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(s.Hash()): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	tx.assetMint = &assetMint
}

func applyCertPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	tx.certificates = []lcommon.Certificate{
		&lcommon.StakeDeregistrationCertificate{
			StakeCredential: lcommon.Credential{
				CredType:   lcommon.CredentialTypeScriptHash,
				Credential: lcommon.Blake2b224(s.Hash()),
			},
		},
	}
}

func applyWithdrawalPurpose(
	t *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	addr := newTestScriptStakeAddress(t, s)
	tx.withdrawals = map[*lcommon.Address]*big.Int{
		&addr: big.NewInt(1),
	}
}

func applyVotingPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	voter := lcommon.Voter{
		Type: lcommon.VoterTypeDRepScriptHash,
		Hash: s.Hash(),
	}
	tx.votingProcedures = lcommon.VotingProcedures{
		&voter: nil,
	}
}

func applyProposalPurpose(
	_ *testing.T,
	tx *mockConwayFeeTx,
	s lcommon.PlutusV1Script,
) {
	tx.proposalProcedures = []lcommon.ProposalProcedure{
		conway.ConwayProposalProcedure{
			PPGovAction: conway.ConwayGovAction{
				Type: uint(lcommon.GovActionTypeParameterChange),
				Action: &conway.ConwayParameterChangeGovAction{
					PolicyHash: s.Hash().Bytes(),
				},
			},
		},
	}
}

// TestConwayInlineDatumRuleIgnoresUnusedPlutusV1ReferenceScript is the case
// gouroboros fixed. An unrelated PlutusV1 reference script sits on a
// spent UTxO and another spent UTxO carries an inline datum, but no script
// purpose needs the V1 script, so the transaction is valid.
//
// The rule used to gate on *available* scripts and reject this shape, which
// turned an ordinary transaction into a permanent validation failure. This
// test once asserted that rejection against the then-current pin; the
// assertion is inverted here because the upstream rule now gates on needed
// scripts.
func TestConwayInlineDatumRuleIgnoresUnusedPlutusV1ReferenceScript(
	t *testing.T,
) {
	inlineInput := newTestInput(0x01, 0)
	scriptInput := newTestInput(0x02, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x01, 0x02})
	addr := newTestKeyAddress(t)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{witnesses: &mockWitnessSet{}},
		inputs:    []lcommon.TransactionInput{inlineInput, scriptInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(inlineInput, newBabbageInlineDatumOutput(t, addr))
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       addr,
		scriptRef:  plutusScript,
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleAcceptsUnusedPlutusV1WithNeededPlutusV2 is the
// accept path for a transaction that does need a Plutus script, so the
// needed-script scan runs on a non-empty set.
//
// The spending input is a PlutusV2 script address, so there is a real script
// purpose. A PlutusV1 script is also reachable -- in the witness set and as a
// reference script on a reference input -- but no purpose needs it, and an
// inline datum is present. Only the *needed* script is PlutusV2, so the
// transaction is valid. An implementation that scans available scripts instead
// of needed ones rejects this.
func TestConwayInlineDatumRuleAcceptsUnusedPlutusV1WithNeededPlutusV2(
	t *testing.T,
) {
	v2Input := newTestInput(0x11, 0)
	refInput := newTestInput(0x12, 0)
	v2Script := lcommon.PlutusV2Script([]byte{0x0a, 0x0b})
	unusedV1 := lcommon.PlutusV1Script([]byte{0x01, 0x02})

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{unusedV1},
				plutusV2Scripts: []lcommon.PlutusV2Script{v2Script},
			},
		},
		inputs:          []lcommon.TransactionInput{v2Input},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	// The spent UTxO carries the inline datum and is guarded by PlutusV2.
	ls.addUtxo(
		v2Input,
		newBabbageInlineDatumOutput(t, newTestScriptAddress(t, v2Script)),
	)
	// The unused PlutusV1 script is only reachable, never required.
	ls.addUtxo(refInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestKeyAddress(t),
		scriptRef:  unusedV1,
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleAllowsReferenceScriptOnOutput pins that a reference
// script on a produced output is not rejected. Conway's transTxOutV1 shadows
// Babbage's and drops the ReferenceScriptsNotSupported branch, checking only
// the inline datum, so a needed PlutusV1 script coexists legitimately with a
// produced output carrying a reference script.
func TestConwayInlineDatumRuleAllowsReferenceScriptOnOutput(t *testing.T) {
	input := newTestInput(0x51, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x0f, 0x10})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs: []lcommon.TransactionInput{input},
		outputs: []lcommon.TransactionOutput{
			testAddressScriptOutput{
				testOutput: newTestOutput(1_000_000),
				addr:       newTestKeyAddress(t),
				scriptRef:  plutusScript,
			},
		},
	}
	ls := newMockLedgerState()
	// No inline datum anywhere; the needed V1 script is the spending one.
	ls.addUtxo(input, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       scriptAddr,
		scriptRef:  plutusScript,
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleAllowsReferenceInputsWithNeededPlutusV1 pins that
// the mere presence of a reference input is not disqualifying. cardano-ledger's
// Babbage-era V1 instance rejects any reference input, but the Conway vector
// "UTXOS/can use reference scripts" expects success, so the Conway rule must
// look at inline datums only.
func TestConwayInlineDatumRuleAllowsReferenceInputsWithNeededPlutusV1(
	t *testing.T,
) {
	scriptInput := newTestInput(0x61, 0)
	refInput := newTestInput(0x62, 0)
	plutusScript := lcommon.PlutusV1Script([]byte{0x0b, 0x0c})
	scriptAddr := newTestScriptAddress(t, plutusScript)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
			},
		},
		inputs:          []lcommon.TransactionInput{scriptInput},
		referenceInputs: []lcommon.TransactionInput{refInput},
	}
	ls := newMockLedgerState()
	ls.addUtxo(scriptInput, testAddressScriptOutput{
		testOutput: newTestOutput(2_000_000),
		addr:       scriptAddr,
	})
	ls.addUtxo(refInput, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestKeyAddress(t),
	})

	require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{},
	))
}

// TestConwayInlineDatumRuleSkipsUnresolvableInput pins the rule's contract for
// an input that is not in the ledger state: defer to UtxoValidateBadInputsUtxo,
// which reports it with the right error, rather than becoming a second source
// of input-resolution failures.
//
// The transaction would otherwise be rejected: its first input is a PlutusV1
// script address carrying an inline datum, so a rule that resolved what it
// could and carried on would return InlineDatumsNotSupportedError here.
func TestConwayInlineDatumRuleSkipsUnresolvableInput(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing func(*mockConwayFeeTx, lcommon.TransactionInput)
	}{
		{
			name: "Input",
			missing: func(
				tx *mockConwayFeeTx,
				in lcommon.TransactionInput,
			) {
				tx.inputs = append(tx.inputs, in)
			},
		},
		{
			name: "ReferenceInput",
			missing: func(
				tx *mockConwayFeeTx,
				in lcommon.TransactionInput,
			) {
				tx.referenceInputs = append(tx.referenceInputs, in)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scriptInput := newTestInput(0x71, 0)
			missingInput := newTestInput(0x72, 0)
			plutusScript := lcommon.PlutusV1Script([]byte{0x0d, 0x0e})
			scriptAddr := newTestScriptAddress(t, plutusScript)

			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					witnesses: &mockWitnessSet{
						plutusV1Scripts: []lcommon.PlutusV1Script{
							plutusScript,
						},
					},
				},
				inputs: []lcommon.TransactionInput{scriptInput},
			}
			tc.missing(tx, missingInput)
			ls := newMockLedgerState()
			ls.addUtxo(
				scriptInput,
				newBabbageInlineDatumOutput(t, scriptAddr),
			)
			// missingInput is deliberately absent from the ledger state.

			require.NoError(t, conway.UtxoValidateInlineDatumsWithPlutusV1(
				tx,
				0,
				ls,
				&conway.ConwayProtocolParameters{},
			))
		})
	}
}

// TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch is the Conway
// counterpart to TestPlutusBudgetComparisonIncludesFinalSlippageBatch.
//
// evaluateConwayPlutusScript is where this change did the most: the flag
// selecting restrictive mode was renamed, and the suppression of the CEK
// machine's trailing slippage flush was dropped from each of the V1, V2 and
// V3 language branches. The Alonzo and Babbage cases cannot reach it, and
// the immutable corpus that measured this change contains no Conway Plutus
// evaluations at all, so the path that changed most had no guard.
//
// The purpose is minting rather than spending on purpose. A V1 minting
// script is applied to two arguments, redeemer and script context, which is
// the same shape the Alonzo and Babbage cases evaluate. That keeps the
// expected cost equal to the Haskell-derived 112100 CPU / 800 memory those
// cases assert, instead of a third figure whose only provenance is this
// implementation's own output. Spending would require a datum under Conway
// and so a three-argument program, whose cost nothing external pins.
//
// The declared budget is zero on purpose. Restrictive validation runs the
// script against the protocol transaction budget and compares afterwards, so
// the entire measured cost appears in the overage — including the trailing
// batch the Haskell machine flushes on a successful return. Under-reporting
// that batch lowers these figures and fails this test.
func TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch(t *testing.T) {
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

	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	// Clear the phase-1 rule set so the phase-2 budget comparison is what
	// fails, rather than a fee or UTxO check on the mock transaction.
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	plutusScript := lcommon.PlutusV1Script(scriptBytes)
	scriptHash := plutusScript.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
				redeemers: &mockRedeemers{
					entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagMint,
								Index: 0,
							},
							val: lcommon.RedeemerValue{
								ExUnits: lcommon.ExUnits{},
							},
						},
					},
				},
			},
		},
		assetMint: &assetMint,
	}

	err = ValidateTxConway(
		tx,
		0,
		newMockLedgerState(),
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  1_000_000,
				Memory: 1_000_000,
			},
			CostModels: map[uint][]int64{
				0: defaultMachineCostModel(t, lang.LanguageVersionV1),
			},
		},
	)
	require.Error(t, err)

	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	assert.Equal(t, lcommon.RedeemerTagMint, plutusErr.Tag)
	assert.Equal(t, uint32(0), plutusErr.Index)
	assert.Contains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget: used (112100 cpu, 800 mem)",
	)

	t.Run(
		"restrictive evaluation is capped by protocol transaction budget",
		func(t *testing.T) {
			err := ValidateTxConway(
				tx,
				0,
				newMockLedgerState(),
				&conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: 9,
					},
					MaxTxExUnits: lcommon.ExUnits{
						Steps:  1_000,
						Memory: 100,
					},
					CostModels: map[uint][]int64{
						0: defaultMachineCostModel(t, lang.LanguageVersionV1),
					},
				},
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "out of budget")
		},
	)

	t.Run(
		"missing cost model fails closed instead of reaching evaluation",
		func(t *testing.T) {
			// A protocol-parameters map that never populated
			// the PlutusV1 entry (e.g. a hard-fork/governance update, or a
			// malformed genesis) must return a configuration error rather
			// than silently evaluating the script under plutigo's built-in
			// default cost model. CostModels is nil here -- not merely
			// present-but-empty, as the other subtests use -- reproducing a
			// genuinely missing entry.
			err := ValidateTxConway(
				tx,
				0,
				newMockLedgerState(),
				&conway.ConwayProtocolParameters{
					ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
						Major: 9,
					},
					MaxTxExUnits: lcommon.ExUnits{
						Steps:  1_000_000,
						Memory: 1_000_000,
					},
				},
			)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
		},
	)
}

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

// Mainnet parameters for the fixture's epoch. The transaction is in epoch 653;
// epochs 652 and 653 carry an identical protocol version and cost model, so one
// parameter file serves the block and its inputs.
const (
	mainnetFixtureTxFile    = "mainnet-conway-tx-2d295dbf.cbor"
	mainnetFixtureInputFile = "mainnet-conway-inputs-2d295dbf.cbor"
	mainnetFixtureCostFile  = "mainnet-costmodels-pv11-epoch653.json"
	mainnetFixtureTxId      = "2d295dbffc2898b14ccc9b359a342305006fdcf6ccd7907b444419520a98f351"
	mainnetFixtureSlot      = 196_783_015
	mainnetProtoMajor       = 11
	mainnetProtoMinor       = 0
	mainnetMaxTxExMem       = 16_500_000
	mainnetMaxTxExSteps     = 10_000_000_000

	// Mainnet Shelley genesis: systemStart 1506203091, a Byron prefix of
	// 4492800 20-second slots, 1-second slots afterwards.
	mainnetSystemStart   = 1_506_203_091
	mainnetByronSlots    = 4_492_800
	mainnetByronSlotSecs = 20
)

// mainnetFixtureLedgerState gives the mock ledger state mainnet's real
// slot/time conversion, so the script context carries the same validity
// range in POSIX milliseconds that the block producer's evaluator saw.
type mainnetFixtureLedgerState struct {
	*mockLedgerState
}

type cancelingEvaluationLedgerState struct {
	mainnetFixtureLedgerState
	ctx    context.Context
	cancel context.CancelFunc
	reads  int
}

type cancelDuringCekContext struct {
	checks int
	limit  int
	done   chan struct{}
}

func (*cancelDuringCekContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *cancelDuringCekContext) Done() <-chan struct{} { return c.done }

func (c *cancelDuringCekContext) Err() error {
	c.checks++
	if c.checks == c.limit {
		close(c.done)
	}
	if c.checks >= c.limit {
		return context.Canceled
	}
	return nil
}

func (*cancelDuringCekContext) Value(any) any { return nil }

type activeEvaluationLedgerState struct {
	*mockLedgerState
	ctx context.Context
}

func (ls *activeEvaluationLedgerState) EvaluationContext() context.Context {
	return ls.ctx
}

func (ls *cancelingEvaluationLedgerState) EvaluationContext() context.Context {
	return ls.ctx
}

func (ls *cancelingEvaluationLedgerState) UtxoById(
	input lcommon.TransactionInput,
) (lcommon.Utxo, error) {
	ls.reads++
	utxo, err := ls.mainnetFixtureLedgerState.UtxoById(input)
	ls.cancel()
	return utxo, err
}

func (mainnetFixtureLedgerState) SlotToTime(slot uint64) (time.Time, error) {
	if slot < mainnetByronSlots {
		return time.Unix(
			mainnetSystemStart+int64(slot)*mainnetByronSlotSecs,
			0,
		).UTC(), nil
	}
	byronEnd := int64(mainnetSystemStart) +
		int64(mainnetByronSlots)*mainnetByronSlotSecs
	return time.Unix(byronEnd+int64(slot-mainnetByronSlots), 0).UTC(), nil
}

func (mainnetFixtureLedgerState) TimeToSlot(t time.Time) (uint64, error) {
	byronEnd := int64(mainnetSystemStart) +
		int64(mainnetByronSlots)*mainnetByronSlotSecs
	if t.Unix() < byronEnd {
		return uint64(
			(t.Unix() - mainnetSystemStart) / mainnetByronSlotSecs,
		), nil
	}
	return mainnetByronSlots + uint64(t.Unix()-byronEnd), nil
}

func mainnetFixtureProtocolParams(t *testing.T) *conway.ConwayProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV1 []int64 `json:"PlutusV1"`
		PlutusV2 []int64 `json:"PlutusV2"`
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(t, json.Unmarshal(
		readErasFixture(t, mainnetFixtureCostFile),
		&costModels,
	))
	require.Len(t, costModels.PlutusV3, 350)
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: mainnetProtoMajor,
			Minor: mainnetProtoMinor,
		},
		CostModels: map[uint][]int64{
			0: costModels.PlutusV1,
			1: costModels.PlutusV2,
			2: costModels.PlutusV3,
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: mainnetMaxTxExMem,
			Steps:  mainnetMaxTxExSteps,
		},
	}
}

// mainnetFixtureInputTxs decodes the transactions that funded the fixture
// transaction's inputs and reference inputs.
func mainnetFixtureInputTxs(t *testing.T) []*conway.ConwayTransaction {
	t.Helper()
	var inputTxBytes [][]byte
	_, err := cbor.Decode(
		readErasFixture(t, mainnetFixtureInputFile),
		&inputTxBytes,
	)
	require.NoError(t, err)
	ret := make([]*conway.ConwayTransaction, 0, len(inputTxBytes))
	for _, raw := range inputTxBytes {
		inputTx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(t, err)
		ret = append(ret, inputTx)
	}
	return ret
}

// TestValidateTxPlutusConwayMainnetStorageDecodedUtxos replays a canonical
// mainnet transaction through phase-2 validation with its inputs resolved the
// way the node resolves them: from stored output CBOR re-decoded by
// ledger.NewTransactionOutputFromCbor, which is what database/models.Utxo.Decode
// calls. Resolving inputs from block-decoded outputs instead bypasses that
// decode and cannot observe a rendering that depends on the concrete output
// type it selects.
//
// One of the fixture's inputs uses the legacy array output encoding carrying a
// datum hash, which that decoder resolves to *alonzo.AlonzoTransactionOutput.
// A rendering of that type that drops the datum hash puts NoOutputDatum in the
// PlutusV3 script context, and the transaction's withdrawal validator calls
// Plutus `error` on it, rejecting a block the network accepted. See
// and blinklabs-io/gouroboros.
func TestValidateTxPlutusConwayMainnetStorageDecodedUtxos(t *testing.T) {
	pp := mainnetFixtureProtocolParams(t)
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, mainnetFixtureTxFile),
	)
	require.NoError(t, err)
	require.Equal(t, mainnetFixtureTxId, tx.Hash().String())
	require.True(t, tx.IsValid())

	ls := mainnetFixtureLedgerState{mockLedgerState: newMockLedgerState()}
	ls.networkId = uint(lcommon.AddressNetworkMainnet)
	for _, inputTx := range mainnetFixtureInputTxs(t) {
		for idx, output := range inputTx.Outputs() {
			stored, err := gledger.NewTransactionOutputFromCbor(output.Cbor())
			require.NoError(
				t,
				err,
				"decode stored output %s#%d",
				inputTx.Hash().String(),
				idx,
			)
			ls.addUtxo(
				shelley.NewShelleyTransactionInput(
					inputTx.Hash().String(),
					idx,
				),
				stored,
			)
		}
	}

	// The network accepted this transaction, so every script in it must
	// succeed within its declared execution budget.
	require.NoError(
		t,
		ValidateTxPlutusConway(tx, mainnetFixtureSlot, ls, pp),
	)

	// cardano-node computed the declared budgets with the reference
	// evaluator, so equality in both directions catches an overcharge, which
	// rejects a block the network accepted, and an undercharge, which accepts
	// a transaction the network rejects.
	_, _, redeemerExUnits, err := EvaluateTxConway(tx, ls, pp)
	require.NoError(t, err)
	declared := map[lcommon.RedeemerKey]lcommon.ExUnits{}
	for key, value := range tx.Witnesses().Redeemers().Iter() {
		declared[key] = value.ExUnits
	}
	require.NotEmpty(t, declared)
	require.Equal(
		t,
		declared,
		redeemerExUnits,
		"evaluated execution units must equal the "+
			"producer-declared budget exactly",
	)
}

// TestMainnetFixtureStorageDecodePreservesScriptContextRendering pins the
// invariant the phase-2 replay depends on for every output in the fixture, not
// only the one input whose validator noticed: re-decoding a stored output must
// render the same script-context PlutusData as the block-decoded output it was
// stored from. The concrete type the decoder selects may differ from the
// block's era type; the rendering may not.
func TestMainnetFixtureStorageDecodePreservesScriptContextRendering(t *testing.T) {
	for _, inputTx := range mainnetFixtureInputTxs(t) {
		for idx, output := range inputTx.Outputs() {
			ref := fmt.Sprintf("%s#%d", inputTx.Hash().String(), idx)
			stored, err := gledger.NewTransactionOutputFromCbor(output.Cbor())
			require.NoError(t, err, "decode stored output %s", ref)
			want, err := data.Encode(output.ToPlutusData())
			require.NoError(t, err, "encode block-decoded output %s", ref)
			got, err := data.Encode(stored.ToPlutusData())
			require.NoError(t, err, "encode stored output %s", ref)
			require.Equal(
				t,
				hex.EncodeToString(want),
				hex.EncodeToString(got),
				"stored output %s re-decoded as %T must render the same "+
					"script-context PlutusData as the block-decoded %T",
				ref,
				stored,
				output,
			)
		}
	}
}

// Preprod protocol parameters for both fixture blocks. Epochs 309 and 310
// carry an identical PlutusV3 cost model, so one parameter file serves both.
const (
	preprodCostModelsFile = "preprod-costmodels-plutusv3-pv11.json"
	preprodPlutusV3Params = 350
	preprodProtoMajor     = 11
	preprodProtoMinor     = 0
	preprodMaxTxExMem     = 17_500_000
	preprodMaxTxExSteps   = 10_000_000_000

	// Preprod Shelley genesis: systemStart 1654041600, a Byron prefix of four
	// epochs of 21600 20-second slots, 1-second slots afterwards.
	preprodSystemStart   = 1_654_041_600
	preprodByronSlots    = 86_400
	preprodByronSlotSecs = 20
)

// preprodPlutusFixture is a producer-accepted Preprod block and the
// transactions that funded the inputs and reference inputs of one Plutus
// transaction inside it, both taken from the chain over NtN blockfetch.
type preprodPlutusFixture struct {
	name       string
	blockFile  string
	inputsFile string
	txId       string
}

// Both transactions declare invalidHereafter and no invalidBefore, so their
// script context validity interval has a finite upper bound and no lower
// bound. cardano-ledger's Conway translation encodes that upper bound
// EXCLUSIVE; encoding it CLOSED changes what every validator reading
// txInfoValidRange sees.
var preprodPlutusFixtures = []preprodPlutusFixture{
	{
		// Hydra Head V2 increment. The affected spending validator still
		// succeeds under a CLOSED upper bound but takes a longer path: two
		// extra equalsInteger calls, two extra ifThenElse calls and 24 extra
		// CEK machine steps, for 640764 CPU and 2404 memory over the declared
		// budget. The transaction's other redeemer does not read the validity
		// range and is unaffected, so it holds the era gating to the one
		// value that changed.
		name:       "hydra_head_v2_increment_slot_132228934",
		blockFile:  "preprod-conway-block-132228934.cbor",
		inputsFile: "preprod-conway-inputs-132228934.cbor",
		txId:       "d81392f4def652323c5648067ffbe6d45812e415a53d867336aa5026db9ea2eb",
	},
	{
		// Midgard hub oracle mint. Under a CLOSED upper bound the minting
		// validator does not merely cost more, it calls Plutus `error`, so the
		// producer-accepted block is rejected outright rather than for a
		// budget overage.
		name:       "midgard_hub_oracle_mint_slot_132657325",
		blockFile:  "preprod-conway-block-132657325.cbor",
		inputsFile: "preprod-conway-inputs-132657325.cbor",
		txId:       "5728f55704a202ce7c627d59eac086ee7a756fc61e8951855accbaf137f677e7",
	},
}

// preprodLedgerState gives the mock ledger state preprod's real slot/time
// conversion. The script context carries the transaction's validity range as
// POSIX milliseconds, so a placeholder conversion would not reproduce the
// bytes the block producer's evaluator saw.
type preprodLedgerState struct {
	*mockLedgerState
}

func (preprodLedgerState) SlotToTime(slot uint64) (time.Time, error) {
	if slot < preprodByronSlots {
		return time.Unix(
			preprodSystemStart+int64(slot)*preprodByronSlotSecs,
			0,
		).UTC(), nil
	}
	byronEnd := int64(preprodSystemStart) +
		int64(preprodByronSlots)*preprodByronSlotSecs
	return time.Unix(byronEnd+int64(slot-preprodByronSlots), 0).UTC(), nil
}

func (preprodLedgerState) TimeToSlot(t time.Time) (uint64, error) {
	byronEnd := int64(preprodSystemStart) +
		int64(preprodByronSlots)*preprodByronSlotSecs
	if t.Unix() < byronEnd {
		return uint64(
			(t.Unix() - preprodSystemStart) / preprodByronSlotSecs,
		), nil
	}
	return preprodByronSlots + uint64(t.Unix()-byronEnd), nil
}

func readErasFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return raw
}

func preprodFixtureProtocolParams(
	t *testing.T,
) *conway.ConwayProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(t, json.Unmarshal(
		readErasFixture(t, preprodCostModelsFile),
		&costModels,
	))
	require.Len(t, costModels.PlutusV3, preprodPlutusV3Params)
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: preprodProtoMajor,
			Minor: preprodProtoMinor,
		},
		CostModels: map[uint][]int64{2: costModels.PlutusV3},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: preprodMaxTxExMem,
			Steps:  preprodMaxTxExSteps,
		},
	}
}

// TestEvaluateTxConwayPreprodFixtures pins the execution units of real
// producer-accepted Preprod transactions against the budgets their producers
// declared. cardano-node computed those budgets with the reference evaluator,
// so each is an external oracle: equality in both directions catches an
// overcharge, which rejects a block the network accepted, and an undercharge,
// which accepts a transaction the network rejects.
func TestEvaluateTxConwayPreprodFixtures(t *testing.T) {
	pp := preprodFixtureProtocolParams(t)
	for _, fixture := range preprodPlutusFixtures {
		t.Run(fixture.name, func(t *testing.T) {
			blk, err := gledger.NewBlockFromCbor(
				gledger.BlockTypeConway,
				readErasFixture(t, fixture.blockFile),
			)
			require.NoError(t, err)

			var tx lcommon.Transaction
			for _, candidate := range blk.Transactions() {
				if candidate.Hash().String() == fixture.txId {
					tx = candidate
				}
			}
			require.NotNil(
				t,
				tx,
				"fixture block must contain %s",
				fixture.txId,
			)

			var inputTxBytes [][]byte
			_, err = cbor.Decode(
				readErasFixture(t, fixture.inputsFile),
				&inputTxBytes,
			)
			require.NoError(t, err)

			ls := preprodLedgerState{mockLedgerState: newMockLedgerState()}
			ls.networkId = uint(lcommon.AddressNetworkTestnet)
			for _, raw := range inputTxBytes {
				inputTx, err := conway.NewConwayTransactionFromCbor(raw)
				require.NoError(t, err)
				for idx, output := range inputTx.Outputs() {
					ls.addUtxo(
						shelley.NewShelleyTransactionInput(
							inputTx.Hash().String(),
							idx,
						),
						output,
					)
				}
			}

			_, _, redeemerExUnits, err := EvaluateTxConway(tx, ls, pp)
			require.NoError(t, err)

			declared := map[lcommon.RedeemerKey]lcommon.ExUnits{}
			for key, value := range tx.Witnesses().Redeemers().Iter() {
				declared[key] = value.ExUnits
			}
			require.NotEmpty(t, declared)
			require.Equal(
				t,
				declared,
				redeemerExUnits,
				"evaluated execution units must equal the "+
					"producer-declared budget exactly",
			)
		})
	}
}

const (
	preprodSerialiseDataTxFile     = "preprod-conway-tx-133016611.cbor"
	preprodSerialiseDataInputsFile = "preprod-conway-inputs-133016611.cbor"

	preprodSerialiseDataTxId = "2c528f4e28e2b8fd47539fe51eca8b7dafced3d1b82f48edf4a57e075b525ff5"

	// The policy this transaction mints under. It names its asset
	// blake2b_256(serialiseData(the seed TxOutRef carried in the redeemer)).
	preprodSerialiseDataPolicy = "22f3deb8008f5843205e0bd52f912bd3ee546238c95cfeae94bf7edd"

	// The asset name the chain actually minted: the hash of the
	// INDEFINITE-length encoding of that TxOutRef, which is what the Plutus
	// reference encoder writes. The redeemer is definite-encoded on the wire,
	// so rendering the wire encoding into the script context makes the policy
	// compute 5e13e57434d51b2bf8a3693608a308936b206485ca9bc405dbee44f6af21668b
	// instead, and call error.
	preprodSerialiseDataAssetName = "c714816533babf58a158870eaa49db187e1e1f83f67579070d2025974129a858"
)

// preprodSerialiseDataFundingTxIds funded the three spent inputs and the two
// reference inputs, in fixture order. One reference input carries the PlutusV3
// minting policy as a reference script, the other the config datum it reads.
var preprodSerialiseDataFundingTxIds = []string{
	"3fd10d2c06901f5440c23f1dd757c8ed61679a064e5975cbf2e2b571fd3da0d5",
	"edb451ce575ceaa1190189b91afe67d197460f1dc4f0c67f2fc32ee208cd810d",
	"eca0c28d10de6e36c7c671fd6260713f845d41c5430befefa529f03902658304",
	"100b74c93a22172ed2fa7ad8a4a4b6d72b2538f14a19414f8684c941cea75b6f",
}

// TestEvaluateTxConwayPreprodSerialiseData replays preprod transaction
// 2c528f4e... (slot 133016611, epoch 311, protocol version 11), whose minting
// policy names its asset after blake2b_256(serialiseData(seed TxOutRef)).
//
// cardano-ledger rebuilds every script-visible value rather than carrying the
// transaction CBOR into the script context, so a script always observes the
// indefinite-length field list the Plutus encoder writes. Passing the
// definite-length wire encoding through instead changes what serialiseData
// returns, so the policy computed a different asset name than the network
// minted, called error, and wedged a preprod replay at this block.
func TestEvaluateTxConwayPreprodSerialiseData(t *testing.T) {
	t.Parallel()
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, preprodSerialiseDataTxFile),
	)
	require.NoError(t, err)
	require.Equal(t, preprodSerialiseDataTxId, tx.Hash().String())
	require.True(t, tx.IsValid())

	mint := tx.AssetMint()
	require.NotNil(t, mint)
	policies := mint.Policies()
	require.Len(t, policies, 1)
	require.Equal(t, preprodSerialiseDataPolicy, policies[0].String())
	assetNames := mint.Assets(policies[0])
	require.Len(t, assetNames, 1)
	require.Equal(
		t,
		preprodSerialiseDataAssetName,
		hex.EncodeToString(assetNames[0]),
		"fixture must carry the asset name the network minted",
	)

	var inputTxBytes [][]byte
	_, err = cbor.Decode(
		readErasFixture(t, preprodSerialiseDataInputsFile),
		&inputTxBytes,
	)
	require.NoError(t, err)
	require.Len(t, inputTxBytes, len(preprodSerialiseDataFundingTxIds))

	ls := preprodLedgerState{mockLedgerState: newMockLedgerState()}
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	for idx, raw := range inputTxBytes {
		inputTx, err := conway.NewConwayTransactionFromCbor(raw)
		require.NoError(t, err)
		require.Equal(
			t,
			preprodSerialiseDataFundingTxIds[idx],
			inputTx.Hash().String(),
		)
		for outputIdx, output := range inputTx.Outputs() {
			input := shelley.NewShelleyTransactionInput(
				inputTx.Hash().String(),
				outputIdx,
			)
			ls.addUtxo(&input, output)
		}
	}

	_, _, redeemerExUnits, err := EvaluateTxConway(
		tx,
		ls,
		preprodFixtureProtocolParams(t),
	)
	require.NoError(
		t,
		err,
		"the network accepted this transaction, so its minting policy must succeed",
	)

	declared := map[lcommon.RedeemerKey]lcommon.ExUnits{}
	for key, value := range tx.Witnesses().Redeemers().Iter() {
		declared[key] = value.ExUnits
	}
	require.Len(t, declared, 1)
	require.Len(t, redeemerExUnits, 1)
	for key, used := range redeemerExUnits {
		budget, ok := declared[key]
		require.True(
			t,
			ok,
			"evaluated a redeemer the transaction does not declare",
		)
		require.LessOrEqual(t, used.Steps, budget.Steps)
		require.LessOrEqual(t, used.Memory, budget.Memory)
	}

	// Block validation constructs its own redeemer rather than using the
	// evaluator's TxInfo. Exercise that entry point with the same wire data.
	require.NoError(t, ValidateTxPlutusConway(
		tx,
		133016611,
		ls,
		preprodFixtureProtocolParams(t),
	))
}

// TestConwayPhase2RejectsGenuineScriptFailureNotAsBudgetOverage covers the
// discriminator between the two ways phase-2 can reject a Conway transaction.
//
// TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch pins the overage
// arm: a script that succeeds but costs more than its redeemer declares is
// rejected with "script exceeded declared budget". This is the other arm. A
// script that evaluates to error is also rejected, but for a different reason,
// and the two must not be conflated.
//
// The distinction is load-bearing because restrictive evaluation raises the
// machine limit to MaxTxExUnits and compares the consumed amount against the
// declared budget only *after* the script returns. A genuine evaluation error
// returns before that comparison, so it must surface as the Plutus failure it
// is. Reporting it as a budget overage would misattribute a script bug to the
// fee the submitter declared, and would make an over-generous declared budget
// look like the cause of a failure it cannot affect.
//
// The script is a V1 minting script applied to two arguments, redeemer and
// script context, matching the shape the overage test uses so both arms
// exercise the same production path: ValidateTxConway ->
// validateTxPlutusConwayWithContext -> evaluateConwayPlutusScript.
func TestConwayPhase2RejectsGenuineScriptFailureNotAsBudgetOverage(
	t *testing.T,
) {
	// (redeemer, ctx) -> Error. The program is well formed and fully applied,
	// so it reaches the CEK machine and fails there rather than failing to
	// decode.
	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Lambda[syn.DeBruijn]{
				Body: &syn.Error{},
			},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)

	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	// Clear the phase-1 rule set so the phase-2 outcome is what fails, rather
	// than a fee or UTxO check on the mock transaction.
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	plutusScript := lcommon.PlutusV1Script(scriptBytes)
	scriptHash := plutusScript.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)

	// The declared budget is deliberately far above the script's cost, so a
	// budget overage cannot be the reason for rejection.
	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			txType: txTypeAlonzo,
			witnesses: &mockWitnessSet{
				plutusV1Scripts: []lcommon.PlutusV1Script{plutusScript},
				redeemers: &mockRedeemers{
					entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagMint,
								Index: 0,
							},
							val: lcommon.RedeemerValue{
								ExUnits: lcommon.ExUnits{
									Steps:  1_000_000,
									Memory: 1_000_000,
								},
							},
						},
					},
				},
			},
		},
		assetMint: &assetMint,
	}

	err = ValidateTxConway(
		tx,
		0,
		newMockLedgerState(),
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  1_000_000,
				Memory: 1_000_000,
			},
			CostModels: map[uint][]int64{
				0: defaultMachineCostModel(t, lang.LanguageVersionV1),
			},
		},
	)
	require.Error(t, err, "a genuine Plutus failure must be rejected")

	var plutusErr conway.PlutusScriptFailedError
	require.ErrorAs(t, err, &plutusErr)
	assert.Equal(t, scriptHash, plutusErr.ScriptHash)
	assert.Equal(t, lcommon.RedeemerTagMint, plutusErr.Tag)
	assert.Equal(t, uint32(0), plutusErr.Index)
	// The wrapped error is what distinguishes the two rejection reasons, so a
	// nil here is itself a defect; assert it rather than panicking on the
	// dereference below.
	require.NotNil(t, plutusErr.Err)
	assert.NotContains(
		t,
		plutusErr.Err.Error(),
		"script exceeded declared budget",
		"a genuine script failure must not be reported as a budget overage",
	)
}

// previewOracleBlockContext is the off-chain state a canonical preview block
// needs before its transaction can be re-validated: the resolved inputs and
// reference inputs, the era's cost models and execution unit ceiling, and the
// network's slot-to-time mapping.
type previewOracleBlockContext struct {
	SystemStartUnix int64  `json:"systemStartUnix"`
	ProtocolVersion []uint `json:"protocolVersion"`
	MaxTxExUnits    struct {
		Memory int64 `json:"memory"`
		Steps  int64 `json:"steps"`
	} `json:"maxTxExUnits"`
	CostModels    map[string][]int64 `json:"costModels"`
	ResolvedUtxos map[string]string  `json:"resolvedUtxos"`
}

// TestValidateTxPlutusConwayPreviewWithdrawalOracle re-validates preview block
// 4625519 (slot 121707875), whose single transaction
// e8691d7c4003815928bc9de9017a3388d7fc0a168383c08d14633732a7c0bb33 carries a
// Plutus V3 withdrawal oracle validator that verifies an Ed25519 signature over
// serialiseData of a payload containing integers with 101-byte magnitudes. A
// serialiseData encoding that does not chunk a bignum magnitude makes that
// validator return "error explicitly called", which rejects a canonical block
// and freezes the tip.
func TestValidateTxPlutusConwayPreviewWithdrawalOracle(t *testing.T) {
	raw, err := os.ReadFile(
		filepath.Join("testdata", "preview-block-121707875.cbor"),
	)
	require.NoError(t, err)
	blk, err := gledger.NewBlockFromCbor(conway.BlockTypeConway, raw)
	require.NoError(t, err)
	txs := blk.Transactions()
	require.Len(t, txs, 1)
	tx := txs[0]
	require.Equal(
		t,
		"e8691d7c4003815928bc9de9017a3388d7fc0a168383c08d14633732a7c0bb33",
		tx.Hash().String(),
	)
	require.True(t, tx.IsValid())

	ctxRaw, err := os.ReadFile(
		filepath.Join("testdata", "preview-block-121707875-context.json"),
	)
	require.NoError(t, err)
	var blockCtx previewOracleBlockContext
	require.NoError(t, json.Unmarshal(ctxRaw, &blockCtx))
	require.Len(t, blockCtx.ProtocolVersion, 2)

	ls := newMockLedgerState()
	ls.networkId = uint(lcommon.AddressNetworkTestnet)
	systemStart := blockCtx.SystemStartUnix
	ls.slotToTime = func(slot uint64) (time.Time, error) {
		return time.Unix(int64(slot)+systemStart, 0).UTC(), nil
	}
	for ref, outputHex := range blockCtx.ResolvedUtxos {
		txId, idx, found := strings.Cut(ref, "#")
		require.True(t, found, "malformed utxo reference %q", ref)
		outputIdx, err := strconv.Atoi(idx)
		require.NoError(t, err)
		outputCbor, err := hex.DecodeString(outputHex)
		require.NoError(t, err)
		var output babbage.BabbageTransactionOutput
		_, err = cbor.Decode(outputCbor, &output)
		require.NoError(t, err, "decode output %s", ref)
		input := shelley.NewShelleyTransactionInput(txId, outputIdx)
		ls.addUtxo(&input, &output)
	}
	require.Len(t, ls.utxos, len(tx.Inputs())+len(tx.ReferenceInputs()))

	costModels := make(map[uint][]int64, len(blockCtx.CostModels))
	for language, model := range blockCtx.CostModels {
		languageId, err := strconv.ParseUint(language, 10, 8)
		require.NoError(t, err)
		costModels[uint(languageId)] = model
	}
	pp := &conway.ConwayProtocolParameters{
		CostModels: costModels,
		MaxTxExUnits: lcommon.ExUnits{
			Memory: blockCtx.MaxTxExUnits.Memory,
			Steps:  blockCtx.MaxTxExUnits.Steps,
		},
	}
	pp.ProtocolVersion.Major = blockCtx.ProtocolVersion[0]
	pp.ProtocolVersion.Minor = blockCtx.ProtocolVersion[1]

	// The network accepted this block, so every script in it must succeed
	// within its declared execution budget.
	require.NoError(t, ValidateTxPlutusConway(tx, blk.SlotNumber(), ls, pp))

	// A correct serialiseData encoding must still reject a payload the oracle
	// key did not sign, so flipping one bit of the redeemer's signature has to
	// fail the same validator.
	tamperedTxCbor := make([]byte, len(tx.Cbor()))
	copy(tamperedTxCbor, tx.Cbor())
	sigOffset := bytes.Index(tamperedTxCbor, oracleSignature)
	require.NotEqual(t, -1, sigOffset, "oracle signature not found in tx")
	tamperedTxCbor[sigOffset] ^= 0x01
	tamperedTx, err := conway.NewConwayTransactionFromCbor(tamperedTxCbor)
	require.NoError(t, err)
	err = ValidateTxPlutusConway(tamperedTx, blk.SlotNumber(), ls, pp)
	require.ErrorContains(
		t,
		err,
		"plutus script failed (hash=473da51f9b910d257655e18d57ee6454ee345cb8d674d120ffd8f9c1, tag=3, index=0): execute script: error explicitly called",
	)
}

// oracleSignature is the Ed25519 signature carried by the reward redeemer at
// index 0 of the fixture block's transaction.
var oracleSignature = mustDecodeHex(
	"cfe0ef14f622ba7da09f40f815e61649437e2f1aa3d03567fde5a7c897adb693" +
		"87aa55dab9d23b7b42a2c8ee63f1bf997da7672166582865d12dc44515733d07",
)

func mustDecodeHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

// TestConwayFeaturesRuleDistinguishesAbsentFromDeclaredZeroTreasury drives the
// production Conway rule -- resolved out of the composed conwayUtxoValidationRules
// slice, not called bare -- with real decoded Conway transactions.
//
// A declared current treasury value of zero is an assertion about the treasury
// and is a Conway-only feature; an absent key 21 is not. The pinned gouroboros
// release stores key 21 in an int64 with omitempty and returns a non-nil zero
// for both, so a Sign() > 0 test collapses the two states and lets a
// transaction declare a zero treasury alongside a needed PlutusV1/V2 script.
func TestConwayFeaturesRuleDistinguishesAbsentFromDeclaredZeroTreasury(
	t *testing.T,
) {
	for _, tc := range []struct {
		name          string
		script        lcommon.Script
		plutusVersion string
	}{
		{
			name:          "PlutusV1",
			script:        lcommon.PlutusV1Script{0x05},
			plutusVersion: "PlutusV1",
		},
		{
			name:          "PlutusV2",
			script:        lcommon.PlutusV2Script{0x06},
			plutusVersion: "PlutusV2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := conwayFeaturesRule(t)

			// Absent key 21: accepted.
			absentTx, absentInput := decodeConwayTreasuryTx(t, tc.script, nil)
			require.NoError(t, rule(
				absentTx,
				0,
				newConwayTreasuryLedgerState(t, absentInput, tc.script),
				&conway.ConwayProtocolParameters{},
			))

			// Declared zero: rejected, same as any other declared value.
			zero := int64(0)
			zeroTx, zeroInput := decodeConwayTreasuryTx(t, tc.script, &zero)
			var zeroErr conway.CurrentTreasuryValueWithPlutusV1V2Error
			require.ErrorAs(t, rule(
				zeroTx,
				0,
				newConwayTreasuryLedgerState(t, zeroInput, tc.script),
				&conway.ConwayProtocolParameters{},
			), &zeroErr)
			assert.Equal(t, tc.plutusVersion, zeroErr.PlutusVersion)

			// Declared non-zero: rejected.
			nonZero := int64(42)
			nonZeroTx, nonZeroInput := decodeConwayTreasuryTx(
				t,
				tc.script,
				&nonZero,
			)
			var nonZeroErr conway.CurrentTreasuryValueWithPlutusV1V2Error
			require.ErrorAs(t, rule(
				nonZeroTx,
				0,
				newConwayTreasuryLedgerState(t, nonZeroInput, tc.script),
				&conway.ConwayProtocolParameters{},
			), &nonZeroErr)
			assert.Equal(t, tc.plutusVersion, nonZeroErr.PlutusVersion)
		})
	}
}

func newConwayTreasuryLedgerState(
	t *testing.T,
	input shelley.ShelleyTransactionInput,
	s lcommon.Script,
) *mockLedgerState {
	t.Helper()
	ls := newMockLedgerState()
	ls.addUtxo(input, testAddressScriptOutput{
		testOutput: newTestOutput(1_000_000),
		addr:       newTestScriptAddress(t, s),
		scriptRef:  s,
	})
	return ls
}

// decodeConwayTreasuryTx builds a Conway transaction by encoding a
// transaction-body map and decoding it, so key 21's presence comes from real
// CBOR rather than from a Go field value.
func decodeConwayTreasuryTx(
	t *testing.T,
	plutusScript lcommon.Script,
	treasuryValue *int64,
) (*conway.ConwayTransaction, shelley.ShelleyTransactionInput) {
	t.Helper()
	input := shelley.ShelleyTransactionInput{
		TxId:        lcommon.Blake2b256{0x83},
		OutputIndex: 0,
	}
	bodyFields := map[uint]any{
		0: cbor.NewSetType(
			[]shelley.ShelleyTransactionInput{input},
			true,
		),
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1_000_000)}},
		2: uint64(200_000),
	}
	if treasuryValue != nil {
		bodyFields[21] = *treasuryValue
	}
	bodyCbor, err := cbor.Encode(bodyFields)
	require.NoError(t, err)
	var body conway.ConwayTransactionBody
	require.NoError(t, body.UnmarshalCBOR(bodyCbor))

	witnesses := conway.ConwayTransactionWitnessSet{}
	switch script := plutusScript.(type) {
	case lcommon.PlutusV1Script:
		witnesses.WsPlutusV1Scripts = cbor.NewSetType(
			[]lcommon.PlutusV1Script{script},
			true,
		)
	case lcommon.PlutusV2Script:
		witnesses.WsPlutusV2Scripts = cbor.NewSetType(
			[]lcommon.PlutusV2Script{script},
			true,
		)
	default:
		t.Fatalf("unexpected script type %T", plutusScript)
	}
	return &conway.ConwayTransaction{
		Body:       body,
		WitnessSet: witnesses,
		TxIsValid:  true,
	}, input
}

const conwayExplicitStakeAmount = int64(2_000_000)

// TestConwayTxInfoV3UsesProtocolVersionForExplicitStakeAmounts verifies the
// PV9/PV10 TxInfo boundary with a V3 script that serialises the translated
// deposit/refund option. Each transaction and script is evaluated at each
// protocol version to ensure the active version controls the context.
// Not t.Parallel: this test swaps the package-level Conway validation rules.
func TestConwayTxInfoV3UsesProtocolVersionForExplicitStakeAmounts(
	t *testing.T,
) {
	withoutConwayUtxoValidationRules(t)
	for _, certificateCase := range []struct {
		name        string
		certificate func(lcommon.Credential) lcommon.Certificate
	}{
		{
			name: "registration deposit",
			certificate: func(credential lcommon.Credential) lcommon.Certificate {
				return &lcommon.RegistrationCertificate{
					CertType:        uint(lcommon.CertificateTypeRegistration),
					StakeCredential: credential,
					Amount:          conwayExplicitStakeAmount,
				}
			},
		},
		{
			name: "deregistration refund",
			certificate: func(credential lcommon.Credential) lcommon.Certificate {
				return &lcommon.DeregistrationCertificate{
					CertType:        uint(lcommon.CertificateTypeDeregistration),
					StakeCredential: credential,
					Amount:          conwayExplicitStakeAmount,
				}
			},
		},
	} {
		t.Run(certificateCase.name, func(t *testing.T) {
			for _, scriptExpectation := range []struct {
				name   string
				major  uint
				option data.PlutusData
			}{
				{
					name:   "script expects PV9 Nothing",
					major:  lcommon.ProtocolVersionConway,
					option: data.NewConstr(1),
				},
				{
					name:  "script expects PV10 Just amount",
					major: lcommon.ProtocolVersionPlomin,
					option: data.NewConstr(
						0,
						data.NewInteger(big.NewInt(conwayExplicitStakeAmount)),
					),
				},
			} {
				t.Run(scriptExpectation.name, func(t *testing.T) {
					plutusScript := lcommon.PlutusV3Script(
						conwayV3StakeAmountObserver(
							t,
							scriptExpectation.option,
						),
					)
					certificate := certificateCase.certificate(
						lcommon.Credential{
							CredType:   lcommon.CredentialTypeScriptHash,
							Credential: plutusScript.Hash(),
						},
					)
					tx := conwayTxInfoCertificateTx(
						plutusScript,
						certificate,
					)

					for _, activeVersion := range []struct {
						name  string
						major uint
					}{
						{name: "PV9", major: lcommon.ProtocolVersionConway},
						{name: "PV10", major: lcommon.ProtocolVersionPlomin},
						{name: "PV11", major: lcommon.ProtocolVersionVanRossem},
					} {
						t.Run(activeVersion.name, func(t *testing.T) {
							pp := &conway.ConwayProtocolParameters{
								ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
									Major: activeVersion.major,
								},
								CostModels: map[uint][]int64{
									2: defaultMachineCostModel(
										t,
										lang.LanguageVersionV3,
									),
								},
								MaxTxExUnits: lcommon.ExUnits{
									Memory: 10_000_000,
									Steps:  100_000_000,
								},
							}
							shouldPass := activeVersion.major >= lcommon.ProtocolVersionPlomin
							if scriptExpectation.major == lcommon.ProtocolVersionConway {
								shouldPass = activeVersion.major == lcommon.ProtocolVersionConway
							}
							if shouldPass {
								require.NoError(t, ValidateTxConway(
									tx, 0, newMockLedgerState(), pp,
								))
								_, _, _, err := EvaluateTxConway(
									tx, newMockLedgerState(), pp,
								)
								require.NoError(t, err)
								return
							}
							err := ValidateTxConway(
								tx, 0, newMockLedgerState(), pp,
							)
							require.Error(t, err)
							_, _, _, err = EvaluateTxConway(
								tx, newMockLedgerState(), pp,
							)
							require.Error(t, err)
						})
					}
				})
			}
		})
	}
}

func conwayTxInfoCertificateTx(
	plutusScript lcommon.PlutusV3Script,
	certificate lcommon.Certificate,
) *mockConwayFeeTxV3 {
	return &mockConwayFeeTxV3{
		mockConwayFeeTx: mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType: txTypeAlonzo,
				fee:    big.NewInt(0),
				witnesses: &mockWitnessSet{
					plutusV3Scripts: []lcommon.PlutusV3Script{plutusScript},
					redeemers: &mockRedeemers{entries: []struct {
						key lcommon.RedeemerKey
						val lcommon.RedeemerValue
					}{
						{
							key: lcommon.RedeemerKey{
								Tag:   lcommon.RedeemerTagCert,
								Index: 0,
							},
							val: lcommon.RedeemerValue{
								Data: lcommon.Datum{Data: data.NewConstr(0)},
								ExUnits: lcommon.ExUnits{
									Memory: 5_000_000,
									Steps:  50_000_000,
								},
							},
						},
					}},
				},
			},
			certificates: []lcommon.Certificate{certificate},
		},
	}
}

func conwayV3StakeAmountObserver(
	t *testing.T,
	expectedOption data.PlutusData,
) []byte {
	t.Helper()
	expected, err := data.Encode(expectedOption)
	require.NoError(t, err)
	context := syn.Term[syn.DeBruijn](&syn.Var[syn.DeBruijn]{Name: 1})
	contextFields := conwayV3SndPair(conwayV3UnConstrData(context))
	txInfo := conwayV3HeadList(contextFields)
	txInfoFields := conwayV3SndPair(conwayV3UnConstrData(txInfo))
	certificatesData := conwayV3HeadList(conwayV3TailList(txInfoFields, 5))
	certificates := conwayV3UnListData(certificatesData)
	certificate := conwayV3HeadList(certificates)
	certificateFields := conwayV3SndPair(conwayV3UnConstrData(certificate))
	amountOption := conwayV3HeadList(conwayV3TailList(certificateFields, 1))
	serializedOption := conwayV3Apply(builtin.SerialiseData, amountOption)
	equal := conwayV3Apply(
		builtin.EqualsByteString,
		serializedOption,
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

func conwayV3UnConstrData(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.UnConstrData, term)
}

func conwayV3SndPair(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.SndPair, term)
}

func conwayV3HeadList(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.HeadList, term)
}

func conwayV3UnListData(
	term syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	return conwayV3Apply(builtin.UnListData, term)
}

func conwayV3TailList(
	term syn.Term[syn.DeBruijn],
	count int,
) syn.Term[syn.DeBruijn] {
	for range count {
		term = conwayV3Apply(builtin.TailList, term)
	}
	return term
}

func conwayV3Apply(
	function builtin.DefaultFunction,
	args ...syn.Term[syn.DeBruijn],
) syn.Term[syn.DeBruijn] {
	var term syn.Term[syn.DeBruijn] = &syn.Builtin{DefaultFunction: function}
	for range function.ForceCount() {
		term = &syn.Force[syn.DeBruijn]{Term: term}
	}
	for _, arg := range args {
		term = &syn.Apply[syn.DeBruijn]{Function: term, Argument: arg}
	}
	return term
}

// conwayDivergencePparams are the minimum protocol parameters needed to reach
// the bad-input and value-conservation rules without tripping an earlier rule.
func conwayDivergencePparams() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: conway.MinProtocolVersionConway,
		},
		MaxTxSize:            16_384,
		MaxValueSize:         5_000,
		CollateralPercentage: 150,
		MaxCollateralInputs:  3,
	}
}

// newConwayDivergenceTx builds a real ConwayTransaction from CBOR so that
// validation runs against the production decoder rather than a hand-rolled
// mock. Passing outputAmount == 0 omits the output list entirely.
func newConwayDivergenceTx(
	t *testing.T,
	inputHashByte byte,
	fee uint64,
	outputAmount uint64,
) *conway.ConwayTransaction {
	return newConwayDivergenceTxWithReference(
		t,
		inputHashByte,
		fee,
		outputAmount,
		nil,
	)
}

func newConwayDivergenceTxWithReference(
	t *testing.T,
	inputHashByte byte,
	fee uint64,
	outputAmount uint64,
	referenceHash []byte,
) *conway.ConwayTransaction {
	t.Helper()

	inputHash := make([]byte, 32)
	inputHash[0] = inputHashByte
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number: 258,
			Content: []any{
				[]any{inputHash, uint64(0)},
			},
		},
		1: []any{},
		2: fee,
	}
	if outputAmount > 0 {
		// Shelley-era output form: [address, coin]. A 29-byte payment-key
		// address (header byte + 28-byte key hash) is the smallest form the
		// address decoder accepts.
		addr := make([]byte, 29)
		addr[0] = 0x60
		bodyMap[1] = []any{
			[]any{addr, outputAmount},
		}
	}
	if referenceHash != nil {
		bodyMap[18] = cbor.Tag{
			Number:  258,
			Content: []any{[]any{referenceHash, uint64(0)}},
		}
	}
	txCbor, err := cbor.Encode([]any{bodyMap, map[uint]any{}, true, nil})
	require.NoError(t, err)

	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	return tx
}

// conwayUtxoValidationRuleIndex resolves the position of the upstream Conway
// rule carrying the stable semantic identifier id, exactly as production
// resolution does in resolveUtxoValidationSkipIndex (ledger/eras/validation.go):
// by descriptor Id, never by a pinned position.
//
// Position is not a stable property. gouroboros composes
// conway.UtxoValidationRules from the ordered descriptor list, so any upstream
// insertion renumbers every rule after it; the Id does not move. Tests that
// pinned literal positions broke on the v0.202.5 and v0.202.9 bumps,
// while production, which keys on the Id, did
// not.
func conwayUtxoValidationRuleIndex(
	t *testing.T,
	id lcommon.UtxoValidationRuleId,
) int {
	t.Helper()
	descriptors := conway.UtxoValidationRuleDescriptors()
	require.Len(
		t,
		conway.UtxoValidationRules,
		len(descriptors),
		"upstream descriptor list and composed rule list must agree in length",
	)
	index := slices.IndexFunc(
		descriptors,
		func(d lcommon.UtxoValidationRuleDescriptor) bool {
			return d.Id == id
		},
	)
	require.GreaterOrEqual(
		t,
		index,
		0,
		"upstream Conway must declare validation rule %q",
		id,
	)
	require.NotNil(
		t,
		conway.UtxoValidationRules[index],
		"upstream Conway rule %q must compose to a callable rule",
		id,
	)
	return index
}

// TestConwayUtxoValidationRulesRemainInProductionRuleSet fails if a rule the
// tests below depend on silently disappears from the slice Dingo actually
// runs. buildConwayValidationRules drops upstream rules by Id (the skip list in
// ledger/eras/conway.go), so a rule added to that list, or removed upstream,
// would stop firing while the transactions that should be rejected quietly
// start validating.
//
// It asserts presence and reachability, not position: the index each rule
// occupies is read from the upstream descriptors at run time, so an upstream
// insertion renumbers the expectation along with the rule.
func TestConwayUtxoValidationRulesRemainInProductionRuleSet(t *testing.T) {
	for _, id := range []lcommon.UtxoValidationRuleId{
		lcommon.UtxoValidationRuleBadInputs,
		lcommon.UtxoValidationRuleValueNotConserved,
	} {
		index := conwayUtxoValidationRuleIndex(t, id)
		assert.True(
			t,
			slices.ContainsFunc(
				conwayUtxoValidationRules,
				func(rule indexedUtxoValidationRule) bool {
					return rule.index == index &&
						rule.validationFunc != nil
				},
			),
			"conway rule %q (upstream index %d) must remain in the composed production rule set",
			id,
			index,
		)
	}
}

// TestValidateTxConwayGenuinelyMissingInputStillRejected is the negative case
// for the rollback restore fix in LedgerState.rollback: an input that is
// genuinely absent from the ledger must still be rejected as a bad input, and
// must still be reported under the bad-inputs rule. A restore fix that made
// input resolution more permissive would turn this into a consensus hazard.
func TestValidateTxConwayGenuinelyMissingInputStillRejected(t *testing.T) {
	badInputsIndex := conwayUtxoValidationRuleIndex(
		t,
		lcommon.UtxoValidationRuleBadInputs,
	)
	tx := newConwayDivergenceTx(t, 0xaa, 200_000, 0)

	err := ValidateTxConway(
		tx,
		0,
		newMockLedgerState(),
		conwayDivergencePparams(),
	)
	require.Error(t, err)

	var badInputs shelley.BadInputsUtxoError
	require.ErrorAs(t, err, &badInputs)
	require.Len(t, badInputs.Inputs, 1)
	assert.Equal(t, tx.Inputs()[0].String(), badInputs.Inputs[0].String())
	assert.Contains(
		t,
		err.Error(),
		fmt.Sprintf("conway utxo validation rule %d:", badInputsIndex),
	)
}

// TestValidateTxConwayGenuinelyMissingReferenceInputStillRejected confirms
// that the ordinary Conway transaction rules still reject an absent reference
// input. Block-level PV10 accounting may omit that key for its size total, but
// it must not make the transaction itself valid.
func TestValidateTxConwayGenuinelyMissingReferenceInputStillRejected(
	t *testing.T,
) {
	referenceHash := make([]byte, 32)
	tx := newConwayDivergenceTxWithReference(
		t,
		0xab,
		200_000,
		4_800_000,
		referenceHash,
	)
	referenceID := lcommon.NewBlake2b256(referenceHash)

	ls := newMockLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(5_000_000))

	err := ValidateTxConway(
		tx,
		0,
		ls,
		func() *conway.ConwayProtocolParameters {
			pp := conwayDivergencePparams()
			pp.ProtocolVersion.Major = lcommon.ProtocolVersionPlomin
			return pp
		}(),
	)
	require.Error(t, err)
	var referenceInput lcommon.ReferenceInputResolutionError
	require.ErrorAs(t, err, &referenceInput)
	assert.Contains(t, err.Error(), referenceID.String())
}

// TestValidateTxConwayGenuinelyUnbalancedStillRejected is the negative case for
// the value-conservation half: a transaction whose inputs all
// resolve but whose consumed and produced values genuinely differ must still be
// rejected, and must still be reported under the value-not-conserved rule.
func TestValidateTxConwayGenuinelyUnbalancedStillRejected(t *testing.T) {
	notConservedIndex := conwayUtxoValidationRuleIndex(
		t,
		lcommon.UtxoValidationRuleValueNotConserved,
	)
	tx := newConwayDivergenceTx(t, 0xbb, 200_000, 200_000)

	ls := newMockLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(5_000_000))

	err := ValidateTxConway(
		tx,
		0,
		ls,
		conwayDivergencePparams(),
	)
	require.Error(t, err)

	var notConserved shelley.ValueNotConservedUtxoError
	require.ErrorAs(t, err, &notConserved)
	require.NotNil(t, notConserved.Consumed)
	require.NotNil(t, notConserved.Produced)
	assert.Equal(
		t,
		big.NewInt(5_000_000),
		notConserved.Consumed,
		"consumed should be the resolved input value",
	)
	assert.Equal(t, big.NewInt(400_000), notConserved.Produced)
	assert.Contains(
		t,
		err.Error(),
		fmt.Sprintf("conway utxo validation rule %d:", notConservedIndex),
	)

	// The input resolved, so bad-inputs must NOT also fire. This is what
	// separates a genuinely unbalanced transaction from the single-cause
	// pairing, where one unresolvable input produces both.
	var badInputs shelley.BadInputsUtxoError
	assert.NotErrorAs(t, err, &badInputs)
}

// Preview parameters for epoch 672 (protocol version 9), from the chain's own
// epoch parameters for that epoch.
const (
	previewConwayTxFile     = "preview-conway-tx-58083610.cbor"
	previewConwayCostModels = "preview-costmodels-pv9-epoch672.json"

	previewConwayTxId       = "f5a0e06f3147c499c324041d16f510db07dd875aa0a80b7b0b2f2e990f9d7e45"
	previewConwaySlot       = 58_083_610
	previewConwayProtoMajor = 9

	// The malformed reference script's hash, matching log text
	// ("malformed reference scripts: [e985ee15...]") exactly.
	previewConwayMalformedScriptHash = "e985ee15101d2cef31eab9bd0e6ea55423b26aabf78171b87ed440af"
)

func previewConwayProtocolParams(
	t *testing.T,
) *conway.ConwayProtocolParameters {
	t.Helper()
	var costModels struct {
		PlutusV1 []int64 `json:"PlutusV1"`
		PlutusV2 []int64 `json:"PlutusV2"`
		PlutusV3 []int64 `json:"PlutusV3"`
	}
	require.NoError(t, json.Unmarshal(
		readErasFixture(t, previewConwayCostModels),
		&costModels,
	))
	return &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: previewConwayProtoMajor,
			Minor: 0,
		},
		CostModels: map[uint][]int64{
			0: costModels.PlutusV1,
			1: costModels.PlutusV2,
			2: costModels.PlutusV3,
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 14_000_000,
			Steps:  10_000_000_000,
		},
	}
}

// TestConwayUtxoRule45AcceptsPreviewMalformedVersionReferenceScript is the
// regression test: a genesis Preview sync
// deterministically and permanently halted at block 2,423,581 / slot
// 58083610 because conway UTXO validation rule 45
// (conway.UtxoValidateMalformedReferenceScripts, wired unmodified into
// ValidateTxConway's rule table) rejected a real, canonical, producer-accepted
// transaction. The rejected transaction carries a PlutusV2 reference script
// (hash e985ee15..., 123 bytes) whose flat header declares UPLC program
// version 89.49.145 -- not a real compiled script, but nothing in upstream
// plutus-ledger-api's deserialiseScript inspects the program version for a
// script that is merely present as a reference script output and never
// executed. The UPLC-version gate belongs only at execution time
// (mkTermToEvaluate), which plutigo already applies correctly via
// syn.ValidateTermVersionForExecution.
//
// plutigo v0.7.1's validateProgramVersion applied that whitelist at decode
// time instead, in the same code path used for well-formedness checks of
// unexecuted reference scripts, so dingo rejected every peer's identical copy
// of this canonical block and could not resync Preview from genesis on any
// build. plutigo v0.7.2 moves the gate to
// execution time; this test fails before that bump and passes after it.
func TestConwayUtxoRule45AcceptsPreviewMalformedVersionReferenceScript(
	t *testing.T,
) {
	t.Parallel()
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, previewConwayTxFile),
	)
	require.NoError(t, err)
	require.Equal(t, previewConwayTxId, tx.Hash().String())
	require.True(t, tx.IsValid())

	require.Len(t, tx.Outputs(), 2)
	refScript := tx.Outputs()[0].ScriptRef()
	require.NotNil(
		t,
		refScript,
		"fixture must carry output 0's reference script",
	)
	require.Equal(
		t,
		previewConwayMalformedScriptHash,
		refScript.Hash().String(),
	)

	pp := previewConwayProtocolParams(t)

	// This is the exact, unwrapped validation function
	// buildConwayValidationRules wires into ValidateTxConway's rule table for
	// upstream rule Id UtxoValidationRuleMalformedReferenceScripts --
	// "conway utxo validation rule 45" -- not an extracted
	// helper the production entry point bypasses.
	err = conway.UtxoValidateMalformedReferenceScripts(
		tx,
		previewConwaySlot,
		nil,
		pp,
	)
	require.NoError(
		t,
		err,
		"a real, producer-accepted Preview reference script must not be "+
			"rejected for its unexecuted UPLC program version",
	)

	var malformed lcommon.MalformedReferenceScriptsError
	require.False(
		t,
		errors.As(err, &malformed),
		"must not classify the reference script as malformed",
	)
}

// TestConwayPlutusV2ScriptStillRejectsMalformedVersionAtExecution is the
// negative case root-cause analysis requires alongside the fix:
// the same 123-byte script that rule 45 must now accept as an unexecuted
// reference script must still be rejected if anything ever tries to execute
// it, so the fix does not also remove the execution-time gate
// (syn.ValidateTermVersionForExecution) that a script actually invoked as a
// witness/redeemer target still needs. Script.Evaluate decodes and validates
// the program before applying any arguments, so this observes the gate
// directly without needing a spending context for this particular script.
func TestConwayPlutusV2ScriptStillRejectsMalformedVersionAtExecution(
	t *testing.T,
) {
	t.Parallel()
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, previewConwayTxFile),
	)
	require.NoError(t, err)

	refScript := tx.Outputs()[0].ScriptRef()
	require.NotNil(t, refScript)
	v2Script, ok := refScript.(lcommon.PlutusV2Script)
	require.True(
		t,
		ok,
		"fixture script must decode as PlutusV2Script, got %T",
		refScript,
	)

	evalContext := cek.NewDefaultEvalContext(
		cek.LanguageVersionV2,
		cek.ProtoVersion{Major: previewConwayProtoMajor},
	)
	zero := data.NewInteger(big.NewInt(0))
	_, evalErr := v2Script.Evaluate(
		zero,
		zero,
		zero,
		lcommon.ExUnits{Memory: 14_000_000, Steps: 10_000_000_000},
		evalContext,
	)
	require.ErrorContains(
		t,
		evalErr,
		"unsupported UPLC program version 89.49.145",
		"a script with an unsupported UPLC program version must still be "+
			"rejected when actually executed",
	)
}

// newConwayRewardAddress builds a CIP-0019 reward account for the given
// credential type.
func newConwayRewardAddress(
	t *testing.T,
	addrType uint8,
	stakeHash []byte,
) *lcommon.Address {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeHash,
	)
	require.NoError(t, err)
	return &addr
}

// TestConwayWithdrawalOrderPlacesScriptCredentialFirst pins the Rewarding
// redeemer index for a transaction that withdraws from both a key credential
// and a script credential. cardano-ledger keys withdrawals by RewardAccount,
// ordered by Credential, whose constructors are declared ScriptHashObj before
// KeyHashObj, so the script withdrawal takes index 0. Reward address bytes put
// the key-hash header (0xe0) before the script-hash header (0xf0), which is the
// opposite order.
func TestConwayWithdrawalOrderPlacesScriptCredentialFirst(t *testing.T) {
	plutusScript := lcommon.PlutusV2Script([]byte{0x01, 0x02})
	scriptHash := plutusScript.Hash()
	keyHash := make([]byte, lcommon.AddressHashSize)
	keyHash[0] = 0xaa

	scriptReward := newConwayRewardAddress(
		t,
		lcommon.AddressTypeNoneScript,
		scriptHash.Bytes(),
	)
	keyReward := newConwayRewardAddress(
		t,
		lcommon.AddressTypeNoneKey,
		keyHash,
	)

	// The two orderings only disagree when the raw bytes rank the key
	// credential first. Assert that premise so the test cannot pass because
	// both orderings happen to agree.
	scriptBytes, err := scriptReward.Bytes()
	require.NoError(t, err)
	keyBytes, err := keyReward.Bytes()
	require.NoError(t, err)
	require.Negative(t, bytes.Compare(keyBytes, scriptBytes))

	bothWithdrawals := map[*lcommon.Address]*big.Int{
		keyReward:    big.NewInt(1),
		scriptReward: big.NewInt(1),
	}

	newTx := func(
		withdrawals map[*lcommon.Address]*big.Int,
		redeemers lcommon.TransactionWitnessRedeemers,
	) *mockConwayFeeTx {
		return &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType: txTypeAlonzo,
				witnesses: &mockWitnessSet{
					plutusV2Scripts: []lcommon.PlutusV2Script{plutusScript},
					redeemers:       redeemers,
				},
			},
			withdrawals: withdrawals,
		}
	}

	validate := func(
		withdrawals map[*lcommon.Address]*big.Int,
		redeemers lcommon.TransactionWitnessRedeemers,
	) error {
		return ValidateTxPlutusConway(
			newTx(withdrawals, redeemers),
			0,
			newMockLedgerState(),
			&conway.ConwayProtocolParameters{},
		)
	}

	t.Run("sort order", func(t *testing.T) {
		sorted := sortedConwayWithdrawalAddresses(bothWithdrawals)
		require.Len(t, sorted, 2)
		assert.Same(t, scriptReward, sorted[0])
		assert.Same(t, keyReward, sorted[1])
	})

	t.Run("required redeemer index", func(t *testing.T) {
		err := validate(bothWithdrawals, nil)
		var missing conway.MissingRedeemerForScriptError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, scriptHash, missing.ScriptHash)
		assert.Equal(t, lcommon.RedeemerTagReward, missing.Tag)
		assert.Equal(t, uint32(0), missing.Index)
	})

	t.Run(
		"every index maps back to the withdrawal it demanded",
		func(t *testing.T) {
			// script.BuildScriptPurpose turns a redeemer key back into the
			// credential the script is evaluated against, so an index the
			// required-redeemer check demands has to resolve to the same
			// script through it. The two use separate orderings, and this is
			// where they have to agree.
			sorted := sortedConwayWithdrawalAddresses(bothWithdrawals)
			checked := 0
			for idx, addr := range sorted {
				cred, ok := addr.StakeCredential()
				require.True(t, ok)
				if cred.CredType != lcommon.CredentialTypeScriptHash {
					continue
				}
				purpose, err := script.BuildScriptPurpose(
					lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagReward,
						Index: uint32(idx), // #nosec G115 -- two entries
					},
					nil,
					nil,
					lcommon.MultiAsset[lcommon.MultiAssetTypeMint]{},
					nil,
					bothWithdrawals,
					nil,
					nil,
					nil,
					0,
				)
				require.NoError(t, err)
				rewarding, ok := purpose.(script.ScriptPurposeRewarding)
				require.True(t, ok)
				assert.Equal(
					t,
					uint(lcommon.CredentialTypeScriptHash),
					rewarding.StakeCredential.CredType,
				)
				assert.Equal(
					t,
					lcommon.ScriptHash(cred.Credential),
					purpose.ScriptHash(),
				)
				checked++
			}
			require.Equal(t, 1, checked)
		},
	)

	t.Run(
		"supplying the redeemer at that index satisfies the requirement",
		func(t *testing.T) {
			redeemers := &mockRedeemers{
				entries: []struct {
					key lcommon.RedeemerKey
					val lcommon.RedeemerValue
				}{
					{
						key: lcommon.RedeemerKey{
							Tag:   lcommon.RedeemerTagReward,
							Index: 0,
						},
					},
				},
			}
			err := validate(bothWithdrawals, redeemers)
			// Phase-2 still runs and the stub script cannot be evaluated, but
			// the redeemer must no longer be reported missing and the purpose
			// must not resolve to some other hash.
			var missing conway.MissingRedeemerForScriptError
			assert.NotErrorAs(t, err, &missing)
			var missingWitness lcommon.MissingScriptWitnessesError
			assert.NotErrorAs(t, err, &missingWitness)
			var extra conway.ExtraRedeemerError
			assert.NotErrorAs(t, err, &extra)
		},
	)

	t.Run("control: a single script withdrawal", func(t *testing.T) {
		// One withdrawal leaves no ordering choice, so this subtest holds
		// under either comparator.
		err := validate(map[*lcommon.Address]*big.Int{
			scriptReward: big.NewInt(1),
		}, nil)
		var missing conway.MissingRedeemerForScriptError
		require.ErrorAs(t, err, &missing)
		assert.Equal(t, scriptHash, missing.ScriptHash)
		assert.Equal(t, uint32(0), missing.Index)
	})

	t.Run(
		"control: a single key withdrawal needs no redeemer",
		func(t *testing.T) {
			require.NoError(t, validate(map[*lcommon.Address]*big.Int{
				keyReward: big.NewInt(1),
			}, nil))
		},
	)
}

// TestValidateTxConwayRejectsParameterChangeProtocolVersion is a production
// ValidateTxConway regression ( "test through production
// ValidateTxConway" and "end-to-end PV9, PV10, and PV11 rejection coverage"
// criteria). Every other Conway rule is stubbed to a no-op so only the new
// rule's contribution to the joined error is under test, matching the
// isolation technique TestValidateTxDijkstraDoesNotTreatPhase1FailureAsPhase2Failure
// uses below. The table covers every Conway-era major protocol version: the
// rule must reject a protocol-version-setting ParameterChange regardless of
// which Conway PV the ledger currently runs.
func TestValidateTxConwayRejectsParameterChangeProtocolVersion(t *testing.T) {
	originalRules := conwayUtxoValidationRules
	conwayUtxoValidationRules = nil
	t.Cleanup(func() { conwayUtxoValidationRules = originalRules })

	for _, currentMajor := range []uint{9, 10, 11} {
		t.Run(fmt.Sprintf("PV%d", currentMajor), func(t *testing.T) {
			pp := conwayDivergencePparams()
			pp.ProtocolVersion.Major = currentMajor

			tx := &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{witnesses: &mockWitnessSet{}},
				proposalProcedures: []lcommon.ProposalProcedure{
					conwayParameterChangeProposal(
						&lcommon.ProtocolParametersProtocolVersion{
							Major: currentMajor + 1,
						},
					),
				},
			}
			err := ValidateTxConway(tx, 0, newMockLedgerState(), pp)
			var protocolVersionErr ParameterChangeProtocolVersionError
			require.ErrorAs(t, err, &protocolVersionErr)
		})
	}
}

// TestPParamsUpdateConwayIgnoresProtocolVersion is the defense-in-depth
// regression for "remove protocol-version mutation from Conway
// PPU application" criterion: even called directly with an update that sets
// protocol version, PParamsUpdateConway must not change it, while still
// applying every other field normally.
func TestPParamsUpdateConwayIgnoresProtocolVersion(t *testing.T) {
	current := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: conway.MinProtocolVersionConway,
			Minor: 0,
		},
	}
	minFeeA := uint(500)
	updated, err := PParamsUpdateConway(
		current,
		conway.ConwayProtocolParameterUpdate{
			MinFeeA: &minFeeA,
			ProtocolVersion: &lcommon.ProtocolParametersProtocolVersion{
				Major: conway.MinProtocolVersionConway + 1,
			},
		},
	)
	require.NoError(t, err)
	conwayUpdated, ok := updated.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	require.Equal(
		t,
		uint(conway.MinProtocolVersionConway),
		conwayUpdated.ProtocolVersion.Major,
		"ParameterChange must not move protocol version",
	)
	require.Equal(
		t,
		minFeeA,
		conwayUpdated.MinFeeA,
		"other fields must still apply",
	)
}

// EvaluateTxConway (also used for Dijkstra) builds the V3 context up front, so
// it needs the same gate: estimating a redeemerless transaction's execution
// units must not depend on translating its TTL.
func TestEvaluateTxConwaySkipsScriptContextWithoutRedeemers(t *testing.T) {
	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	bodyMap := map[uint]any{
		0: cbor.Tag{
			Number: 258,
			Content: []any{
				[]any{inputHash, uint64(0)},
			},
		},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1_000_000)}},
		2: uint64(200_000),
		3: uint64(testPastHorizonSlot),
	}
	txCbor, err := cbor.Encode(
		[]any{bodyMap, map[uint]any{}, true, nil},
	)
	require.NoError(t, err)
	tx, err := conway.NewConwayTransactionFromCbor(txCbor)
	require.NoError(t, err)
	require.False(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	_, exUnits, redeemerExUnits, err := EvaluateTxConway(
		tx,
		ls,
		&conway.ConwayProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{}, exUnits)
	assert.Empty(t, redeemerExUnits)
	assert.Zero(t, ls.slotToTimeCalls)
}

// TestValidateTxConwayRejectsPlutusV2WhenSynthetic mirrors
// TestValidateTxBabbageRejectsPlutusV2WhenSynthetic for the Conway era: the
// synthetic marker persists across era transitions until real data actually
// clears it (LedgerState.syntheticV2CostModel's doc comment), so a chain
// that reaches Conway without ever receiving a real PlutusV2 update has the
// identical exposure ValidateTxBabbage does.
func TestValidateTxConwayRejectsPlutusV2WhenSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxConwayAllowsPlutusV2WhenNotSynthetic mirrors
// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic for Conway.
func TestValidateTxConwayAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// TestEvaluateTxConwayRejectsPlutusV2WhenSynthetic mirrors
// TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic for Conway.
func TestEvaluateTxConwayRejectsPlutusV2WhenSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxConway(
		tx,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxConwayRejectsPlutusV2WhenSyntheticEvenIfDeclaredInvalid
// verifies that validatePlutusOutcome
// (ledger/eras/validation.go) treats a failed script as the expected,
// acceptable outcome for a transaction declared invalid -- but only when
// the phase-2 error is a conway.PlutusScriptFailedError specifically.
// ErrNoCostModelForPlutusV2 is a hard UTXOW-level rejection (real
// cardano-ledger raises it before any script runs), not a script-execution
// failure, so it must still reject the transaction outright even when the
// transaction declares itself invalid and provides collateral -- it must
// not be silently accepted as "failed as declared."
func TestValidateTxConwayRejectsPlutusV2WhenSyntheticEvenIfDeclaredInvalid(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		false,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxConway(
		tx,
		0,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestEvaluateTxConwayAllowsPlutusV2WhenNotSynthetic mirrors
// TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic for Conway.
func TestEvaluateTxConwayAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxConway(
		tx,
		ls,
		&conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// TestEvaluateTxConwayStopsAtTransactionWideBudget evaluates a multi-redeemer
// transaction with MaxTxExUnits set at, and one step below, the work its
// scripts consume in total. Each script alone fits either limit, so only a
// budget shared across the redeemers can refuse the second.
func TestEvaluateTxConwayStopsAtTransactionWideBudget(t *testing.T) {
	t.Parallel()

	pp := mainnetFixtureProtocolParams(t)
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, mainnetFixtureTxFile),
	)
	require.NoError(t, err)
	ls := mainnetFixtureLedgerState{mockLedgerState: newMockLedgerState()}
	ls.networkId = uint(lcommon.AddressNetworkMainnet)
	for _, inputTx := range mainnetFixtureInputTxs(t) {
		for idx, output := range inputTx.Outputs() {
			stored, err := gledger.NewTransactionOutputFromCbor(output.Cbor())
			require.NoError(t, err)
			ls.addUtxo(
				shelley.NewShelleyTransactionInput(
					inputTx.Hash().String(),
					idx,
				),
				stored,
			)
		}
	}
	_, total, perRedeemer, err := EvaluateTxConway(tx, ls, pp)
	require.NoError(t, err)
	require.Greater(t, len(perRedeemer), 1, "fixture needs several redeemers")

	atLimit := *pp
	atLimit.MaxTxExUnits = total
	_, _, _, err = EvaluateTxConway(tx, ls, &atLimit)
	require.NoError(t, err, "a transaction using exactly the limit is valid")

	overLimit := *pp
	overLimit.MaxTxExUnits = lcommon.ExUnits{
		Memory: total.Memory,
		Steps:  total.Steps - 1,
	}
	_, _, _, err = EvaluateTxConway(tx, ls, &overLimit)
	require.Error(t, err, "aggregate steps beyond the limit must be refused")
}

func TestEvaluateTxConwayStopsAfterCanceledInputLookup(t *testing.T) {
	t.Parallel()

	pp := mainnetFixtureProtocolParams(t)
	tx, err := conway.NewConwayTransactionFromCbor(
		readErasFixture(t, mainnetFixtureTxFile),
	)
	require.NoError(t, err)
	require.Greater(t, len(tx.Inputs()), 1, "fixture needs multiple inputs")
	base := mainnetFixtureLedgerState{mockLedgerState: newMockLedgerState()}
	base.networkId = uint(lcommon.AddressNetworkMainnet)
	for _, inputTx := range mainnetFixtureInputTxs(t) {
		for idx, output := range inputTx.Outputs() {
			stored, err := gledger.NewTransactionOutputFromCbor(output.Cbor())
			require.NoError(t, err)
			base.addUtxo(
				shelley.NewShelleyTransactionInput(inputTx.Hash().String(), idx),
				stored,
			)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	ls := &cancelingEvaluationLedgerState{
		mainnetFixtureLedgerState: base,
		ctx:                       ctx,
		cancel:                    cancel,
	}
	pending := lcommon.NewBlockLedgerState(ls)
	predecessor := mockledger.NewTransactionBuilder().WithType(
		conway.EraIdConway,
	).WithCertificates(&lcommon.RegistrationCertificate{
		CertType: uint(lcommon.CertificateTypeRegistration),
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash{0x7f},
		},
		Amount: 1,
	})
	require.NoError(t, pending.ApplyTransaction(predecessor, pp))

	_, _, _, err = EvaluateTxConway(tx, pending, pp)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, ls.reads)
}

func TestEvaluateTxConwayCancelsActivePlutusMachine(t *testing.T) {
	t.Parallel()

	selfApply := &syn.Lambda[syn.DeBruijn]{
		Body: &syn.Apply[syn.DeBruijn]{
			Function: &syn.Var[syn.DeBruijn]{Name: 1},
			Argument: &syn.Var[syn.DeBruijn]{Name: 1},
		},
	}
	term := syn.Term[syn.DeBruijn](&syn.Lambda[syn.DeBruijn]{
		Body: &syn.Apply[syn.DeBruijn]{
			Function: selfApply,
			Argument: selfApply,
		},
	})
	flatProgram, err := syn.Encode(&syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersion{1, 1, 0},
		Term:    term,
	})
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	plutusScript := lcommon.PlutusV3Script(scriptBytes)
	credential := lcommon.Credential{
		CredType:   lcommon.CredentialTypeScriptHash,
		Credential: plutusScript.Hash(),
	}
	tx := conwayTxInfoCertificateTx(
		plutusScript,
		&lcommon.RegistrationCertificate{
			CertType:        uint(lcommon.CertificateTypeRegistration),
			StakeCredential: credential,
			Amount:          conwayExplicitStakeAmount,
		},
	)
	ctx := &cancelDuringCekContext{limit: 64, done: make(chan struct{})}
	ls := &activeEvaluationLedgerState{
		mockLedgerState: newMockLedgerState(),
		ctx:             ctx,
	}
	pp := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: lcommon.ProtocolVersionVanRossem,
		},
		CostModels: map[uint][]int64{
			2: defaultMachineCostModel(t, lang.LanguageVersionV3),
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 10_000_000,
			Steps:  100_000_000,
		},
	}
	pending := lcommon.NewBlockLedgerState(ls)
	predecessor := mockledger.NewTransactionBuilder().WithType(
		conway.EraIdConway,
	).WithCertificates(&lcommon.RegistrationCertificate{
		CertType: uint(lcommon.CertificateTypeRegistration),
		StakeCredential: lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash{0x7e},
		},
		Amount: 1,
	})
	require.NoError(t, pending.ApplyTransaction(predecessor, pp))

	_, _, _, err = EvaluateTxConway(tx, pending, pp)
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, ctx.checks, ctx.limit)
}
