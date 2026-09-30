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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/plutigo/lang"
	"github.com/blinklabs-io/plutigo/syn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dingoMetadataRule returns the validation function dingo runs for the
// upstream metadata rule, failing the test when dingo's era rule list does
// not carry it. It resolves the position by rule Id, like the list builder.
func dingoMetadataRule(
	t *testing.T,
	descriptors []lcommon.UtxoValidationRuleDescriptor,
	upstream []lcommon.UtxoValidationRuleFunc,
	dingoRules []indexedUtxoValidationRule,
) lcommon.UtxoValidationRuleFunc {
	t.Helper()
	index := resolveUtxoValidationSkipIndex(
		descriptors, upstream, lcommon.UtxoValidationRuleMetadata,
	)
	for _, rule := range dingoRules {
		if rule.index == index {
			return rule.validationFunc
		}
	}
	require.FailNow(t, "metadata rule is not in dingo's era rule list")
	return nil
}

// txWithAuxBytes returns transaction CBOR whose body auxiliary_data_hash
// matches the exact auxiliary-data bytes, so only the auxiliary-data content
// can fail the metadata rule. The layout is the same from Alonzo to Conway.
func txWithAuxBytes(aux []byte, isValid bool) []byte {
	hash := lcommon.Blake2b256Hash(aux)
	// {0: [], 1: [], 2: 0, 7: h'<hash>'}
	body := append(
		[]byte{0xa4, 0x00, 0x80, 0x01, 0x80, 0x02, 0x00, 0x07, 0x58, 0x20},
		hash.Bytes()...,
	)
	validByte := byte(0xf5)
	if !isValid {
		validByte = 0xf4
	}
	raw := append([]byte{0x84}, body...)
	raw = append(raw, 0xa0, validByte)
	return append(raw, aux...)
}

// taggedAuxWithPlutusV1 returns a tag-259 auxiliary-data map carrying one
// Plutus V1 script whose wire bytes are script.
func taggedAuxWithPlutusV1(t *testing.T, script []byte) []byte {
	t.Helper()
	element, err := cbor.Encode(script)
	require.NoError(t, err)
	aux := []byte{0xd9, 0x01, 0x03, 0xa1, 0x02, 0x81}
	return append(aux, element...)
}

// TestMetadataRuleRejectsMalformedAuxiliaryPlutusScripts covers the
// well-formedness of Plutus scripts carried in auxiliary data. They are never
// executed, so a phase-1 rule is the only thing that can refuse them, and it
// must refuse them for isValid=false transactions too. Each case runs both
// dingo's metadata rule alone and the era's full ValidateTx, which is what
// block application and mempool admission call.
func TestMetadataRuleRejectsMalformedAuxiliaryPlutusScripts(t *testing.T) {
	t.Parallel()

	program := &syn.Program[syn.DeBruijn]{
		Version: lang.LanguageVersionV1,
		Term:    &syn.Lambda[syn.DeBruijn]{Body: &syn.Error{}},
	}
	flat, err := syn.Encode(program)
	require.NoError(t, err)
	validScript, err := cbor.Encode(flat)
	require.NoError(t, err)
	badFlatScript, err := cbor.Encode([]byte{0x01, 0x02, 0x03})
	require.NoError(t, err)
	noWrapperScript := []byte{0x01, 0x02, 0x03}

	type decodeTx func(t *testing.T, raw []byte) lcommon.Transaction
	eras := []struct {
		name       string
		rule       lcommon.UtxoValidationRuleFunc
		validateTx lcommon.UtxoValidationRuleFunc
		pp         lcommon.ProtocolParameters
		decode     decodeTx
	}{
		{
			name: "alonzo",
			rule: dingoMetadataRule(t,
				alonzo.UtxoValidationRuleDescriptors(),
				alonzo.UtxoValidationRules,
				alonzoUtxoValidationRules,
			),
			validateTx: ValidateTxAlonzo,
			pp:         &alonzo.AlonzoProtocolParameters{ProtocolMajor: 6},
			decode: func(t *testing.T, raw []byte) lcommon.Transaction {
				var tx alonzo.AlonzoTransaction
				_, err := cbor.Decode(raw, &tx)
				require.NoError(t, err)
				return &tx
			},
		},
		{
			name: "babbage",
			rule: dingoMetadataRule(t,
				babbage.UtxoValidationRuleDescriptors(),
				babbage.UtxoValidationRules,
				babbageUtxoValidationRules,
			),
			validateTx: ValidateTxBabbage,
			pp:         &babbage.BabbageProtocolParameters{ProtocolMajor: 8},
			decode: func(t *testing.T, raw []byte) lcommon.Transaction {
				var tx babbage.BabbageTransaction
				_, err := cbor.Decode(raw, &tx)
				require.NoError(t, err)
				return &tx
			},
		},
		{
			name: "conway",
			rule: dingoMetadataRule(t,
				conway.UtxoValidationRuleDescriptors(),
				conway.UtxoValidationRules,
				conwayUtxoValidationRules,
			),
			validateTx: ValidateTxConway,
			pp: func() lcommon.ProtocolParameters {
				pp := &conway.ConwayProtocolParameters{}
				pp.ProtocolVersion.Major = 10
				return pp
			}(),
			decode: func(t *testing.T, raw []byte) lcommon.Transaction {
				var tx conway.ConwayTransaction
				_, err := cbor.Decode(raw, &tx)
				require.NoError(t, err)
				return &tx
			},
		},
	}

	tests := []struct {
		name    string
		script  []byte
		wantErr string
	}{
		{name: "well-formed script", script: validScript},
		{
			name:    "malformed UPLC",
			script:  badFlatScript,
			wantErr: "decode Plutus program",
		},
		{
			name:    "missing CBOR script wrapper",
			script:  noWrapperScript,
			wantErr: "decode CBOR script wrapper",
		},
	}
	for _, era := range eras {
		for _, tc := range tests {
			for _, isValid := range []bool{true, false} {
				name := era.name + "/" + tc.name + "/isValid=true"
				if !isValid {
					name = era.name + "/" + tc.name + "/isValid=false"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					tx := era.decode(t, txWithAuxBytes(
						taggedAuxWithPlutusV1(t, tc.script), isValid,
					))
					ruleErr := era.rule(tx, 0, nil, era.pp)
					// The transaction spends nothing, so the full validation
					// always fails on other rules; only the auxiliary-data
					// script error is asserted.
					txErr := era.validateTx(
						tx, 0, newMockLedgerState(), era.pp,
					)
					if tc.wantErr == "" {
						require.NoError(t, ruleErr)
						if txErr != nil {
							require.NotContains(t, txErr.Error(), "auxiliary-data")
						}
						return
					}
					require.ErrorContains(t, ruleErr, tc.wantErr)
					require.ErrorContains(t, txErr, tc.wantErr)
				})
			}
		}
	}
}

// TestCertDepositRejectsTypedNilParams pins the guard both Conway-era and
// Dijkstra-era deposit functions need.
//
// A typed-nil parameter pointer satisfies the type assertion, so testing only
// the ok result lets every case in the switch dereference nil. Conway has
// always checked for it; Dijkstra checked only ok and panicked instead of
// reporting incompatible parameters. Both are asserted here so the pair cannot
// drift apart again.
func TestCertDepositRejectsTypedNilParams(t *testing.T) {
	certificates := map[string]lcommon.Certificate{
		"drep registration":  &lcommon.RegistrationDrepCertificate{},
		"pool registration":  &lcommon.PoolRegistrationCertificate{},
		"stake registration": &lcommon.StakeRegistrationCertificate{},
	}
	eras := map[string]struct {
		fn     func(lcommon.Certificate, lcommon.ProtocolParameters) (uint64, error)
		params lcommon.ProtocolParameters
	}{
		"conway": {
			fn:     CertDepositConway,
			params: (*conway.ConwayProtocolParameters)(nil),
		},
		"dijkstra": {
			fn:     CertDepositDijkstra,
			params: (*gdijkstra.DijkstraProtocolParameters)(nil),
		},
	}
	for eraName, era := range eras {
		for certName, cert := range certificates {
			t.Run(eraName+"/"+certName, func(t *testing.T) {
				require.NotPanics(t, func() {
					deposit, err := era.fn(cert, era.params)
					require.ErrorIs(t, err, ErrIncompatibleProtocolParams)
					require.Zero(t, deposit)
				})
			})
		}
	}
}

// conwayParameterChangeProposal builds a proposal procedure carrying a
// ConwayParameterChangeGovAction. When protocolVersion is non-nil, the
// action sets protocol-version key 14, which dingo#4439 requires rejecting.
func conwayParameterChangeProposal(
	protocolVersion *lcommon.ProtocolParametersProtocolVersion,
) lcommon.ProposalProcedure {
	minFeeA := uint(1)
	return conway.ConwayProposalProcedure{
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeParameterChange),
			Action: &conway.ConwayParameterChangeGovAction{
				ParamUpdate: conway.ConwayProtocolParameterUpdate{
					MinFeeA:         &minFeeA,
					ProtocolVersion: protocolVersion,
				},
			},
		},
	}
}

// dijkstraParameterChangeProposal is the Dijkstra analogue of
// conwayParameterChangeProposal.
func dijkstraParameterChangeProposal(
	protocolVersion *lcommon.ProtocolParametersProtocolVersion,
) lcommon.ProposalProcedure {
	minFeeA := uint(1)
	return conway.ConwayProposalProcedure{
		PPGovAction: conway.ConwayGovAction{
			Type: uint(lcommon.GovActionTypeParameterChange),
			Action: &gdijkstra.DijkstraParameterChangeGovAction{
				ParamUpdate: gdijkstra.DijkstraProtocolParameterUpdate{
					MinFeeA:         &minFeeA,
					ProtocolVersion: protocolVersion,
				},
			},
		},
	}
}

// TestValidateParameterChangeExcludesProtocolVersionRejectsConway pins
// validateParameterChangeExcludesProtocolVersion's Conway branch directly,
// independent of any other Conway validation rule.
func TestValidateParameterChangeExcludesProtocolVersionRejectsConway(
	t *testing.T,
) {
	for _, major := range []uint{9, 10, 11} {
		t.Run(
			fmt.Sprintf("PV%d", major),
			func(t *testing.T) {
				tx := &mockConwayFeeTx{
					proposalProcedures: []lcommon.ProposalProcedure{
						conwayParameterChangeProposal(
							&lcommon.ProtocolParametersProtocolVersion{
								Major: major,
							},
						),
					},
				}
				err := validateParameterChangeExcludesProtocolVersion(
					tx, 0, newMockLedgerState(), conwayDivergencePparams(),
				)
				var protocolVersionErr ParameterChangeProtocolVersionError
				require.ErrorAs(t, err, &protocolVersionErr)
				require.Equal(t, 0, protocolVersionErr.ProposalIndex)
			},
		)
	}
}

// TestValidateParameterChangeExcludesProtocolVersionRejectsDijkstra is the
// Dijkstra analogue: the same protocol-version key 14 exclusion carries into
// Dijkstra's ParameterChange action (dingo#4439's "apply the same protection
// to Dijkstra" acceptance criterion, PV12).
func TestValidateParameterChangeExcludesProtocolVersionRejectsDijkstra(
	t *testing.T,
) {
	tx := &mockConwayFeeTx{
		proposalProcedures: []lcommon.ProposalProcedure{
			dijkstraParameterChangeProposal(
				&lcommon.ProtocolParametersProtocolVersion{
					Major: gdijkstra.MinProtocolVersionDijkstra,
				},
			),
		},
	}
	err := validateParameterChangeExcludesProtocolVersion(
		tx, 0, newMockLedgerState(), conwayDivergencePparams(),
	)
	var protocolVersionErr ParameterChangeProtocolVersionError
	require.ErrorAs(t, err, &protocolVersionErr)
	require.Equal(t, 0, protocolVersionErr.ProposalIndex)
}

// TestValidateParameterChangeExcludesProtocolVersionAllowsOrdinaryUpdate is
// the negative case: a ParameterChange that never touches protocol version
// must not be rejected by this rule, in either era's action type.
func TestValidateParameterChangeExcludesProtocolVersionAllowsOrdinaryUpdate(
	t *testing.T,
) {
	for _, tc := range []struct {
		name     string
		proposal lcommon.ProposalProcedure
	}{
		{"Conway", conwayParameterChangeProposal(nil)},
		{"Dijkstra", dijkstraParameterChangeProposal(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockConwayFeeTx{
				proposalProcedures: []lcommon.ProposalProcedure{
					tc.proposal,
				},
			}
			require.NoError(
				t,
				validateParameterChangeExcludesProtocolVersion(
					tx, 0, newMockLedgerState(), conwayDivergencePparams(),
				),
			)
		})
	}
}

// decodeConwayParamUpdateFromRawFields CBOR-encodes fields as a map and
// decodes it into a ConwayProtocolParameterUpdate through the type's own
// UnmarshalCBOR, so the returned value's Cbor() carries the real raw bytes
// -- unlike a struct literal, which leaves Cbor() empty. This is what lets a
// test exercise parameterChangeSetsProtocolVersionKey's raw-CBOR path.
func decodeConwayParamUpdateFromRawFields(
	t *testing.T,
	fields map[uint]any,
) conway.ConwayProtocolParameterUpdate {
	t.Helper()
	raw, err := cbor.Encode(fields)
	require.NoError(t, err)
	var update conway.ConwayProtocolParameterUpdate
	_, err = cbor.Decode(raw, &update)
	require.NoError(t, err)
	return update
}

// decodeDijkstraParamUpdateFromRawFields is the Dijkstra analogue of
// decodeConwayParamUpdateFromRawFields.
func decodeDijkstraParamUpdateFromRawFields(
	t *testing.T,
	fields map[uint]any,
) gdijkstra.DijkstraProtocolParameterUpdate {
	t.Helper()
	raw, err := cbor.Encode(fields)
	require.NoError(t, err)
	var update gdijkstra.DijkstraProtocolParameterUpdate
	_, err = cbor.Decode(raw, &update)
	require.NoError(t, err)
	return update
}

// TestValidateParameterChangeExcludesProtocolVersionRejectsPresentNullKey14
// verifies that a decoded ParamUpdate whose raw CBOR carries key 14 with an
// explicit null value decodes ProtocolVersion
// to the same nil the field takes when key 14 is absent entirely, so the
// decoded-pointer check alone cannot reject it. The reference rejects a
// ParameterChange carrying key 14 at all, regardless of its value, so this
// must be rejected via the raw-CBOR path in
// parameterChangeSetsProtocolVersionKey.
func TestValidateParameterChangeExcludesProtocolVersionRejectsPresentNullKey14(
	t *testing.T,
) {
	minFeeA := uint(1)
	conwayRaw, err := cbor.Encode(map[uint]any{0: minFeeA, 14: nil})
	require.NoError(t, err)
	dijkstraRaw, err := cbor.Encode(map[uint]any{0: minFeeA, 14: nil})
	require.NoError(t, err)
	var conwayUpdate conway.ConwayProtocolParameterUpdate
	conwayUpdate.MinFeeA = &minFeeA
	conwayUpdate.SetCbor(conwayRaw)
	var dijkstraUpdate gdijkstra.DijkstraProtocolParameterUpdate
	dijkstraUpdate.MinFeeA = &minFeeA
	dijkstraUpdate.SetCbor(dijkstraRaw)
	for _, tc := range []struct {
		name   string
		action lcommon.GovAction
	}{
		{
			"Conway",
			&conway.ConwayParameterChangeGovAction{
				ParamUpdate: conwayUpdate,
			},
		},
		{
			"Dijkstra",
			&gdijkstra.DijkstraParameterChangeGovAction{
				ParamUpdate: dijkstraUpdate,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockConwayFeeTx{
				proposalProcedures: []lcommon.ProposalProcedure{
					conway.ConwayProposalProcedure{
						PPGovAction: conway.ConwayGovAction{
							Type: uint(
								lcommon.GovActionTypeParameterChange,
							),
							Action: tc.action,
						},
					},
				},
			}
			err := validateParameterChangeExcludesProtocolVersion(
				tx, 0, newMockLedgerState(), conwayDivergencePparams(),
			)
			var protocolVersionErr ParameterChangeProtocolVersionError
			require.ErrorAs(t, err, &protocolVersionErr)
		})
	}
}

// TestValidateParameterChangeExcludesProtocolVersionAllowsRawUpdateWithoutKey14
// is the negative case alongside the test above: a raw-CBOR-decoded update
// that never carries key 14 must still pass, proving
// parameterChangeSetsProtocolVersionKey does not over-reject an ordinary
// decoded update.
func TestValidateParameterChangeExcludesProtocolVersionAllowsRawUpdateWithoutKey14(
	t *testing.T,
) {
	minFeeA := uint(1)
	for _, tc := range []struct {
		name   string
		action lcommon.GovAction
	}{
		{
			"Conway",
			&conway.ConwayParameterChangeGovAction{
				ParamUpdate: decodeConwayParamUpdateFromRawFields(
					t,
					map[uint]any{0: minFeeA},
				),
			},
		},
		{
			"Dijkstra",
			&gdijkstra.DijkstraParameterChangeGovAction{
				ParamUpdate: decodeDijkstraParamUpdateFromRawFields(
					t,
					map[uint]any{0: minFeeA},
				),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockConwayFeeTx{
				proposalProcedures: []lcommon.ProposalProcedure{
					conway.ConwayProposalProcedure{
						PPGovAction: conway.ConwayGovAction{
							Type: uint(
								lcommon.GovActionTypeParameterChange,
							),
							Action: tc.action,
						},
					},
				},
			}
			require.NoError(
				t,
				validateParameterChangeExcludesProtocolVersion(
					tx, 0, newMockLedgerState(), conwayDivergencePparams(),
				),
			)
		})
	}
}

// TestValidateTxConwayRejectsParameterChangeProtocolVersion is a production
// ValidateTxConway regression (dingo#4439's "test through production
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

// TestValidateTxDijkstraRejectsParameterChangeProtocolVersion is the
// Dijkstra analogue of TestValidateTxConwayRejectsParameterChangeProtocolVersion,
// through the production ValidateTxDijkstra entry point.
func TestValidateTxDijkstraRejectsParameterChangeProtocolVersion(t *testing.T) {
	originalRules := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = nil
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalRules })

	tx := &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{witnesses: &mockWitnessSet{}},
		proposalProcedures: []lcommon.ProposalProcedure{
			dijkstraParameterChangeProposal(
				&lcommon.ProtocolParametersProtocolVersion{
					Major: gdijkstra.MinProtocolVersionDijkstra,
				},
			),
		},
	}
	err := ValidateTxDijkstra(
		tx,
		0,
		newMockLedgerState(),
		&gdijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters: *conwayDivergencePparams(),
		},
	)
	var protocolVersionErr ParameterChangeProtocolVersionError
	require.ErrorAs(t, err, &protocolVersionErr)
}

// TestPParamsUpdateConwayIgnoresProtocolVersion is the defense-in-depth
// regression for dingo#4439's "remove protocol-version mutation from Conway
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

// TestPParamsUpdateDijkstraIgnoresProtocolVersion is the Dijkstra analogue
// of TestPParamsUpdateConwayIgnoresProtocolVersion.
func TestPParamsUpdateDijkstraIgnoresProtocolVersion(t *testing.T) {
	current := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
		},
	}
	minFeeA := uint(500)
	updated, err := PParamsUpdateDijkstra(
		current,
		gdijkstra.DijkstraProtocolParameterUpdate{
			MinFeeA: &minFeeA,
			ProtocolVersion: &lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra + 1,
			},
		},
	)
	require.NoError(t, err)
	dijkstraUpdated, ok := updated.(*gdijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	require.Equal(
		t,
		uint(gdijkstra.MinProtocolVersionDijkstra),
		dijkstraUpdated.ProtocolVersion.Major,
		"ParameterChange must not move protocol version",
	)
	require.Equal(
		t,
		minFeeA,
		dijkstraUpdated.MinFeeA,
		"other fields must still apply",
	)
}

// TestEraDescWiresProtocolVersionProtection is the dingo#4439 "protect
// replay, import, and backfill paths" regression. ConwayEraDesc and
// DijkstraEraDesc.{ValidateTxFunc,PParamsUpdateFunc} are the single
// implementation every caller shares -- live block application
// (ledger/delta.go), Mithril bootstrap (mithril/sync_gap.go), backfill
// (internal/node/backfill.go), and conformance replay
// (internal/test/conformance/state_manager.go) all resolve validation and
// enactment through these fields rather than calling ValidateTxConway or
// PParamsUpdateConway directly. There is no separate replay-specific
// validation or enactment path to protect; this pins that the era
// descriptors actually point at the protected functions, so a future
// refactor cannot quietly rewire one path to a stale copy while leaving
// this test's direct-call coverage green.
func TestEraDescWiresProtocolVersionProtection(t *testing.T) {
	require.Equal(
		t,
		reflect.ValueOf(ValidateTxConway).Pointer(),
		reflect.ValueOf(ConwayEraDesc.ValidateTxFunc).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(PParamsUpdateConway).Pointer(),
		reflect.ValueOf(ConwayEraDesc.PParamsUpdateFunc).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(ValidateTxDijkstra).Pointer(),
		reflect.ValueOf(DijkstraEraDesc.ValidateTxFunc).Pointer(),
	)
	require.Equal(
		t,
		reflect.ValueOf(PParamsUpdateDijkstra).Pointer(),
		reflect.ValueOf(DijkstraEraDesc.PParamsUpdateFunc).Pointer(),
	)
}

// ParameterChange update keys (Conway CDDL, shared by Dijkstra).
const (
	ppuKeyMaxBlockBodySize = 2
	ppuKeyMaxTxSize        = 3
	ppuKeyMaxBHSize        = 4
	ppuKeyPoolDeposit      = 6
	ppuKeyMaxEpoch         = 7
	ppuKeyNOpt             = 8
	ppuKeyAdaPerUtxoByte   = 17
	ppuKeyCostModels       = 18
	ppuKeyMaxValueSize     = 22
	ppuKeyCollateralPct    = 23
	ppuKeyMaxCollInputs    = 24
	ppuKeyMinCommittee     = 27
	ppuKeyCommitteeTerm    = 28
	ppuKeyGovActionPeriod  = 29
	ppuKeyGovActionDeposit = 30
	ppuKeyDRepDeposit      = 31
	ppuKeyDRepInactivity   = 32
	ppuKeyRefScriptMult    = 37
	ppuKeyMinPoolMargin    = 39
	ppuKeyLeiosQuorum      = 44
	ppuKeyMaxEBExUnits     = 47
)

func ppuRat(num, den int64) cbor.Tag {
	return cbor.Tag{Number: 30, Content: []any{num, den}}
}

// parameterChangeTxCbor builds a transaction carrying one ParameterChange
// proposal with update map ppu. The transaction is otherwise minimal: these
// tests assert which rule rejects the proposal, not that the whole
// transaction is valid.
func parameterChangeTxCbor(t *testing.T, ppu map[uint]any) []byte {
	t.Helper()
	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	addr := make([]byte, 29)
	addr[0] = 0x60
	rewardAccount := make([]byte, 29)
	rewardAccount[0] = 0xe0
	body := map[uint]any{
		0: cbor.Tag{
			Number:  258,
			Content: []any{[]any{inputHash, uint64(0)}},
		},
		1: []any{[]any{addr, uint64(1_000_000)}},
		2: uint64(200_000),
		20: []any{
			[]any{
				uint64(1_000_000_000),
				rewardAccount,
				[]any{
					uint64(lcommon.GovActionTypeParameterChange),
					nil,
					ppu,
					nil,
				},
				[]any{"https://example.invalid/a", make([]byte, 32)},
			},
		},
	}
	txCbor, err := cbor.Encode([]any{body, map[uint]any{}, true, nil})
	require.NoError(t, err)
	return txCbor
}

// validateParameterChange runs the production validator for era over a
// transaction carrying ppu. A decode failure is reported with a "decode:"
// prefix: the reference also rejects an undecodable update.
func validateParameterChange(
	t *testing.T,
	era string,
	major uint,
	ppu map[uint]any,
) error {
	t.Helper()
	txCbor := parameterChangeTxCbor(t, ppu)
	pp := conwayDivergencePparams()
	pp.ProtocolVersion.Major = major
	switch era {
	case "Conway":
		tx, err := conway.NewConwayTransactionFromCbor(txCbor)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		return ValidateTxConway(tx, 0, newMockLedgerState(), pp)
	case "Dijkstra":
		tx, err := gdijkstra.NewDijkstraTransactionFromCbor(txCbor)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		return ValidateTxDijkstra(
			tx,
			0,
			newMockLedgerState(),
			&gdijkstra.DijkstraProtocolParameters{
				ConwayProtocolParameters: *pp,
			},
		)
	}
	t.Fatalf("unknown era %q", era)
	return nil
}

// requireRejectedBy asserts err names the rule text want, so a rejection by
// an unrelated rule (bad inputs, fee) cannot satisfy the test.
func requireRejectedBy(t *testing.T, err error, want string) {
	t.Helper()
	require.Error(t, err)
	require.ErrorContains(t, err, want)
}

// requireNotRejectedBy asserts err, if any, does not mention any of the
// parameter-change diagnostics; these minimal transactions still fail
// unrelated rules.
func requireNotRejectedBy(t *testing.T, err error, unwanted ...string) {
	t.Helper()
	if err == nil {
		return
	}
	for _, s := range unwanted {
		require.NotContains(t, err.Error(), s)
	}
}

// TestValidateTxParameterChangeZeroFields covers dingo#4438: six
// unconditional zero-valued fields, and the PV-gated AdaPerUtxoByte (PV10+)
// and NOpt (PV11+) boundaries.
func TestValidateTxParameterChangeZeroFields(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		major      uint
		ppu        map[uint]any
		diagnostic string
		reject     bool
		conwayOnly bool // the case is below Dijkstra's PV12 floor
	}{
		{
			"CollateralPercentage",
			9,
			map[uint]any{ppuKeyCollateralPct: 0},
			"collateralPercentage",
			true,
			false,
		},
		{
			"CommitteeTermLimit",
			9,
			map[uint]any{ppuKeyCommitteeTerm: 0},
			"committeeMaxTermLength",
			true,
			false,
		},
		{
			"GovActionValidityPeriod",
			9,
			map[uint]any{ppuKeyGovActionPeriod: 0},
			"govActionLifetime",
			true,
			false,
		},
		{
			"PoolDeposit",
			9,
			map[uint]any{ppuKeyPoolDeposit: 0},
			"poolDeposit",
			true,
			false,
		},
		{
			"GovActionDepositPV11",
			11,
			map[uint]any{ppuKeyGovActionDeposit: 0},
			"govActionDeposit",
			true,
			false,
		},
		{
			"DRepDeposit",
			9,
			map[uint]any{ppuKeyDRepDeposit: 0},
			"drepDeposit",
			true,
			false,
		},
		{
			"AdaPerUtxoBytePV10",
			10,
			map[uint]any{ppuKeyAdaPerUtxoByte: 0},
			"coinsPerUTxOByte",
			true,
			false,
		},
		{
			"AdaPerUtxoBytePV11",
			11,
			map[uint]any{ppuKeyAdaPerUtxoByte: 0},
			"coinsPerUTxOByte",
			true,
			false,
		},
		{
			"NOptPV11",
			11,
			map[uint]any{ppuKeyNOpt: 0},
			"nOptimalPoolCount",
			true,
			false,
		},
		{
			"AdaPerUtxoBytePV9Allowed",
			9,
			map[uint]any{ppuKeyAdaPerUtxoByte: 0},
			"coinsPerUTxOByte",
			false,
			true,
		},
		{
			"NOptPV9Allowed",
			9,
			map[uint]any{ppuKeyNOpt: 0},
			"nOptimalPoolCount",
			false,
			true,
		},
		{
			"NOptPV10Allowed",
			10,
			map[uint]any{ppuKeyNOpt: 0},
			"nOptimalPoolCount",
			false,
			true,
		},
		{
			"CollateralPercentageNonzero",
			9,
			map[uint]any{ppuKeyCollateralPct: 1},
			"collateralPercentage",
			false,
			false,
		},
		{
			"GovActionDepositNonzero",
			11,
			map[uint]any{ppuKeyGovActionDeposit: 1},
			"govActionDeposit",
			false,
			false,
		},
		{
			"AdaPerUtxoByteNonzero",
			11,
			map[uint]any{ppuKeyAdaPerUtxoByte: 1},
			"coinsPerUTxOByte",
			false,
			false,
		},
		{
			"NOptNonzero",
			11,
			map[uint]any{ppuKeyNOpt: 1},
			"nOptimalPoolCount",
			false,
			false,
		},
	}
	for _, era := range []string{"Conway", "Dijkstra"} {
		for _, tc := range cases {
			major := tc.major
			if era == "Dijkstra" {
				if tc.conwayOnly {
					continue
				}
				major = gdijkstra.MinProtocolVersionDijkstra
			}
			t.Run(era+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				err := validateParameterChange(t, era, major, tc.ppu)
				if tc.reject {
					requireRejectedBy(t, err, tc.diagnostic)
				} else {
					requireNotRejectedBy(t, err, tc.diagnostic, "decode:")
				}
			})
		}
	}
}

// TestValidateTxParameterChangeIntegerWidths covers dingo#4478: the exact
// maximum of each .size 2 and .size 4 field is accepted, maximum+1 rejected.
func TestValidateTxParameterChangeIntegerWidths(t *testing.T) {
	t.Parallel()
	fields := []struct {
		name string
		key  uint
		max  uint64
	}{
		{"maxBlockBodySize", ppuKeyMaxBlockBodySize, math.MaxUint32},
		{"maxTxSize", ppuKeyMaxTxSize, math.MaxUint32},
		{"maxEpoch", ppuKeyMaxEpoch, math.MaxUint32},
		{"maxValueSize", ppuKeyMaxValueSize, math.MaxUint32},
		{"committeeTermLimit", ppuKeyCommitteeTerm, math.MaxUint32},
		{"govActionValidityPeriod", ppuKeyGovActionPeriod, math.MaxUint32},
		{"dRepInactivityPeriod", ppuKeyDRepInactivity, math.MaxUint32},
		{"maxBlockHeaderSize", ppuKeyMaxBHSize, math.MaxUint16},
		{"nOpt", ppuKeyNOpt, math.MaxUint16},
		{"collateralPercentage", ppuKeyCollateralPct, math.MaxUint16},
		{"maxCollateralInputs", ppuKeyMaxCollInputs, math.MaxUint16},
		{"minCommitteeSize", ppuKeyMinCommittee, math.MaxUint16},
	}
	for _, era := range []string{"Conway", "Dijkstra"} {
		for _, f := range fields {
			t.Run(era+"/"+f.name+"/max", func(t *testing.T) {
				t.Parallel()
				err := validateParameterChange(
					t, era, 11, map[uint]any{f.key: f.max},
				)
				requireNotRejectedBy(t, err, "must fit Word", "decode:")
			})
			t.Run(era+"/"+f.name+"/max+1", func(t *testing.T) {
				t.Parallel()
				err := validateParameterChange(
					t, era, 11, map[uint]any{f.key: f.max + 1},
				)
				require.Error(t, err)
				require.ErrorContains(t, err, "must fit Word")
			})
		}
	}
}

// TestValidateTxParameterChangeDijkstraDomains covers dingo#4596: the
// Dijkstra-only tags and the inherited Conway tags Dijkstra must not bypass.
func TestValidateTxParameterChangeDijkstraDomains(t *testing.T) {
	t.Parallel()
	exUnits := func(mem, steps int64) []any { return []any{mem, steps} }
	cases := []struct {
		name       string
		ppu        map[uint]any
		diagnostic string
		reject     bool
	}{
		{
			"tag37-zero",
			map[uint]any{ppuKeyRefScriptMult: ppuRat(0, 1)},
			"refScriptCostMultiplier",
			true,
		},
		{
			"tag39-above-one",
			map[uint]any{ppuKeyMinPoolMargin: ppuRat(2, 1)},
			"minPoolMargin",
			true,
		},
		{
			"tag44-above-one",
			map[uint]any{ppuKeyLeiosQuorum: ppuRat(2, 1)},
			"leiosQuorumStakeThreshold",
			true,
		},
		{
			"tag47-negative",
			map[uint]any{ppuKeyMaxEBExUnits: exUnits(-1, 0)},
			"cannot unmarshal negative integer",
			true,
		},
		{
			"tag9-negative",
			map[uint]any{9: ppuRat(-1, 1)},
			"tag 9: rational numerator must be in Word64",
			true,
		},
		{
			"tag10-above-one",
			map[uint]any{10: ppuRat(2, 1)},
			"rho: must be in [0,1]",
			true,
		},
		{
			"tag11-above-one",
			map[uint]any{11: ppuRat(2, 1)},
			"tau: must be in [0,1]",
			true,
		},
		{
			"tag19-negative",
			map[uint]any{19: []any{ppuRat(-1, 1), ppuRat(1, 1)}},
			"tag 19: rational at array index 0: rational numerator must be in Word64",
			true,
		},
		{
			"tag20-negative",
			map[uint]any{20: exUnits(-1, 0)},
			"cannot unmarshal negative integer",
			true,
		},
		{
			"tag21-negative",
			map[uint]any{21: exUnits(0, -1)},
			"cannot unmarshal negative integer",
			true,
		},
		{
			"tag25-above-one",
			map[uint]any{
				25: []any{
					ppuRat(2, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
				},
			},
			"poolVotingThresholds",
			true,
		},
		{
			"tag26-above-one",
			map[uint]any{
				26: []any{
					ppuRat(2, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
					ppuRat(1, 1),
				},
			},
			"drepVotingThresholds",
			true,
		},
		{
			"tag33-negative",
			map[uint]any{33: ppuRat(-1, 1)},
			"tag 33: rational numerator must be in Word64",
			true,
		},
		{
			"valid-field-with-null-tag39",
			map[uint]any{ppuKeyMaxTxSize: 16384, ppuKeyMinPoolMargin: nil},
			"tag 39",
			true,
		},
		{
			"tag39-zero",
			map[uint]any{ppuKeyMinPoolMargin: ppuRat(0, 1)},
			"minPoolMargin",
			false,
		},
		{
			"tag39-one",
			map[uint]any{ppuKeyMinPoolMargin: ppuRat(1, 1)},
			"minPoolMargin",
			false,
		},
		{
			"tag44-zero",
			map[uint]any{ppuKeyLeiosQuorum: ppuRat(0, 1)},
			"leiosQuorumStakeThreshold",
			false,
		},
		{
			"tag44-one",
			map[uint]any{ppuKeyLeiosQuorum: ppuRat(1, 1)},
			"leiosQuorumStakeThreshold",
			false,
		},
		{
			"tag47-zero",
			map[uint]any{ppuKeyMaxEBExUnits: exUnits(0, 0)},
			"maxEndorserBlockExUnits",
			false,
		},
		{
			"tag47-maxint64",
			map[uint]any{
				ppuKeyMaxEBExUnits: exUnits(math.MaxInt64, math.MaxInt64),
			},
			"maxEndorserBlockExUnits",
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateParameterChange(
				t, "Dijkstra", gdijkstra.MinProtocolVersionDijkstra, tc.ppu,
			)
			if tc.reject {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.diagnostic)
			} else {
				requireNotRejectedBy(t, err, tc.diagnostic, "decode:")
			}
		})
	}
}

// TestValidateTxParameterChangeCostModelLanguageIDWidth covers dingo#4607:
// a cost-model language ID above Word8 is rejected, 255 stays an accepted
// unknown future language.
func TestValidateTxParameterChangeCostModelLanguageIDWidth(t *testing.T) {
	t.Parallel()
	for _, era := range []string{"Conway", "Dijkstra"} {
		t.Run(era+"/256", func(t *testing.T) {
			t.Parallel()
			err := validateParameterChange(t, era, 11, map[uint]any{
				ppuKeyCostModels: map[uint]any{256: []int64{1, 2, 3}},
			})
			require.Error(t, err)
			require.ErrorContains(t, err, "exceeds Word8 maximum 255")
		})
		t.Run(era+"/255", func(t *testing.T) {
			t.Parallel()
			err := validateParameterChange(t, era, 11, map[uint]any{
				ppuKeyCostModels: map[uint]any{255: []int64{1, 2, 3}},
			})
			requireNotRejectedBy(t, err, "costModels", "language", "decode:")
		})
	}
}

// Slots used by the tests below: the transaction is applied inside the era
// forecast horizon but its TTL falls past it, which is the shape of a real
// preview transaction (block slot 699109, TTL 785381, horizon 777600) that
// wedged `dingo load`.
const (
	testAppliedSlot     = 699_109
	testHorizonSlot     = 777_600
	testPastHorizonSlot = 785_381
)

// pastHorizonLedgerState resolves slots to times only inside the era forecast
// horizon, mirroring ledger.LedgerState.SlotToTime once the current era is
// bounded by its safe zone.
type pastHorizonLedgerState struct {
	*mockLedgerState
	horizonSlot     uint64
	slotToTimeCalls int
}

func newPastHorizonLedgerState() *pastHorizonLedgerState {
	return &pastHorizonLedgerState{
		mockLedgerState: newMockLedgerState(),
		horizonSlot:     testHorizonSlot,
	}
}

func (s *pastHorizonLedgerState) SlotToTime(
	slot uint64,
) (time.Time, error) {
	s.slotToTimeCalls++
	if slot >= s.horizonSlot {
		return time.Time{}, hardfork.ErrPastHorizon
	}
	// #nosec G115 -- test slots are small
	return time.Unix(int64(slot), 0), nil
}

// withoutBabbageUtxoValidationRules drops the gouroboros phase-1 rule set so a
// test can exercise the dingo-side script handling in isolation, mirroring
// withoutConwayUtxoValidationRules.
func withoutBabbageUtxoValidationRules(t *testing.T) {
	t.Helper()

	orig := babbageUtxoValidationRules
	babbageUtxoValidationRules = nil
	t.Cleanup(func() {
		babbageUtxoValidationRules = orig
	})
}

func withoutAlonzoUtxoValidationRules(t *testing.T) {
	t.Helper()

	orig := alonzoUtxoValidationRules
	alonzoUtxoValidationRules = nil
	t.Cleanup(func() {
		alonzoUtxoValidationRules = orig
	})
}

// newTestTxCbor builds transaction CBOR with a single input, a fee, and a TTL.
func newTestTxCbor(
	t *testing.T,
	ttl uint64,
	witnessSet map[uint]any,
) []byte {
	t.Helper()

	inputHash := make([]byte, 32)
	inputHash[0] = 0xaa
	bodyMap := map[uint]any{
		0: []any{
			[]any{inputHash, uint64(0)},
		},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1_000_000)}},
		2: uint64(200_000),
		3: ttl,
	}
	txCbor, err := cbor.Encode(
		[]any{bodyMap, witnessSet, true, nil},
	)
	require.NoError(t, err)
	return txCbor
}

// redeemerWitnessSet is a witness set carrying one spend redeemer, which is
// what makes a transaction require Plutus evaluation.
func redeemerWitnessSet() map[uint]any {
	return map[uint]any{
		5: []any{
			[]any{
				uint64(0), // tag: spend
				uint64(0), // index
				uint64(42),
				[]any{uint64(1_000), uint64(2_000)},
			},
		},
	}
}

// A transaction with no redeemers runs no Plutus script, so no script context
// may be built for it: building one translates its TTL to wall-clock time,
// which fails past the era forecast horizon and rejects a canonical block.
func TestValidateTxBabbageSkipsScriptContextWithoutRedeemers(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, map[uint]any{}),
	)
	require.NoError(t, err)
	require.False(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	err = ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Zero(
		t,
		ls.slotToTimeCalls,
		"no slot/time translation may happen for a redeemerless transaction",
	)
}

// The gate must not weaken the horizon for transactions that do run scripts:
// those still translate their validity interval, and a past-horizon TTL is a
// genuine translation failure (cardano-ledger's TimeTranslationPastHorizon).
func TestValidateTxBabbageKeepsHorizonForRedeemerTx(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, redeemerWitnessSet()),
	)
	require.NoError(t, err)
	require.True(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	err = ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)
	assert.Positive(t, ls.slotToTimeCalls)
}

// The accept half of the redeemer class: a transaction that does run scripts
// and whose validity bound is inside the horizon must have its script context
// built, which means its validity interval is translated and the translation
// succeeds. Only the reject half was pinned before, so a regression that
// refused every redeemer transaction's translation would have gone unnoticed.
//
// The transaction carries no matching script, so evaluation cannot proceed past
// the redeemer lookup; reaching that lookup is the proof that the script
// context was built rather than skipped or refused.
func TestValidateTxBabbageBuildsScriptContextInsideHorizon(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot+100, redeemerWitnessSet()),
	)
	require.NoError(t, err)
	require.True(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	err = ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.Error(t, err)
	require.NotErrorIs(t, err, hardfork.ErrPastHorizon,
		"a validity bound inside the horizon must translate")
	assert.ErrorContains(t, err, "could not find script with hash",
		"validation must reach redeemer resolution, which only happens once "+
			"the script context has been built")
	assert.Positive(t, ls.slotToTimeCalls,
		"building the script context must translate the validity interval")
}

// A redeemerless transaction whose TTL is inside the horizon behaves the same
// either way, so the gate cannot be hiding a translation that used to succeed.
func TestValidateTxBabbageWithoutRedeemersInsideHorizon(t *testing.T) {
	withoutBabbageUtxoValidationRules(t)

	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot+100, map[uint]any{}),
	)
	require.NoError(t, err)

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	require.NoError(t, ValidateTxBabbage(
		tx,
		testAppliedSlot,
		ls,
		&babbage.BabbageProtocolParameters{},
	))
}

func TestValidateTxAlonzoSkipsScriptContextWithoutRedeemers(t *testing.T) {
	withoutAlonzoUtxoValidationRules(t)

	tx, err := alonzo.NewAlonzoTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, map[uint]any{}),
	)
	require.NoError(t, err)
	require.False(t, txHasRedeemers(tx))

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	err = ValidateTxAlonzo(
		tx,
		testAppliedSlot,
		ls,
		&alonzo.AlonzoProtocolParameters{MaxTxSize: 16_384},
	)
	require.NoError(t, err)
	assert.Zero(t, ls.slotToTimeCalls)
}

func TestEvaluateTxBabbageSkipsScriptContextWithoutRedeemers(t *testing.T) {
	tx, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testPastHorizonSlot, map[uint]any{}),
	)
	require.NoError(t, err)

	ls := newPastHorizonLedgerState()
	ls.addUtxo(tx.Inputs()[0], newTestOutput(1_000_000))

	_, exUnits, redeemerExUnits, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{},
	)
	require.NoError(t, err)
	assert.Equal(t, lcommon.ExUnits{}, exUnits)
	assert.Empty(t, redeemerExUnits)
	assert.Zero(t, ls.slotToTimeCalls)
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

func TestTxHasRedeemers(t *testing.T) {
	withRedeemer, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot, redeemerWitnessSet()),
	)
	require.NoError(t, err)
	assert.True(t, txHasRedeemers(withRedeemer))

	withoutRedeemer, err := babbage.NewBabbageTransactionFromCbor(
		newTestTxCbor(t, testAppliedSlot, map[uint]any{}),
	)
	require.NoError(t, err)
	assert.False(t, txHasRedeemers(withoutRedeemer))
}

// uplcProgramVersion110 is UPLC program version 1.1.0, the version Plutus V3
// and V4 scripts are encoded with.
var uplcProgramVersion110 = lang.LanguageVersion{1, 1, 0}

type declaredValidityConwayTx struct {
	*mockConwayFeeTx
	valid bool
}

func newDijkstraGuardingValidityOutcomeTx(
	t *testing.T,
	valid bool,
	version lang.LanguageVersion,
	scriptFails bool,
	exUnits lcommon.ExUnits,
) *gdijkstra.DijkstraTransaction {
	t.Helper()
	program := &syn.Program[syn.DeBruijn]{
		// The UPLC program version is not the ledger language version. Plutus
		// V3 and V4 scripts carry UPLC 1.1.0; lang.LanguageVersionV3/V4
		// ({1,2,0} and {1,3,0}) select the cost model and are not valid
		// program versions.
		Version: uplcProgramVersion110,
		Term: &syn.Lambda[syn.DeBruijn]{
			Body: &syn.Constant{Con: &syn.Unit{}},
		},
	}
	flatProgram, err := syn.Encode(program)
	require.NoError(t, err)
	scriptBytes, err := cbor.Encode(flatProgram)
	require.NoError(t, err)
	if scriptFails {
		// The deliberately malformed Flat payload reaches the concrete Dijkstra
		// guarding evaluator and is reported as PlutusScriptFailedError.
		scriptBytes = []byte{0x41, 0x00}
	}

	var script lcommon.Script
	var subTxWitnesses gdijkstra.DijkstraTransactionWitnessSet
	switch version {
	case lang.LanguageVersionV3:
		plutusScript := lcommon.PlutusV3Script(scriptBytes)
		script = plutusScript
		subTxWitnesses.WsPlutusV3Scripts = cbor.NewSetType(
			[]lcommon.PlutusV3Script{plutusScript},
			false,
		)
	case lang.LanguageVersionV4:
		plutusScript := lcommon.PlutusV4Script(scriptBytes)
		script = plutusScript
		subTxWitnesses.WsPlutusV4Scripts = cbor.NewSetType(
			[]lcommon.PlutusV4Script{plutusScript},
			false,
		)
	default:
		t.Fatalf("unsupported guarding script version %v", version)
	}

	return &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxGuards: &gdijkstra.DijkstraGuards{
				Credentials: []lcommon.Credential{{
					CredType:   lcommon.CredentialTypeScriptHash,
					Credential: script.Hash(),
				}},
			},
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					WitnessSet: subTxWitnesses,
				}},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagGuarding, Index: 0}: {
						ExUnits: exUnits,
					},
				},
			},
		},
		TxIsValid: valid,
	}
}

// dijkstraValidityOutcomePParams supplies real Preview epoch-672 cost models.
// plutigo costs every parameter missing from a supplied list at
// math.MaxInt64, as plutus-ledger-api does, so an empty CostModels map makes
// the first CEK machine step exhaust any budget. PlutusV4 reuses the PlutusV3
// list: its machine-step parameters are costed, and the builtins it leaves at
// MaxInt64 are never called by these scripts.
func dijkstraValidityOutcomePParams(
	t *testing.T,
) *gdijkstra.DijkstraProtocolParameters {
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
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				0: costModels.PlutusV1,
				1: costModels.PlutusV2,
				2: costModels.PlutusV3,
				3: costModels.PlutusV3,
			},
		},
	}
}

func TestValidateTxDijkstraRequiresDeclaredValidityToMatchGuardingExecution(
	t *testing.T,
) {
	originalPhase1 := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = nil
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalPhase1 })

	for _, scriptVersion := range []struct {
		name    string
		version lang.LanguageVersion
	}{
		{name: "Plutus V3", version: lang.LanguageVersionV3},
		{name: "Plutus V4", version: lang.LanguageVersionV4},
	} {
		t.Run(scriptVersion.name, func(t *testing.T) {
			for _, outcome := range []struct {
				name          string
				declaredValid bool
				scriptFails   bool
				exUnits       lcommon.ExUnits
				assert        func(*testing.T, error)
			}{
				{
					name:          "declared valid and script passes",
					declaredValid: true,
					scriptFails:   false,
					exUnits: lcommon.ExUnits{
						Steps: 10_000_000, Memory: 10_000_000,
					},
					assert: func(t *testing.T, err error) { require.NoError(t, err) },
				},
				{
					name:          "declared invalid and script fails",
					declaredValid: false,
					scriptFails:   true,
					exUnits:       lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
					assert:        func(t *testing.T, err error) { require.NoError(t, err) },
				},
				{
					name:          "declared valid and script fails",
					declaredValid: true,
					scriptFails:   true,
					exUnits:       lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
					assert: func(t *testing.T, err error) {
						var scriptErr conway.PlutusScriptFailedError
						require.ErrorAs(t, err, &scriptErr)
					},
				},
				{
					name:          "declared invalid and script passes",
					declaredValid: false,
					scriptFails:   false,
					exUnits: lcommon.ExUnits{
						Steps: 10_000_000, Memory: 10_000_000,
					},
					assert: func(t *testing.T, err error) {
						require.ErrorContains(
							t,
							err,
							"declared invalid but Plutus scripts succeeded",
						)
					},
				},
			} {
				t.Run(outcome.name, func(t *testing.T) {
					tx := newDijkstraGuardingValidityOutcomeTx(
						t,
						outcome.declaredValid,
						scriptVersion.version,
						outcome.scriptFails,
						outcome.exUnits,
					)
					err := ValidateTxDijkstra(
						tx,
						0,
						newMockLedgerState(),
						dijkstraValidityOutcomePParams(t),
					)
					outcome.assert(t, err)
				})
			}
		})
	}
}

func TestValidateTxDijkstraDoesNotTreatPhase1FailureAsPhase2Failure(
	t *testing.T,
) {
	originalPhase1 := dijkstraPhase1UtxoValidationRules
	phase1Sentinel := errors.New("Dijkstra phase-1 sentinel")
	dijkstraPhase1UtxoValidationRules = []indexedUtxoValidationRule{{
		index: 0,
		validationFunc: func(
			lcommon.Transaction,
			uint64,
			lcommon.LedgerState,
			lcommon.ProtocolParameters,
		) error {
			return phase1Sentinel
		},
	}}
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalPhase1 })

	tx := newDijkstraGuardingValidityOutcomeTx(
		t,
		false,
		lang.LanguageVersionV4,
		true,
		lcommon.ExUnits{},
	)
	err := ValidateTxDijkstra(
		tx,
		0,
		newMockLedgerState(),
		dijkstraValidityOutcomePParams(t),
	)
	require.ErrorIs(t, err, phase1Sentinel)
}

// TestValidateTxDijkstraSkipPhase2StillValidatesRequiredRedeemers pins the
// Dijkstra phase-1 boundary through Dingo's production entry point. Historical
// replay may skip script execution, but a reference-script spend must still
// carry its required redeemer.
func TestValidateTxDijkstraSkipPhase2StillValidatesRequiredRedeemers(
	t *testing.T,
) {
	requiredRedeemerIndex := noUtxoValidationRuleIndex
	for index, descriptor := range gdijkstra.UtxoValidationRuleDescriptors() {
		if descriptor.Id == lcommon.UtxoValidationRuleRequiredRedeemers {
			requiredRedeemerIndex = index
			break
		}
	}
	require.NotEqual(
		t,
		noUtxoValidationRuleIndex,
		requiredRedeemerIndex,
		"Dijkstra must declare the required-redeemer rule",
	)

	var requiredRedeemerRule *indexedUtxoValidationRule
	for _, rule := range dijkstraPhase1UtxoValidationRules {
		if rule.index == requiredRedeemerIndex {
			ruleCopy := rule
			requiredRedeemerRule = &ruleCopy
			break
		}
	}
	require.NotNil(
		t,
		requiredRedeemerRule,
		"Dijkstra phase-1 validation must retain the required-redeemer rule",
	)

	originalPhase1 := dijkstraPhase1UtxoValidationRules
	dijkstraPhase1UtxoValidationRules = []indexedUtxoValidationRule{
		*requiredRedeemerRule,
	}
	t.Cleanup(func() { dijkstraPhase1UtxoValidationRules = originalPhase1 })

	plutusScript := lcommon.PlutusV1Script{0x01, 0x02, 0x03}
	scriptAddr := newTestScriptAddress(t, plutusScript)
	spendInput := shelley.NewShelleyTransactionInput(
		"6666666666666666666666666666666666666666666666666666666666666666",
		0,
	)
	ls := newMockLedgerState()
	ls.skipPhase2Validation = true
	ls.addUtxo(
		spendInput,
		testAddressScriptOutput{
			testOutput: newTestOutput(1_000),
			addr:       scriptAddr,
			scriptRef:  plutusScript,
		},
	)

	newTx := func() *gdijkstra.DijkstraTransaction {
		return &gdijkstra.DijkstraTransaction{
			Body: gdijkstra.DijkstraTransactionBody{
				TxInputs: conway.NewConwayTransactionInputSet(
					[]shelley.ShelleyTransactionInput{spendInput},
				),
			},
			TxIsValid: true,
		}
	}

	t.Run("missing redeemer", func(t *testing.T) {
		err := ValidateTxDijkstra(
			newTx(),
			0,
			ls,
			dijkstraValidityOutcomePParams(t),
		)
		var missing lcommon.MissingRedeemerForScriptError
		require.ErrorAs(t, err, &missing)
		require.Equal(t, plutusScript.Hash(), missing.ScriptHash)
		require.Equal(t, lcommon.RedeemerTagSpend, missing.Tag)
		require.Equal(t, uint32(0), missing.Index)
	})

	t.Run("matching redeemer", func(t *testing.T) {
		tx := newTx()
		tx.WitnessSet = gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
						ExUnits: lcommon.ExUnits{Steps: 1, Memory: 1},
					},
				},
			},
		}
		require.NoError(t, ValidateTxDijkstra(
			tx,
			0,
			ls,
			dijkstraValidityOutcomePParams(t),
		))
	})
}

func (t *declaredValidityConwayTx) IsValid() bool {
	return t.valid
}

type validityOutcomeRedeemers struct {
	*mockRedeemers
}

func (r *validityOutcomeRedeemers) Value(
	idx uint,
	tag lcommon.RedeemerTag,
) lcommon.RedeemerValue {
	for _, entry := range r.entries {
		if entry.key.Index == uint32(idx) && entry.key.Tag == tag {
			return entry.val
		}
	}
	return lcommon.RedeemerValue{}
}

func newConwayValidityOutcomeTx(
	t *testing.T,
	valid bool,
	version lang.LanguageVersion,
	scriptFails bool,
	exUnits lcommon.ExUnits,
) *declaredValidityConwayTx {
	t.Helper()
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
	if scriptFails {
		// This malformed Flat payload reaches the evaluator rather than the
		// budget check, exercising the execution-error outcome path.
		scriptBytes = []byte{0x41, 0x00}
	}

	var script lcommon.Script
	witnesses := &mockWitnessSet{redeemers: &validityOutcomeRedeemers{
		mockRedeemers: &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagMint,
						Index: 0,
					},
					val: lcommon.RedeemerValue{ExUnits: exUnits},
				},
			},
		},
	}}
	switch version {
	case lang.LanguageVersionV1:
		plutusScript := lcommon.PlutusV1Script(scriptBytes)
		script = plutusScript
		witnesses.plutusV1Scripts = []lcommon.PlutusV1Script{plutusScript}
	case lang.LanguageVersionV2:
		plutusScript := lcommon.PlutusV2Script(scriptBytes)
		script = plutusScript
		witnesses.plutusV2Scripts = []lcommon.PlutusV2Script{plutusScript}
	default:
		t.Fatalf("unsupported Plutus version %v", version)
	}
	scriptHash := script.Hash()
	assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
			lcommon.Blake2b224(scriptHash): {
				cbor.NewByteString([]byte("asset")): big.NewInt(1),
			},
		},
	)
	return &declaredValidityConwayTx{
		valid: valid,
		mockConwayFeeTx: &mockConwayFeeTx{
			mockFeeTx: mockFeeTx{
				txType:    txTypeAlonzo,
				witnesses: witnesses,
			},
			assetMint: &assetMint,
		},
	}
}

func TestValidateTxRequiresDeclaredValidityToMatchExecution(
	t *testing.T,
) {
	origAlonzo := alonzoUtxoValidationRules
	origBabbage := babbageUtxoValidationRules
	origAll := conwayUtxoValidationRules
	origPhase1 := conwayPhase1UtxoValidationRules
	t.Cleanup(func() {
		alonzoUtxoValidationRules = origAlonzo
		babbageUtxoValidationRules = origBabbage
		conwayUtxoValidationRules = origAll
		conwayPhase1UtxoValidationRules = origPhase1
	})
	// Keep the test focused on the phase-2 outcome contract. Phase-1 behavior
	// is covered separately by the validation-rule suite.
	alonzoUtxoValidationRules = nil
	babbageUtxoValidationRules = nil
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil

	tests := []struct {
		name     string
		version  lang.LanguageVersion
		validate func(lcommon.Transaction) error
	}{
		{
			name:    "alonzo Plutus V1",
			version: lang.LanguageVersionV1,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxAlonzo(
					tx,
					0,
					newMockLedgerState(),
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 5,
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							0: syntheticFullCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
			},
		},
		{
			name:    "babbage Plutus V1",
			version: lang.LanguageVersionV1,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxBabbage(
					tx,
					0,
					newMockLedgerState(),
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							0: syntheticFullCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
			},
		},
		{
			name:    "babbage Plutus V2",
			version: lang.LanguageVersionV2,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxBabbage(
					tx,
					0,
					newMockLedgerState(),
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							1: syntheticFullCostModel(
								t,
								lang.LanguageVersionV2,
							),
						},
					},
				)
			},
		},
		{
			name:    "conway Plutus V1",
			version: lang.LanguageVersionV1,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxConway(
					tx,
					0,
					newMockLedgerState(),
					&conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: 9,
						},
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							0: syntheticFullCostModel(
								t,
								lang.LanguageVersionV1,
							),
						},
					},
				)
			},
		},
		{
			name:    "conway Plutus V2",
			version: lang.LanguageVersionV2,
			validate: func(tx lcommon.Transaction) error {
				return ValidateTxConway(
					tx,
					0,
					newMockLedgerState(),
					&conway.ConwayProtocolParameters{
						ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
							Major: 9,
						},
						MaxTxExUnits: lcommon.ExUnits{
							Steps:  10_000_000,
							Memory: 10_000_000,
						},
						CostModels: map[uint][]int64{
							1: syntheticFullCostModel(
								t,
								lang.LanguageVersionV2,
							),
						},
					},
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("declared valid and scripts succeed", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					true,
					test.version,
					false,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				require.NoError(t, test.validate(tx))
			})

			t.Run("declared invalid but scripts succeed", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					false,
					test.version,
					false,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				err := test.validate(tx)
				require.ErrorContains(
					t,
					err,
					"declared invalid but Plutus scripts succeeded",
				)
			})

			t.Run("declared valid but scripts fail", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					true,
					test.version,
					false,
					lcommon.ExUnits{},
				)
				err := test.validate(tx)
				_, ok := errors.AsType[conway.PlutusScriptFailedError](err)
				require.True(
					t,
					ok,
					"expected Plutus script failure, got %v",
					err,
				)
			})

			t.Run("declared invalid and scripts fail", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					false,
					test.version,
					false,
					lcommon.ExUnits{},
				)
				require.NoError(t, test.validate(tx))
			})

			t.Run("declared valid but evaluator errors", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					true,
					test.version,
					true,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				var scriptErr conway.PlutusScriptFailedError
				require.ErrorAs(t, test.validate(tx), &scriptErr)
			})

			t.Run("declared invalid and evaluator errors", func(t *testing.T) {
				tx := newConwayValidityOutcomeTx(
					t,
					false,
					test.version,
					true,
					lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
				)
				require.NoError(t, test.validate(tx))
			})
		})
	}
}

// mockConwayFeeTxV3 adds CurrentTreasuryValue, which
// script.NewTxInfoV3FromTransaction requires and mockConwayFeeTx does not
// implement.
type mockConwayFeeTxV3 struct {
	mockConwayFeeTx
}

func (m *mockConwayFeeTxV3) CurrentTreasuryValue() *big.Int {
	return nil
}

func (m *mockConwayFeeTxV3) Donation() *big.Int {
	return nil
}

// TestRequiredCostModelWiringMissingEntryFailsClosed is a regression test
// for a human-review finding: requiredCostModel (issue #3528) is exercised
// by nine call sites across alonzo.go, babbage.go and conway.go, but before
// this test only one of them -- conway.go's PlutusV1 branch, via
// TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch -- had a test
// that failed if it were reverted to a bare, unguarded map index. The other
// eight (ValidateTxAlonzo, EvaluateTxAlonzo, ValidateTxBabbage's V1 and V2
// branches, EvaluateTxBabbage's V1 and V2 branches, and
// evaluateConwayPlutusScript's V2 and V3 branches) could each silently
// regress to evaluating a missing cost model under plutigo's built-in
// defaults without any test noticing. This pins all eight remaining sites
// to the same fail-closed contract.
func TestRequiredCostModelWiringMissingEntryFailsClosed(t *testing.T) {
	// A minimal unit-returning program. Its behavior under evaluation is
	// irrelevant here: a missing cost model must be rejected by
	// requiredCostModel before plutigo's evaluator ever runs.
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

	spendRedeemers := func() *mockRedeemers {
		return &mockRedeemers{
			entries: []struct {
				key lcommon.RedeemerKey
				val lcommon.RedeemerValue
			}{
				{
					key: lcommon.RedeemerKey{
						Tag:   lcommon.RedeemerTagSpend,
						Index: 0,
					},
					val: lcommon.RedeemerValue{ExUnits: lcommon.ExUnits{}},
				},
			},
		}
	}

	// newSpendFixture builds a transaction spending a single UTxO locked by
	// script, whose witness set is witnesses. Alonzo and Babbage phase-2
	// validation resolve the script purpose from this spent input.
	newSpendFixture := func(
		script lcommon.Script,
		witnesses *mockWitnessSet,
	) (*mockConwayFeeTx, *mockLedgerState) {
		spendInput := newTestInput(0x01, 0)
		addr := newTestScriptAddress(t, script)
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
		return tx, ls
	}

	maxTxExUnits := lcommon.ExUnits{Steps: 1_000_000, Memory: 1_000_000}

	t.Run("alonzo", func(t *testing.T) {
		v1Script := lcommon.PlutusV1Script(scriptBytes)
		witnesses := &mockWitnessSet{
			redeemers:       spendRedeemers(),
			plutusV1Scripts: []lcommon.PlutusV1Script{v1Script},
		}

		t.Run(
			"ValidateTxAlonzo missing PlutusV1 cost model",
			func(t *testing.T) {
				// Clear the phase-1 rule set so the missing-cost-model failure
				// is what's under test, not a fee or metadata check the mock
				// transaction can't support (mirrors
				// TestPlutusBudgetComparisonIncludesFinalSlippageBatch).
				origRules := alonzoUtxoValidationRules
				t.Cleanup(func() { alonzoUtxoValidationRules = origRules })
				alonzoUtxoValidationRules = nil

				tx, ls := newSpendFixture(v1Script, witnesses)
				err := ValidateTxAlonzo(
					tx,
					0,
					ls,
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 5,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)

		t.Run(
			"EvaluateTxAlonzo missing PlutusV1 cost model",
			func(t *testing.T) {
				tx, ls := newSpendFixture(v1Script, witnesses)
				_, _, _, err := EvaluateTxAlonzo(
					tx,
					ls,
					&alonzo.AlonzoProtocolParameters{
						ProtocolMajor: 5,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)
	})

	t.Run("babbage", func(t *testing.T) {
		v1Script := lcommon.PlutusV1Script(scriptBytes)
		v1Witnesses := &mockWitnessSet{
			redeemers:       spendRedeemers(),
			plutusV1Scripts: []lcommon.PlutusV1Script{v1Script},
		}
		v2Script := lcommon.PlutusV2Script(scriptBytes)
		v2Witnesses := &mockWitnessSet{
			redeemers:       spendRedeemers(),
			plutusV2Scripts: []lcommon.PlutusV2Script{v2Script},
		}

		t.Run(
			"ValidateTxBabbage missing PlutusV1 cost model",
			func(t *testing.T) {
				origRules := babbageUtxoValidationRules
				t.Cleanup(func() { babbageUtxoValidationRules = origRules })
				babbageUtxoValidationRules = nil

				tx, ls := newSpendFixture(v1Script, v1Witnesses)
				err := ValidateTxBabbage(
					tx,
					0,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)

		t.Run(
			"ValidateTxBabbage missing PlutusV2 cost model",
			func(t *testing.T) {
				origRules := babbageUtxoValidationRules
				t.Cleanup(func() { babbageUtxoValidationRules = origRules })
				babbageUtxoValidationRules = nil

				tx, ls := newSpendFixture(v2Script, v2Witnesses)
				err := ValidateTxBabbage(
					tx,
					0,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV2 cost model")
			},
		)

		t.Run(
			"EvaluateTxBabbage missing PlutusV1 cost model",
			func(t *testing.T) {
				tx, ls := newSpendFixture(v1Script, v1Witnesses)
				_, _, _, err := EvaluateTxBabbage(
					tx,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV1 cost model")
			},
		)

		t.Run(
			"EvaluateTxBabbage missing PlutusV2 cost model",
			func(t *testing.T) {
				tx, ls := newSpendFixture(v2Script, v2Witnesses)
				_, _, _, err := EvaluateTxBabbage(
					tx,
					ls,
					&babbage.BabbageProtocolParameters{
						ProtocolMajor: 7,
						MaxTxExUnits:  maxTxExUnits,
					},
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "missing PlutusV2 cost model")
			},
		)
	})

	// Conway's V1 branch of evaluateConwayPlutusScript already has this
	// coverage via TestConwayPlutusBudgetComparisonIncludesFinalSlippageBatch.
	// This covers the V2 and V3 branches the same way that test covers V1:
	// a minting purpose, so the same unit-returning program applies to
	// every language version without needing a per-version datum shape.
	t.Run("conway", func(t *testing.T) {
		newMintFixture := func(
			script lcommon.Script,
			witnesses *mockWitnessSet,
		) *mockConwayFeeTx {
			scriptHash := script.Hash()
			assetMint := lcommon.NewMultiAsset[lcommon.MultiAssetTypeMint](
				map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeMint{
					lcommon.Blake2b224(scriptHash): {
						cbor.NewByteString([]byte("asset")): big.NewInt(1),
					},
				},
			)
			return &mockConwayFeeTx{
				mockFeeTx: mockFeeTx{
					txType:    txTypeAlonzo,
					witnesses: witnesses,
				},
				assetMint: &assetMint,
			}
		}
		mintRedeemers := func() *mockRedeemers {
			return &mockRedeemers{
				entries: []struct {
					key lcommon.RedeemerKey
					val lcommon.RedeemerValue
				}{
					{
						key: lcommon.RedeemerKey{
							Tag:   lcommon.RedeemerTagMint,
							Index: 0,
						},
						val: lcommon.RedeemerValue{ExUnits: lcommon.ExUnits{}},
					},
				},
			}
		}
		pp := &conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 9,
			},
			MaxTxExUnits: maxTxExUnits,
		}
		clearConwayRules := func(t *testing.T) {
			t.Helper()
			origAll := conwayUtxoValidationRules
			origPhase1 := conwayPhase1UtxoValidationRules
			t.Cleanup(func() {
				conwayUtxoValidationRules = origAll
				conwayPhase1UtxoValidationRules = origPhase1
			})
			conwayUtxoValidationRules = nil
			conwayPhase1UtxoValidationRules = nil
		}

		t.Run("missing PlutusV2 cost model", func(t *testing.T) {
			clearConwayRules(t)
			v2Script := lcommon.PlutusV2Script(scriptBytes)
			tx := newMintFixture(v2Script, &mockWitnessSet{
				redeemers:       mintRedeemers(),
				plutusV2Scripts: []lcommon.PlutusV2Script{v2Script},
			})
			err := ValidateTxConway(tx, 0, newMockLedgerState(), pp)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing PlutusV2 cost model")
		})

		t.Run("missing PlutusV3 cost model", func(t *testing.T) {
			clearConwayRules(t)
			v3Script := lcommon.PlutusV3Script(scriptBytes)
			base := newMintFixture(v3Script, &mockWitnessSet{
				redeemers:       mintRedeemers(),
				plutusV3Scripts: []lcommon.PlutusV3Script{v3Script},
			})
			tx := &mockConwayFeeTxV3{mockConwayFeeTx: *base}
			err := ValidateTxConway(tx, 0, newMockLedgerState(), pp)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing PlutusV3 cost model")
		})
	})
}

// disablePhase1RulesForTest replaces every era's phase-1 UTXO validation
// rule table with nil for the duration of t, restoring the originals on
// cleanup. Mirrors TestValidateTxRequiresDeclaredValidityToMatchExecution's
// setup: these tests exercise only the phase-2 ErrNoCostModelForPlutusV2
// check, not the full phase-1 rule suite, which needs a much more complete
// (fee/TTL/deposit-correct) transaction than these fixtures build.
func disablePhase1RulesForTest(t *testing.T) {
	t.Helper()
	origBabbage := babbageUtxoValidationRules
	origConwayAll := conwayUtxoValidationRules
	origConwayPhase1 := conwayPhase1UtxoValidationRules
	origDijkstra := dijkstraPhase1UtxoValidationRules
	t.Cleanup(func() {
		babbageUtxoValidationRules = origBabbage
		conwayUtxoValidationRules = origConwayAll
		conwayPhase1UtxoValidationRules = origConwayPhase1
		dijkstraPhase1UtxoValidationRules = origDijkstra
	})
	babbageUtxoValidationRules = nil
	conwayUtxoValidationRules = nil
	conwayPhase1UtxoValidationRules = nil
	dijkstraPhase1UtxoValidationRules = nil
}

// TestValidateTxBabbageRejectsPlutusV2WhenSynthetic covers blinklabs-io/dingo#3962:
// real cardano-ledger rejects a transaction using a PlutusV2 script outright,
// at the UTXOW level before any script evaluation runs, whenever PlutusV2 has
// no real cost model configured yet (NoCostModel, the formal rule "languages
// txw ⊆ dom(costmdls pp)"). Dingo's HardForkBabbage instead fabricates a
// value specifically so internal validation always has one, which -- absent
// this check -- would let Dingo accept the same transaction a real network
// rejects.
func TestValidateTxBabbageRejectsPlutusV2WhenSynthetic(t *testing.T) {
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

	err := ValidateTxBabbage(
		tx,
		0,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic covers the common case
// once real data exists (or on a database predating this tracking, where the
// bootstrap fallback resolves to false for a real, non-default value): the
// same PlutusV2 transaction must validate exactly as it did before this
// check existed.
func TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	// syntheticV2CostModel left at its zero value (false).
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
			// A real (non-synthetic) PlutusV2 cost model, same as production
			// carries once an on-chain update lands. requiredCostModel
			// (issue #3528) fails closed on a missing entry, so this must be
			// populated for the "not synthetic" case to actually reach
			// evaluation instead of being rejected before it ever does.
			CostModels: map[uint][]int64{
				1: defaultMachineCostModel(t, lang.LanguageVersionV2),
			},
		},
	)

	require.NoError(t, err)
}

// TestValidateTxBabbageAllowsPlutusV2WhenLedgerStateReportsNothing covers the
// fail-safe default: an lcommon.LedgerState implementation that does not
// implement syntheticV2CostModelReporter at all (any caller other than
// ledger.LedgerView's own *ledger.LedgerView) must not be treated as
// synthetic -- this check is additive and must never fire for a caller that
// simply doesn't carry the signal.
func TestValidateTxBabbageAllowsPlutusV2WhenLedgerStateReportsNothing(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	err := ValidateTxBabbage(
		tx,
		0,
		plainMockLedgerState{newMockLedgerState()},
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
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

// plainMockLedgerState embeds the lcommon.LedgerState INTERFACE (not the
// concrete *mockLedgerState type), so method promotion exposes exactly that
// interface's method set and nothing more -- in particular, not
// SyntheticV2CostModelInEffect, even though the concrete value stored in it
// (a *mockLedgerState) happens to have that extra method. This stands in for
// any lcommon.LedgerState implementation other than *ledger.LedgerView,
// which is the only production type this check's type assertion expects to
// see.
type plainMockLedgerState struct {
	lcommon.LedgerState
}

// TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic covers the fee/ex-units
// estimation counterpart: a transaction ValidateTxBabbage would reject must
// not be quoted a fee estimate implying it's valid.
func TestEvaluateTxBabbageRejectsPlutusV2WhenSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
			MaxTxExUnits: lcommon.ExUnits{
				Steps:  10_000_000,
				Memory: 10_000_000,
			},
		},
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic mirrors
// TestValidateTxBabbageAllowsPlutusV2WhenNotSynthetic for EvaluateTxBabbage.
func TestEvaluateTxBabbageAllowsPlutusV2WhenNotSynthetic(t *testing.T) {
	ls := newMockLedgerState()
	tx := newConwayValidityOutcomeTx(
		t,
		true,
		lang.LanguageVersionV2,
		false,
		lcommon.ExUnits{Steps: 10_000_000, Memory: 10_000_000},
	)

	_, _, _, err := EvaluateTxBabbage(
		tx,
		ls,
		&babbage.BabbageProtocolParameters{
			ProtocolMajor: 7,
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

// dijkstraSyntheticV2Tx returns a minimal lcommon.Transaction -- not a
// concrete *gdijkstra.DijkstraTransaction -- witnessing a PlutusV2 script.
// dijkstraSyntheticV2CostModelGuard's version detection
// (gdijkstra.UtxoValidateCostModelsPresent's usedPlutusVersions) falls back
// to a plain witness-set scan for any non-concrete transaction type, so this
// avoids needing a fully-shaped Dijkstra transaction with real spend/redeemer
// wiring just to prove the guard fires.
func dijkstraSyntheticV2Tx() *mockConwayFeeTx {
	return &mockConwayFeeTx{
		mockFeeTx: mockFeeTx{
			witnesses: &mockWitnessSet{
				plutusV2Scripts: []lcommon.PlutusV2Script{{0x01}},
			},
		},
	}
}

// dijkstraGuardPParams returns Dijkstra protocol parameters carrying a
// PlutusV2 cost model, standing in for HardForkBabbage's fabricated default
// that dijkstraSyntheticV2CostModelGuard prunes before checking presence.
func dijkstraGuardPParams() *gdijkstra.DijkstraProtocolParameters {
	return &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				1: {1, 2, 3},
			},
		},
	}
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticNormalPath covers a
// human-reviewer finding on blinklabs-io/dingo#3962's PR: ValidateTxDijkstra
// delegates phase-2 validation entirely to
// gdijkstra.UtxoValidatePlutusScripts, which has no idea about Dingo's
// synthetic marker -- without dijkstraSyntheticV2CostModelGuard, a
// transaction using a synthetic-cost-model PlutusV2 script would reach that
// delegate and be priced against the fabricated value instead of rejected.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticNormalPath(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true

	err := ValidateTxDijkstra(
		dijkstraSyntheticV2Tx(),
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSkipPhase2Path covers
// the other half of the same finding: shouldSkipPhase2Validation returns nil
// before ever reaching the delegate, so the guard must run before that
// shortcut too, not only before the delegation call.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSkipPhase2Path(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true

	err := ValidateTxDijkstra(
		dijkstraSyntheticV2Tx(),
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxDijkstraAllowsNonPlutusV2TxWhenSynthetic proves the guard is
// additive: a transaction that never witnesses a PlutusV2 script is not
// rejected merely because the synthetic marker happens to be set.
func TestValidateTxDijkstraAllowsNonPlutusV2TxWhenSynthetic(t *testing.T) {
	disablePhase1RulesForTest(t)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true

	err := ValidateTxDijkstra(
		&mockConwayFeeTx{},
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.NoError(t, err)
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSubTransaction verifies
// the concrete Dijkstra transaction path, in addition to the mock path used
// by other tests. The earlier Dijkstra tests all used a *mockConwayFeeTx (not
// a concrete *gdijkstra.DijkstraTransaction),
// which takes usedPlutusVersions' plain witness-set-scan fallback rather
// than the dijkstraScriptLevels resolution a real Dijkstra transaction
// actually goes through -- leaving the sub-transaction and reference-script
// resolution paths dijkstraSyntheticV2CostModelGuard's doc comment claims to
// cover unverified. This uses a real concrete transaction with the PlutusV2
// script witnessed and needed only inside a sub-transaction's own spend
// input, empirically confirming dijkstraScriptLevels' per-level Needed
// computation (which dijkstraConwayFeatureTransaction scopes to each level's
// own body/witnesses) folds a sub-transaction's needed languages into the
// top-level usedPlutusVersions result -- the same mechanism
// UtxoValidatePlutusScripts itself already depends on to enforce
// UnsupportedScriptInSubtransactionError, so this guard's coverage cannot
// regress silently if that upstream mechanism ever changed.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSubTransaction(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	plutusV2Script := lcommon.PlutusV2Script([]byte{0x01})
	scriptAddr := newTestScriptAddress(t, plutusV2Script)
	subTxInput := shelley.NewShelleyTransactionInput(
		strings.Repeat("ab", 32),
		0,
	)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true
	ls.utxos[subTxInput.Id().String()+"#0"] = lcommon.Utxo{
		Id: subTxInput,
		Output: testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       scriptAddr,
		},
	}

	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]gdijkstra.DijkstraSubTransaction{{
					Body: gdijkstra.DijkstraSubTransactionBody{
						TxInputs: conway.NewConwayTransactionInputSet(
							[]shelley.ShelleyTransactionInput{subTxInput},
						),
					},
					WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
						WsPlutusV2Scripts: cbor.NewSetType(
							[]lcommon.PlutusV2Script{plutusV2Script},
							false,
						),
						WsRedeemers: gdijkstra.DijkstraRedeemers{
							Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
								{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
									ExUnits: lcommon.ExUnits{
										Steps:  1,
										Memory: 1,
									},
								},
							},
						},
					},
				}},
				false,
			),
		},
		TxIsValid: true,
	}

	err := ValidateTxDijkstra(
		tx,
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}

// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticReferenceScript is
// TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticSubTransaction's
// counterpart for the reference-script resolution path: a real concrete
// transaction whose PlutusV2 script is never directly witnessed, only
// resolved from a reference input, and needed by a separate spend input at
// that script's address.
func TestValidateTxDijkstraRejectsPlutusV2WhenSyntheticReferenceScript(
	t *testing.T,
) {
	disablePhase1RulesForTest(t)

	plutusV2Script := lcommon.PlutusV2Script([]byte{0x01})
	scriptAddr := newTestScriptAddress(t, plutusV2Script)
	spendInput := shelley.NewShelleyTransactionInput(
		strings.Repeat("cd", 32),
		0,
	)
	refInput := shelley.NewShelleyTransactionInput(
		strings.Repeat("ef", 32),
		0,
	)

	ls := newMockLedgerState()
	ls.syntheticV2CostModel = true
	ls.skipPhase2Validation = true
	ls.utxos[spendInput.Id().String()+"#0"] = lcommon.Utxo{
		Id: spendInput,
		Output: testAddressOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       scriptAddr,
		},
	}
	ls.utxos[refInput.Id().String()+"#0"] = lcommon.Utxo{
		Id: refInput,
		Output: testAddressScriptOutput{
			testOutput: newTestOutput(1_000_000),
			addr:       newTestKeyAddress(t),
			scriptRef:  plutusV2Script,
		},
	}

	tx := &gdijkstra.DijkstraTransaction{
		Body: gdijkstra.DijkstraTransactionBody{
			TxInputs: conway.NewConwayTransactionInputSet(
				[]shelley.ShelleyTransactionInput{spendInput},
			),
			TxReferenceInputs: cbor.NewSetType(
				[]shelley.ShelleyTransactionInput{refInput},
				false,
			),
		},
		WitnessSet: gdijkstra.DijkstraTransactionWitnessSet{
			WsRedeemers: gdijkstra.DijkstraRedeemers{
				Redeemers: map[lcommon.RedeemerKey]lcommon.RedeemerValue{
					{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
						ExUnits: lcommon.ExUnits{Steps: 1, Memory: 1},
					},
				},
			},
		},
		TxIsValid: true,
	}

	err := ValidateTxDijkstra(
		tx,
		0,
		ls,
		dijkstraGuardPParams(),
	)

	require.ErrorIs(t, err, ErrNoCostModelForPlutusV2)
}
