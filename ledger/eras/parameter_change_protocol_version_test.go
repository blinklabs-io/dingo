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
	"fmt"
	"reflect"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

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
// is the CodeRabbit-flagged case on PR #4699: a decoded ParamUpdate whose
// raw CBOR carries key 14 with an explicit null value decodes ProtocolVersion
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
