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

package governance

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeGovAction_InfoRoundtrip(t *testing.T) {
	original := &lcommon.InfoGovAction{Type: 6}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeInfo),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.InfoGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_ParameterChangeRoundtrip(t *testing.T) {
	fee := uint(1234)
	original := &conway.ConwayParameterChangeGovAction{
		Type: 0,
		ParamUpdate: conway.ConwayProtocolParameterUpdate{
			MinFeeA: &fee,
		},
	}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeParameterChange),
	)
	require.NoError(t, err)
	concrete, ok := decoded.(*conway.ConwayParameterChangeGovAction)
	require.True(t, ok)
	require.NotNil(t, concrete.ParamUpdate.MinFeeA)
	assert.Equal(t, uint(1234), *concrete.ParamUpdate.MinFeeA)
}

func TestEnactProposal_DijkstraParameterChange(t *testing.T) {
	db, _ := newTallyTestDB(t)

	fee := uint(1234)
	action := &gdijkstra.DijkstraParameterChangeGovAction{
		Type: uint(lcommon.GovActionTypeParameterChange),
		ParamUpdate: gdijkstra.DijkstraProtocolParameterUpdate{
			MinFeeA: &fee,
		},
	}
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)

	pparams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MinFeeA: 1,
		},
	}
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 0xD1),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeParameterChange),
		GovActionCbor: encoded,
		AddedSlot:     500,
		ExpiresEpoch:  100,
		AnchorURL:     "https://example.invalid/dijkstra-param",
		AnchorHash:    testBytes(32, 0xD2),
		ReturnAddress: testBytes(29, 0xD3),
		Deposit:       0,
	}

	result, err := EnactProposal(&EnactmentContext{
		DB:       db,
		Slot:     2000,
		Epoch:    42,
		PParams:  pparams,
		UpdateFn: eras.PParamsUpdateDijkstra,
	}, proposal)
	require.NoError(t, err)
	require.True(t, result.PParamsChanged)
	require.False(t, result.CostModelsChanged)
	require.Equal(t, uint(1234), pparams.MinFeeA)
}

func TestDecodeGovAction_HardForkRoundtrip(t *testing.T) {
	original := &lcommon.HardForkInitiationGovAction{Type: 1}
	original.ProtocolVersion.Major = 10
	original.ProtocolVersion.Minor = 0
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeHardForkInitiation),
	)
	require.NoError(t, err)
	concrete, ok := decoded.(*lcommon.HardForkInitiationGovAction)
	require.True(t, ok)
	assert.Equal(t, uint(10), concrete.ProtocolVersion.Major)
}

func TestDecodeGovAction_TreasuryWithdrawalRoundtrip(t *testing.T) {
	original := &lcommon.TreasuryWithdrawalGovAction{
		Type:       2,
		PolicyHash: []byte{0xAB, 0xCD, 0xEF},
	}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeTreasuryWithdrawal),
	)
	require.NoError(t, err)
	concrete, ok := decoded.(*lcommon.TreasuryWithdrawalGovAction)
	require.True(t, ok)
	assert.Equal(t, []byte{0xAB, 0xCD, 0xEF}, concrete.PolicyHash)
}

func TestDecodeGovAction_NoConfidenceRoundtrip(t *testing.T) {
	original := &lcommon.NoConfidenceGovAction{Type: 3}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeNoConfidence),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.NoConfidenceGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_UpdateCommitteeRoundtrip(t *testing.T) {
	original := &lcommon.UpdateCommitteeGovAction{
		Type:        4,
		Credentials: []lcommon.Credential{},
		Quorum:      newRat(2, 3),
	}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeUpdateCommittee),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.UpdateCommitteeGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_NewConstitutionRoundtrip(t *testing.T) {
	original := &lcommon.NewConstitutionGovAction{Type: 5}
	encoded, err := cbor.Encode(original)
	require.NoError(t, err)
	decoded, err := decodeGovAction(
		encoded, uint8(lcommon.GovActionTypeNewConstitution),
	)
	require.NoError(t, err)
	_, ok := decoded.(*lcommon.NewConstitutionGovAction)
	assert.True(t, ok)
}

func TestDecodeGovAction_EmptyCbor(t *testing.T) {
	_, err := decodeGovAction(
		nil, uint8(lcommon.GovActionTypeInfo),
	)
	assert.Error(t, err)
}

func TestDecodeGovAction_UnknownType(t *testing.T) {
	_, err := decodeGovAction(
		[]byte{0x00}, 99,
	)
	assert.Error(t, err)
}

func TestDecodeGovActionRejectsStoredAndEmbeddedTypeMismatch(t *testing.T) {
	encoded, err := cbor.Encode(&lcommon.NoConfidenceGovAction{
		Type: uint(lcommon.GovActionTypeUpdateCommittee),
	})
	require.NoError(t, err)
	_, err = decodeGovAction(
		encoded,
		uint8(lcommon.GovActionTypeNoConfidence),
	)
	require.ErrorContains(t, err, "type mismatch")
}

func TestDecodeGovActionRejectsTruncatedAndTrailingData(t *testing.T) {
	encoded, err := cbor.Encode(&lcommon.InfoGovAction{
		Type: uint(lcommon.GovActionTypeInfo),
	})
	require.NoError(t, err)
	require.Greater(t, len(encoded), 1)

	_, err = decodeGovAction(
		encoded[:len(encoded)-1],
		uint8(lcommon.GovActionTypeInfo),
	)
	require.Error(t, err)

	_, err = decodeGovAction(
		append(append([]byte(nil), encoded...), 0x00),
		uint8(lcommon.GovActionTypeInfo),
	)
	require.ErrorContains(t, err, "consumed")
}

func TestSetProtocolVersion_ConwayParams(t *testing.T) {
	pparams := &conway.ConwayProtocolParameters{}
	pparams.ProtocolVersion.Major = 9
	pparams.ProtocolVersion.Minor = 0
	updated, err := setProtocolVersion(pparams, 10, 0)
	require.NoError(t, err)
	concrete, ok := updated.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Equal(t, uint(10), concrete.ProtocolVersion.Major)
	// Original must remain unmutated to preserve the previous epoch's
	// pparams for rollback safety.
	assert.Equal(t, uint(9), pparams.ProtocolVersion.Major)
}

func TestEnactProposal_DijkstraHardForkPreservesPParams(t *testing.T) {
	db, _ := newTallyTestDB(t)
	action := &lcommon.HardForkInitiationGovAction{
		Type: uint(lcommon.GovActionTypeHardForkInitiation),
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: gdijkstra.MinProtocolVersionDijkstra + 1,
			Minor: 2,
		},
	}
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)

	refScriptMultiplier := newRat(3, 2)
	committeeCoverage := newRat(2, 3)
	quorumThreshold := newRat(3, 5)
	pparams := &gdijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MinFeeA: 44,
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: gdijkstra.MinProtocolVersionDijkstra,
				Minor: 1,
			},
			CostModels:              map[uint][]int64{3: {1, 2, 3}},
			GovActionValidityPeriod: 7,
		},
		MaxRefScriptSizePerBlock: 99_000,
		MaxRefScriptSizePerTx:    9_000,
		RefScriptCostStride:      128,
		RefScriptCostMultiplier:  &refScriptMultiplier,
		CommitteeStakeCoverage:   &committeeCoverage,
		QuorumStakeThreshold:     &quorumThreshold,
	}
	original := *pparams
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 0xD4),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		GovActionCbor: encoded,
		AddedSlot:     500,
		ExpiresEpoch:  100,
		AnchorURL:     "https://example.invalid/dijkstra-hard-fork",
		AnchorHash:    testBytes(32, 0xD5),
		ReturnAddress: testBytes(29, 0xD6),
	}

	result, err := EnactProposal(&EnactmentContext{
		DB:      db,
		Slot:    2_000,
		Epoch:   42,
		PParams: pparams,
	}, proposal)
	require.NoError(t, err)
	require.True(t, result.PParamsChanged)
	updated, ok := result.UpdatedPParams.(*gdijkstra.DijkstraProtocolParameters)
	require.True(t, ok, "hard-fork enactment must retain the Dijkstra type")
	require.Equal(
		t,
		action.ProtocolVersion.Major,
		updated.ProtocolVersion.Major,
	)
	require.Equal(
		t,
		action.ProtocolVersion.Minor,
		updated.ProtocolVersion.Minor,
	)

	gotNonVersion := *updated
	gotNonVersion.ProtocolVersion = original.ProtocolVersion
	require.Equal(
		t,
		original,
		gotNonVersion,
		"hard-fork enactment must preserve every non-version field",
	)
	require.Equal(t, original.ProtocolVersion, pparams.ProtocolVersion)
	require.NotNil(t, proposal.EnactedEpoch)
	assert.Equal(t, uint64(42), *proposal.EnactedEpoch)
}

func TestEnactProposalHardForkRejectsTypedNilDijkstraPParams(t *testing.T) {
	db, _ := newTallyTestDB(t)
	action := &lcommon.HardForkInitiationGovAction{
		Type: uint(lcommon.GovActionTypeHardForkInitiation),
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: gdijkstra.MinProtocolVersionDijkstra + 1,
		},
	}
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)

	_, err = EnactProposal(&EnactmentContext{
		DB:      db,
		PParams: (*gdijkstra.DijkstraProtocolParameters)(nil),
	}, &models.GovernanceProposal{
		TxHash:        testBytes(32, 0xE1),
		ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
		GovActionCbor: encoded,
	})
	require.ErrorContains(t, err, "nil Dijkstra protocol parameters")
}

func TestEnactProposalHardForkReturnsMutationIsolatedPParams(t *testing.T) {
	tests := []struct {
		name    string
		pparams func() lcommon.ProtocolParameters
		mutate  func(*testing.T, lcommon.ProtocolParameters)
		extra   func(lcommon.ProtocolParameters) []string
	}{
		{
			name: "Conway",
			pparams: func() lcommon.ProtocolParameters {
				return mutableConwayPParamsFixture()
			},
			mutate: func(t *testing.T, pparams lcommon.ProtocolParameters) {
				mutateConwayPParams(
					t,
					pparams.(*conway.ConwayProtocolParameters),
				)
			},
		},
		{
			name: "Dijkstra",
			pparams: func() lcommon.ProtocolParameters {
				return &gdijkstra.DijkstraProtocolParameters{
					ConwayProtocolParameters: *mutableConwayPParamsFixture(),
					MaxRefScriptSizePerBlock: 1_000,
					MaxRefScriptSizePerTx:    500,
					RefScriptCostStride:      64,
					RefScriptCostMultiplier:  testRatPtr(3, 2),
					CommitteeStakeCoverage:   testRatPtr(2, 3),
					QuorumStakeThreshold:     testRatPtr(3, 5),
				}
			},
			mutate: func(t *testing.T, pparams lcommon.ProtocolParameters) {
				p := pparams.(*gdijkstra.DijkstraProtocolParameters)
				mutateConwayPParams(t, &p.ConwayProtocolParameters)
				for i, rat := range []*cbor.Rat{
					p.RefScriptCostMultiplier,
					p.CommitteeStakeCoverage,
					p.QuorumStakeThreshold,
				} {
					rat.Rat.SetInt64(int64(500 + i))
				}
			},
			extra: func(pparams lcommon.ProtocolParameters) []string {
				p := pparams.(*gdijkstra.DijkstraProtocolParameters)
				return []string{
					p.CommitteeStakeCoverage.String(),
					p.QuorumStakeThreshold.String(),
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, _ := newTallyTestDB(t)
			action := &lcommon.HardForkInitiationGovAction{
				Type: uint(lcommon.GovActionTypeHardForkInitiation),
				ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
					Major: gdijkstra.MinProtocolVersionDijkstra + 1,
					Minor: 1,
				},
			}
			encoded, err := cbor.Encode(action)
			require.NoError(t, err)
			pparams := test.pparams()
			before, err := cbor.Encode(pparams)
			require.NoError(t, err)
			var extraBefore []string
			if test.extra != nil {
				extraBefore = test.extra(pparams)
			}

			result, err := EnactProposal(&EnactmentContext{
				DB:      db,
				PParams: pparams,
			}, &models.GovernanceProposal{
				TxHash:        testBytes(32, 0xE2),
				ActionType:    uint8(lcommon.GovActionTypeHardForkInitiation),
				GovActionCbor: encoded,
				AnchorURL:     "https://example.invalid/mutation-isolation",
				AnchorHash:    testBytes(32, 0xE3),
				ReturnAddress: testBytes(29, 0xE4),
			})
			require.NoError(t, err)
			test.mutate(t, result.UpdatedPParams)

			after, err := cbor.Encode(pparams)
			require.NoError(t, err)
			require.Equal(
				t,
				before,
				after,
				"mutating enacted pparams must not mutate the input",
			)
			if test.extra != nil {
				require.Equal(t, extraBefore, test.extra(pparams))
			}
		})
	}
}

func testRatPtr(num, denom int64) *cbor.Rat {
	return &cbor.Rat{Rat: big.NewRat(num, denom)}
}

func mutableConwayPParamsFixture() *conway.ConwayProtocolParameters {
	return &conway.ConwayProtocolParameters{
		A0:         testRatPtr(1, 2),
		Rho:        testRatPtr(1, 3),
		Tau:        testRatPtr(1, 4),
		CostModels: map[uint][]int64{3: {1, 2, 3}},
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  testRatPtr(1, 5),
			StepPrice: testRatPtr(1, 6),
		},
		PoolVotingThresholds: conway.PoolVotingThresholds{
			MotionNoConfidence:    newRat(1, 7),
			CommitteeNormal:       newRat(1, 8),
			CommitteeNoConfidence: newRat(1, 9),
			HardForkInitiation:    newRat(1, 10),
			PpSecurityGroup:       newRat(1, 11),
		},
		DRepVotingThresholds: conway.DRepVotingThresholds{
			MotionNoConfidence:    newRat(1, 12),
			CommitteeNormal:       newRat(1, 13),
			CommitteeNoConfidence: newRat(1, 14),
			UpdateToConstitution:  newRat(1, 15),
			HardForkInitiation:    newRat(1, 16),
			PpNetworkGroup:        newRat(1, 17),
			PpEconomicGroup:       newRat(1, 18),
			PpTechnicalGroup:      newRat(1, 19),
			PpGovGroup:            newRat(1, 20),
			TreasuryWithdrawal:    newRat(1, 21),
		},
		MinFeeRefScriptCostPerByte: testRatPtr(1, 22),
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: conway.MinProtocolVersionConway,
		},
	}
}

func mutateConwayPParams(
	t *testing.T,
	pparams *conway.ConwayProtocolParameters,
) {
	t.Helper()
	costModel, ok := pparams.CostModels[3]
	if !ok {
		t.Fatal("expected cost model 3")
	}
	if len(costModel) == 0 {
		t.Fatal("expected cost model 3 to contain parameters")
	}
	costModel[0] = 999
	pparams.CostModels[4] = []int64{4, 5, 6}
	for i, rat := range []*cbor.Rat{
		pparams.A0,
		pparams.Rho,
		pparams.Tau,
		pparams.ExecutionCosts.MemPrice,
		pparams.ExecutionCosts.StepPrice,
		&pparams.PoolVotingThresholds.MotionNoConfidence,
		&pparams.PoolVotingThresholds.CommitteeNormal,
		&pparams.PoolVotingThresholds.CommitteeNoConfidence,
		&pparams.PoolVotingThresholds.HardForkInitiation,
		&pparams.PoolVotingThresholds.PpSecurityGroup,
		&pparams.DRepVotingThresholds.MotionNoConfidence,
		&pparams.DRepVotingThresholds.CommitteeNormal,
		&pparams.DRepVotingThresholds.CommitteeNoConfidence,
		&pparams.DRepVotingThresholds.UpdateToConstitution,
		&pparams.DRepVotingThresholds.HardForkInitiation,
		&pparams.DRepVotingThresholds.PpNetworkGroup,
		&pparams.DRepVotingThresholds.PpEconomicGroup,
		&pparams.DRepVotingThresholds.PpTechnicalGroup,
		&pparams.DRepVotingThresholds.PpGovGroup,
		&pparams.DRepVotingThresholds.TreasuryWithdrawal,
		pparams.MinFeeRefScriptCostPerByte,
	} {
		rat.Rat.SetInt64(int64(100 + i))
	}
}

func TestStakeEpochFor(t *testing.T) {
	tests := []struct {
		newEpoch uint64
		expected uint64
	}{
		{0, 0},
		{1, 0},
		{2, 0},
		{3, 1},
		{10, 8},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.expected, stakeEpochFor(tt.newEpoch))
	}
}

// Expiry is exercised end-to-end via the DB query and ProcessEpoch
// integration rather than a split-in-memory helper, so there is no unit
// test for a partition function here.

func TestApplyUpdateCommittee_PersistsEnactedQuorum(t *testing.T) {
	db, _ := newTallyTestDB(t)

	action := &lcommon.UpdateCommitteeGovAction{
		Credentials: []lcommon.Credential{},
		CredEpochs:  map[*lcommon.Credential]uint{},
		Quorum:      cbor.Rat{Rat: big.NewRat(3, 5)},
	}
	err := applyUpdateCommittee(
		&EnactmentContext{DB: db, Slot: 4242},
		action,
		4000,
	)
	require.NoError(t, err)

	got, err := db.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 0, got.Cmp(big.NewRat(3, 5)))
}

func TestApplyUpdateCommittee_ReelectionStartsFreshCredentialTerm(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	coldHash := testBytes(28, 41)
	oldHotHash := testBytes(28, 42)
	newHotHash := testBytes(28, 43)
	coldCredential := &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(coldHash),
	}
	require.NoError(t, store.SetCommitteeMembers(
		[]*models.CommitteeMember{{
			ColdCredentialTag: uint8(coldCredential.CredType),
			ColdCredHash:      coldHash,
			ExpiresEpoch:      20,
			TermStartSlot:     10,
			AddedSlot:         10,
		}},
		nil,
	))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldHash,
		HotCredential:  oldHotHash,
		CertificateID:  1,
		AddedSlot:      20,
	})
	seedTallyCommitteeResignation(t, store, models.ResignCommitteeCold{
		ColdCredential: coldHash,
		CertificateID:  2,
		AddedSlot:      30,
	})

	require.NoError(t, applyUpdateCommittee(
		&EnactmentContext{DB: db, Slot: 40},
		&lcommon.UpdateCommitteeGovAction{
			Credentials: []lcommon.Credential{*coldCredential},
			Quorum:      cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		35,
	))
	seedTallyCommitteeAuth(t, store, models.AuthCommitteeHot{
		ColdCredential: coldHash,
		HotCredential:  newHotHash,
		CertificateID:  3,
		AddedSlot:      60,
	})
	require.NoError(t, applyUpdateCommittee(
		&EnactmentContext{DB: db, Slot: 70},
		&lcommon.UpdateCommitteeGovAction{
			CredEpochs: map[*lcommon.Credential]uint{
				coldCredential: 30,
			},
			Quorum: cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		50,
	))

	members, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, uint64(50), members[0].TermStartSlot)
	assert.Equal(t, uint64(70), members[0].AddedSlot)
	resigned, err := db.IsCommitteeMemberResigned(
		uint8(coldCredential.CredType), coldHash, 50, nil,
	)
	require.NoError(t, err)
	assert.False(t, resigned)
	authorization, err := db.GetCommitteeMember(
		uint8(coldCredential.CredType), coldHash, 50, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, newHotHash, authorization.HotCredential)

	require.NoError(t, db.DeleteCommitteeMembersAfterSlot(65, nil))
	members, err = db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	assert.Empty(t, members)

	require.NoError(t, db.DeleteCommitteeMembersAfterSlot(35, nil))
	members, err = db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, uint64(10), members[0].TermStartSlot)
	resigned, err = db.IsCommitteeMemberResigned(
		uint8(coldCredential.CredType), coldHash, 10, nil,
	)
	require.NoError(t, err)
	assert.True(t, resigned)
}

func TestEnactProposal_NoConfidence_ClearsCommitteeQuorum(
	t *testing.T,
) {
	db, _ := newTallyTestDB(t)

	// Seed an enacted quorum from a prior UpdateCommittee.
	require.NoError(
		t,
		db.SetCommitteeQuorum(big.NewRat(3, 5), 1000, nil),
	)

	// Build a NoConfidence proposal with a zero deposit so the
	// reward-account refund path short-circuits.
	action := &lcommon.NoConfidenceGovAction{
		Type: uint(lcommon.GovActionTypeNoConfidence),
	}
	encoded, err := cbor.Encode(action)
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        testBytes(32, 7),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeNoConfidence),
		GovActionCbor: encoded,
		AddedSlot:     500,
		ExpiresEpoch:  100,
		AnchorURL:     "https://example.invalid/noconf",
		AnchorHash:    testBytes(32, 8),
		ReturnAddress: testBytes(29, 9),
		// Zero deposit keeps the refund path a no-op so this test
		// isolates the committee-quorum clear behavior.
		Deposit: 0,
	}

	_, err = EnactProposal(
		&EnactmentContext{DB: db, Slot: 2000, Epoch: 42},
		proposal,
	)
	require.NoError(t, err)

	got, err := db.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	assert.Nil(t, got, "NoConfidence should clear the enacted quorum")
}

func TestApplyUpdateCommitteePreservesZeroTermStartSlot(t *testing.T) {
	db, _ := newTallyTestDB(t)
	credential := &lcommon.Credential{
		CredType:   lcommon.CredentialTypeAddrKeyHash,
		Credential: lcommon.NewBlake2b224(testBytes(28, 0x7a)),
	}
	require.NoError(t, applyUpdateCommittee(
		&EnactmentContext{DB: db, Slot: 50},
		&lcommon.UpdateCommitteeGovAction{
			CredEpochs: map[*lcommon.Credential]uint{credential: 20},
			Quorum:     cbor.Rat{Rat: big.NewRat(1, 2)},
		},
		0,
	))

	members, err := db.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Zero(
		t,
		members[0].TermStartSlot,
		"slot zero is a valid membership term start, not an unset marker",
	)
}

func TestApplyTreasuryWithdrawal_CreditsRewardsAndDebitsTreasury(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 1)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	a := &lcommon.TreasuryWithdrawalGovAction{
		Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
	}
	err = applyTreasuryWithdrawal(&EnactmentContext{
		DB:   db,
		Slot: 123,
	}, a, &models.GovernanceProposal{TxHash: testBytes(32, 0xA0)})
	require.NoError(t, err)

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(12), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(93), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}

func TestApplyTreasuryWithdrawal_DistinguishesSameTxActionIndex(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 0x21)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	ctx := &EnactmentContext{DB: db, Slot: 123}
	txHash := testBytes(32, 0x22)
	first := &models.GovernanceProposal{TxHash: txHash, ActionIndex: 0}
	second := &models.GovernanceProposal{TxHash: txHash, ActionIndex: 1}
	require.NoError(t, applyTreasuryWithdrawal(
		ctx,
		&lcommon.TreasuryWithdrawalGovAction{
			Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
		},
		first,
	))
	require.NoError(t, applyTreasuryWithdrawal(
		ctx,
		&lcommon.TreasuryWithdrawalGovAction{
			Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 11},
		},
		second,
	))

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(18), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(82), uint64(state.Treasury))

	rows, err := store.raw.Query(`
SELECT tx_hash, amount FROM account_reward_delta
WHERE credential_tag = ? AND staking_key = ? AND added_slot = ?`,
		0, stakeCred, uint64(123),
	)
	require.NoError(t, err)
	var deltas []models.AccountRewardDelta
	for rows.Next() {
		var delta models.AccountRewardDelta
		require.NoError(t, rows.Scan(&delta.TxHash, &delta.Amount))
		deltas = append(deltas, delta)
	}
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	require.Len(t, deltas, 2)
	assert.NotEqual(
		t,
		proposalRewardSourceHash(first),
		proposalRewardSourceHash(second),
	)
	// Pin the caller contract: each journaled row must carry the per-proposal
	// source hash as its replay discriminator, so the same tx hash at two
	// action indexes cannot collapse into one row. Rows are matched by their
	// stored discriminator rather than by query order.
	bySourceHash := make(map[string]models.AccountRewardDelta, len(deltas))
	for _, delta := range deltas {
		bySourceHash[string(delta.TxHash)] = delta
	}
	require.Len(t, bySourceHash, 2, "journaled TxHash values must be distinct")
	for _, tc := range []struct {
		name     string
		proposal *models.GovernanceProposal
		amount   uint64
	}{
		{name: "first", proposal: first, amount: 7},
		{name: "second", proposal: second, amount: 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wantHash := proposalRewardSourceHash(tc.proposal)
			delta, ok := bySourceHash[string(wantHash)]
			require.True(
				t,
				ok,
				"no reward delta journaled with proposalRewardSourceHash",
			)
			assert.Equal(t, wantHash, delta.TxHash)
			assert.Equal(t, tc.amount, uint64(delta.Amount))
		})
	}
}

func TestApplyTreasuryWithdrawal_RejectsOverdrawnTreasury(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 2)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))
	require.NoError(t, store.SetNetworkState(6, 20, 1, nil))

	a := &lcommon.TreasuryWithdrawalGovAction{
		Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
	}
	err = applyTreasuryWithdrawal(&EnactmentContext{
		DB:   db,
		Slot: 123,
	}, a, &models.GovernanceProposal{TxHash: testBytes(32, 0xA1)})
	require.ErrorContains(
		t,
		err,
		"treasury withdrawal of 7 exceeds tracked treasury withdrawal capacity 6",
	)

	account, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, uint64(5), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(6), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}

func TestApplyTreasuryWithdrawal_LeavesMissingRewardAccountInTreasury(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 2)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	a := &lcommon.TreasuryWithdrawalGovAction{
		Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
	}
	err = applyTreasuryWithdrawal(&EnactmentContext{
		DB:   db,
		Slot: 123,
	}, a, &models.GovernanceProposal{TxHash: testBytes(32, 0xA2)})
	require.NoError(t, err)

	active, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, active, "withdrawal must not create a reward account")
	account, err := store.GetAccountByCredential(0, stakeCred, true, nil)
	require.NoError(t, err)
	assert.Nil(t, account)
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(100), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}

func TestApplyTreasuryWithdrawal_LeavesInactiveRewardAccountInTreasury(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 3)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeCred,
		Reward:     types.Uint64(5),
		Active:     true,
	}))
	_, err = store.raw.Exec(
		"UPDATE account SET active = FALSE WHERE staking_key = ?",
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	a := &lcommon.TreasuryWithdrawalGovAction{
		Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 7},
	}
	err = applyTreasuryWithdrawal(&EnactmentContext{
		DB:   db,
		Slot: 123,
	}, a, &models.GovernanceProposal{TxHash: testBytes(32, 0xA3)})
	require.NoError(t, err)

	active, err := store.GetAccountByCredential(0, stakeCred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, active, "withdrawal must not reactivate the reward account")
	account, err := store.GetAccountByCredential(0, stakeCred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.False(t, account.Active)
	assert.Equal(t, uint64(5), uint64(account.Reward))
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(100), uint64(state.Treasury))
	assert.Equal(t, uint64(20), uint64(state.Reserves))
}

func TestApplyTreasuryWithdrawal_UnclaimedStillCountsAgainstCapacity(
	t *testing.T,
) {
	db, store := newTallyTestDB(t)
	stakeCred := testBytes(28, 4)
	rewardAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCred,
	)
	require.NoError(t, err)
	require.NoError(t, store.SetNetworkState(100, 20, 1, nil))

	ctx := &EnactmentContext{
		DB:   db,
		Slot: 123,
	}
	first := &lcommon.TreasuryWithdrawalGovAction{
		Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 70},
	}
	require.NoError(t, applyTreasuryWithdrawal(
		ctx,
		first,
		&models.GovernanceProposal{TxHash: testBytes(32, 70)},
	))

	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Equal(t, uint64(100), uint64(state.Treasury))
	assert.Equal(t, uint64(30), ctx.TreasuryWithdrawalRemaining)

	second := &lcommon.TreasuryWithdrawalGovAction{
		Withdrawals: map[*lcommon.Address]uint64{&rewardAddr: 40},
	}
	err = applyTreasuryWithdrawal(
		ctx,
		second,
		&models.GovernanceProposal{TxHash: testBytes(32, 40)},
	)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"exceeds tracked treasury withdrawal capacity",
	)
}
